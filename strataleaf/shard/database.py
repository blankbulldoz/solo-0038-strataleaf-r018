"""Embedded sharding entry point.

``ShardDatabase`` keeps the caller-facing collection/key API of the baseline
:class:`~strataleaf.Database` while spreading documents across independent
SQLite shard files.  Routing decisions come from a persisted hash ring stored in
a separate router database, so reopening a directory restores every placement
and every in-flight migration.
"""
import base64
import hashlib
import hmac
import json
import threading
import time
from pathlib import Path

from .. import Database
from ..codec import digest, key as checked_key, loads, name
from ..errors import (Conflict, InvalidCursor, InvalidDocument, MigrationInProgress,
                      NotFound, ShardError, UnknownShard)
from ..mvcc import Record
from ..predicates import matches, validate_predicate
from . import migration
from .batches import ShardWriteBatch
from .ring import affected_slots, build_ring, key_slot, route, slot_ranges
from .router_store import RouterStorage

SHARD_FILE = "shard-{shard_id}.db"
ROUTER_FILE = "router.db"


class ShardPage:
    def __init__(self, records, manifest, cursor, plan):
        self.records = records
        self.manifest = manifest
        self.cursor = cursor
        self.plan = plan

    def as_dict(self):
        return {"records": [record.__dict__ for record in self.records],
                "manifest": self.manifest, "cursor": self.cursor, "plan": self.plan}


class ShardDatabase:
    """A reopenable collection/key database backed by several shard files."""

    def __init__(self, directory, shard_ids=("s0",), timeout=30.0, clock=time.time):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.clock = clock
        self._topology_lock = threading.RLock()
        self._store = RouterStorage(self.directory / ROUTER_FILE, timeout)
        self._shards = {}
        paths = self._store.shard_paths()
        if not paths:
            self._bootstrap(shard_ids)
        self._open_shards()

    # ------------------------------------------------------------------ open

    def _bootstrap(self, shard_ids):
        shard_ids = tuple(dict.fromkeys(shard_ids))
        if not shard_ids or len(shard_ids) > 1024:
            raise InvalidDocument("initial topology needs between 1 and 1024 shard ids")
        for shard_id in shard_ids:
            name(shard_id)
        now = self.clock()
        ring = build_ring(shard_ids)
        with self._store.atomic():
            for shard_id in shard_ids:
                self._store.connection.execute(
                    "INSERT INTO shards VALUES (?,?,?)",
                    (shard_id, str(self.directory / SHARD_FILE.format(shard_id=shard_id)), 1))
            self._store.write_ring(1, ring, now)

    def _open_shards(self):
        for shard_id, path in self._store.shard_paths().items():
            self._shards[shard_id] = Database(path)

    # --------------------------------------------------------------- topology

    @property
    def ring(self):
        return self._store.load_ring()

    @property
    def epoch(self):
        return self._store.epoch

    def shard_ids(self):
        return sorted(self._shards)

    def shard(self, shard_id):
        try:
            return self._shards[shard_id]
        except KeyError:
            raise UnknownShard("unknown shard: " + str(shard_id)) from None

    def shard_path(self, shard_id):
        return self._store.shard_paths().get(shard_id)

    def route(self, collection, key):
        return route(self.ring, collection, key)

    def explain(self, collection, key):
        """Return the checkable routing facts for one document key."""
        name(collection)
        checked_key(key)
        slot = key_slot(collection, key)
        ring = self.ring
        owner = route(ring, collection, key)
        ledger = self._store.migration_row(collection, key)
        history = self._store.migration_history(collection, key)
        if ledger is not None:
            ledger = dict(ledger)
            ledger["detail"] = loads(ledger["detail"])
        facts = {"collection": collection, "key": key, "slot": slot,
                 "ring_epoch": self.epoch, "owner_shard": owner,
                 "owner_path": self.shard_path(owner),
                 "migration": None if ledger is None else
                 {field: ledger[field] for field in
                  ("from_shard", "to_shard", "epoch", "state", "detail")},
                 "hops": [{"from_shard": hop["from_shard"], "to_shard": hop["to_shard"],
                           "epoch": hop["epoch"], "state": hop["state"],
                           "detail": loads(hop["detail"])} for hop in history]}
        if ledger is not None and ledger["state"] in ("pending", "migrating"):
            facts["served_by"] = ledger["from_shard"]
            facts["served_path"] = self.shard_path(ledger["from_shard"])
        else:
            facts["served_by"] = owner
            facts["served_path"] = self.shard_path(owner)
        return facts

    def _sync_catalog(self, target_db):
        """Copy every collection schema onto a freshly added shard."""
        catalog = {}
        for shard_db in self._shards.values():
            with shard_db._store.read() as connection:
                for row in connection.execute("SELECT name,schema,generation FROM collections"):
                    catalog.setdefault(row["name"], (row["schema"], row["generation"]))
        if not catalog:
            return
        with target_db._store.atomic(writable=True) as connection:
            for collection_name, (schema, generation) in sorted(catalog.items()):
                connection.execute(
                    "INSERT OR IGNORE INTO collections VALUES (?,?,?)",
                    (collection_name, schema, generation))
            target_db._store.set("catalog_epoch",
                                 int(target_db._store.get("catalog_epoch")) + 1)

    def add_shard(self, shard_id, drain=True):
        """Expand the ring with one shard and schedule the affected key moves.

        The new ring epoch is published together with per-key ledger entries, so
        every key has an explicit ``pending`` owner before normal writes resume.
        When ``drain`` is true the migration runs synchronously and the method
        returns a fully verifiable before/after report; otherwise
        :meth:`migrate_drain` finishes it later (or after a reopen).
        """
        name(shard_id)
        with self._topology_lock:
            if self._store.pending_migrations():
                raise MigrationInProgress("drain the current migration before adding another shard")
            existing = self.shard_ids()
            if shard_id in existing:
                raise Conflict("shard already exists: " + shard_id)
            old_ring = self.ring
            new_ids = existing + [shard_id]
            new_ring = build_ring(new_ids)
            moved = affected_slots(old_ring, new_ring)
            targets = {after for _, _, after in moved}
            if targets != {shard_id}:
                raise ShardError("ring expansion must move slots only onto the new shard")
            moved_slots = {slot for slot, _, _ in moved}
            # Open the new physical database first so publishing the epoch never
            # references a missing file, and give it the union of collection
            # catalogs so direct writes after the topology change find a schema.
            new_path = self.directory / SHARD_FILE.format(shard_id=shard_id)
            new_db = Database(new_path)
            self._sync_catalog(new_db)
            now = self.clock()
            new_epoch = self.epoch + 1
            scheduled = []
            with self._store.atomic():
                self._store.connection.execute(
                    "INSERT INTO shards VALUES (?,?,?)", (shard_id, str(new_path), new_epoch))
                self._store.write_ring(new_epoch, new_ring, now)
                # Every key the new ring claims, across every source shard and
                # collection, gets a resumable ledger entry.
                for source_id in existing:
                    source_db = self._shards[source_id]
                    with source_db._store.read() as connection:
                        collections = [row["name"] for row in
                                       connection.execute("SELECT name FROM collections ORDER BY name")]
                        for collection in collections:
                            for row in connection.execute(
                                    "SELECT DISTINCT key FROM versions WHERE collection=? ORDER BY key",
                                    (collection,)):
                                key = row["key"]
                                if key_slot(collection, key) in moved_slots:
                                    # Preserve the completed previous hop before
                                    # scheduling the next one for the same key.
                                    self._store.archive_migration(collection, key, now)
                                    self._store.upsert_migration(
                                        collection, key, source_id, shard_id, new_epoch,
                                        "pending", {"scheduled_at": now})
                                    scheduled.append({"collection": collection, "key": key,
                                                      "from": source_id, "to": shard_id})
            self._shards[shard_id] = new_db
            report = {"added": shard_id, "epoch": new_epoch,
                      "slot_ranges": slot_ranges(sorted(moved_slots)),
                      "slots_moved": len(moved_slots), "keys_scheduled": len(scheduled),
                      "scheduled": scheduled}
            if drain:
                report["migrated"] = self.migrate_drain()
            return report

    # ------------------------------------------------------------- migration

    def migrate_drain(self, limit=None):
        with self._topology_lock:
            return migration.drain(self, limit)

    def migration_status(self):
        rows = self._store.connection.execute(
            "SELECT state, COUNT(*) AS n FROM migration_ledger GROUP BY state").fetchall()
        counts = {row["state"]: row["n"] for row in rows}
        history_count = self._store.connection.execute(
            "SELECT COUNT(*) AS n FROM migration_history").fetchone()["n"]
        return {"epoch": self.epoch, "open": self._store.pending_migrations(limit=1) != [],
                "counts": counts, "completed_hops": history_count,
                "entries": [self._decode_ledger_row(row) for row in self._store.connection.execute(
                    "SELECT collection,key,from_shard,to_shard,epoch,state,detail "
                    "FROM migration_ledger ORDER BY collection,key")]}

    @staticmethod
    def _decode_ledger_row(row):
        item = dict(row)
        item["detail"] = loads(item["detail"])
        return item

    def _migrate_key_now(self, entry):
        """Lazy on-write migration for a key still marked pending/migrating."""
        source_db = self.shard(entry["from_shard"])
        destination_db = self.shard(entry["to_shard"])
        migration.migrate_one(self, source_db, destination_db, entry, self.clock())

    def resolve_for_write(self, collection, key):
        """Return the physical shard that must accept the next write."""
        shard_id = route(self.ring, collection, key)
        entry = self._store.migration_row(collection, key)
        if entry is not None and entry["state"] in ("pending", "migrating"):
            if entry["to_shard"] != shard_id:
                raise ShardError("ledger and ring disagree on target for " + key)
            self._migrate_key_now(entry)
        return shard_id

    # ---------------------------------------------------------------- catalog

    def create_collection(self, collection, schema=None):
        """Create a collection on every shard as a coordinated DDL transaction."""
        name(collection)
        request_key = f"catalog:{collection}"
        existing = self._store.xact(request_key)
        if existing is not None and existing["state"] == "committed":
            return {"collection": collection, "already_existed": True}
        shard_ids = self.shard_ids()
        plan = {shard_id: ["create_collection"] for shard_id in shard_ids}
        # Preflight: a duplicate collection anywhere fails before any DDL runs.
        for shard_id in shard_ids:
            with self._shards[shard_id]._store.read() as connection:
                if connection.execute("SELECT 1 FROM collections WHERE name=?",
                                      (collection,)).fetchone():
                    raise Conflict("collection already exists on shard: " + shard_id)
        self._store.record_xact(request_key, "catalog",
                                digest({"collection": collection, "schema": schema}),
                                "prepared", plan, [], {"phase": "prepared"})
        committed = []
        try:
            for shard_id in shard_ids:
                self._shards[shard_id].catalog.create(collection, schema)
                committed.append(shard_id)
                self._store.record_xact(request_key, "catalog",
                                        digest({"collection": collection, "schema": schema}),
                                        "partially_committed", plan, committed, {"phase": "partial"})
        except BaseException as exc:
            self._store.record_xact(request_key, "catalog",
                                    digest({"collection": collection, "schema": schema}),
                                    "partially_committed", plan, committed,
                                    {"phase": "partial", "error": repr(exc)})
            raise ShardError(
                f"collection DDL stopped after shards {committed}; retry create_collection to resume"
            ) from exc
        self._store.record_xact(request_key, "catalog",
                                digest({"collection": collection, "schema": schema}),
                                "committed", plan, shard_ids, {"phase": "done"})
        return {"collection": collection, "already_existed": False}

    def list_collections(self):
        merged = {}
        for shard_id, shard_db in self._shards.items():
            for item in shard_db.catalog.list():
                merged.setdefault(item["name"], {"collection": item["name"],
                                                 "schema": item["schema"],
                                                 "generation": item["generation"], "shards": []})
                merged[item["name"]]["shards"].append(shard_id)
        return [merged[key] for key in sorted(merged)]

    # ------------------------------------------------------------------ reads

    def _physical_shard(self, collection, key):
        entry = self._store.migration_row(collection, key)
        if entry is not None and entry["state"] in ("pending", "migrating"):
            return entry["from_shard"], entry
        return route(self.ring, collection, key), entry

    def get(self, collection, key, revisions=None):
        """Read one document.

        ``revisions`` optionally pins per-shard revisions (mapping shard id to
        revision) as returned by a previous query manifest, giving a repeatable
        cross-shard view.  Default reads each shard at its current head.
        """
        name(collection)
        checked_key(key)
        shard_id, entry = self._physical_shard(collection, key)
        shard_db = self.shard(shard_id)
        revision = None if revisions is None else revisions.get(shard_id)
        record = shard_db.get(collection, key, revision=revision)
        return record, self._read_manifest([shard_id])

    def _read_manifest(self, shard_ids=None):
        shard_ids = self.shard_ids() if shard_ids is None else shard_ids
        return {"ring_epoch": self.epoch,
                "consistency": "per_shard_head" if shard_ids == self.shard_ids()
                else "per_shard_head_single_shard",
                "revisions": {shard_id: self._shards[shard_id].revision for shard_id in shard_ids}}

    def query(self, collection, predicate=None, limit=100, cursor=None, revisions=None):
        """Fan a query out across shards and merge by document key order."""
        name(collection)
        validate_predicate(predicate)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise InvalidDocument("page size must be between 1 and 1000")
        after = None
        pinned = None
        if cursor is not None:
            payload = self._decode_cursor(cursor)
            if payload["collection"] != collection or digest(predicate) != payload["predicate_hash"]:
                raise InvalidCursor("cursor does not belong to this query")
            after = payload["after"]
            pinned = payload["revisions"]
        if revisions is not None:
            pinned = dict(revisions)
        if pinned is None:
            pinned = {shard_id: shard_db.revision for shard_id, shard_db in self._shards.items()}
        unknown = set(pinned) - set(self._shards)
        if unknown:
            raise InvalidCursor("cursor references removed shards: " + ",".join(sorted(unknown)))
        missing = set(self._shards) - set(pinned)
        if missing:
            raise InvalidCursor(
                "pinned revision manifest predates shards "
                + ",".join(sorted(missing))
                + "; re-run the query against the current ring epoch")
        output, examined = [], 0
        per_shard_plan = {}
        for shard_id, shard_db in self._shards.items():
            revision = pinned[shard_id]
            with shard_db._store.read() as connection:
                if not connection.execute("SELECT 1 FROM collections WHERE name=?",
                                          (collection,)).fetchone():
                    raise NotFound("unknown collection: " + collection)
                rows = connection.execute(
                    """SELECT v.* FROM versions v JOIN
                       (SELECT key,MAX(rev) AS r FROM versions WHERE collection=? AND rev<=? GROUP BY key) latest
                       ON v.collection=? AND v.key=latest.key AND v.rev=latest.r
                       WHERE v.body IS NOT NULL ORDER BY v.key""",
                    (collection, revision, collection)).fetchall()
                for row in rows:
                    if after is not None and row["key"] <= after:
                        continue
                    examined += 1
                    document = loads(row["body"])
                    if matches(document, predicate):
                        output.append(Record(row["collection"], row["key"], row["rev"],
                                             document, row["expires"]))
            per_shard_plan[shard_id] = {"revision": revision}
        output.sort(key=lambda record: record.key)
        continuation = None
        if len(output) > limit:
            output = output[:limit]
            continuation = self._encode_cursor(
                {"database": self._store.get("router_id"), "collection": collection,
                 "predicate_hash": digest(predicate), "after": output[-1].key, "revisions": pinned})
        manifest = {"ring_epoch": self.epoch, "consistency": "pinned_per_shard_revisions",
                    "revisions": pinned}
        return ShardPage(output, manifest, continuation,
                         {"access": "cross_shard_merge", "shards": per_shard_plan, "examined": examined})

    # ---------------------------------------------------------------- cursors

    def _encode_cursor(self, payload):
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        signature = hmac.new(bytes.fromhex(self._store.get("cursor_secret")),
                             raw, hashlib.sha256).digest()
        return base64.urlsafe_b64encode(signature + raw).decode().rstrip("=")

    def _decode_cursor(self, token):
        try:
            data = base64.b64decode(token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
            signature, raw = data[:32], data[32:]
            expected = hmac.new(bytes.fromhex(self._store.get("cursor_secret")),
                                raw, hashlib.sha256).digest()
            if not hmac.compare_digest(signature, expected):
                raise ValueError("signature mismatch")
            payload = json.loads(raw)
            if payload.get("database") != self._store.get("router_id"):
                raise ValueError("wrong router")
            return payload
        except (ValueError, TypeError, KeyError, UnicodeDecodeError) as exc:
            raise InvalidCursor("invalid or foreign shard continuation cursor") from exc

    # ---------------------------------------------------------------- writes

    def batch(self, request_key=None):
        return ShardWriteBatch(self, request_key)

    def put(self, collection, key, document, expected=None, expires_at=None, request_key=None):
        return self.batch(request_key).put(collection, key, document, expected, expires_at).commit()

    def delete(self, collection, key, expected=None, request_key=None):
        return self.batch(request_key).delete(collection, key, expected).commit()

    # ------------------------------------------------------------- verification

    def verify(self):
        """Cross-check ring ownership, ledger state and physical shard contents."""
        ring = self.ring
        issues = []
        ledger = self._store.connection.execute("SELECT * FROM migration_ledger").fetchall()
        open_entries = [row for row in ledger if row["state"] in ("pending", "migrating")]
        physical = {}
        for shard_id, shard_db in self._shards.items():
            with shard_db._store.read() as connection:
                keys = {(row["collection"], row["key"]) for row in
                        connection.execute("SELECT DISTINCT collection,key FROM versions")}
                physical[shard_id] = keys
        ledger_index = {(row["collection"], row["key"]): row for row in ledger}
        all_keys = set().union(*physical.values())
        for collection, key in sorted(all_keys):
            owner = route(ring, collection, key)
            row = ledger_index.get((collection, key))
            if row is not None and row["state"] in ("pending", "migrating"):
                expected_shard = row["from_shard"]
            else:
                expected_shard = owner
            present_on = [shard_id for shard_id, keys in physical.items() if (collection, key) in keys]
            if present_on != [expected_shard]:
                issues.append(f"{collection}/{key} physically on {present_on}, expected {expected_shard}")
            if row is not None and row["state"] == "migrated":
                if present_on != [row["to_shard"]]:
                    issues.append(f"{collection}/{key} marked migrated but not solely on target")
        for row in open_entries:
            if route(ring, row["collection"], row["key"]) != row["to_shard"]:
                issues.append(f"open ledger entry for {row['collection']}/{row['key']} disagrees with ring")
        return {"ok": not issues, "issues": issues, "ring_epoch": self.epoch,
                "open_migrations": len(open_entries),
                "physical_key_counts": {shard_id: len(keys) for shard_id, keys in physical.items()},
                "ledger_counts": {state: sum(1 for row in ledger if row["state"] == state)
                                  for state in ("pending", "migrating", "migrated", "aborted")}}

    def xact_status(self, request_key):
        item = self._store.xact(request_key)
        if item is None:
            raise NotFound("unknown coordinated transaction: " + request_key)
        return item

    def close(self):
        with self._topology_lock:
            for shard_db in self._shards.values():
                shard_db.close()
            self._store.close()

    def __enter__(self):
        return self

    def __exit__(self, *exception):
        self.close()

    # ------------------------------------------------------------- internals

    @staticmethod
    def _op_id(op):
        return {"collection": op["collection"], "key": op["key"]}

"""Coordinated write batches spanning multiple physical shards.

Atomicity inside one SQLite shard is guaranteed by the baseline commit.  A batch
that touches several shards cannot be a single SQLite transaction, so the
coordinator instead makes the outcome explicit *before* any byte is written:

1. ``preflight`` resolves every key to its physical shard, checks expected
   revisions, schemas and unique constraints indirectly through the normal
   shard commit, and rejects the whole request before writing anything.
2. The plan is recorded in the router ledger as ``prepared``.
3. Shards commit one at a time using idempotent request keys; the ledger moves
   to ``partially_committed`` and finally ``committed``.

A failure after some shards committed raises :class:`CrossShardCommitError`
carrying the partial result; retrying with the same request key resumes the
remaining shards.  The caller is never left guessing which shard changed.
"""
import math
from dataclasses import dataclass
from ..codec import digest, dumps, key as checked_key, loads, name
from ..errors import Conflict, CrossShardCommitError, InvalidDocument, NotFound
from ..transaction import WriteBatch


@dataclass(frozen=True)
class ShardCommitResult:
    request_key: str
    count: int
    shards: tuple
    per_shard: dict
    state: str
    revision: int = 0

    def as_dict(self):
        return {"request_key": self.request_key, "count": self.count,
                "shards": list(self.shards), "per_shard": self.per_shard,
                "state": self.state, "revision": self.revision}


class ShardWriteBatch:
    def __init__(self, coordinator, request_key=None):
        self.coordinator = coordinator
        self.request_key = checked_key(request_key) if request_key is not None else None
        self.operations = []
        self.state = "open"

    def _append(self, operation):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        identity = (operation["collection"], operation["key"])
        if any((op["collection"], op["key"]) == identity for op in self.operations):
            raise InvalidDocument("one operation per document per batch")
        expected = operation["expected"]
        if expected is not None and (isinstance(expected, bool) or not isinstance(expected, int) or expected < 0):
            raise InvalidDocument("expected revision must be nonnegative or None")
        self.operations.append(operation)
        return self

    def put(self, collection, key, document, expected=None, expires_at=None):
        name(collection)
        checked_key(key)
        if not isinstance(document, dict):
            raise InvalidDocument("document must be an object")
        if expires_at is not None and (isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)) or not math.isfinite(expires_at)):
            raise InvalidDocument("expires_at must be a finite timestamp")
        return self._append({"collection": collection, "key": key, "document": loads(dumps(document)),
                             "expected": expected, "expires_at": expires_at})

    def delete(self, collection, key, expected=None):
        return self._append({"collection": name(collection), "key": checked_key(key), "document": None,
                             "expected": expected, "expires_at": None})

    def abort(self):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        self.operations.clear()
        self.state = "aborted"

    # -- planning -----------------------------------------------------------

    def _plan(self, skip_expected=False):
        """Resolve placement and current revisions without writing anything."""
        placement = {}
        migrated_keys = set()
        for op in self.operations:
            before = self.coordinator._store.migration_row(op["collection"], op["key"])
            shard_id = self.coordinator.resolve_for_write(op["collection"], op["key"])
            after = self.coordinator._store.migration_row(op["collection"], op["key"])
            if before is not None and (after is None or before["state"] != after["state"]):
                migrated_keys.add((op["collection"], op["key"]))
            placement.setdefault(shard_id, []).append(op)
        plan = {}
        for shard_id, ops in placement.items():
            shard_db = self.coordinator.shard(shard_id)
            with shard_db._store.read() as connection:
                head = int(shard_db._store.get("head"))
                for op in ops:
                    schema = connection.execute(
                        "SELECT schema FROM collections WHERE name=?", (op["collection"],)).fetchone()
                    if schema is None:
                        raise NotFound("unknown collection: " + op["collection"])
                    if skip_expected or op["expected"] is None:
                        continue
                    row = connection.execute(
                        "SELECT rev FROM versions WHERE collection=? AND key=? AND rev<=? ORDER BY rev DESC LIMIT 1",
                        (op["collection"], op["key"], head)).fetchone()
                    current = row["rev"] if row is not None else 0
                    if op["expected"] != current:
                        if (op["collection"], op["key"]) in migrated_keys:
                            raise Conflict(
                                f"expected revision {op['expected']} for {op['collection']}/{op['key']} "
                                f"originated on the source shard; migration replayed the version chain "
                                f"on shard {shard_id} where the current revision is {current}. "
                                "Re-read the key and submit its current revision")
                        raise Conflict(
                            f"revision conflict for {op['collection']}/{op['key']}: "
                            f"expected {op['expected']}, found {current} (shard {shard_id})")
            plan[shard_id] = ops
        return plan

    # -- commit -------------------------------------------------------------

    def commit(self):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        if not self.operations or len(self.operations) > 10000:
            raise InvalidDocument("batch must contain between 1 and 10000 operations")
        router = self.coordinator._store
        fingerprint = digest(self.operations)
        shard_ids_preview = {self.coordinator.route(op["collection"], op["key"])
                             for op in self.operations}
        cross_shard = len(shard_ids_preview) > 1
        if cross_shard and self.request_key is None:
            raise InvalidDocument(
                "a batch spanning multiple shards must carry a request_key so the "
                "two-phase commit can be resumed safely after a partial failure")

        if self.request_key is not None:
            prior = router.xact(self.request_key)
            if prior is not None:
                if prior["fingerprint"] != fingerprint:
                    raise Conflict("request key was used with different operations")
                # Idempotent replay short-circuits before preflight: identical
                # operations return the recorded outcome, expected revisions and
                # current state are not re-evaluated.
                return self._resume(prior, self._plan(skip_expected=prior["state"] == "committed"),
                                    fingerprint, replaying=True)

        plan = self._plan()  # all-or-nothing validation, no writes yet
        shard_ids = tuple(sorted(plan))

        if cross_shard:
            router.record_xact(self.request_key, "batch", fingerprint, "prepared",
                               {shard: [self.coordinator._op_id(op) for op in ops]
                                for shard, ops in plan.items()}, [], {"phase": "prepared"})
        per_shard, committed = {}, []
        try:
            for shard_id in shard_ids:
                result = self._commit_shard(shard_id, plan[shard_id], cross_shard)
                per_shard[shard_id] = result
                committed.append(shard_id)
                if cross_shard:
                    router.record_xact(self.request_key, "batch", fingerprint, "partially_committed",
                                       {shard: [self.coordinator._op_id(op) for op in ops]
                                        for shard, ops in plan.items()},
                                       committed, {"phase": "partial"})
        except BaseException as exc:
            if cross_shard:
                router.record_xact(self.request_key, "batch", fingerprint, "partially_committed",
                                   {shard: [self.coordinator._op_id(op) for op in ops]
                                    for shard, ops in plan.items()},
                                   committed, {"phase": "partial", "error": repr(exc)})
                self.state = "aborted"
                raise CrossShardCommitError(
                    f"multi-shard commit stopped after shards {committed}; "
                    f"retry request_key {self.request_key!r} to finish the remaining shards",
                    ShardCommitResult(self.request_key, len(self.operations), shard_ids,
                                     dict(per_shard), "partially_committed")) from exc
            raise
        if cross_shard:
            router.record_xact(self.request_key, "batch", fingerprint, "committed",
                               {shard: [self.coordinator._op_id(op) for op in ops]
                                for shard, ops in plan.items()},
                               list(shard_ids), {"phase": "done"})
        self.state = "committed"
        return ShardCommitResult(self.request_key, len(self.operations), shard_ids, per_shard,
                                 "committed" if cross_shard else "single_shard")

    def _commit_shard(self, shard_id, ops, cross_shard):
        shard_db = self.coordinator.shard(shard_id)
        request_key = None
        if self.request_key is not None:
            request_key = f"{self.request_key}:{shard_id}" if cross_shard else self.request_key
        batch = WriteBatch(shard_db._store, request_key)
        for op in ops:
            if op["document"] is None:
                batch.delete(op["collection"], op["key"], expected=op["expected"])
            else:
                batch.put(op["collection"], op["key"], op["document"],
                          expected=op["expected"], expires_at=op["expires_at"])
        result = batch.commit()
        return {"revision": result.revision, "count": result.count, "replayed": result.replayed}

    def _resume(self, prior, plan, fingerprint, replaying=False):
        """Finish a previously prepared/partial multi-shard ledger transaction.

        An identical request that already reached ``committed`` simply returns the
        recorded per-shard revisions; expected revisions are not re-evaluated.
        """
        def receipt(shard_id):
            shard_db = self.coordinator.shard(shard_id)
            row = shard_db._store.connection.execute(
                "SELECT rev,count FROM receipts WHERE request_key=?",
                (f"{self.request_key}:{shard_id}",)).fetchone()
            if row is None:
                raise CrossShardCommitError(
                    f"ledger marks shard {shard_id} committed but its receipt is missing")
            return {"revision": row["rev"], "count": row["count"], "replayed": True}

        if prior["state"] == "committed":
            per_shard = {shard_id: receipt(shard_id) for shard_id in prior["committed"]}
            self.state = "committed"
            return ShardCommitResult(self.request_key, len(self.operations),
                                     tuple(sorted(plan)), per_shard, "committed")
        committed = list(prior["committed"])
        per_shard = {shard_id: receipt(shard_id) for shard_id in committed}
        for shard_id, ops in plan.items():
            if shard_id in committed:
                continue
            result = self._commit_shard(shard_id, ops, True)
            per_shard[shard_id] = result
            committed.append(shard_id)
            self.coordinator._store.record_xact(
                self.request_key, "batch", fingerprint, "partially_committed", prior["plan"],
                committed, {"phase": "resume"})
        self.coordinator._store.record_xact(
            self.request_key, "batch", fingerprint, "committed", prior["plan"],
            list(plan.keys()), {"phase": "done"})
        self.state = "committed"
        return ShardCommitResult(self.request_key, len(self.operations),
                                 tuple(sorted(plan)), per_shard, "committed")

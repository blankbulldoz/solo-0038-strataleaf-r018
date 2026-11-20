"""Persistent routing metadata for the embedded sharding layer.

The router keeps its own SQLite file, separate from the document shards.  Every
fact needed to reopen a router after a crash lives here:

* ``meta``        - format marker, current ring epoch, cursor signing secret
* ``shards``      - physical shard files and the ring epoch that added them
* ``rings``       - immutable ring definitions per epoch (JSON virtual-node map)
* ``migration_ledger`` - per-key migration state machine, resumable across opens
* ``xact_ledger`` - coordinated multi-shard commit bookkeeping (two phases)
"""
import contextlib
import secrets
import sqlite3
import threading
import uuid
from ..codec import dumps, loads
from ..errors import DatabaseError, ShardError

DDL = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS shards (
 shard_id TEXT PRIMARY KEY, path TEXT NOT NULL UNIQUE, added_epoch INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS rings (
 epoch INTEGER PRIMARY KEY, definition TEXT NOT NULL, created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS migration_ledger (
 collection TEXT NOT NULL, key TEXT NOT NULL, from_shard TEXT NOT NULL, to_shard TEXT NOT NULL,
 epoch INTEGER NOT NULL, state TEXT NOT NULL, detail TEXT NOT NULL,
 PRIMARY KEY(collection, key));
CREATE INDEX IF NOT EXISTS migration_state ON migration_ledger(state, to_shard);
CREATE TABLE IF NOT EXISTS migration_history (
 collection TEXT NOT NULL, key TEXT NOT NULL, from_shard TEXT NOT NULL, to_shard TEXT NOT NULL,
 epoch INTEGER NOT NULL, state TEXT NOT NULL, detail TEXT NOT NULL,
 finished_at REAL NOT NULL);
CREATE INDEX IF NOT EXISTS migration_history_key ON migration_history(collection,key);
CREATE TABLE IF NOT EXISTS xact_ledger (
 request_key TEXT PRIMARY KEY, kind TEXT NOT NULL, fingerprint TEXT NOT NULL,
 state TEXT NOT NULL, plan TEXT NOT NULL, committed TEXT NOT NULL, detail TEXT NOT NULL);
"""

VALID_STATES = {"pending", "migrating", "migrated", "aborted"}
XACT_STATES = {"prepared", "partially_committed", "committed", "failed"}


class RouterStorage:
    def __init__(self, path, timeout=30.0):
        self.path = str(path)
        self.lock = threading.RLock()
        self.closed = False
        self.connection = sqlite3.connect(self.path, timeout=timeout, isolation_level=None,
                                          check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript(DDL)
        with self.atomic() as connection:
            initial = {"format": "1", "router_id": str(uuid.uuid4()),
                       "ring_epoch": "0", "cursor_secret": secrets.token_hex(32)}
            for key, value in initial.items():
                connection.execute("INSERT OR IGNORE INTO meta VALUES (?,?)", (key, value))
            if self.get("format") != "1":
                raise DatabaseError("unsupported router format")

    def get(self, key):
        row = self.connection.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        if row is None:
            raise ShardError("missing router metadata: " + key)
        return row[0]

    def set(self, key, value):
        self.connection.execute("UPDATE meta SET value=? WHERE key=?", (str(value), key))

    @property
    def epoch(self):
        return int(self.get("ring_epoch"))

    def load_ring(self, epoch=None):
        epoch = self.epoch if epoch is None else epoch
        row = self.connection.execute("SELECT definition FROM rings WHERE epoch=?", (epoch,)).fetchone()
        if row is None:
            raise ShardError(f"unknown ring epoch {epoch}")
        return [(int(point), str(shard_id)) for point, shard_id in loads(row[0])]

    def shard_paths(self):
        return {row["shard_id"]: row["path"]
                for row in self.connection.execute("SELECT * FROM shards ORDER BY shard_id")}

    def write_ring(self, epoch, ring, now):
        self.connection.execute("INSERT INTO rings VALUES (?,?,?)",
                                (epoch, dumps([[point, shard_id] for point, shard_id in ring]), now))
        self.set("ring_epoch", epoch)

    def upsert_migration(self, collection, key, from_shard, to_shard, epoch, state, detail):
        if state not in VALID_STATES:
            raise ShardError("invalid migration state: " + state)
        self.connection.execute(
            "INSERT INTO migration_ledger VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(collection,key) DO UPDATE SET state=excluded.state,detail=excluded.detail",
            (collection, key, from_shard, to_shard, epoch, state, dumps(detail)))

    def migration_row(self, collection, key):
        row = self.connection.execute(
            "SELECT * FROM migration_ledger WHERE collection=? AND key=?", (collection, key)).fetchone()
        return dict(row) if row is not None else None

    def migration_history(self, collection, key):
        return [dict(row) for row in self.connection.execute(
            "SELECT * FROM migration_history WHERE collection=? AND key=? ORDER BY finished_at, epoch",
            (collection, key))]

    def archive_migration(self, collection, key, finished_at):
        """Move a finished ledger row into append-only hop history."""
        row = self.migration_row(collection, key)
        if row is None:
            return
        self.connection.execute(
            "INSERT INTO migration_history VALUES (?,?,?,?,?,?,?,?)",
            (row["collection"], row["key"], row["from_shard"], row["to_shard"],
             row["epoch"], row["state"], row["detail"], finished_at))
        self.connection.execute(
            "DELETE FROM migration_ledger WHERE collection=? AND key=?", (collection, key))

    def pending_migrations(self, to_shard=None, limit=None):
        query = "SELECT * FROM migration_ledger WHERE state IN ('pending','migrating')"
        arguments = []
        if to_shard is not None:
            query += " AND to_shard=?"
            arguments.append(to_shard)
        query += " ORDER BY collection,key"
        if limit is not None:
            query += " LIMIT ?"
            arguments.append(int(limit))
        return [dict(row) for row in self.connection.execute(query, arguments)]

    def record_xact(self, request_key, kind, fingerprint, state, plan, committed, detail):
        if state not in XACT_STATES:
            raise ShardError("invalid xact state: " + state)
        self.connection.execute(
            "INSERT INTO xact_ledger VALUES (?,?,?,?,?,?,?) "
            "ON CONFLICT(request_key) DO UPDATE SET state=excluded.state,"
            "committed=excluded.committed,detail=excluded.detail",
            (request_key, kind, fingerprint, state, dumps(plan), dumps(committed), dumps(detail)))

    def xact(self, request_key):
        row = self.connection.execute("SELECT * FROM xact_ledger WHERE request_key=?",
                                      (request_key,)).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["plan"] = loads(item["plan"])
        item["committed"] = loads(item["committed"])
        item["detail"] = loads(item["detail"])
        return item

    @staticmethod
    def json(value):
        return dumps(value)

    @staticmethod
    def unjson(value):
        return loads(value)

    @contextlib.contextmanager
    def atomic(self):
        with self.lock:
            if self.closed:
                raise ShardError("router handle is closed")
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                yield self.connection
                self.connection.commit()
            except BaseException:
                self.connection.rollback()
                raise

    @contextlib.contextmanager
    def read(self):
        with self.lock:
            if self.closed:
                raise ShardError("router handle is closed")
            self.connection.execute("BEGIN")
            try:
                yield self.connection
            finally:
                self.connection.rollback()

    def close(self):
        with self.lock:
            if not self.closed:
                self.connection.close()
                self.closed = True

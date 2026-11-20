"""Resumable key migration between document shards.

A migration replays a key's *entire* retained version chain onto the destination
shard through its normal write path, verifies the visible document and revision
chain match, and only then removes the source copy.  The router ledger records
the state of every key, so an interrupted migration resumes after reopen without
duplicating or losing data.
"""
from ..codec import loads
from ..errors import ShardError
from ..mvcc import latest_row
from ..transaction import WriteBatch


def ensure_catalog(destination, source_store, collection):
    """Copy a collection schema onto a shard that does not host it yet."""
    with destination._store.atomic(writable=True) as connection:
        if connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
            return
        row = source_store.connection.execute(
            "SELECT schema,generation FROM collections WHERE name=?", (collection,)).fetchone()
        if row is None:
            raise ShardError("source shard lost collection catalog: " + collection)
        connection.execute("INSERT INTO collections VALUES (?,?,?)",
                           (collection, row["schema"], row["generation"]))
        destination._store.set("catalog_epoch", int(destination._store.get("catalog_epoch")) + 1)


def chain_rows(store, collection, key):
    return store.connection.execute(
        "SELECT * FROM versions WHERE collection=? AND key=? ORDER BY rev",
        (collection, key)).fetchall()


def replay_chain(destination, collection, key, rows, request_key):
    """Replay a key's retained versions, one commit per version.

    Each commit uses a deterministic per-version request key, so rerunning after
    a crash reuses the shard receipt instead of duplicating a version.  A
    partially replayed chain left by a crash is detected by counting the
    destination's existing versions for the key; only the tail is replayed.
    """
    existing = chain_rows(destination._store, collection, key)
    if existing:
        # A prior attempt got this far.  Its deterministic request keys make the
        # already replayed prefix identical; verify that prefix before resuming.
        prefix = rows[:len(existing)]
        if len(existing) > len(rows):
            raise ShardError(
                f"migration resume for {collection}/{key}: destination holds more versions than source")
        for source, copied in zip(prefix, existing):
            if source["body"] != copied["body"] or source["expires"] != copied["expires"]:
                raise ShardError(
                    f"migration resume for {collection}/{key}: replayed prefix diverges from source")
        start = len(existing)
    else:
        start = 0
    last = existing[-1] if existing else None
    for index in range(start, len(rows)):
        row = rows[index]
        batch = WriteBatch(destination._store, f"{request_key}:v{index}")
        if row["body"] is None:
            batch.delete(collection, key)
        else:
            batch.put(collection, key, loads(row["body"]), expires_at=row["expires"])
        last = batch.commit()
    return last


def verify_destination(destination, collection, key, source_rows):
    dest_rows = chain_rows(destination._store, collection, key)
    if len(dest_rows) != len(source_rows):
        raise ShardError(f"migration verification failed for {collection}/{key}: version count differs")
    for source, copied in zip(source_rows, dest_rows):
        if source["body"] != copied["body"] or source["expires"] != copied["expires"]:
            raise ShardError(f"migration verification failed for {collection}/{key}: body differs")
    head = int(destination._store.get("head"))
    source_head = latest_row(destination._store.connection, collection, key, head)
    source_latest = source_rows[-1]
    if (source_head is None or source_head["body"] != source_latest["body"]
            or source_head["expires"] != source_latest["expires"]):
        raise ShardError(f"migration verification failed for {collection}/{key}: visible state differs")


def purge_source(source_store, collection, key):
    with source_store.atomic(writable=True) as connection:
        connection.execute("DELETE FROM versions WHERE collection=? AND key=?", (collection, key))
        connection.execute("DELETE FROM index_values WHERE collection=? AND key=?", (collection, key))


def migrate_one(router, source_db, destination_db, entry, now):
    """Execute the ledger state machine for a single key."""
    collection, key = entry["collection"], entry["key"]
    from_shard, to_shard, epoch = entry["from_shard"], entry["to_shard"], entry["epoch"]
    if entry["state"] == "migrated":
        return False
    # Ring sanity: the current topology must still claim the key on the target.
    if router.route(collection, key) != to_shard:
        raise ShardError(f"ledger target {to_shard} no longer owns {collection}/{key}")
    ensure_catalog(destination_db, source_db._store, collection)
    source_rows = chain_rows(source_db._store, collection, key)
    if not source_rows:
        # Source was already purged: either a prior crash after purge, or a stale
        # ledger entry. The destination must already hold a verified chain.
        verify_destination(destination_db, collection, key,
                           chain_rows(destination_db._store, collection, key))
        with router._store.atomic():
            router._store.upsert_migration(collection, key, from_shard, to_shard, epoch,
                                           "migrated", {"finished_at": now, "resumed": True})
        return True
    request_key = f"migrate:{epoch}:{collection}:{key}"
    with router._store.atomic():
        router._store.upsert_migration(collection, key, from_shard, to_shard, epoch,
                                       "migrating", {"attempted_at": now})
    destination_rows = chain_rows(destination_db._store, collection, key)
    if destination_rows and entry["state"] == "pending":
        # Fresh attempt but the destination already holds versions for this key:
        # never silently mix two chains.  This indicates a prior interrupted hop
        # whose ledger was archived; require an explicit drain/rebuild decision.
        raise ShardError(
            f"cannot migrate {collection}/{key}: destination shard {to_shard} already holds versions; "
            "resolve the stale destination before retrying")
    result = replay_chain(destination_db, collection, key, source_rows, request_key)
    verify_destination(destination_db, collection, key, source_rows)
    purge_source(source_db._store, collection, key)
    with router._store.atomic():
        router._store.upsert_migration(
            collection, key, from_shard, to_shard, epoch, "migrated",
            {"finished_at": now, "versions": len(source_rows),
             "source_revisions": [row["rev"] for row in source_rows],
             "destination_head": result.revision, "replayed": result.replayed})
    return True


def drain(router, limit=None):
    """Advance every pending/migrating ledger entry; returns what was completed."""
    done = []
    while True:
        entries = router._store.pending_migrations(limit=limit)
        if not entries:
            return done
        for entry in entries:
            source_db = router.shard(entry["from_shard"])
            destination_db = router.shard(entry["to_shard"])
            if migrate_one(router, source_db, destination_db, entry, router.clock()):
                done.append({"collection": entry["collection"], "key": entry["key"],
                             "from": entry["from_shard"], "to": entry["to_shard"]})
        if limit is not None:
            return done

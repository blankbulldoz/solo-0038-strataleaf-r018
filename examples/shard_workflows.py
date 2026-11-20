"""Finite, real-API demonstrations of the embedded sharding layer.

Run with ``python3 -m examples.shard_workflows shard``.  Every step prints
checkable facts: hash-ring placement, migration ledger transitions, the
consistency manifest attached to cross-shard reads, and the two-phase ledger
that makes multi-shard batch outcomes explicit before and after execution.
"""
import json
import sys
import tempfile
from pathlib import Path

from strataleaf import (Conflict, CrossShardCommitError, InvalidCursor,
                        InvalidDocument, MigrationInProgress, ShardDatabase)

SCHEMA = {"fields": {"region": "string", "sequence": "integer"},
          "required": ["region", "sequence"]}


def rejected(call, error):
    try:
        call()
    except error as exc:
        return {"error": type(exc).__name__, "message": str(exc)}
    raise RuntimeError("operation unexpectedly succeeded")


def run():
    with tempfile.TemporaryDirectory(prefix="strataleaf-shard-") as directory:
        root = Path(directory) / "cluster"
        output = {}

        # 1. Open a two-shard database; routing metadata is its own file.
        db = ShardDatabase(root, shard_ids=("s0", "s1"))
        db.create_collection("segments", SCHEMA)
        seed = db.batch("shard_seed")
        for index in range(24):
            seed.put("segments", f"seg_{index:02}",
                     {"region": "east" if index % 2 == 0 else "west", "sequence": index})
        seed.commit()
        output["topology"] = {"ring_epoch": db.epoch, "shards": db.shard_ids(),
                              "router_file": "router.db",
                              "shard_files": [f"shard-{sid}.db" for sid in db.shard_ids()]}

        # 2. Placement is a pure function of the persisted ring.
        sample = db.explain("segments", "seg_05")
        output["routing_before"] = {key: sample[key] for key in
                                    ("slot", "ring_epoch", "owner_shard", "served_by")}

        # 3. A cross-shard query carries an explicit consistency manifest.
        first_page = db.query("segments", {"field": "sequence", "op": "ge", "value": 0}, limit=8)
        keys = [record.key for record in first_page.records]
        cursor = first_page.cursor
        while cursor:
            page = db.query("segments", {"field": "sequence", "op": "ge", "value": 0},
                            limit=8, cursor=cursor)
            keys.extend(record.key for record in page.records)
            cursor = page.cursor
        output["cross_shard_query"] = {"keys_returned": len(keys),
                                       "globally_sorted": keys == sorted(keys),
                                       "manifest": first_page.manifest,
                                       "plan": first_page.plan["access"]}
        pinned = dict(first_page.manifest["revisions"])

        # 4. Expand the ring without draining: keys get explicit pending owners.
        expansion = db.add_shard("s2", drain=False)
        moving_key = expansion["scheduled"][0]
        output["expansion_published"] = {"new_epoch": expansion["epoch"],
                                         "slots_moved": expansion["slots_moved"],
                                         "slot_ranges": expansion["slot_ranges"][:3],
                                         "keys_scheduled": expansion["keys_scheduled"]}
        output["ownership_during_migration"] = db.explain(
            moving_key["collection"], moving_key["key"])
        # A pending key remains readable from its old shard.
        pending_record, _ = db.get(moving_key["collection"], moving_key["key"])
        output["pending_read"] = {"key": moving_key["key"],
                                  "sequence": pending_record.document["sequence"]}

        # 5. A topology change cannot start while another one is open.
        output["concurrent_expansion"] = rejected(
            lambda: db.add_shard("s3", drain=False), MigrationInProgress)

        # 6. Drain part of the ledger, reopen, then resume: metadata is durable.
        first_batch = db.migrate_drain(limit=3)
        db.close()
        reopened = ShardDatabase(root)
        resumed = reopened.migrate_drain()
        output["resumable_migration"] = {
            "first_batch": len(first_batch), "resumed_after_reopen": len(resumed),
            "status": reopened.migration_status()["counts"],
            "verify": reopened.verify()["ok"]}

        # 7. Every pre-expansion key is still readable and matches the ring.
        placement_check = []
        for index in range(24):
            key = f"seg_{index:02}"
            record, _ = reopened.get("segments", key)
            explain = reopened.explain("segments", key)
            placement_check.append(
                {"key": key, "sequence": record.document["sequence"],
                 "owner": explain["owner_shard"], "served_by": explain["served_by"],
                 "hops": len(explain["hops"])})
        output["post_expansion_readability"] = {
            "all_24_keys_readable": len(placement_check) == 24,
            "served_by_equals_owner": all(item["owner"] == item["served_by"]
                                          for item in placement_check),
            "examples": placement_check[:4]}

        # 8. Cross-shard batch: preflight rejects before any shard is written.
        cross_keys = {}
        for index in range(24):
            key = f"seg_{index:02}"
            cross_keys.setdefault(reopened.route("segments", key), key)
        chosen = list(cross_keys.values())[:2]
        preflight = rejected(
            lambda: reopened.batch("rejected_batch")
            .put("segments", chosen[0], {"region": "z", "sequence": 999}, expected=999)
            .put("segments", chosen[1], {"region": "z", "sequence": 998})
            .commit(), Conflict)
        output["batch_preflight"] = {"rejected": preflight,
                                     "keys_untouched": all(
                                         reopened.get("segments", key)[0].document["sequence"] != 999
                                         for key in chosen)}
        output["request_key_required"] = rejected(
            lambda: reopened.batch()
            .put("segments", chosen[0], {"region": "n", "sequence": 1})
            .put("segments", chosen[1], {"region": "n", "sequence": 2})
            .commit(), InvalidDocument)

        # 9. A coordinated multi-shard commit and its idempotent replay.
        coordinated = (reopened.batch("coordinated_update")
                       .put("segments", chosen[0], {"region": "north", "sequence": 100})
                       .put("segments", chosen[1], {"region": "north", "sequence": 101})
                       .commit())
        replay = (reopened.batch("coordinated_update")
                  .put("segments", chosen[0], {"region": "north", "sequence": 100})
                  .put("segments", chosen[1], {"region": "north", "sequence": 101})
                  .commit())
        output["coordinated_commit"] = {"result": coordinated.as_dict(),
                                        "replay_state": replay.state,
                                        "ledger": reopened.xact_status(
                                            "coordinated_update")["state"]}

        # 9b. Simulate a shard outage after some shards committed: the outcome is
        #     reported explicitly, never silently partial, and the same request
        #     key resumes the remaining shards.
        target_keys = []
        seen_shards = set()
        for index in range(24):
            key = f"seg_{index:02}"
            owner = reopened.route("segments", key)
            if owner not in seen_shards:
                seen_shards.add(owner)
                target_keys.append(key)
        target_keys = target_keys[:3]
        batch_cls = type(reopened.batch("probe"))
        original = batch_cls._commit_shard
        gate = {"fail_shard": target_keys[2] and reopened.route("segments", target_keys[2]),
                "tripped": False}

        def flaky(self, shard_id, operations, cross_shard):
            if not gate["tripped"] and shard_id == gate["fail_shard"]:
                gate["tripped"] = True
                raise RuntimeError("simulated shard outage")
            return original(self, shard_id, operations, cross_shard)

        batch_cls._commit_shard = flaky
        partial = reopened.batch("resilient_wide")
        for ordinal, key in enumerate(target_keys):
            partial.put("segments", key, {"region": "west", "sequence": 200 + ordinal})
        partial_failure = rejected(partial.commit, CrossShardCommitError)
        ledger_at_failure = reopened.xact_status("resilient_wide")
        batch_cls._commit_shard = original
        resume_batch = reopened.batch("resilient_wide")
        for ordinal, key in enumerate(target_keys):
            resume_batch.put("segments", key, {"region": "west", "sequence": 200 + ordinal})
        resumed_commit = resume_batch.commit()
        output["partial_commit_resume"] = {
            "failure_reported_before_return": partial_failure["error"],
            "ledger_state_at_failure": ledger_at_failure["state"],
            "shards_committed_at_failure": ledger_at_failure["committed"],
            "resume_state": resumed_commit.state,
            "all_keys_visible": all(
                reopened.get("segments", key)[0].document["sequence"] == 200 + ordinal
                for ordinal, key in enumerate(target_keys))}

        # 10. Pinned per-shard revisions give a repeatable cross-shard snapshot.
        # A manifest captured before the expansion cannot address the new shard.
        output["stale_manifest"] = rejected(
            lambda: reopened.query("segments", None, limit=100, revisions=pinned),
            InvalidCursor)
        current = reopened.query("segments", None, limit=100)
        current_pinned = dict(current.manifest["revisions"])
        reopened.batch("late_write").put(
            "segments", "seg_late", {"region": "south", "sequence": 777}).commit()
        pinned_page = reopened.query("segments", None, limit=100, revisions=current_pinned)
        head_page = reopened.query("segments", None, limit=100)
        output["consistency_manifest"] = {
            "pinned_excludes_late_write": "seg_late" not in
            [r.key for r in pinned_page.records],
            "head_includes_late_write": "seg_late" in [r.key for r in head_page.records],
            "pinned_consistency": pinned_page.manifest["consistency"],
            "head_consistency": head_page.manifest["consistency"]}
        output["final_verify"] = reopened.verify()
        reopened.close()

        print(json.dumps({"workflow": "shard", "result": output},
                         ensure_ascii=False, sort_keys=True, default=str))


if __name__ == "__main__":
    run()

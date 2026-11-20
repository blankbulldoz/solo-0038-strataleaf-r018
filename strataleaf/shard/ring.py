"""Deterministic hash-ring routing.

The ring is the single fact that maps a document key to a physical shard.  It is
purely a function of the persisted ring definition, so a router reopened on a new
process computes identical placements.  Adding a shard only adds virtual nodes;
existing slots keep the same point and therefore keep their position on the
circle, which is why data written under an older topology stays addressable.
"""
import hashlib

RING_SLOTS = 65536
VIRTUAL_NODES = 256


def key_slot(collection, key):
    """Stable 16-bit slot for a (collection, key) pair."""
    digest = hashlib.sha256(collection.encode("utf-8") + b"\x00" + key.encode("utf-8")).digest()
    return int.from_bytes(digest[:2], "big")


def node_point(shard_id, virtual):
    """Ring coordinate of one virtual node."""
    digest = hashlib.sha256(f"shard:{shard_id}:vnode:{virtual}".encode("utf-8")).digest()
    return int.from_bytes(digest[:2], "big")


def build_ring(shard_ids, virtual_nodes=VIRTUAL_NODES):
    """Return ``[(point, shard_id), ...]`` sorted by point, unique points kept."""
    points = {}
    for shard_id in shard_ids:
        for virtual in range(virtual_nodes):
            point = node_point(shard_id, virtual)
            points.setdefault(point, shard_id)
    return sorted(points.items())


def owner_at(ring, slot):
    """Shard id that owns a slot: first node at or after the slot, wrapping."""
    if not ring:
        raise ValueError("cannot route on an empty ring")
    low, high = 0, len(ring)
    while low < high:
        middle = (low + high) // 2
        if ring[middle][0] < slot:
            low = middle + 1
        else:
            high = middle
    return ring[low][1] if low < len(ring) else ring[0][1]


def route(ring, collection, key):
    return owner_at(ring, key_slot(collection, key))


def affected_slots(old_ring, new_ring):
    """Slots whose owner differs between two ring generations, with both owners."""
    moved = []
    for slot in range(RING_SLOTS):
        before = owner_at(old_ring, slot)
        after = owner_at(new_ring, slot)
        if before != after:
            moved.append((slot, before, after))
    return moved


def slot_ranges(slots):
    """Compress a sorted slot list into inclusive ``(start, end)`` ranges."""
    if not slots:
        return []
    ranges = []
    start = previous = slots[0]
    for slot in slots[1:]:
        if slot == previous + 1:
            previous = slot
            continue
        ranges.append((start, previous))
        start = previous = slot
    ranges.append((start, previous))
    return ranges

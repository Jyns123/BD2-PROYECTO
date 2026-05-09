import os
import time
import tempfile
from typing import Iterable

from algoritmos.external_hashing import partition_records, iter_bucket_records


def hash_join(left_records: Iterable[bytes], left_record_size: int, left_key_fn,
              right_records: Iterable[bytes], right_record_size: int, right_key_fn,
              buckets: int = 16) -> dict:
    """
    Run a Hash JOIN of `left` ⋈ `right` on equality of join keys.

    Returns a dict with the joined records (as `(left_bytes, right_bytes)` pairs)
    plus timing stats.
    """
    if buckets < 1:
        raise ValueError("buckets must be >= 1")

    tmp_dir = tempfile.mkdtemp(prefix="hashjoin_")
    left_tmp = os.path.join(tmp_dir, "L")
    right_tmp = os.path.join(tmp_dir, "R")
    os.makedirs(left_tmp, exist_ok=True)
    os.makedirs(right_tmp, exist_ok=True)

    t0 = time.time()
    left_buckets = partition_records(
        left_records, left_record_size, left_key_fn, buckets, left_tmp
    )
    right_buckets = partition_records(
        right_records, right_record_size, right_key_fn, buckets, right_tmp
    )
    t1 = time.time()

    matches: list[tuple[bytes, bytes]] = []

    for i in range(buckets):
        ht: dict = {}
        for rec in iter_bucket_records(left_buckets[i], left_record_size):
            k = left_key_fn(rec)
            ht.setdefault(k, []).append(rec)

        if not ht:
            continue

        for s_rec in iter_bucket_records(right_buckets[i], right_record_size):
            k = right_key_fn(s_rec)
            hits = ht.get(k)
            if hits:
                for l_rec in hits:
                    matches.append((l_rec, s_rec))

    t2 = time.time()

    for path in left_buckets + right_buckets:
        try:
            os.remove(path)
        except OSError:
            pass
    for d in (left_tmp, right_tmp, tmp_dir):
        try:
            os.rmdir(d)
        except OSError:
            pass

    return {
        "matches": matches,
        "match_count": len(matches),
        "buckets": buckets,
        "phase1_sec": round(t1 - t0, 4),
        "phase2_sec": round(t2 - t1, 4),
        "total_sec": round(t2 - t0, 4),
    }

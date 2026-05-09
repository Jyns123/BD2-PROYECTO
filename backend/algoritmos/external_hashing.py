import os
import time
import tempfile
from typing import Iterable

from storage.disk_manager import DiskManager
from storage.page import Page


def _records_per_page(record_size: int) -> int:
    return (4096 - 4) // record_size


def partition_records(records: Iterable[bytes], record_size: int, key_fn,
                      buckets: int, tmp_dir: str | None = None) -> list[str]:
    """
    Partition a stream of records into `buckets` files on disk.

    Returns the list of bucket file paths. Page 0 of each bucket file is meta;
    data starts at page 1.
    """
    if buckets < 1:
        raise ValueError("buckets must be >= 1")

    if tmp_dir is None:
        tmp_dir = tempfile.mkdtemp(prefix="exthash_")

    rpp = _records_per_page(record_size)

    bucket_paths = [os.path.join(tmp_dir, f"bucket_{i}.bin") for i in range(buckets)]
    bucket_dms = [DiskManager(p) for p in bucket_paths]
    bucket_buffers: list[list[bytes]] = [[] for _ in range(buckets)]

    def flush(i: int):
        if not bucket_buffers[i]:
            return
        page = Page(record_size)
        for r in bucket_buffers[i]:
            page.insert_record(r)
        page_id = bucket_dms[i].allocate_page()
        bucket_dms[i].write_page(page_id, page.to_bytes())
        bucket_buffers[i].clear()

    try:
        for rec in records:
            idx = hash(key_fn(rec)) % buckets
            bucket_buffers[idx].append(rec)
            if len(bucket_buffers[idx]) == rpp:
                flush(idx)

        for i in range(buckets):
            flush(i)
    finally:
        for dm in bucket_dms:
            dm.close()

    return bucket_paths


def iter_bucket_records(bucket_path: str, record_size: int):
    """Yield every record in a bucket file."""
    dm = DiskManager(bucket_path)
    try:
        total = dm._get_total_pages()
        for page_id in range(1, total):
            page = Page.from_bytes(dm.read_page(page_id), record_size)
            for rec in page.read_records():
                yield rec
    finally:
        dm.close()


# ---------------------------------------------------------------------------
# GROUP BY via external hashing
# ---------------------------------------------------------------------------

class _AggState:
    __slots__ = ("op", "value", "count")

    def __init__(self, op: str):
        self.op = op
        self.value = None
        self.count = 0

    def update(self, x):
        self.count += 1
        if self.op == "COUNT":
            return
        if self.op in ("SUM", "AVG"):
            self.value = (self.value or 0) + (x or 0)
        elif self.op == "MIN":
            self.value = x if self.value is None else min(self.value, x)
        elif self.op == "MAX":
            self.value = x if self.value is None else max(self.value, x)

    def finalize(self):
        if self.op == "COUNT":
            return self.count
        if self.op == "AVG":
            if self.count == 0:
                return None
            return self.value / self.count
        return self.value


def external_hash_group_by(records: Iterable[bytes], record_size: int, key_fn,
                           buckets: int = 16, value_fn=None, op: str = "COUNT") -> dict:
    """
    GROUP BY <group_col> with one aggregate.

    `key_fn(record)`     -> the group key.
    `value_fn(record)`   -> the value fed to the aggregator (ignored for COUNT).
    `op` ∈ {COUNT, SUM, MIN, MAX, AVG}.
    """
    op = op.upper()
    if op not in ("COUNT", "SUM", "MIN", "MAX", "AVG"):
        raise ValueError(f"unsupported aggregate op: {op}")
    if op != "COUNT" and value_fn is None:
        raise ValueError(f"op={op} requires a value_fn")

    t0 = time.time()
    tmp_dir = tempfile.mkdtemp(prefix="groupby_")
    bucket_paths = partition_records(records, record_size, key_fn, buckets, tmp_dir)
    t1 = time.time()

    final: dict = {}
    for path in bucket_paths:
        local: dict = {}
        for rec in iter_bucket_records(path, record_size):
            k = key_fn(rec)
            st = local.get(k)
            if st is None:
                st = _AggState(op)
                local[k] = st
            st.update(value_fn(rec) if value_fn else None)

        for k, st in local.items():
            global_st = final.get(k)
            if global_st is None:
                final[k] = st
            else:
                if op == "COUNT":
                    global_st.count += st.count
                elif op in ("SUM", "AVG"):
                    global_st.value = (global_st.value or 0) + (st.value or 0)
                    global_st.count += st.count
                elif op == "MIN":
                    global_st.value = (st.value if global_st.value is None
                                       else min(global_st.value, st.value))
                    global_st.count += st.count
                elif op == "MAX":
                    global_st.value = (st.value if global_st.value is None
                                       else max(global_st.value, st.value))
                    global_st.count += st.count
    t2 = time.time()

    result = {k: st.finalize() for k, st in final.items()}

    for p in bucket_paths:
        try:
            os.remove(p)
        except OSError:
            pass
    try:
        os.rmdir(tmp_dir)
    except OSError:
        pass

    return {
        "result": result,
        "groups": len(result),
        "buckets": buckets,
        "op": op,
        "phase1_sec": round(t1 - t0, 4),
        "phase2_sec": round(t2 - t1, 4),
        "total_sec": round(t2 - t0, 4),
    }

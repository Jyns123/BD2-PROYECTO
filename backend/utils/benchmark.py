import os
import time
import random
import struct
import json
import tempfile

from index.bplustree  import BPlusTree
from index.hash       import ExtendibleHash
from index.sequential import SequentialFile

N_SIZES     = [1_000, 10_000, 100_000]
LOG_PATH    = "utils/experiment_log.json"
RECORD_SIZE = 40
ORDER       = 50
N_SEARCH    = 20
N_RANGE     = 10


def make_record(key):
    return struct.pack(">I", key) + b"\x00" * (RECORD_SIZE - 4)

def key_extractor(record):
    return struct.unpack(">I", record[:4])[0]

def generate_keys(n):
    return random.sample(range(1, n * 10), n)

def tmp_path():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.remove(path)
    return path

def remove(*paths):
    for p in paths:
        if p and os.path.exists(p):
            os.remove(p)


def run_benchmark():
    entries = []

    for n in N_SIZES:
        print(f"\n{'='*50}")
        print(f"  n = {n:,}")
        print(f"{'='*50}")

        keys        = generate_keys(n)
        records     = [make_record(k) for k in keys]
        sample_keys = random.sample(keys, min(N_SEARCH, len(keys)))

        # ── B+ TREE ──────────────────────────────────────────
        path = tmp_path()
        tree = BPlusTree(path, RECORD_SIZE, key_extractor, order=ORDER)
        print(f"[B+ Tree] Insertando {n:,} registros...")

        tree.dm.reset_stats()
        t0 = time.perf_counter()
        for rec in records:
            tree.insert(rec)
        t1 = time.perf_counter()
        entries.append({"structure": "bplustree", "operation": "insert", "n": n,
                        "reads": tree.dm.read_count / n, "writes": tree.dm.write_count / n,
                        "disk_accesses": (tree.dm.read_count + tree.dm.write_count) / n,
                        "time_ms": round((t1 - t0) * 1000 / n, 4)})

        reads = writes = 0
        t0 = time.perf_counter()
        for k in sample_keys:
            tree.dm.reset_stats()
            tree.search(k)
            reads  += tree.dm.read_count
            writes += tree.dm.write_count
        t1 = time.perf_counter()
        entries.append({"structure": "bplustree", "operation": "search", "n": n,
                        "reads": reads / N_SEARCH, "writes": writes / N_SEARCH,
                        "disk_accesses": (reads + writes) / N_SEARCH,
                        "time_ms": round((t1 - t0) * 1000 / N_SEARCH, 4)})

        reads = writes = 0
        t0 = time.perf_counter()
        for _ in range(N_RANGE):
            start = random.randint(1, n * 10 - n // 10)
            tree.dm.reset_stats()
            tree.range_search(start, start + n // 10)
            reads  += tree.dm.read_count
            writes += tree.dm.write_count
        t1 = time.perf_counter()
        entries.append({"structure": "bplustree", "operation": "range_search", "n": n,
                        "reads": reads / N_RANGE, "writes": writes / N_RANGE,
                        "disk_accesses": (reads + writes) / N_RANGE,
                        "time_ms": round((t1 - t0) * 1000 / N_RANGE, 4)})

        tree.close()
        remove(path)

        # ── EXTENDIBLE HASHING ───────────────────────────────
        path = tmp_path()
        htable = ExtendibleHash(path, RECORD_SIZE, key_extractor)
        print(f"[Hash]    Insertando {n:,} registros...")

        htable.dm.reset_stats()
        t0 = time.perf_counter()
        for rec in records:
            htable.insert(rec)
        t1 = time.perf_counter()
        entries.append({"structure": "hash", "operation": "insert", "n": n,
                        "reads": htable.dm.read_count / n, "writes": htable.dm.write_count / n,
                        "disk_accesses": (htable.dm.read_count + htable.dm.write_count) / n,
                        "time_ms": round((t1 - t0) * 1000 / n, 4)})

        reads = writes = 0
        t0 = time.perf_counter()
        for k in sample_keys:
            htable.dm.reset_stats()
            htable.search(k)
            reads  += htable.dm.read_count
            writes += htable.dm.write_count
        t1 = time.perf_counter()
        entries.append({"structure": "hash", "operation": "search", "n": n,
                        "reads": reads / N_SEARCH, "writes": writes / N_SEARCH,
                        "disk_accesses": (reads + writes) / N_SEARCH,
                        "time_ms": round((t1 - t0) * 1000 / N_SEARCH, 4)})

        htable.close()
        remove(path, path + ".dir")

        # ── SEQUENTIAL FILE ──────────────────────────────────
        path    = tmp_path()
        ov_path = tmp_path()
        seq = SequentialFile(path, ov_path, RECORD_SIZE, key_extractor)
        print(f"[Seq]     Insertando {n:,} registros...")

        seq.dm.reset_stats()
        t0 = time.perf_counter()
        for rec in records:
            seq.insert(rec)
        t1 = time.perf_counter()
        s = seq.dm.get_stats()
        entries.append({"structure": "sequential", "operation": "insert", "n": n,
                        "reads": s["reads"] / n, "writes": s["writes"] / n,
                        "disk_accesses": (s["reads"] + s["writes"]) / n,
                        "time_ms": round((t1 - t0) * 1000 / n, 4)})

        reads = writes = 0
        t0 = time.perf_counter()
        for k in sample_keys:
            seq.dm.reset_stats()
            seq.search(k)
            s = seq.dm.get_stats()
            reads  += s["reads"]
            writes += s["writes"]
        t1 = time.perf_counter()
        entries.append({"structure": "sequential", "operation": "search", "n": n,
                        "reads": reads / N_SEARCH, "writes": writes / N_SEARCH,
                        "disk_accesses": (reads + writes) / N_SEARCH,
                        "time_ms": round((t1 - t0) * 1000 / N_SEARCH, 4)})

        reads = writes = 0
        t0 = time.perf_counter()
        for _ in range(N_RANGE):
            start = random.randint(1, n * 10 - n // 10)
            seq.dm.reset_stats()
            seq.range_search(start, start + n // 10)
            s = seq.dm.get_stats()
            reads  += s["reads"]
            writes += s["writes"]
        t1 = time.perf_counter()
        entries.append({"structure": "sequential", "operation": "range_search", "n": n,
                        "reads": reads / N_RANGE, "writes": writes / N_RANGE,
                        "disk_accesses": (reads + writes) / N_RANGE,
                        "time_ms": round((t1 - t0) * 1000 / N_RANGE, 4)})

        seq.close()
        remove(path, ov_path)

    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    with open(LOG_PATH, "w") as f:
        json.dump(entries, f, indent=2)

    print(f"\n{'Estructura':<15} {'Operación':<15} {'n':>8} {'Reads':>7} {'Writes':>7} {'Total I/O':>10} {'ms':>10}")
    print("-" * 75)
    for e in entries:
        print(f"{e['structure']:<15} {e['operation']:<15} {e['n']:>8,} "
              f"{e['reads']:>7.1f} {e['writes']:>7.1f} "
              f"{e['disk_accesses']:>10.1f} {e['time_ms']:>10.4f}")

    print(f"\nLog guardado en {LOG_PATH}")
    return entries


if __name__ == "__main__":
    run_benchmark()
import os
import random
import struct
import tempfile

from utils.metrics import MetricsLogger
from index.bplustree import BPlusTree
from index.hash import ExtendibleHash
from index.sequential import SequentialFile

N_SIZES = [1_000, 10_000, 100_000]
LOG_PATH = "utils/experiment_log.json"
RECORD_SIZE = 40
ORDER = 50

def make_record(key: int) -> bytes:
    return struct.pack(">I", key) + b"\x00" * (RECORD_SIZE - 4)

def key_extractor(record: bytes) -> int:
    return struct.unpack(">I", record[:4])[0]

def generate_keys(n: int):
    return random.sample(range(1, n * 10), n)

def tmp_file():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.remove(path)
    return path

def run_benchmark():
    logger = MetricsLogger()

    for n in N_SIZES:
        print(f"\n{'='*50}")
        print(f"  n = {n:,}")
        print(f"{'='*50}")

        keys = generate_keys(n)
        records = [make_record(k) for k in keys]
        sample_keys = random.sample(keys, min(10, len(keys)))

        # ------------------------------------------------
        # B+ TREE
        # ------------------------------------------------
        path = tmp_file()
        tree = BPlusTree(path, RECORD_SIZE, key_extractor, order=ORDER)
        print(f"[B+ Tree] Insertando {n} registros...")

        for rec in records:
            tree.cache.clear()                              # FIX: fuera del with
            with logger.measure("bplustree", "insert", n, dm=tree.dm):
                tree.insert(rec)

        for sk in sample_keys:
            with logger.measure("bplustree", "search", n, dm=tree.dm):
                tree.search(sk)

        for _ in range(5):
            start = random.randint(1, n * 10 - n // 10)
            end = start + n // 10
            with logger.measure("bplustree", "range_search", n, dm=tree.dm):
                tree.range_search(start, end)

        tree.close()
        os.remove(path)

        # ------------------------------------------------
        # EXTENDIBLE HASHING
        # ------------------------------------------------
        path = tmp_file()
        htable = ExtendibleHash(path, RECORD_SIZE, key_extractor)
        print(f"[Hash] Insertando {n} registros...")

        for rec in records:
            with logger.measure("hash", "insert", n, dm=htable.dm):
                htable.insert(rec)

        for sk in sample_keys:
            with logger.measure("hash", "search", n, dm=htable.dm):
                htable.search(sk)

        htable.close()
        os.remove(path)

        # ------------------------------------------------
        # SEQUENTIAL FILE
        # ------------------------------------------------
        path = tmp_file()
        overflow_path = tmp_file()                          # FIX: segundo path
        seq = SequentialFile(path, overflow_path, RECORD_SIZE, key_extractor)
        print(f"[Sequential] Insertando {n} registros...")

        for rec in records:
            with logger.measure("sequential", "insert", n, dm=seq.dm):
                seq.insert(rec)

        for sk in sample_keys:
            with logger.measure("sequential", "search", n, dm=seq.dm):
                seq.search(sk)

        for _ in range(5):
            start = random.randint(1, n * 10 - n // 10)
            end = start + n // 10
            with logger.measure("sequential", "range_search", n, dm=seq.dm):
                seq.range_search(start, end)

        seq.close()
        if os.path.exists(path):
            os.remove(path)
        if os.path.exists(overflow_path):                   # FIX: limpiar overflow también
            os.remove(overflow_path)

    logger.save(LOG_PATH)
    logger.summary()
    print(f"\nLog guardado en {LOG_PATH}")
    return logger


if __name__ == "__main__":
    run_benchmark()
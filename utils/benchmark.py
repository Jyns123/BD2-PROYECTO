"""
utils/benchmark.py

Corre experimentos automáticos con n = 1_000, 10_000, 100_000 registros
para B+ Tree, Extendible Hashing y Sequential File.

Uso:
    python -m utils.benchmark

Requiere que cada estructura tenga la interfaz:
    .insert(record)
    .search(key)
    .range_search(start, end)   # solo B+ Tree y Sequential
    .dm                         # DiskManager con read_count / write_count
"""

import os
import random
import struct
import tempfile
import time

from utils.metrics import MetricsLogger

# -------------------------------------------------------
# CONFIG
# -------------------------------------------------------
N_SIZES = [1_000, 10_000, 100_000]
LOG_PATH = "utils/experiment_log.json"
RECORD_SIZE = 40  # bytes por registro (ajusta según tu esquema)
ORDER = 50        # orden del B+ Tree para datasets grandes

# -------------------------------------------------------
# HELPERS
# -------------------------------------------------------

def make_record(key: int) -> bytes:
    """Crea un registro de RECORD_SIZE bytes con la key en los primeros 4 bytes."""
    data = struct.pack(">I", key) + b"\x00" * (RECORD_SIZE - 4)
    return data

def key_extractor(record: bytes) -> int:
    return struct.unpack(">I", record[:4])[0]

def generate_keys(n: int):
    """Genera n claves enteras aleatorias sin repetición."""
    return random.sample(range(1, n * 10), n)

def tmp_file():
    """Crea un archivo temporal y devuelve su path."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.remove(path)
    return path

# -------------------------------------------------------
# BENCHMARK RUNNER
# -------------------------------------------------------

def run_benchmark():
    from index.bplustree import BPlusTree
    # Importa tus otras estructuras aquí cuando estén listas:
    # from index.hash import ExtendibleHash
    # from index.sequential import SequentialFile

    logger = MetricsLogger()

    for n in N_SIZES:
        print(f"\n{'='*50}")
        print(f"  n = {n:,}")
        print(f"{'='*50}")

        keys = generate_keys(n)
        records = [make_record(k) for k in keys]

        # ------------------------------------------------
        # B+ TREE
        # ------------------------------------------------
        path = tmp_file()
        tree = BPlusTree(path, RECORD_SIZE, key_extractor, order=ORDER)

        print(f"\n[B+ Tree] Insertando {n} registros...")
        for rec in records:
            k = key_extractor(rec)
            with logger.measure("bplustree", "insert", n, dm=tree.dm):
                tree.dm.reset_stats()
                tree.cache.clear()
                tree.insert(rec)
        # promedia inserts (el logger guarda uno por registro → resumimos abajo)

        # Search puntual (10 búsquedas aleatorias, promediamos)
        sample_keys = random.sample(keys, min(10, len(keys)))
        for sk in sample_keys:
            with logger.measure("bplustree", "search", n, dm=tree.dm):
                tree.search(sk)

        # Range search (5 rangos aleatorios del 1% del rango total)
        for _ in range(5):
            start = random.randint(1, n * 10 - n // 10)
            end = start + n // 10
            with logger.measure("bplustree", "range_search", n, dm=tree.dm):
                tree.range_search(start, end)

        tree.close()
        os.remove(path)

    
        path = tmp_file()
        htable = ExtendibleHash(path, RECORD_SIZE, key_extractor)
        for rec in records:
            with logger.measure("hash", "insert", n, dm=htable.dm):
                htable.insert(rec)
        for sk in sample_keys:
            with logger.measure("hash", "search", n, dm=htable.dm):
                htable.search(sk)
        htable.close()
        os.remove(path)


        path = tmp_file()
        seq = SequentialFile(path, RECORD_SIZE, key_extractor)
        for rec in records:
            with logger.measure("sequential", "insert", n, dm=seq.dm):
                seq.insert(rec)
        for sk in sample_keys:
            with logger.measure("sequential", "search", n, dm=seq.dm):
                seq.search(sk)
        for _ in range(5):
            start = random.randint(1, n*10 - n//10)
            end = start + n//10
            with logger.measure("sequential", "range_search", n, dm=seq.dm):
                seq.range_search(start, end)
        seq.close()
        os.remove(path)

    logger.save(LOG_PATH)
    logger.summary()
    print(f"\n✅ Log guardado en {LOG_PATH}")
    return logger


if __name__ == "__main__":
    run_benchmark()
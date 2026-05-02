"""
utils/metrics.py

Captura métricas por operación (reads, writes, tiempo) y las guarda en un log JSON.
Uso:
    from utils.metrics import MetricsLogger
    logger = MetricsLogger()
    with logger.measure("bplustree", "search", n=10000):
        result = tree.search(key)
    logger.save("utils/experiment_log.json")
"""

import time
import json
import os
from contextlib import contextmanager


class MetricsLogger:
    def __init__(self):
        # lista de entradas: [{structure, operation, n, reads, writes, time_ms}]
        self.entries = []
        self._current_dm = None

    @contextmanager
    def measure(self, structure: str, operation: str, n: int, dm=None):
        """
        Context manager que mide reads, writes y tiempo de una operación.

        Args:
            structure: nombre de la estructura ("bplustree", "hash", "sequential")
            operation: nombre de la operación ("insert", "search", "range_search")
            n: tamaño del dataset en ese momento
            dm: DiskManager del índice (para leer read_count / write_count)
        """
        self._current_dm = dm

        if dm:
            dm.reset_stats()

        t_start = time.perf_counter()

        yield  # aquí se ejecuta la operación

        t_end = time.perf_counter()
        elapsed_ms = (t_end - t_start) * 1000

        reads = dm.read_count if dm else 0
        writes = dm.write_count if dm else 0

        self.entries.append({
            "structure": structure,
            "operation": operation,
            "n": n,
            "reads": reads,
            "writes": writes,
            "disk_accesses": reads + writes,
            "time_ms": round(elapsed_ms, 4),
        })

    def save(self, path: str):
        """Guarda todas las entradas en un archivo JSON."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.entries, f, indent=2)
        print(f"[MetricsLogger] Log guardado en: {path}")

    def load(self, path: str):
        """Carga entradas desde un JSON existente (para acumular runs)."""
        if os.path.exists(path):
            with open(path, "r") as f:
                self.entries = json.load(f)

    def summary(self):
        """Imprime resumen en consola."""
        print(f"\n{'Estructura':<15} {'Operación':<15} {'n':>8} {'Reads':>7} {'Writes':>7} {'Total I/O':>10} {'ms':>10}")
        print("-" * 75)
        for e in self.entries:
            print(f"{e['structure']:<15} {e['operation']:<15} {e['n']:>8} "
                  f"{e['reads']:>7} {e['writes']:>7} {e['disk_accesses']:>10} {e['time_ms']:>10.2f}")
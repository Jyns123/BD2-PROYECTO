# index/sequential.py

import os
from storage.disk_manager import DiskManager
from index.heap import HeapFile


class SequentialFile:
    def __init__(self, main_path: str, overflow_path: str, record_size: int, key_extractor):
        if not callable(key_extractor):
            raise ValueError("key_extractor debe ser una función")
        if not isinstance(record_size, int) or record_size <= 0:
            raise ValueError("record_size inválido")

        self.record_size = record_size
        self.key = key_extractor
        self.main_path = main_path
        self.overflow_path = overflow_path

        self.main_dm = DiskManager(main_path)
        self.overflow_dm = DiskManager(overflow_path)

        self.main = HeapFile(self.main_dm, record_size)
        self.overflow = HeapFile(self.overflow_dm, record_size)

        # FIX: exponer un dm unificado para que el benchmark/engine
        # pueda hacer reset_stats() y get_stats() en un solo punto
        self.dm = self._UnifiedDM(self.main_dm, self.overflow_dm)

        # FIX: umbral de rebuild configurable
        self.overflow_limit = 200
        self._overflow_count = 0

    # -------------------------
    # DM UNIFICADO (inner class)
    # -------------------------
    class _UnifiedDM:
        """Suma reads/writes de main y overflow para métricas consistentes."""
        def __init__(self, main_dm, overflow_dm):
            self._main = main_dm
            self._overflow = overflow_dm

        def reset_stats(self):
            self._main.reset_stats()
            self._overflow.reset_stats()

        def get_stats(self):
            return {
                "reads":  self._main.read_count  + self._overflow.read_count,
                "writes": self._main.write_count + self._overflow.write_count,
            }

        @property
        def read_count(self):
            return self._main.read_count + self._overflow.read_count

        @property
        def write_count(self):
            return self._main.write_count + self._overflow.write_count

    # -----------------------------
    # INSERT → overflow
    # -----------------------------
    def insert(self, record: bytes):
        if not isinstance(record, (bytes, bytearray)):
            raise ValueError("record debe ser bytes")
        if len(record) != self.record_size:
            raise ValueError("tamaño de record incorrecto")

        self.overflow.insert(record)
        self._overflow_count += 1

        # FIX: resetear ANTES de incrementar para que el rebuild
        # no cuente el registro que acaba de entrar
        if self._overflow_count >= self.overflow_limit:
            self.rebuild()
            # _overflow_count ya se resetea a 0 dentro de rebuild()
            # pero el registro actual ya fue insertado antes del rebuild,
            # así que el conteo es correcto

    # -----------------------------
    # SEARCH
    # -----------------------------
    def search(self, key_value):
        results = []
        try:
            # FIX: early exit en main si clave ya pasó (está ordenado)
            for r in self.main.scan():
                k = self.key(r)
                if k == key_value:
                    results.append(r)
                elif k > key_value:
                    break  # main está ordenado → no puede haber más

            # overflow siempre full scan (no está ordenado)
            for r in self.overflow.scan():
                if self.key(r) == key_value:
                    results.append(r)

            return results
        except Exception as e:
            raise IOError(f"Error en search: {e}")

    # -----------------------------
    # RANGE SEARCH
    # -----------------------------
    def range_search(self, begin, end):
        results = []
        try:
            # FIX: early exit cuando k > end en main
            for r in self.main.scan():
                k = self.key(r)
                if k > end:
                    break
                if begin <= k:
                    results.append(r)

            for r in self.overflow.scan():
                k = self.key(r)
                if begin <= k <= end:
                    results.append(r)

            results.sort(key=self.key)
            return results
        except Exception as e:
            raise IOError(f"Error en range_search: {e}")

    # -----------------------------
    # REBUILD
    # -----------------------------
    def rebuild(self):
        try:
            all_records = self.main.scan() + self.overflow.scan()
            all_records.sort(key=self.key)

            self.main_dm.close()
            if os.path.exists(self.main_path):
                os.remove(self.main_path)
            self.main_dm = DiskManager(self.main_path)
            self.main = HeapFile(self.main_dm, self.record_size)

            for r in all_records:
                self.main.insert(r)

            self.overflow_dm.close()
            if os.path.exists(self.overflow_path):
                os.remove(self.overflow_path)
            self.overflow_dm = DiskManager(self.overflow_path)
            self.overflow = HeapFile(self.overflow_dm, self.record_size)

            self.dm = self._UnifiedDM(self.main_dm, self.overflow_dm)
            self._overflow_count = 0   # una sola vez, al final

        except Exception as e:
            raise IOError(f"Error en rebuild: {e}")

    # -----------------------------
    # CLOSE
    # -----------------------------
    def close(self):
        try:
            self.main_dm.close()
            self.overflow_dm.close()
        except Exception as e:
            raise IOError(f"Error cerrando SequentialFile: {e}")
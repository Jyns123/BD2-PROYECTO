"""
SEQUENTIAL FILE

Implementa archivo ordenado con overflow.

Qué hace:
- Mantiene un archivo principal ordenado por clave.
- Inserciones nuevas van a overflow (no rompe orden).
- Permite búsqueda en principal + overflow.
- Soporta range search.

Cómo funciona:
- Archivo principal: páginas ordenadas
- Overflow: inserciones recientes
- Cuando overflow crece mucho → rebuild

Funciones principales:
- insert → agrega en overflow
- search → busca en ambos
- range_search → rango ordenado
- rebuild → fusiona principal + overflow

Qué NO hace:
- No mantiene orden en tiempo real
- No optimiza inserciones (usa overflow)

Importante:
Es un punto medio entre Heap (simple) y B+Tree (óptimo).
"""
from storage.disk_manager import DiskManager
from index.heap import HeapFile


class SequentialFile:
    def __init__(self, main_path: str, overflow_path: str, record_size: int, key_extractor):
        """
        main_path: archivo para datos ordenados
        overflow_path: archivo para inserciones
        key_extractor: función que extrae la clave desde un record (bytes)
        """

        if not callable(key_extractor):
            raise ValueError("key_extractor debe ser una función")

        if not isinstance(record_size, int) or record_size <= 0:
            raise ValueError("record_size inválido")

        self.record_size = record_size
        self.key = key_extractor

        # DiskManagers separados
        self.main_dm = DiskManager(main_path)
        self.overflow_dm = DiskManager(overflow_path)

        # Heaps sobre cada archivo
        self.main = HeapFile(self.main_dm, record_size)
        self.overflow = HeapFile(self.overflow_dm, record_size)

    # -----------------------------
    # INSERT → siempre a overflow
    # -----------------------------
    def insert(self, record: bytes):
        if not isinstance(record, (bytes, bytearray)):
            raise ValueError("record debe ser bytes")

        if len(record) != self.record_size:
            raise ValueError("tamaño de record incorrecto")

        return self.overflow.insert(record)

    # -----------------------------
    # SEARCH → main + overflow
    # -----------------------------
    def search(self, key_value):
        results = []

        try:
            # buscar en main
            for r in self.main.scan():
                if self.key(r) == key_value:
                    results.append(r)

            # buscar en overflow
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
            for r in self.main.scan():
                k = self.key(r)
                if begin <= k <= end:
                    results.append(r)

            for r in self.overflow.scan():
                k = self.key(r)
                if begin <= k <= end:
                    results.append(r)

            # mantener orden lógico
            results.sort(key=self.key)

            return results

        except Exception as e:
            raise IOError(f"Error en range_search: {e}")

    # -----------------------------
    # REBUILD
    # -----------------------------
    def rebuild(self):
        """
        Fusiona main + overflow en un nuevo archivo principal ordenado.
        Luego limpia overflow.
        """
        import os

        try:
            # 1. Obtener todos los registros
            all_records = self.main.scan() + self.overflow.scan()

            # 2. Ordenar por clave
            all_records.sort(key=self.key)

            # 3. Recrear archivo principal
            main_path = self.main_dm.file_path

            self.main_dm.close()
            if os.path.exists(main_path):
                os.remove(main_path)

            self.main_dm = DiskManager(main_path)
            self.main = HeapFile(self.main_dm, self.record_size)

            for r in all_records:
                self.main.insert(r)

            # 4. Limpiar overflow
            overflow_path = self.overflow_dm.file_path

            self.overflow_dm.close()
            if os.path.exists(overflow_path):
                os.remove(overflow_path)

            self.overflow_dm = DiskManager(overflow_path)
            self.overflow = HeapFile(self.overflow_dm, self.record_size)

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
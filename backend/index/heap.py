# index/heap.py

from storage.page import Page
from storage.disk_manager import DiskManager


class HeapFile:
    """
    Heap file: insert al final + scan lineal. Sirve como tabla sin índice.

    Acepta:
      - HeapFile(disk_manager, record_size)             -> uso interno (sequential)
      - HeapFile(path, record_size, key_extractor)      -> uso como índice "HEAP"
    """

    def __init__(self, dm_or_path, record_size: int, key_extractor=None):
        if not isinstance(record_size, int) or record_size <= 0:
            raise ValueError("record_size debe ser un entero positivo")

        # Polimorfismo: ruta crea su propio DiskManager y lo cierra al cerrar
        if isinstance(dm_or_path, str):
            self.dm = DiskManager(dm_or_path)
            self._owns_dm = True
        else:
            self.dm = dm_or_path
            self._owns_dm = False

        self.record_size = record_size
        self.key = key_extractor
        self._free_page = None

    # INSERT
    def insert(self, record: bytes):
        if not isinstance(record, (bytes, bytearray)):
            raise ValueError("record debe ser bytes")
        if len(record) != self.record_size:
            raise ValueError("tamaño de record incorrecto")

        try:
            if self._free_page is not None:
                raw = self.dm.read_page(self._free_page)
                page = Page.from_bytes(raw, self.record_size)
                if page.has_space():
                    page.insert_record(record)
                    self.dm.write_page(self._free_page, page.to_bytes())
                    return self._free_page

            total_pages = self.dm._get_total_pages()
            for page_id in range(total_pages - 1, 0, -1):
                raw = self.dm.read_page(page_id)
                page = Page.from_bytes(raw, self.record_size)
                if page.has_space():
                    page.insert_record(record)
                    self.dm.write_page(page_id, page.to_bytes())
                    self._free_page = page_id
                    return page_id

            new_page_id = self.dm.allocate_page()
            page = Page(self.record_size)
            page.insert_record(record)
            self.dm.write_page(new_page_id, page.to_bytes())
            self._free_page = new_page_id
            return new_page_id

        except Exception as e:
            raise IOError(f"Error en insert: {e}")

    # SCAN
    def scan(self) -> list:
        results = []
        try:
            total_pages = self.dm._get_total_pages()
            for page_id in range(1, total_pages):
                raw = self.dm.read_page(page_id)
                page = Page.from_bytes(raw, self.record_size)
                results.extend(page.read_records())
            return results
        except Exception as e:
            raise IOError(f"Error en scan: {e}")

    # SEARCH
    # - Si recibe callable: predicado libre (uso interno).
    # - Si recibe valor: busca por key_extractor (índice HEAP).
    def search(self, predicate_or_key) -> list:
        if callable(predicate_or_key):
            predicate = predicate_or_key
        else:
            if self.key is None:
                raise ValueError("HeapFile sin key_extractor: usar predicado")
            target = predicate_or_key
            predicate = lambda r: self.key(r) == target

        results = []
        try:
            total_pages = self.dm._get_total_pages()
            for page_id in range(1, total_pages):
                raw = self.dm.read_page(page_id)
                page = Page.from_bytes(raw, self.record_size)
                for record in page.read_records():
                    if predicate(record):
                        results.append(record)
            return results
        except Exception as e:
            raise IOError(f"Error en search: {e}")

    # RANGE SEARCH lineal (sin orden, recorre todo)
    def range_search(self, begin, end) -> list:
        if self.key is None:
            raise ValueError("HeapFile sin key_extractor: range_search no disponible")
        results = []
        for r in self.scan():
            k = self.key(r)
            if begin <= k <= end:
                results.append(r)
        return results

    # REMOVE: reescribe páginas que tengan registros con esa key
    def remove(self, key_value):
        if self.key is None:
            raise ValueError("HeapFile sin key_extractor: remove no disponible")
        removed = 0
        try:
            total_pages = self.dm._get_total_pages()
            for page_id in range(1, total_pages):
                raw = self.dm.read_page(page_id)
                page = Page.from_bytes(raw, self.record_size)
                records = page.read_records()
                kept = [r for r in records if self.key(r) != key_value]
                diff = len(records) - len(kept)
                if diff > 0:
                    new_page = Page(self.record_size)
                    for r in kept:
                        new_page.insert_record(r)
                    self.dm.write_page(page_id, new_page.to_bytes())
                    removed += diff
            # invalidar hint de página libre tras compactar
            self._free_page = None
            return removed
        except Exception as e:
            raise IOError(f"Error en remove: {e}")

    # CLOSE: solo cierra el dm si lo creó este heap
    def close(self):
        if self._owns_dm:
            try:
                self.dm.close()
            except Exception as e:
                raise IOError(f"Error cerrando HeapFile: {e}")

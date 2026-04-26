# index/heap.py

from storage.page import Page
from storage.disk_manager import DiskManager


class HeapFile:
    def __init__(self, disk_manager: DiskManager, record_size: int):
        if not isinstance(record_size, int) or record_size <= 0:
            raise ValueError("record_size debe ser un entero positivo")

        self.dm = disk_manager
        self.record_size = record_size
        self._free_page = None  # FIX: cache de la última página con espacio

    # -----------------------------
    # INSERT
    # -----------------------------
    def insert(self, record: bytes):
        if not isinstance(record, (bytes, bytearray)):
            raise ValueError("record debe ser bytes")
        if len(record) != self.record_size:
            raise ValueError("tamaño de record incorrecto")

        try:
            # FIX: intentar última página conocida con espacio primero
            if self._free_page is not None:
                raw = self.dm.read_page(self._free_page)
                page = Page.from_bytes(raw, self.record_size)
                if page.has_space():
                    page.insert_record(record)
                    self.dm.write_page(self._free_page, page.to_bytes())
                    return self._free_page

            # FIX: si no, buscar desde el final hacia atrás (más probable encontrar espacio)
            total_pages = self.dm._get_total_pages()
            for page_id in range(total_pages - 1, 0, -1):
                raw = self.dm.read_page(page_id)
                page = Page.from_bytes(raw, self.record_size)
                if page.has_space():
                    page.insert_record(record)
                    self.dm.write_page(page_id, page.to_bytes())
                    self._free_page = page_id
                    return page_id

            # no hay espacio → nueva página
            new_page_id = self.dm.allocate_page()
            page = Page(self.record_size)
            page.insert_record(record)
            self.dm.write_page(new_page_id, page.to_bytes())
            self._free_page = new_page_id
            return new_page_id

        except Exception as e:
            raise IOError(f"Error en insert: {e}")

    # -----------------------------
    # SCAN
    # -----------------------------
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

    # -----------------------------
    # SEARCH
    # -----------------------------
    def search(self, predicate) -> list:
        if not callable(predicate):
            raise ValueError("predicate debe ser una función")
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
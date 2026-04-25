"""
HEAP FILE

Implementa almacenamiento sin orden (heap file).

Qué hace:
- Guarda registros en múltiples páginas.
- Inserta en la primera página con espacio disponible.
- Si no hay espacio, crea una nueva página.
- Permite escanear todos los registros.

Cómo funciona:
- Usa DiskManager para leer/escribir páginas.
- Usa Page para manejar registros dentro de cada página.
- Recorre páginas secuencialmente (sin índice).

Funciones principales:
- insert → agrega un registro
- scan → devuelve todos los registros
- search → filtra registros con una condición

Qué NO hace:
- No mantiene orden
- No tiene índice
- No optimiza búsquedas (full scan)

Importante:
Es la implementación base sobre la cual se comparan
otras estructuras más eficientes (Hash, B+Tree).
"""
from storage.page import Page
from storage.disk_manager import DiskManager


class HeapFile:
    def __init__(self, disk_manager: DiskManager, record_size: int):
        if not isinstance(record_size, int) or record_size <= 0:
            raise ValueError("record_size debe ser un entero positivo")

        self.dm = disk_manager
        self.record_size = record_size

    # -----------------------------
    # INSERT
    # -----------------------------
    def insert(self, record: bytes):
        if not isinstance(record, (bytes, bytearray)):
            raise ValueError("record debe ser bytes")

        if len(record) != self.record_size:
            raise ValueError("tamaño de record incorrecto")

        try:
            total_pages = self.dm._get_total_pages()

            # recorrer páginas existentes (skip metadata = 0)
            for page_id in range(1, total_pages):
                raw = self.dm.read_page(page_id)
                page = Page.from_bytes(raw, self.record_size)

                if page.has_space():
                    page.insert_record(record)
                    self.dm.write_page(page_id, page.to_bytes())
                    return page_id

            # no hay espacio → crear nueva página
            new_page_id = self.dm.allocate_page()
            page = Page(self.record_size)

            page.insert_record(record)
            self.dm.write_page(new_page_id, page.to_bytes())

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
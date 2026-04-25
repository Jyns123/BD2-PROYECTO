import os

PAGE_SIZE = 4096
META_PAGE_ID = 0

"""Hola chicos, ojo que la pagina 0 será solo metadata, ya el resto contendrá datos. ademas
quise tener los try y except en caso de errores para debug

DISK MANAGER

Este módulo maneja TODO el acceso a disco del sistema.
Es la única capa que puede leer o escribir en el archivo.

Qué hace:
- Trabaja con páginas de tamaño fijo (4096 bytes).
- Permite leer y escribir páginas usando page_id.
- Asigna nuevas páginas (allocate_page).
- Lleva conteo de accesos a disco (reads/writes).

Cómo funciona:
- Cada página está en: offset = page_id * PAGE_SIZE
- La página 0 se usa como metadata (guarda total_pages).
- No se carga el archivo completo en memoria.

Qué garantiza:
- Persistencia correcta en disco.
- Acceso controlado y medible.
- Independencia del sistema operativo.

Qué NO hace:
- No maneja registros (eso lo hace Page).
- No reutiliza páginas eliminadas.
- No tiene buffer ni caché.

Uso típico:
    read_page → modificar → write_page

IMPORTANTE:
Todo el sistema (Heap, Hash, B+Tree, etc.) debe usar este módulo
para acceder a disco.
"""
import os

PAGE_SIZE = 4096
META_PAGE_ID = 0


class DiskManager:
    """
    Maneja acceso a disco a nivel de páginas.
    Página 0 = metadata:
        bytes 0-4  -> total_pages
        bytes 4-8  -> root_page_id (usado por B+ Tree)
    """

    def __init__(self, file_path: str):
        self.file_path = file_path
        new_file = not os.path.exists(file_path)

        self.file = open(file_path, "r+b" if not new_file else "w+b")

        self.read_count = 0
        self.write_count = 0

        if new_file:
            self._init_meta_page()

    # -----------------------------
    # META
    # -----------------------------
    def _init_meta_page(self):
        data = bytearray(PAGE_SIZE)
        data[0:4] = (1).to_bytes(4, 'big')  # total_pages = 1
        data[4:8] = (0).to_bytes(4, 'big')  # root = 0
        self.file.seek(0)
        self.file.write(data)
        self.file.flush()

    def _get_total_pages(self):
        self.file.seek(0)
        return int.from_bytes(self.file.read(4), 'big')

    def _set_total_pages(self, value):
        self.file.seek(0)
        self.file.write(value.to_bytes(4, 'big'))
        self.file.flush()

    # -----------------------------
    # READ
    # -----------------------------
    def read_page(self, page_id: int) -> bytes:
        if page_id < 0:
            raise ValueError("page_id inválido")

        offset = page_id * PAGE_SIZE
        self.file.seek(offset)

        data = self.file.read(PAGE_SIZE)

        if len(data) < PAGE_SIZE:
            data += b'\x00' * (PAGE_SIZE - len(data))

        self.read_count += 1
        return data

    # -----------------------------
    # WRITE
    # -----------------------------
    def write_page(self, page_id: int, data: bytes):
        if len(data) != PAGE_SIZE:
            raise ValueError("Página debe ser PAGE_SIZE")

        offset = page_id * PAGE_SIZE
        self.file.seek(offset)

        self.file.write(data)
        self.file.flush()

        self.write_count += 1

    # -----------------------------
    # ALLOCATE
    # -----------------------------
    def allocate_page(self):
        total = self._get_total_pages()
        new_page_id = total

        self.write_page(new_page_id, b'\x00' * PAGE_SIZE)
        self._set_total_pages(total + 1)

        return new_page_id

    # -----------------------------
    def get_stats(self):
        return {"reads": self.read_count, "writes": self.write_count}

    # -----------------------------
    def close(self):
        self.file.close()
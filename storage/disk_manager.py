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

    def get_root(self):
        self.file.seek(4)
        return int.from_bytes(self.file.read(4), 'big')

    def set_root(self, root_id):
        self.file.seek(4)
        self.file.write(root_id.to_bytes(4, 'big'))
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
    # STATS
    # -----------------------------
    def get_stats(self):
        return {"reads": self.read_count, "writes": self.write_count}

    def reset_stats(self):
        self.read_count = 0
        self.write_count = 0

    # -----------------------------
    def close(self):
        self.file.close()
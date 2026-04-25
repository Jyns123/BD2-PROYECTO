"""
EXTENDIBLE HASHING

Qué hace:
- Acceso casi O(1) por clave usando hashing.
- Directorio en memoria → apunta a buckets en disco.
- Buckets = páginas (Page).

Cómo funciona:
- hash(key) → bits → índice en directorio
- Inserta en bucket correspondiente
- Si bucket se llena → split
- Si no alcanza → duplicar directorio (global depth)

Qué NO hace:
- No soporta range queries
- Directorio no persistente (en esta versión)

Importante:
Optimiza búsquedas puntuales frente a Heap/Sequential.
"""
from storage.disk_manager import DiskManager
from storage.page import Page


class ExtendibleHash:
    def __init__(self, file_path: str, record_size: int, key_extractor):
        if not callable(key_extractor):
            raise ValueError("key_extractor debe ser función")

        if record_size <= 0:
            raise ValueError("record_size inválido")

        self.dm = DiskManager(file_path)
        self.record_size = record_size
        self.key = key_extractor

        # directorio en memoria
        self.global_depth = 1
        self.directory = []

        # crear 2 buckets iniciales
        b0 = self.dm.allocate_page()
        b1 = self.dm.allocate_page()

        self._init_bucket(b0)
        self._init_bucket(b1)

        self.directory = [b0, b1]

    # -----------------------------
    # BUCKET INIT
    # -----------------------------
    def _init_bucket(self, page_id):
        page = Page(self.record_size)
        self.dm.write_page(page_id, page.to_bytes())

    # -----------------------------
    # HASH
    # -----------------------------
    def _hash(self, key):
        return hash(key)

    def _get_index(self, key):
        mask = (1 << self.global_depth) - 1
        return self._hash(key) & mask

    # -----------------------------
    # INSERT
    # -----------------------------
    def insert(self, record: bytes):
        if len(record) != self.record_size:
            raise ValueError("record size incorrecto")

        key = self.key(record)

        while True:
            idx = self._get_index(key)
            bucket_id = self.directory[idx]

            raw = self.dm.read_page(bucket_id)
            page = Page.from_bytes(raw, self.record_size)

            if page.has_space():
                page.insert_record(record)
                self.dm.write_page(bucket_id, page.to_bytes())
                return

            # bucket lleno → split
            self._split(bucket_id, idx)

    # -----------------------------
    # SPLIT
    # -----------------------------
    def _split(self, bucket_id, idx):
        # duplicar directorio si hace falta
        if self._need_double(idx):
            self._double_directory()

        # nuevo bucket
        new_bucket = self.dm.allocate_page()
        self._init_bucket(new_bucket)

        # redistribuir
        self._redistribute(bucket_id, new_bucket)

        # actualizar punteros
        self._update_directory(bucket_id, new_bucket)

    def _need_double(self, idx):
        # condición simple: todos los índices apuntan al mismo bucket
        return self.directory.count(self.directory[idx]) == len(self.directory)

    def _double_directory(self):
        self.directory = self.directory * 2
        self.global_depth += 1

    def _redistribute(self, old_bucket, new_bucket):
        raw = self.dm.read_page(old_bucket)
        page = Page.from_bytes(raw, self.record_size)

        records = page.read_records()

        # limpiar bucket viejo
        page = Page(self.record_size)

        for r in records:
            key = self.key(r)
            idx = self._get_index(key)

            if idx % 2 == 0:
                page.insert_record(r)
            else:
                raw_new = self.dm.read_page(new_bucket)
                new_page = Page.from_bytes(raw_new, self.record_size)
                new_page.insert_record(r)
                self.dm.write_page(new_bucket, new_page.to_bytes())

        self.dm.write_page(old_bucket, page.to_bytes())

    def _update_directory(self, old_bucket, new_bucket):
        for i in range(len(self.directory)):
            if self.directory[i] == old_bucket:
                if i % 2 == 1:
                    self.directory[i] = new_bucket

    # -----------------------------
    # SEARCH
    # -----------------------------
    def search(self, key_value):
        idx = self._get_index(key_value)
        bucket_id = self.directory[idx]

        raw = self.dm.read_page(bucket_id)
        page = Page.from_bytes(raw, self.record_size)

        results = []

        for r in page.read_records():
            if self.key(r) == key_value:
                results.append(r)

        return results

    # -----------------------------
    # CLOSE
    # -----------------------------
    def close(self):
        self.dm.close()
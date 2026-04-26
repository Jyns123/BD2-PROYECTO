# index/hash.py

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

        self.global_depth = 1
        self.directory = []
        self.local_depths = {}   # NUEVO: profundidad local por bucket

        # FIX: solo crear buckets si el archivo es nuevo
        if self.dm._get_total_pages() == 1:
            b0 = self.dm.allocate_page()
            b1 = self.dm.allocate_page()
            self._init_bucket(b0)
            self._init_bucket(b1)
            self.directory = [b0, b1]
            self.local_depths[b0] = 1   # NUEVO
            self.local_depths[b1] = 1   # NUEVO
        else:
            self.directory = [1, 2]
            self.local_depths = {1: 1, 2: 1}

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
        return hash(key) & 0x7FFFFFFF  # FIX: evitar negativos en Python

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

        for _ in range(64):   # FIX: aumentar límite, splits profundos necesitan más iteraciones
            idx = self._get_index(key)
            if idx >= len(self.directory):
                idx = idx % len(self.directory)
            bucket_id = self.directory[idx]

            raw = self.dm.read_page(bucket_id)
            page = Page.from_bytes(raw, self.record_size)

            if page.has_space():
                page.insert_record(record)
                self.dm.write_page(bucket_id, page.to_bytes())
                return

            self._split(bucket_id, idx)

        raise RuntimeError("No se pudo insertar tras multiples splits")

    # -----------------------------
    # SPLIT
    # -----------------------------
    def _split(self, bucket_id, idx):
        local_d = self.local_depths.get(bucket_id, 1)

        # FIX: si profundidad local == global → hay que duplicar directorio
        if local_d >= self.global_depth:
            self._double_directory()

        new_bucket = self.dm.allocate_page()
        self._init_bucket(new_bucket)

        new_local_d = local_d + 1
        self.local_depths[bucket_id] = new_local_d
        self.local_depths[new_bucket] = new_local_d

        self._redistribute(bucket_id, new_bucket, new_local_d)
        self._update_directory(bucket_id, new_bucket, new_local_d)

    def _double_directory(self):
        self.directory = self.directory * 2
        self.global_depth += 1

    def _redistribute(self, old_bucket, new_bucket, local_depth):
        raw = self.dm.read_page(old_bucket)
        page = Page.from_bytes(raw, self.record_size)
        records = page.read_records()

        old_page = Page(self.record_size)
        new_page = Page(self.record_size)

        mask = (1 << local_depth) - 1   # FIX: usar máscara basada en profundidad local
        split_bit = 1 << (local_depth - 1)

        for r in records:
            key = self.key(r)
            if self._hash(key) & split_bit:
                new_page.insert_record(r)
            else:
                old_page.insert_record(r)

        self.dm.write_page(old_bucket, old_page.to_bytes())
        self.dm.write_page(new_bucket, new_page.to_bytes())

    def _update_directory(self, old_bucket, new_bucket, local_depth):
        # FIX: actualizar entradas del directorio que apuntan al old bucket
        # y cuyo índice tiene el split_bit activado
        split_bit = 1 << (local_depth - 1)
        for i in range(len(self.directory)):
            if self.directory[i] == old_bucket and (i & split_bit):
                self.directory[i] = new_bucket

    # -----------------------------
    # SEARCH
    # -----------------------------
    def search(self, key_value):
        idx = self._get_index(key_value)

        # FIX: verificar que idx no esté fuera de rango
        if idx >= len(self.directory):
            return []

        bucket_id = self.directory[idx]
        raw = self.dm.read_page(bucket_id)
        page = Page.from_bytes(raw, self.record_size)

        return [r for r in page.read_records() if self.key(r) == key_value]

    # -----------------------------
    # CLOSE
    # -----------------------------
    def close(self):
        self.dm.close()
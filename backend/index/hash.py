# index/hash.py

from storage.disk_manager import DiskManager, PAGE_SIZE
from storage.page import Page


class ExtendibleHash:
    def __init__(self, file_path: str, record_size: int, key_extractor):
        if not callable(key_extractor):
            raise ValueError("key_extractor debe ser función")
        if record_size <= 0:
            raise ValueError("record_size inválido")

        self.dm = DiskManager(file_path)
        self.meta_path = file_path + ".dir"
        self.meta_dm = DiskManager(self.meta_path)
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
            self._save_metadata()
        else:
            if not self._load_metadata():
                self.directory = [1, 2]
                self.local_depths = {1: 1, 2: 1}
                self._save_metadata()

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
        # ✅ hash determinista para enteros y strings
        if isinstance(key, int):
            return key & 0x7FFFFFFF
        return int.from_bytes(str(key).encode(), 'big') & 0x7FFFFFFF

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
        self._save_metadata()

    # -----------------------------
    # METADATA (DIRECTORY)
    # -----------------------------
    def _save_metadata(self):
        dir_len = len(self.directory)
        payload = bytearray()
        payload += int(self.global_depth).to_bytes(4, "big")
        payload += int(dir_len).to_bytes(4, "big")

        for bucket in self.directory:
            payload += int(bucket).to_bytes(4, "big")

        for bucket in self.directory:
            local_d = self.local_depths.get(bucket, 1)
            payload += int(local_d).to_bytes(4, "big")

        header = b"EH02" + int(len(payload)).to_bytes(4, "big")
        data = header + payload

        needed_pages = (len(data) + PAGE_SIZE - 1) // PAGE_SIZE
        total_pages = self.meta_dm._get_total_pages()
        while total_pages < needed_pages:
            self.meta_dm.allocate_page()
            total_pages += 1

        for page_id in range(needed_pages):
            start = page_id * PAGE_SIZE
            end = start + PAGE_SIZE
            chunk = data[start:end]
            if len(chunk) < PAGE_SIZE:
                chunk = chunk + b"\x00" * (PAGE_SIZE - len(chunk))
            self.meta_dm.write_page(page_id, chunk)

    def _load_metadata(self):
        first = self.meta_dm.read_page(0)
        if first[0:4] != b"EH02":
            return False

        total_len = int.from_bytes(first[4:8], "big")
        if total_len <= 0:
            return False

        needed_pages = (8 + total_len + PAGE_SIZE - 1) // PAGE_SIZE
        data = bytearray()
        for page_id in range(needed_pages):
            data.extend(self.meta_dm.read_page(page_id))

        payload = bytes(data[8:8 + total_len])
        if len(payload) < 8:
            return False

        self.global_depth = int.from_bytes(payload[0:4], "big")
        dir_len = int.from_bytes(payload[4:8], "big")
        if dir_len <= 0:
            return False

        offset = 8
        directory = []
        for _ in range(dir_len):
            directory.append(int.from_bytes(payload[offset:offset + 4], "big"))
            offset += 4

        local_depths = {}
        for i in range(dir_len):
            local_d = int.from_bytes(payload[offset:offset + 4], "big")
            offset += 4
            bucket = directory[i]
            prev = local_depths.get(bucket, 0)
            if local_d > prev:
                local_depths[bucket] = local_d

        self.directory = directory
        self.local_depths = local_depths
        return True

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
    # REMOVE
    # -----------------------------
    def remove(self, key_value):
        idx = self._get_index(key_value)

        if idx >= len(self.directory):
            return 0

        bucket_id = self.directory[idx]
        raw = self.dm.read_page(bucket_id)
        page = Page.from_bytes(raw, self.record_size)

        kept = []
        removed = 0
        for r in page.read_records():
            if self.key(r) == key_value:
                removed += 1
            else:
                kept.append(r)

        if removed > 0:
            new_page = Page(self.record_size)
            for r in kept:
                new_page.insert_record(r)
            self.dm.write_page(bucket_id, new_page.to_bytes())

        return removed

    # -----------------------------
    # CLOSE
    # -----------------------------
    def close(self):
        self.dm.close()
        self.meta_dm.close()
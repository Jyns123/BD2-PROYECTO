import os
import heapq
import shutil
from storage.disk_manager import DiskManager, PAGE_SIZE
from storage.page import Page
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

        self.dm = self._UnifiedDM(self.main_dm, self.overflow_dm)

        self.overflow_limit = 200
        self._overflow_count = self._count_overflow_records()
        self._main_record_count = self._count_main_records()

    def _count_overflow_records(self) -> int:
        """Cuenta registros existentes en el archivo overflow (para reaberturas)."""
        count = 0
        total_pages = self.overflow_dm._get_total_pages()
        for page_id in range(1, total_pages):
            raw = self.overflow_dm.read_page(page_id)
            count += Page.from_bytes(raw, self.record_size).get_record_count()
        return count

    def _count_main_records(self) -> int:
        """Cuenta registros en el main file (para reaberturas y tras rebuild/remove)."""
        count = 0
        total_pages = self.main_dm._get_total_pages()
        for page_id in range(1, total_pages):
            raw = self.main_dm.read_page(page_id)
            count += Page.from_bytes(raw, self.record_size).get_record_count()
        return count

    def _get_record_at(self, idx: int):
        """
        Accede directamente al registro en posición global idx del main file.
        El main file siempre se escribe con HeapFile secuencial, por lo que
        todas las páginas excepto la última están llenas: el mapeo es O(1).
        """
        records_per_page = (PAGE_SIZE - 4) // self.record_size  # 4 = HEADER_SIZE
        page_id = (idx // records_per_page) + 1
        slot    =  idx %  records_per_page
        raw  = self.main_dm.read_page(page_id)
        page = Page.from_bytes(raw, self.record_size)
        if slot >= page.get_record_count():
            return None
        return page.read_record(slot)

    def _find_left(self, key_value) -> int:
        """
        Binary search: retorna el índice del primer registro con key >= key_value.
        Si no existe, retorna _main_record_count.
        """
        lo, hi   = 0, self._main_record_count - 1
        result   = self._main_record_count
        while lo <= hi:
            mid = (lo + hi) // 2
            rec = self._get_record_at(mid)
            if rec is None:
                hi = mid - 1
                continue
            if self.key(rec) < key_value:
                lo = mid + 1
            else:
                result = mid
                hi     = mid - 1
        return result

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

    def insert(self, record: bytes):
        if not isinstance(record, (bytes, bytearray)):
            raise ValueError("record debe ser bytes")
        if len(record) != self.record_size:
            raise ValueError("tamaño de record incorrecto")

        self.overflow.insert(record)
        self._overflow_count += 1

        if self._overflow_count >= self.overflow_limit:
            self.rebuild()
   

    # SCAN (para SELECT *)
    def scan(self):
        # main ya está ordenado; overflow puede no estarlo: sort final por key
        results = list(self.main.scan()) + list(self.overflow.scan())
        results.sort(key=self.key)
        return results

    def search(self, key_value):
        results = []
        try:
            # Binary search en main (O(log n))
            if self._main_record_count > 0:
                idx = self._find_left(key_value)
                while idx < self._main_record_count:
                    rec = self._get_record_at(idx)
                    if rec is None:
                        break
                    k = self.key(rec)
                    if k == key_value:
                        results.append(rec)
                    elif k > key_value:
                        break
                    idx += 1

            # Scan lineal en overflow (siempre desordenado)
            for r in self.overflow.scan():
                if self.key(r) == key_value:
                    results.append(r)

            return results
        except Exception as e:
            raise IOError(f"Error en search: {e}")

    def range_search(self, begin, end):
        results = []
        try:
            # Binary search para encontrar el inicio en main (O(log n + k))
            if self._main_record_count > 0:
                idx = self._find_left(begin)
                while idx < self._main_record_count:
                    rec = self._get_record_at(idx)
                    if rec is None:
                        break
                    k = self.key(rec)
                    if k > end:
                        break
                    if k >= begin:
                        results.append(rec)
                    idx += 1

            # Scan lineal en overflow
            for r in self.overflow.scan():
                k = self.key(r)
                if begin <= k <= end:
                    results.append(r)

            results.sort(key=self.key)
            return results
        except Exception as e:
            raise IOError(f"Error en range_search: {e}")

    # REBUILD
    def rebuild(self):
        temp_dir = os.path.join(os.path.dirname(self.main_path), "seq_tmp")
        main_tmp = self.main_path + ".tmp"

        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir, exist_ok=True)

            if os.path.exists(main_tmp):
                os.remove(main_tmp)

            chunk_size = max(200, (PAGE_SIZE * 8) // self.record_size)
            chunk_files = self._write_sorted_overflow_chunks(temp_dir, chunk_size)

            tmp_main_dm = DiskManager(main_tmp)
            tmp_main = HeapFile(tmp_main_dm, self.record_size)

            main_iter = self._iter_records(self.main_dm)
            overflow_iter = self._merge_chunk_records(chunk_files)

            main_rec = next(main_iter, None)
            overflow_rec = next(overflow_iter, None)

            while main_rec is not None and overflow_rec is not None:
                if self.key(main_rec) <= self.key(overflow_rec):
                    tmp_main.insert(main_rec)
                    main_rec = next(main_iter, None)
                else:
                    tmp_main.insert(overflow_rec)
                    overflow_rec = next(overflow_iter, None)

            while main_rec is not None:
                tmp_main.insert(main_rec)
                main_rec = next(main_iter, None)

            while overflow_rec is not None:
                tmp_main.insert(overflow_rec)
                overflow_rec = next(overflow_iter, None)

            tmp_main_dm.close()

            self.main_dm.close()
            self.overflow_dm.close()

            if os.path.exists(self.main_path):
                os.remove(self.main_path)
            os.replace(main_tmp, self.main_path)

            if os.path.exists(self.overflow_path):
                os.remove(self.overflow_path)

            self.main_dm = DiskManager(self.main_path)
            self.overflow_dm = DiskManager(self.overflow_path)
            self.main = HeapFile(self.main_dm, self.record_size)
            self.overflow = HeapFile(self.overflow_dm, self.record_size)
            self.dm = self._UnifiedDM(self.main_dm, self.overflow_dm)
            self._overflow_count = 0
            self._main_record_count = self._count_main_records()

        except Exception as e:
            if os.path.exists(main_tmp):
                os.remove(main_tmp)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            raise IOError(f"Error en rebuild: {e}")
        finally:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)

    def _iter_records(self, dm):
        total_pages = dm._get_total_pages()
        for page_id in range(1, total_pages):
            raw = dm.read_page(page_id)
            page = Page.from_bytes(raw, self.record_size)
            for record in page.read_records():
                yield record

    def _write_sorted_overflow_chunks(self, temp_dir, chunk_size):
        chunk_files = []
        buffer = []
        idx = 0

        for record in self._iter_records(self.overflow_dm):
            buffer.append(record)
            if len(buffer) >= chunk_size:
                buffer.sort(key=self.key)
                path = os.path.join(temp_dir, f"chunk_{idx}.bin")
                self._write_chunk(path, buffer)
                chunk_files.append(path)
                buffer = []
                idx += 1

        if buffer:
            buffer.sort(key=self.key)
            path = os.path.join(temp_dir, f"chunk_{idx}.bin")
            self._write_chunk(path, buffer)
            chunk_files.append(path)

        return chunk_files

    def _write_chunk(self, path, records):
        with open(path, "wb") as f:
            for record in records:
                f.write(record)

    def _merge_chunk_records(self, chunk_files):
        if not chunk_files:
            return iter(())

        files = [open(p, "rb") for p in chunk_files]
        heap = []

        for i, fh in enumerate(files):
            rec = fh.read(self.record_size)
            if rec:
                heapq.heappush(heap, (self.key(rec), i, rec))

        def generator():
            try:
                while heap:
                    _, i, rec = heapq.heappop(heap)
                    yield rec
                    nxt = files[i].read(self.record_size)
                    if nxt:
                        heapq.heappush(heap, (self.key(nxt), i, nxt))
            finally:
                for fh in files:
                    fh.close()

        return generator()

    # REMOVE
    def remove(self, key_value):
        removed_main = 0
        removed_overflow = 0

        main_tmp = self.main_path + ".tmp"
        overflow_tmp = self.overflow_path + ".tmp"

        try:
            if os.path.exists(main_tmp):
                os.remove(main_tmp)
            if os.path.exists(overflow_tmp):
                os.remove(overflow_tmp)

            # Reescribir main sin cargar todo a memoria
            tmp_main_dm = DiskManager(main_tmp)
            tmp_main = HeapFile(tmp_main_dm, self.record_size)

            total_pages = self.main_dm._get_total_pages()
            for page_id in range(1, total_pages):
                raw = self.main_dm.read_page(page_id)
                page = Page.from_bytes(raw, self.record_size)
                for r in page.read_records():
                    if self.key(r) == key_value:
                        removed_main += 1
                    else:
                        tmp_main.insert(r)

            tmp_main_dm.close()

            # Reescribir overflow sin cargar todo a memoria
            tmp_overflow_dm = DiskManager(overflow_tmp)
            tmp_overflow = HeapFile(tmp_overflow_dm, self.record_size)

            total_pages = self.overflow_dm._get_total_pages()
            for page_id in range(1, total_pages):
                raw = self.overflow_dm.read_page(page_id)
                page = Page.from_bytes(raw, self.record_size)
                for r in page.read_records():
                    if self.key(r) == key_value:
                        removed_overflow += 1
                    else:
                        tmp_overflow.insert(r)

            tmp_overflow_dm.close()

            # Reemplazar archivos
            self.main_dm.close()
            self.overflow_dm.close()

            if os.path.exists(self.main_path):
                os.remove(self.main_path)
            if os.path.exists(self.overflow_path):
                os.remove(self.overflow_path)

            os.replace(main_tmp, self.main_path)
            os.replace(overflow_tmp, self.overflow_path)

            # Reabrir disk managers
            self.main_dm = DiskManager(self.main_path)
            self.overflow_dm = DiskManager(self.overflow_path)
            self.main = HeapFile(self.main_dm, self.record_size)
            self.overflow = HeapFile(self.overflow_dm, self.record_size)
            self.dm = self._UnifiedDM(self.main_dm, self.overflow_dm)

            # Recalcular overflow_count
            overflow_count = 0
            total_pages = self.overflow_dm._get_total_pages()
            for page_id in range(1, total_pages):
                raw = self.overflow_dm.read_page(page_id)
                page = Page.from_bytes(raw, self.record_size)
                overflow_count += page.get_record_count()
            self._overflow_count = overflow_count
            self._main_record_count = self._count_main_records()

            return removed_main + removed_overflow

        except Exception as e:
            # Limpieza de temporales si quedaron
            for tmp_path in (main_tmp, overflow_tmp):
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            raise IOError(f"Error en remove: {e}")

    # CLOSE
    def close(self):
        try:
            self.main_dm.close()
            self.overflow_dm.close()
        except Exception as e:
            raise IOError(f"Error cerrando SequentialFile: {e}")
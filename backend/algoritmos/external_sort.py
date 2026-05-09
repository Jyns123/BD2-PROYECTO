import os
import time
import heapq
import tempfile
from typing import Iterable

from storage.disk_manager import DiskManager
from storage.page import Page


def _records_per_page(record_size: int) -> int:
    return (4096 - 4) // record_size


def _flush_records_as_pages(dm: DiskManager, records, record_size: int):
    rpp = _records_per_page(record_size)
    for i in range(0, len(records), rpp):
        chunk = records[i:i + rpp]
        page = Page(record_size)
        for r in chunk:
            page.insert_record(r)
        page_id = dm.allocate_page()
        dm.write_page(page_id, page.to_bytes())


def _generate_runs(records: Iterable[bytes], record_size: int, key_fn,
                   buffer_pages: int, tmp_dir: str) -> list[str]:
    rpp = _records_per_page(record_size)
    chunk_size = buffer_pages * rpp

    run_paths: list[str] = []
    run_id = 0
    batch: list[bytes] = []

    def flush_run():
        nonlocal run_id
        if not batch:
            return
        batch.sort(key=key_fn)
        run_path = os.path.join(tmp_dir, f"run_{run_id}.bin")
        run_dm = DiskManager(run_path)
        try:
            _flush_records_as_pages(run_dm, batch, record_size)
        finally:
            run_dm.close()
        run_paths.append(run_path)
        run_id += 1
        batch.clear()

    for rec in records:
        batch.append(rec)
        if len(batch) >= chunk_size:
            flush_run()
    flush_run()

    return run_paths


class _RunReader:
    def __init__(self, path: str, record_size: int):
        self.dm = DiskManager(path)
        self.record_size = record_size
        self.total_pages = self.dm._get_total_pages()
        self.page_id = 1
        self.buffer: list[bytes] = []
        self.idx = 0

    def _load_next_page(self) -> bool:
        while self.page_id < self.total_pages:
            page = Page.from_bytes(self.dm.read_page(self.page_id), self.record_size)
            self.page_id += 1
            recs = page.read_records()
            if recs:
                self.buffer = recs
                self.idx = 0
                return True
        return False

    def next(self):
        if self.idx >= len(self.buffer):
            if not self._load_next_page():
                return None
        rec = self.buffer[self.idx]
        self.idx += 1
        return rec

    def close(self):
        self.dm.close()


def _multiway_merge_to_pages(run_paths: list[str], out_dm: DiskManager,
                             record_size: int, key_fn) -> int:
    readers = [_RunReader(p, record_size) for p in run_paths]
    heap: list[tuple] = []
    written = 0

    try:
        for i, r in enumerate(readers):
            rec = r.next()
            if rec is not None:
                heapq.heappush(heap, (key_fn(rec), i, rec))

        rpp = _records_per_page(record_size)
        out_buffer: list[bytes] = []

        def flush():
            nonlocal written
            if not out_buffer:
                return
            page = Page(record_size)
            for r in out_buffer:
                page.insert_record(r)
            page_id = out_dm.allocate_page()
            out_dm.write_page(page_id, page.to_bytes())
            written += len(out_buffer)
            out_buffer.clear()

        while heap:
            _, i, rec = heapq.heappop(heap)
            out_buffer.append(rec)
            if len(out_buffer) == rpp:
                flush()
            nxt = readers[i].next()
            if nxt is not None:
                heapq.heappush(heap, (key_fn(nxt), i, nxt))

        flush()
    finally:
        for r in readers:
            r.close()

    return written


def _multiway_merge_to_list(run_paths: list[str], record_size: int, key_fn) -> list[bytes]:
    readers = [_RunReader(p, record_size) for p in run_paths]
    heap: list[tuple] = []
    out: list[bytes] = []
    try:
        for i, r in enumerate(readers):
            rec = r.next()
            if rec is not None:
                heapq.heappush(heap, (key_fn(rec), i, rec))
        while heap:
            _, i, rec = heapq.heappop(heap)
            out.append(rec)
            nxt = readers[i].next()
            if nxt is not None:
                heapq.heappush(heap, (key_fn(nxt), i, nxt))
    finally:
        for r in readers:
            r.close()
    return out


def _cleanup(paths: list[str], tmp_dir: str):
    for p in paths:
        try:
            os.remove(p)
        except OSError:
            pass
    try:
        os.rmdir(tmp_dir)
    except OSError:
        pass


def external_sort_to_file(records: Iterable[bytes], output_path: str,
                          record_size: int, key_fn, buffer_pages: int = 4) -> dict:
    """Run TPMMS and write sorted output to a new heap-format file at `output_path`."""
    if buffer_pages < 1:
        raise ValueError("buffer_pages must be >= 1")

    tmp_dir = tempfile.mkdtemp(prefix="tpmms_")
    t0 = time.time()
    runs = _generate_runs(records, record_size, key_fn, buffer_pages, tmp_dir)
    t1 = time.time()

    out_dm = DiskManager(output_path)
    try:
        written = _multiway_merge_to_pages(runs, out_dm, record_size, key_fn)
    finally:
        out_dm.close()
    t2 = time.time()

    _cleanup(runs, tmp_dir)

    return {
        "runs_generated": len(runs),
        "buffer_pages": buffer_pages,
        "records_written": written,
        "phase1_sec": round(t1 - t0, 4),
        "phase2_sec": round(t2 - t1, 4),
        "total_sec": round(t2 - t0, 4),
    }


def external_sort(records: Iterable[bytes], record_size: int, key_fn,
                  buffer_pages: int = 4) -> dict:
    """Run TPMMS and return the sorted records as a list, plus stats."""
    if buffer_pages < 1:
        raise ValueError("buffer_pages must be >= 1")

    tmp_dir = tempfile.mkdtemp(prefix="tpmms_")
    t0 = time.time()
    runs = _generate_runs(records, record_size, key_fn, buffer_pages, tmp_dir)
    t1 = time.time()
    sorted_records = _multiway_merge_to_list(runs, record_size, key_fn)
    t2 = time.time()

    _cleanup(runs, tmp_dir)

    return {
        "records": sorted_records,
        "runs_generated": len(runs),
        "buffer_pages": buffer_pages,
        "phase1_sec": round(t1 - t0, 4),
        "phase2_sec": round(t2 - t1, 4),
        "total_sec": round(t2 - t0, 4),
    }

"""
test_all.py

Suite completa de tests para:
  - DiskManager
  - HeapFile
  - BPlusTree
  - ExtendibleHash
  - SequentialFile
  - Engine

Ejecutar:
    python -m pytest test_all.py -v
    python test_all.py          # sin pytest
"""

import os
import struct
import tempfile
import pytest

# -------------------------------------------------------
# HELPERS COMPARTIDOS
# -------------------------------------------------------

RECORD_SIZE = 40  # 4 bytes key + 36 bytes padding

def make_record(key: int) -> bytes:
    return struct.pack(">I", key) + b"\x00" * (RECORD_SIZE - 4)

def key_extractor(record: bytes) -> int:
    return struct.unpack(">I", record[:4])[0]

def tmp_path(suffix=".db"):
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    os.remove(path)
    return path

def tmp_path_existing(suffix=".db"):
    """Crea un archivo temporal que sí existe (para DiskManager en modo r+b)."""
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)
    return path


# ═══════════════════════════════════════════════════════
# 1. DISK MANAGER
# ═══════════════════════════════════════════════════════

class TestDiskManager:
    def setup_method(self):
        from storage.disk_manager import DiskManager, PAGE_SIZE
        self.PAGE_SIZE = PAGE_SIZE
        self.DiskManager = DiskManager
        self.path = tmp_path()

    def teardown_method(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def _make_dm(self):
        return self.DiskManager(self.path)

    def test_crea_archivo_nuevo(self):
        dm = self._make_dm()
        assert os.path.exists(self.path)
        dm.close()

    def test_total_pages_inicial_es_1(self):
        dm = self._make_dm()
        assert dm._get_total_pages() == 1
        dm.close()

    def test_allocate_incrementa_pages(self):
        dm = self._make_dm()
        pid = dm.allocate_page()
        assert pid == 1
        assert dm._get_total_pages() == 2
        dm.close()

    def test_write_y_read_pagina(self):
        dm = self._make_dm()
        pid = dm.allocate_page()
        data = b"A" * self.PAGE_SIZE
        dm.write_page(pid, data)
        result = dm.read_page(pid)
        assert result == data
        dm.close()

    def test_write_pagina_tamaño_incorrecto(self):
        dm = self._make_dm()
        pid = dm.allocate_page()
        with pytest.raises(ValueError):
            dm.write_page(pid, b"corto")
        dm.close()

    def test_read_page_invalida(self):
        dm = self._make_dm()
        with pytest.raises(ValueError):
            dm.read_page(-1)
        dm.close()

    def test_contadores_reads_writes(self):
        dm = self._make_dm()
        pid = dm.allocate_page()
        dm.write_page(pid, b"\x00" * self.PAGE_SIZE)
        dm.read_page(pid)
        # allocate_page hace 1 write interno + 1 write de set_total_pages
        assert dm.read_count >= 1
        assert dm.write_count >= 1
        dm.close()

    def test_reset_stats(self):
        dm = self._make_dm()
        pid = dm.allocate_page()
        dm.read_page(pid)
        dm.reset_stats()
        assert dm.read_count == 0
        assert dm.write_count == 0
        dm.close()

    def test_get_stats(self):
        dm = self._make_dm()
        pid = dm.allocate_page()
        dm.reset_stats()
        dm.read_page(pid)
        stats = dm.get_stats()
        assert "reads" in stats
        assert "writes" in stats
        assert stats["reads"] == 1
        dm.close()

    def test_set_y_get_root(self):
        dm = self._make_dm()
        dm.set_root(42)
        assert dm.get_root() == 42
        dm.close()

    def test_root_persiste_entre_aperturas(self):
        dm = self._make_dm()
        dm.set_root(7)
        dm.close()
        dm2 = self.DiskManager(self.path)
        assert dm2.get_root() == 7
        dm2.close()

    def test_multiples_paginas(self):
        dm = self._make_dm()
        pids = [dm.allocate_page() for _ in range(5)]
        assert pids == [1, 2, 3, 4, 5]
        for pid in pids:
            data = struct.pack(">I", pid) + b"\x00" * (self.PAGE_SIZE - 4)
            dm.write_page(pid, data)
        for pid in pids:
            result = dm.read_page(pid)
            assert struct.unpack(">I", result[:4])[0] == pid
        dm.close()


# ═══════════════════════════════════════════════════════
# 2. HEAP FILE
# ═══════════════════════════════════════════════════════

class TestHeapFile:
    def setup_method(self):
        from storage.disk_manager import DiskManager
        from index.heap import HeapFile
        self.path = tmp_path()
        self.dm = DiskManager(self.path)
        self.heap = HeapFile(self.dm, RECORD_SIZE)

    def teardown_method(self):
        self.dm.close()
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_insert_y_scan_un_registro(self):
        rec = make_record(1)
        self.heap.insert(rec)
        results = self.heap.scan()
        assert rec in results

    def test_insert_multiples_y_scan(self):
        records = [make_record(i) for i in range(10)]
        for r in records:
            self.heap.insert(r)
        results = self.heap.scan()
        assert len(results) == 10

    def test_scan_vacio(self):
        assert self.heap.scan() == []

    def test_search_con_predicate(self):
        for i in range(5):
            self.heap.insert(make_record(i))
        results = self.heap.search(lambda r: key_extractor(r) == 3)
        assert len(results) == 1
        assert key_extractor(results[0]) == 3

    def test_search_sin_resultados(self):
        self.heap.insert(make_record(1))
        results = self.heap.search(lambda r: key_extractor(r) == 99)
        assert results == []

    def test_insert_record_tamaño_invalido(self):
        with pytest.raises((ValueError, IOError)):
            self.heap.insert(b"corto")

    def test_insert_record_no_bytes(self):
        with pytest.raises((ValueError, IOError)):
            self.heap.insert("no es bytes")

    def test_muchos_inserts_llenan_paginas(self):
        # insertar suficientes registros para usar >1 página
        n = 200
        for i in range(n):
            self.heap.insert(make_record(i))
        results = self.heap.scan()
        assert len(results) == n


# ═══════════════════════════════════════════════════════
# 3. B+ TREE
# ═══════════════════════════════════════════════════════

class TestBPlusTree:
    def setup_method(self):
        from index.bplustree import BPlusTree
        self.path = tmp_path()
        self.tree = BPlusTree(self.path, RECORD_SIZE, key_extractor, order=4)

    def teardown_method(self):
        self.tree.close()
        if os.path.exists(self.path):
            os.remove(self.path)

    def _insert_keys(self, keys):
        for k in keys:
            self.tree.insert(make_record(k))

    # --- insert + search básico ---

    def test_insert_y_search_un_registro(self):
        self.tree.insert(make_record(10))
        results = self.tree.search(10)
        assert len(results) == 1
        assert key_extractor(results[0]) == 10

    def test_search_key_inexistente(self):
        self._insert_keys([1, 2, 3])
        assert self.tree.search(99) == []

    def test_insert_ordenado_ascendente(self):
        self._insert_keys(range(1, 20))
        for k in range(1, 20):
            results = self.tree.search(k)
            assert len(results) == 1

    def test_insert_ordenado_descendente(self):
        self._insert_keys(range(20, 0, -1))
        for k in range(1, 21):
            assert len(self.tree.search(k)) == 1

    def test_insert_aleatorio(self):
        import random
        keys = list(range(1, 50))
        random.shuffle(keys)
        self._insert_keys(keys)
        for k in keys:
            assert len(self.tree.search(k)) == 1

    # --- duplicados ---

    def test_duplicados_se_almacenan(self):
        self.tree.insert(make_record(5))
        self.tree.insert(make_record(5))
        results = self.tree.search(5)
        assert len(results) == 2

    def test_tres_duplicados(self):
        for _ in range(3):
            self.tree.insert(make_record(7))
        assert len(self.tree.search(7)) == 3

    # --- range search ---

    def test_range_search_basico(self):
        self._insert_keys(range(1, 11))
        results = self.tree.range_search(3, 7)
        keys_found = sorted([key_extractor(r) for r in results])
        assert keys_found == [3, 4, 5, 6, 7]

    def test_range_search_sin_resultados(self):
        self._insert_keys([1, 2, 3])
        assert self.tree.range_search(10, 20) == []

    def test_range_search_extremos_inclusivos(self):
        self._insert_keys([5, 10, 15])
        results = self.tree.range_search(5, 15)
        keys_found = sorted([key_extractor(r) for r in results])
        assert keys_found == [5, 10, 15]

    def test_range_search_un_elemento(self):
        self._insert_keys([1, 5, 10])
        results = self.tree.range_search(5, 5)
        assert len(results) == 1
        assert key_extractor(results[0]) == 5

    # --- splits ---

    def test_split_hoja(self):
        # con order=4, el split ocurre al insertar el 5to elemento
        self._insert_keys([10, 20, 30, 40, 50])
        for k in [10, 20, 30, 40, 50]:
            assert len(self.tree.search(k)) == 1

    def test_split_nodo_interno(self):
        # suficientes inserts para provocar split de nodo interno
        self._insert_keys(range(1, 30))
        for k in range(1, 30):
            assert len(self.tree.search(k)) == 1

    # --- métricas ---

    def test_search_genera_reads(self):
        self._insert_keys(range(1, 10))
        self.tree.dm.reset_stats()
        self.tree.search(5)
        assert self.tree.dm.read_count > 0

    def test_insert_genera_writes(self):
        self.tree.dm.reset_stats()
        self.tree.insert(make_record(1))
        assert self.tree.dm.write_count > 0

    def test_reads_search_son_logaritmicos(self):
        # con más datos los reads deben crecer lento (no linealmente)
        self._insert_keys(range(1, 100))
        self.tree.dm.reset_stats()
        self.tree.search(50)
        reads_100 = self.tree.dm.read_count

        self.tree.close()
        if os.path.exists(self.path):
            os.remove(self.path)

        from index.bplustree import BPlusTree
        self.path = tmp_path()
        self.tree = BPlusTree(self.path, RECORD_SIZE, key_extractor, order=4)
        self._insert_keys(range(1, 10000))
        self.tree.dm.reset_stats()
        self.tree.search(5000)
        reads_10000 = self.tree.dm.read_count

        # reads no deben crecer más de 10x para 100x más datos
        assert reads_10000 < reads_100 * 10

    # --- persistencia ---

    def test_root_persiste(self):
        from index.bplustree import BPlusTree
        self._insert_keys(range(1, 20))
        root_antes = self.tree.root
        self.tree.close()

        tree2 = BPlusTree(self.path, RECORD_SIZE, key_extractor, order=4)
        assert tree2.root == root_antes
        for k in range(1, 20):
            assert len(tree2.search(k)) == 1
        tree2.close()


# ═══════════════════════════════════════════════════════
# 4. EXTENDIBLE HASHING
# ═══════════════════════════════════════════════════════

class TestExtendibleHash:
    def setup_method(self):
        from index.hash import ExtendibleHash
        self.path = tmp_path()
        self.ht = ExtendibleHash(self.path, RECORD_SIZE, key_extractor)

    def teardown_method(self):
        self.ht.close()
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_insert_y_search(self):
        self.ht.insert(make_record(42))
        results = self.ht.search(42)
        assert len(results) == 1
        assert key_extractor(results[0]) == 42

    def test_search_key_inexistente(self):
        assert self.ht.search(999) == []

    def test_multiples_inserts(self):
        keys = [1, 2, 3, 4, 5, 10, 20, 50]
        for k in keys:
            self.ht.insert(make_record(k))
        for k in keys:
            results = self.ht.search(k)
            assert len(results) >= 1
            assert key_extractor(results[0]) == k

    def test_split_por_overflow(self):
        # insertar suficientes para forzar splits
        for i in range(50):
            self.ht.insert(make_record(i))
        for i in range(50):
            results = self.ht.search(i)
            assert any(key_extractor(r) == i for r in results)

    def test_duplicados(self):
        self.ht.insert(make_record(7))
        self.ht.insert(make_record(7))
        results = self.ht.search(7)
        assert len(results) == 2

    def test_search_genera_reads(self):
        self.ht.insert(make_record(1))
        self.ht.dm.reset_stats()
        self.ht.search(1)
        assert self.ht.dm.read_count > 0

    def test_insert_genera_writes(self):
        self.ht.dm.reset_stats()
        self.ht.insert(make_record(1))
        assert self.ht.dm.write_count > 0

    def test_search_es_O1_aproximado(self):
        # con 10 registros
        for i in range(10):
            self.ht.insert(make_record(i))
        self.ht.dm.reset_stats()
        self.ht.search(5)
        reads_10 = self.ht.dm.read_count

        # con 500 registros
        self.ht.close()
        if os.path.exists(self.path):
            os.remove(self.path)
        from index.hash import ExtendibleHash
        self.path = tmp_path()
        self.ht = ExtendibleHash(self.path, RECORD_SIZE, key_extractor)
        for i in range(500):
            self.ht.insert(make_record(i))
        self.ht.dm.reset_stats()
        self.ht.search(250)
        reads_500 = self.ht.dm.read_count

        # O(1) → reads no deben escalar linealmente
        assert reads_500 < reads_10 * 20  # margen generoso


# ═══════════════════════════════════════════════════════
# 5. SEQUENTIAL FILE
# ═══════════════════════════════════════════════════════

class TestSequentialFile:
    def setup_method(self):
        from index.sequential import SequentialFile
        self.main_path = tmp_path()
        self.overflow_path = tmp_path()
        self.seq = SequentialFile(
            self.main_path, self.overflow_path,
            RECORD_SIZE, key_extractor
        )

    def teardown_method(self):
        self.seq.close()
        for p in [self.main_path, self.overflow_path]:
            if os.path.exists(p):
                os.remove(p)

    def test_insert_y_search(self):
        self.seq.insert(make_record(5))
        results = self.seq.search(5)
        assert len(results) >= 1
        assert key_extractor(results[0]) == 5

    def test_search_inexistente(self):
        self.seq.insert(make_record(1))
        assert self.seq.search(99) == []

    def test_range_search(self):
        for k in range(1, 11):
            self.seq.insert(make_record(k))
        results = self.seq.range_search(3, 7)
        keys_found = sorted([key_extractor(r) for r in results])
        assert keys_found == [3, 4, 5, 6, 7]

    def test_range_search_sin_resultados(self):
        for k in [1, 2, 3]:
            self.seq.insert(make_record(k))
        assert self.seq.range_search(10, 20) == []

    def test_range_resultado_ordenado(self):
        import random
        keys = list(range(1, 20))
        random.shuffle(keys)
        for k in keys:
            self.seq.insert(make_record(k))
        results = self.seq.range_search(1, 20)
        keys_found = [key_extractor(r) for r in results]
        assert keys_found == sorted(keys_found)

    def test_rebuild_fusiona_correctamente(self):
        for k in [10, 5, 1, 8, 3]:
            self.seq.insert(make_record(k))
        self.seq.rebuild()
        for k in [10, 5, 1, 8, 3]:
            results = self.seq.search(k)
            assert len(results) >= 1

    def test_rebuild_limpia_overflow(self):
        for k in range(10):
            self.seq.insert(make_record(k))
        self.seq.rebuild()
        # overflow debe estar vacío
        overflow_records = self.seq.overflow.scan()
        assert overflow_records == []

    def test_unified_dm_reset_stats(self):
        self.seq.insert(make_record(1))
        self.seq.dm.reset_stats()
        assert self.seq.dm.read_count == 0
        assert self.seq.dm.write_count == 0

    def test_unified_dm_suma_ambos(self):
        self.seq.insert(make_record(1))
        self.seq.dm.reset_stats()
        self.seq.search(1)
        # debe reflejar reads de main + overflow
        assert self.seq.dm.read_count > 0

    # En test_all.py, reemplazar el test:
    def test_overflow_limit_trigger_rebuild(self):
        self.seq.overflow_limit = 5
        for k in range(5):        # FIX: exactamente 5 → dispara rebuild → count queda en 0
            self.seq.insert(make_record(k))
        assert self.seq._overflow_count == 0


# ═══════════════════════════════════════════════════════
# 6. ENGINE
# ═══════════════════════════════════════════════════════

class TestEngine:
    def setup_method(self):
        from engine.engine import Engine
        from index.bplustree import BPlusTree
        from index.hash import ExtendibleHash

        self.Engine = Engine
        self.BPlusTree = BPlusTree
        self.ExtendibleHash = ExtendibleHash

        self.paths = []
        self.engine = Engine()

    def teardown_method(self):
        self.engine.close()
        for p in self.paths:
            if os.path.exists(p):
                os.remove(p)

    def _make_bptree(self):
        p = tmp_path()
        self.paths.append(p)
        return self.BPlusTree(p, RECORD_SIZE, key_extractor, order=4)

    def _make_hash(self):
        p = tmp_path()
        self.paths.append(p)
        return self.ExtendibleHash(p, RECORD_SIZE, key_extractor)

    def test_create_table(self):
        self.engine.create_table("estudiantes", self._make_bptree())
        assert "estudiantes" in self.engine.show_tables()

    def test_create_table_duplicada(self):
        self.engine.create_table("t1", self._make_bptree())
        with pytest.raises(Exception, match="ya existe"):
            self.engine.create_table("t1", self._make_bptree())

    def test_insert_y_search_bptree(self):
        self.engine.create_table("t", self._make_bptree())
        result, stats = self.engine.insert("t", make_record(10))
        results, stats = self.engine.search("t", 10)
        assert len(results) == 1
        assert key_extractor(results[0]) == 10

    def test_insert_y_search_hash(self):
        self.engine.create_table("h", self._make_hash())
        self.engine.insert("h", make_record(99))
        results, stats = self.engine.search("h", 99)
        assert len(results) >= 1

    def test_range_search_engine(self):
        self.engine.create_table("r", self._make_bptree())
        for k in range(1, 11):
            self.engine.insert("r", make_record(k))
        results, stats = self.engine.range_search("r", 3, 7)
        keys_found = sorted([key_extractor(r) for r in results])
        assert keys_found == [3, 4, 5, 6, 7]

    def test_stats_tienen_reads_y_writes(self):
        self.engine.create_table("s", self._make_bptree())
        self.engine.insert("s", make_record(1))
        results, stats = self.engine.search("s", 1)
        assert "reads" in stats
        assert "writes" in stats

    def test_stats_se_resetean_por_operacion(self):
        self.engine.create_table("m", self._make_bptree())
        for k in range(20):
            self.engine.insert("m", make_record(k))
        # search individual debe tener reads bajos (no acumulados)
        results, stats = self.engine.search("m", 10)
        assert stats["reads"] < 20  # no puede haber leído todas las páginas

    def test_tabla_inexistente(self):
        with pytest.raises(Exception, match="no existe"):
            self.engine.search("no_existe", 1)

    def test_execute_insert(self):
        self.engine.create_table("e", self._make_bptree())
        query = {"type": "INSERT", "table": "e", "value": 42}
        self.engine.execute(query, lambda v: make_record(v))

    def test_execute_select_equal(self):
        self.engine.create_table("e2", self._make_bptree())
        query_ins = {"type": "INSERT", "table": "e2", "value": 7}
        self.engine.execute(query_ins, lambda v: make_record(v))
        query_sel = {
            "type": "SELECT", "table": "e2",
            "condition": {"type": "EQUAL", "value": 7}
        }
        results, stats = self.engine.execute(query_sel, lambda v: make_record(v))
        assert len(results) >= 1

    def test_execute_select_between(self):
        self.engine.create_table("e3", self._make_bptree())
        for k in range(1, 11):
            self.engine.execute(
                {"type": "INSERT", "table": "e3", "value": k},
                lambda v: make_record(v)
            )
        results, stats = self.engine.execute(
            {"type": "SELECT", "table": "e3",
             "condition": {"type": "BETWEEN", "begin": 2, "end": 5}},
            lambda v: make_record(v)
        )
        keys_found = sorted([key_extractor(r) for r in results])
        assert keys_found == [2, 3, 4, 5]

    def test_close_cierra_todos(self):
        self.engine.create_table("c1", self._make_bptree())
        self.engine.create_table("c2", self._make_hash())
        # no debe lanzar excepción
        self.engine.close()


# ═══════════════════════════════════════════════════════
# 7. MÉTRICAS (MetricsLogger)
# ═══════════════════════════════════════════════════════

class TestMetricsLogger:
    def setup_method(self):
        from utils.metrics import MetricsLogger
        from storage.disk_manager import DiskManager
        from index.bplustree import BPlusTree

        self.MetricsLogger = MetricsLogger
        self.path = tmp_path()
        self.log_path = tmp_path(suffix=".json")
        self.tree = BPlusTree(self.path, RECORD_SIZE, key_extractor, order=4)

    def teardown_method(self):
        self.tree.close()
        for p in [self.path, self.log_path]:
            if os.path.exists(p):
                os.remove(p)

    def test_measure_captura_reads(self):
        logger = self.MetricsLogger()
        self.tree.insert(make_record(1))
        with logger.measure("bplustree", "search", n=1, dm=self.tree.dm):
            self.tree.search(1)
        assert len(logger.entries) == 1
        assert logger.entries[0]["reads"] > 0

    def test_measure_captura_tiempo(self):
        logger = self.MetricsLogger()
        with logger.measure("bplustree", "insert", n=1, dm=self.tree.dm):
            self.tree.insert(make_record(1))
        assert logger.entries[0]["time_ms"] >= 0

    def test_measure_campos_correctos(self):
        logger = self.MetricsLogger()
        with logger.measure("bplustree", "search", n=100, dm=self.tree.dm):
            self.tree.search(999)
        e = logger.entries[0]
        for campo in ["structure", "operation", "n", "reads", "writes", "disk_accesses", "time_ms"]:
            assert campo in e

    def test_save_y_load(self):
        import json
        logger = self.MetricsLogger()
        with logger.measure("bplustree", "search", n=1, dm=self.tree.dm):
            self.tree.search(1)
        logger.save(self.log_path)
        assert os.path.exists(self.log_path)
        with open(self.log_path) as f:
            data = json.load(f)
        assert len(data) == 1

    def test_multiples_mediciones(self):
        logger = self.MetricsLogger()
        for k in range(5):
            self.tree.insert(make_record(k))
            with logger.measure("bplustree", "insert", n=k+1, dm=self.tree.dm):
                pass
        assert len(logger.entries) == 5


# -------------------------------------------------------
# RUNNER MANUAL (sin pytest)
# -------------------------------------------------------

if __name__ == "__main__":
    import sys
    import traceback

    # Fix encoding para Windows (cp1252 no soporta Unicode especial)
    if hasattr(sys.stdout, "buffer"):
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

    SEP1 = "-" * 55
    SEP2 = "=" * 55
    OK   = "[OK]  "
    FAIL = "[FAIL]"

    suites = [
        TestDiskManager,
        TestHeapFile,
        TestBPlusTree,
        TestExtendibleHash,
        TestSequentialFile,
        TestEngine,
        TestMetricsLogger,
    ]

    total = 0
    passed = 0
    failed = 0
    errors_list = []

    for Suite in suites:
        suite_name = Suite.__name__
        methods = [m for m in dir(Suite) if m.startswith("test_")]
        print(f"\n{SEP1}")
        print(f"  {suite_name} ({len(methods)} tests)")
        print(SEP1)

        for method_name in methods:
            total += 1
            instance = Suite()
            try:
                if hasattr(instance, "setup_method"):
                    instance.setup_method()
                getattr(instance, method_name)()
                if hasattr(instance, "teardown_method"):
                    instance.teardown_method()
                print(f"  {OK} {method_name}")
                passed += 1
            except Exception as e:
                if hasattr(instance, "teardown_method"):
                    try:
                        instance.teardown_method()
                    except:
                        pass
                print(f"  {FAIL} {method_name}")
                print(f"         {type(e).__name__}: {e}")
                errors_list.append((suite_name, method_name, traceback.format_exc()))
                failed += 1

    print(f"\n{SEP2}")
    print(f"  RESULTADO: {passed}/{total} pasaron  |  {failed} fallaron")
    print(SEP2)

    if errors_list:
        print("\n-- DETALLE DE FALLAS --")
        for suite, method, tb in errors_list:
            print(f"\n{suite}::{method}")
            print(tb)
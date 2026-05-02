import time
from index.bplustree import BPlusTree


class Engine:
    def __init__(self):
        self.tables = {}

    # -------------------------
    # CREATE TABLE
    # -------------------------

    def create_table(self, name, file_path, record_size, key_extractor):
        if name in self.tables:
            raise Exception("Tabla ya existe")

        tree = BPlusTree(file_path, record_size, key_extractor)

        self.tables[name] = {
            "index": tree,
            "file": file_path
        }

    # -------------------------
    # INSERT
    # -------------------------

    def insert(self, table, record):
        if table not in self.tables:
            raise Exception("Tabla no existe")

        tree = self.tables[table]["index"]

        t0 = time.time()
        tree.insert(record)
        t1 = time.time()

        return {
            "time_ms": (t1 - t0) * 1000
        }

    # -------------------------
    # SEARCH (=)
    # -------------------------

    def search(self, table, key):
        if table not in self.tables:
            raise Exception("Tabla no existe")

        tree = self.tables[table]["index"]

        t0 = time.time()
        res = tree.search(key)
        t1 = time.time()

        stats = tree.dm.get_stats()

        return {
            "result": res,
            "time_ms": (t1 - t0) * 1000,
            "reads": stats["reads"],
            "writes": stats["writes"]
        }

    # -------------------------
    # RANGE SEARCH
    # -------------------------

    def range_search(self, table, start, end):
        if table not in self.tables:
            raise Exception("Tabla no existe")

        tree = self.tables[table]["index"]

        t0 = time.time()
        res = tree.range_search(start, end)
        t1 = time.time()

        stats = tree.dm.get_stats()

        return {
            "result": res,
            "time_ms": (t1 - t0) * 1000,
            "reads": stats["reads"],
            "writes": stats["writes"]
        }

    # -------------------------
    # CLOSE
    # -------------------------

    def close(self):
        for t in self.tables.values():
            t["index"].close()
import os

from index.bplustree import BPlusTree
from index.hash import ExtendibleHash
from index.sequential import SequentialFile
from index.rtree import RTree
from index.heap import HeapFile
from utils.csv_loader import CSVLoader

from algoritmos.external_sort import external_sort
from algoritmos.external_hashing import external_hash_group_by
from algoritmos.hash_join import hash_join as _hash_join


class Table:
    def __init__(self, name, index_structure, columns=None, index_type=None):
        self.name = name
        self.index = index_structure
        self.columns = columns or []
        self.index_type = index_type


class Engine:
    def __init__(self):
        self.tables = {}

    # -----------------------------
    # CREATE TABLE
    # -----------------------------
    def create_table(self, name, index_structure, columns=None, index_type=None):
        if name in self.tables:
            raise Exception(f"Tabla '{name}' ya existe")
        self.tables[name] = Table(name, index_structure, columns, index_type)

    def create_table_from_dict(self, query_dict, record_size, key_extractor,
                               make_record, point_extractor=None,
                               base_path="data", order=4):
        if query_dict.get("type") != "CREATE_TABLE":
            raise Exception("create_table_from_dict espera CREATE_TABLE")

        table = query_dict["table"]
        columns = query_dict.get("columns", [])
        from_file = query_dict.get("from_file")

        # Default: HEAP (sin INDEX explícito). El usuario debe pedir un índice.
        index_type = None
        for col in columns:
            if col.get("index"):
                index_type = col["index"].upper()
                break
        if index_type is None:
            index_type = "HEAP"

        os.makedirs(base_path, exist_ok=True)
        main_path = os.path.join(base_path, f"{table}.db")

        if index_type == "SEQUENTIAL":
            overflow_path = os.path.join(base_path, f"{table}_overflow.db")
            index = SequentialFile(main_path, overflow_path, record_size, key_extractor)
        elif index_type == "HASH":
            index = ExtendibleHash(main_path, record_size, key_extractor)
        elif index_type == "RTREE":
            if not point_extractor:
                raise Exception("RTREE requiere point_extractor")
            index = RTree(main_path, record_size, point_extractor)
        elif index_type == "HEAP":
            index = HeapFile(main_path, record_size, key_extractor)
        elif index_type == "BPLUSTREE":
            key_col_name = None
            for col in columns:
                if col.get("index"):
                    key_col_name = col.get("name")
                    break
            if key_col_name is None and columns:
                key_col_name = columns[0].get("name")
            key_col_meta = next((c for c in columns if c.get("name") == key_col_name), None)
            key_col_type = (key_col_meta.get("type") if key_col_meta else "INT").upper()
            if key_col_type == "TEXT":
                key_size = int(key_col_meta.get("size") or 32)
                index = BPlusTree(main_path, record_size, key_extractor, order=order,
                                  key_type='string', key_size=key_size)
            else:
                index = BPlusTree(main_path, record_size, key_extractor, order=order)
        else:
            raise Exception(f"Tipo de índice no soportado: {index_type}")

        self.create_table(table, index, columns=columns, index_type=index_type)

        inserted = 0
        if from_file:
            # Resolver la ruta del CSV: aceptar ruta absoluta, relativa
            # o relativa a `base_path` (p. ej. el directorio data del servidor).
            file_path = from_file
            if not os.path.isabs(file_path):
                # Si la ruta relativa no existe desde el working dir,
                # intentar buscar dentro de `base_path`.
                if not os.path.exists(file_path):
                    alt = os.path.join(base_path, file_path)
                    if os.path.exists(alt):
                        file_path = alt
                    else:
                        # Manejar casos como 'data/students.csv' cuando
                        # `base_path` ya contiene 'data' (evita doble 'data/').
                        alt2 = os.path.join(base_path, os.path.basename(file_path))
                        if os.path.exists(alt2):
                            file_path = alt2

            inserted = CSVLoader.load(
                file_path,
                table,
                self,
                row_parser=self._row_parser(columns),
                make_record=make_record,
                column_names=[c.get("name") for c in columns],
            )

        return {"table": table, "index": index_type, "inserted": inserted}

    # -----------------------------
    # INSERT
    # -----------------------------
    def insert(self, table_name, record):
        table = self._get_table(table_name)
        dm = self._get_dm(table)
        if dm:
            dm.reset_stats()
        table.index.insert(record)
        return self._get_stats(table)

    # -----------------------------
    # SEARCH (=)
    # -----------------------------
    def search(self, table_name, key):
        table = self._get_table(table_name)
        dm = self._get_dm(table)
        if dm:
            dm.reset_stats()
        result = table.index.search(key)
        return result, self._get_stats(table)

    # -----------------------------
    # RANGE SEARCH (BETWEEN)
    # -----------------------------
    def range_search(self, table_name, begin, end):
        table = self._get_table(table_name)
        dm = self._get_dm(table)
        if dm:
            dm.reset_stats()
        result = table.index.range_search(begin, end)
        return result, self._get_stats(table)

    # -----------------------------
    # EXECUTE (con Parser)
    # -----------------------------
    def execute(self, query_dict, make_record,
                create_table=None, delete_record=None, select_all=None,
                record_size=None, key_extractor=None, point_extractor=None,
                base_path="data", order=4, decode=None):
        qtype = query_dict["type"]

        if qtype == "INSERT":
            table = query_dict["table"]
            values = query_dict.get("values")
            if values is None:
                values = [query_dict.get("value")]
            if len(values) == 1:
                record = make_record(values[0])
            else:
                record = make_record(values)
            stats = self.insert(table, record)
            return None, stats

        if qtype == "SELECT":
            table = query_dict["table"]
            cond = query_dict["condition"]

            if cond["type"] == "EQUAL":
                cond_col = cond.get("column")
                key_col = self._key_column(self._get_table(table))
                if cond_col is None or cond_col == key_col:
                    return self.search(table, cond["value"])
                # Fallback: columna no indexada -> scan + filtro lineal
                return self._scan_filter(table, lambda row: row.get(cond_col) == cond["value"], decode)

            if cond["type"] == "BETWEEN":
                cond_col = cond.get("column")
                key_col = self._key_column(self._get_table(table))
                if cond_col is None or cond_col == key_col:
                    return self.range_search(table, cond["begin"], cond["end"])
                begin, end = cond["begin"], cond["end"]
                return self._scan_filter(
                    table,
                    lambda row: (row.get(cond_col) is not None and begin <= row.get(cond_col) <= end),
                    decode,
                )

            if cond["type"] == "IN_RADIUS":
                table_obj = self._get_table(table)
                dm = self._get_dm(table_obj)
                if dm:
                    dm.reset_stats()
                index = table_obj.index
                if not hasattr(index, "range_search"):
                    raise Exception("Índice no soporta range_search")
                cx, cy = cond["point"]
                results = index.range_search(cx, cy, cond["radius"])
                return results, self._get_stats(table_obj)

            if cond["type"] == "IN_KNN":
                table_obj = self._get_table(table)
                dm = self._get_dm(table_obj)
                if dm:
                    dm.reset_stats()
                index = table_obj.index
                if not hasattr(index, "knn"):
                    raise Exception("Índice no soporta knn")
                cx, cy = cond["point"]
                results = index.knn(cx, cy, cond["k"])
                return results, self._get_stats(table_obj)

            raise Exception("Condición no soportada")

        if qtype == "SELECT_ALL":
            table_obj = self._get_table(query_dict["table"])
            dm = self._get_dm(table_obj)
            if dm:
                dm.reset_stats()

            if select_all:
                results = select_all(table_obj)
                return results, self._get_stats(table_obj)

            if hasattr(table_obj.index, "scan"):
                results = table_obj.index.scan()
                return results, self._get_stats(table_obj)

            raise Exception("SELECT * sin condición no soportado por el índice")

        if qtype == "CREATE_TABLE":
            if create_table:
                create_table(query_dict)
            else:
                if record_size is None or key_extractor is None:
                    raise Exception("CREATE TABLE requiere record_size y key_extractor")
                self.create_table_from_dict(
                    query_dict,
                    record_size,
                    key_extractor,
                    make_record,
                    point_extractor=point_extractor,
                    base_path=base_path,
                    order=order
                )
            return None, {"reads": 0, "writes": 0}

        if qtype == "DELETE":
            table = query_dict["table"]
            cond = query_dict["condition"]

            if delete_record:
                return delete_record(query_dict)

            table_obj = self._get_table(table)
            index = table_obj.index
            dm = self._get_dm(table_obj)
            if dm:
                dm.reset_stats()

            if not hasattr(index, "remove") or cond["type"] != "EQUAL":
                raise Exception("DELETE no soportado por el índice")

            cond_col = cond.get("column")
            key_col = self._key_column(table_obj)
            if cond_col is None or cond_col == key_col:
                index.remove(cond["value"])
                return None, self._get_stats(table_obj)

            # DELETE por columna NO indexada: scan + filtro + remove por key.
            if not hasattr(index, "scan"):
                raise Exception("DELETE sobre columna no indexada requiere scan()")
            if decode is None:
                raise Exception("DELETE sobre columna no indexada requiere decode")
            if not hasattr(index, "key_extractor") and not getattr(index, "key", None):
                raise Exception("Índice no expone key_extractor; DELETE no disponible")
            extractor = getattr(index, "key_extractor", None) or index.key

            target = cond["value"]
            keys_to_remove = []
            for rec in index.scan():
                row = decode(rec)
                if row.get(cond_col) == target:
                    keys_to_remove.append(extractor(rec))
            for k in keys_to_remove:
                index.remove(k)
            return None, self._get_stats(table_obj)

        raise Exception(f"Tipo de query desconocido: {qtype}")

    # -----------------------------
    # External-memory algorithms (External Merge Sort, External Hashing, Hash JOIN)
    # -----------------------------

    def select_order(self, table_name, key_fn, buffer_pages: int = 4):
        """Sort a relation via External Merge Sort. Returns (records, stats)."""
        table = self._get_table(table_name)
        index = table.index
        if not hasattr(index, "scan"):
            raise Exception("Índice no soporta scan(); ORDER BY no disponible")
        dm = self._get_dm(table)
        if dm:
            dm.reset_stats()
        records = index.scan()
        scan_stats = self._get_stats(table)
        result = external_sort(records, table.index.record_size if hasattr(table.index, "record_size") else len(records[0]) if records else 0,
                               key_fn, buffer_pages=buffer_pages)
        result["scan_reads"] = scan_stats.get("reads", 0)
        return result["records"], result

    def select_group(self, table_name, key_fn, value_fn=None, op: str = "COUNT",
                     buckets: int = 16):
        """GROUP BY via External Hashing. Returns (group_dict, stats)."""
        table = self._get_table(table_name)
        index = table.index
        if not hasattr(index, "scan"):
            raise Exception("Índice no soporta scan(); GROUP BY no disponible")
        dm = self._get_dm(table)
        if dm:
            dm.reset_stats()
        records = index.scan()
        scan_stats = self._get_stats(table)
        record_size = self._record_size(records, table)
        result = external_hash_group_by(records, record_size, key_fn,
                                        buckets=buckets, value_fn=value_fn, op=op)
        result["scan_reads"] = scan_stats.get("reads", 0)
        return result["result"], result

    def select_join(self, left_name, right_name,
                    left_key_fn, right_key_fn, buckets: int = 16):
        """Hash JOIN via External Hashing. Returns (matches, stats)."""
        left = self._get_table(left_name)
        right = self._get_table(right_name)
        if not hasattr(left.index, "scan") or not hasattr(right.index, "scan"):
            raise Exception("JOIN requiere que ambos índices soporten scan()")
        l_dm = self._get_dm(left)
        r_dm = self._get_dm(right)
        if l_dm:
            l_dm.reset_stats()
        if r_dm:
            r_dm.reset_stats()
        l_records = left.index.scan()
        r_records = right.index.scan()
        l_size = self._record_size(l_records, left)
        r_size = self._record_size(r_records, right)
        result = _hash_join(l_records, l_size, left_key_fn,
                            r_records, r_size, right_key_fn,
                            buckets=buckets)
        result["left_scan_reads"] = self._get_stats(left).get("reads", 0)
        result["right_scan_reads"] = self._get_stats(right).get("reads", 0)
        return result["matches"], result

    @staticmethod
    def _record_size(records, table):
        if records:
            return len(records[0])
        rs = getattr(table.index, "record_size", None)
        if rs:
            return rs
        return 0

    # -----------------------------
    # UTILS
    # -----------------------------
    def _get_table(self, name):
        if name not in self.tables:
            raise Exception(f"Tabla '{name}' no existe")
        return self.tables[name]

    def _get_dm(self, table):
        """Obtiene el DiskManager del índice si existe."""
        index = table.index
        if hasattr(index, 'dm'):
            return index.dm
        return None

    def _key_column(self, table):
        # Columna indexada (primera con INDEX), fallback a la primera col
        for col in (table.columns or []):
            if col.get("index"):
                return col.get("name")
        if table.columns:
            return table.columns[0].get("name")
        return None

    def _scan_filter(self, table_name, predicate_row, decode):
        # Scan + filtro lineal sobre fila decodificada (para WHERE col != indexada)
        table_obj = self._get_table(table_name)
        index = table_obj.index
        if not hasattr(index, "scan"):
            raise Exception("Índice no soporta scan(); WHERE sobre columna no indexada no disponible")
        if decode is None:
            raise Exception("WHERE sobre columna no indexada requiere decode")
        dm = self._get_dm(table_obj)
        if dm:
            dm.reset_stats()
        results = [r for r in index.scan() if predicate_row(decode(r))]
        return results, self._get_stats(table_obj)

    def _get_stats(self, table):
        """Retorna stats del DiskManager si existe, sino vacío."""
        dm = self._get_dm(table)
        if dm:
            return dm.get_stats()
        return {"reads": 0, "writes": 0}

    def _row_parser(self, columns):
        if not columns:
            return None

        def parse_row(row):
            values = []
            for i, col in enumerate(columns):
                raw = row[i] if i < len(row) else ""
                col_type = (col.get("type") or "TEXT").upper()
                if col_type == "INT":
                    try:
                        values.append(int(raw))
                    except ValueError:
                        values.append(0)
                elif col_type == "FLOAT":
                    try:
                        values.append(float(raw))
                    except ValueError:
                        values.append(0.0)
                else:
                    values.append(raw)

            if len(values) == 1:
                return values[0]
            return values

        return parse_row

    def show_tables(self):
        return list(self.tables.keys())

    def drop_table(self, name, base_path="data"):
        if name not in self.tables:
            raise Exception(f"Tabla '{name}' no existe")

        table = self.tables[name]
        if hasattr(table.index, "close"):
            table.index.close()

        index_type = table.index_type or self._infer_index_type(table.index)

        main_path = os.path.join(base_path, f"{name}.db")
        overflow_path = os.path.join(base_path, f"{name}_overflow.db")
        dir_path = main_path + ".dir"

        for path in [main_path, overflow_path, dir_path]:
            if os.path.exists(path):
                os.remove(path)

        del self.tables[name]

        return {"table": name, "index": index_type}

    def close(self):
        for table in self.tables.values():
            if hasattr(table.index, "close"):
                table.index.close()

    def _infer_index_type(self, index):
        name = index.__class__.__name__.lower()
        if "sequential" in name:
            return "SEQUENTIAL"
        if "hash" in name:
            return "HASH"
        if "rtree" in name:
            return "RTREE"
        if "heap" in name:
            return "HEAP"
        return "BPLUSTREE"
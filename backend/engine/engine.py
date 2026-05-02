import os

from index.bplustree import BPlusTree
from index.hash import ExtendibleHash
from index.sequential import SequentialFile
from index.rtree import RTree
from utils.csv_loader import CSVLoader


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

        index_type = "BPLUSTREE"
        for col in columns:
            if col.get("index"):
                index_type = col["index"].upper()
                break

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
        else:
            index = BPlusTree(main_path, record_size, key_extractor, order=order)

        self.create_table(table, index, columns=columns, index_type=index_type)

        inserted = 0
        if from_file:
            inserted = CSVLoader.load(
                from_file,
                table,
                self,
                row_parser=self._row_parser(columns),
                make_record=make_record
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
                create_table=None, delete_record=None, select_all=None):
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
                return self.search(table, cond["value"])

            if cond["type"] == "BETWEEN":
                return self.range_search(table, cond["begin"], cond["end"])

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
                raise Exception("CREATE TABLE no está conectado al engine")
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

            if hasattr(index, "remove") and cond["type"] == "EQUAL":
                index.remove(cond["value"])
                return None, self._get_stats(table_obj)

            raise Exception("DELETE no soportado por el índice")

        raise Exception(f"Tipo de query desconocido: {qtype}")

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

    def close(self):
        for table in self.tables.values():
            if hasattr(table.index, "close"):
                table.index.close()
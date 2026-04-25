class Table:
    def __init__(self, name, index_structure):
        self.name = name
        self.index = index_structure


class Engine:
    def __init__(self):
        # nombre -> Table
        self.tables = {}

    # -----------------------------
    # CREATE TABLE
    # -----------------------------
    def create_table(self, name, index_structure):
        if name in self.tables:
            raise Exception(f"Tabla '{name}' ya existe")

        self.tables[name] = Table(name, index_structure)

    # -----------------------------
    # INSERT
    # -----------------------------
    def insert(self, table_name, record):
        table = self._get_table(table_name)
        table.index.insert(record)

    # -----------------------------
    # SEARCH (=)
    # -----------------------------
    def search(self, table_name, key):
        table = self._get_table(table_name)
        return table.index.search(key)

    # -----------------------------
    # RANGE SEARCH (BETWEEN)
    # -----------------------------
    def range_search(self, table_name, begin, end):
        table = self._get_table(table_name)
        return table.index.range_search(begin, end)

    # -----------------------------
    # EXECUTE (con Parser)
    # -----------------------------
    def execute(self, query_dict, make_record):
        qtype = query_dict["type"]

        # ---------------- INSERT ----------------
        if qtype == "INSERT":
            table = query_dict["table"]
            value = query_dict["value"]

            record = make_record(value)
            self.insert(table, record)
            return None

        # ---------------- SELECT ----------------
        elif qtype == "SELECT":
            table = query_dict["table"]
            cond = query_dict["condition"]

            if cond["type"] == "EQUAL":
                return self.search(table, cond["value"])

            elif cond["type"] == "BETWEEN":
                return self.range_search(
                    table,
                    cond["begin"],
                    cond["end"]
                )

            else:
                raise Exception("Condición no soportada")

        # ---------------- SELECT ALL (opcional) ----------------
        elif qtype == "SELECT_ALL":
            raise Exception("SELECT * sin condición no implementado aún")

        else:
            raise Exception(f"Tipo de query desconocido: {qtype}")

    # -----------------------------
    # UTILS
    # -----------------------------
    def _get_table(self, name):
        if name not in self.tables:
            raise Exception(f"Tabla '{name}' no existe")
        return self.tables[name]

    def show_tables(self):
        return list(self.tables.keys())

    def close(self):
        # cerrar todos los índices (importante para persistencia)
        for table in self.tables.values():
            if hasattr(table.index, "close"):
                table.index.close()
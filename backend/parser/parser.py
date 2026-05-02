class Parser:

    def parse(self, tokens):
        if not tokens:
            raise Exception("Query vacia")

        if tokens[0] == "SELECT":
            return self._parse_select(tokens)
        if tokens[0] == "INSERT":
            return self._parse_insert(tokens)
        if tokens[0] == "CREATE":
            return self._parse_create(tokens)
        if tokens[0] == "DELETE":
            return self._parse_delete(tokens)
        raise Exception("Query no soportada")

    # -----------------------------
    # SELECT
    # -----------------------------
    def _parse_select(self, tokens):
        # SELECT * FROM table WHERE ...
        if tokens[1] != "*" or tokens[2] != "FROM":
            raise Exception("Sintaxis inválida en SELECT")

        table = tokens[3]

        if "WHERE" not in tokens:
            return {
                "type": "SELECT_ALL",
                "table": table
            }

        where_idx = tokens.index("WHERE")
        col = tokens[where_idx + 1]

        # Caso 1: =
        if "=" in tokens:
            eq_idx = tokens.index("=")
            value = self._parse_value(tokens[eq_idx + 1])
            return {
                "type": "SELECT",
                "table": table,
                "condition": {
                    "type": "EQUAL",
                    "column": col,
                    "value": value
                }
            }

        # Caso 2: BETWEEN
        if "BETWEEN" in tokens:
            idx = tokens.index("BETWEEN")
            v1 = self._parse_value(tokens[idx + 1])
            v2 = self._parse_value(tokens[idx + 3])

            return {
                "type": "SELECT",
                "table": table,
                "condition": {
                    "type": "BETWEEN",
                    "column": col,
                    "begin": v1,
                    "end": v2
                }
            }

        # Caso 3: IN (POINT(x, y), RADIUS r) o IN (POINT(x, y), K k)
        if "IN" in tokens:
            return self._parse_spatial_in(table, col, tokens)

        raise Exception("Condición no soportada")

    # -----------------------------
    # INSERT
    # -----------------------------
    def _parse_insert(self, tokens):
        # INSERT INTO table VALUES ( ... )
        if len(tokens) < 4 or tokens[1] != "INTO":
            raise Exception("INSERT inválido")

        table = tokens[2]

        if "VALUES" not in tokens:
            raise Exception("INSERT inválido")

        val_idx = tokens.index("VALUES")
        if tokens[val_idx + 1] != "(":
            raise Exception("INSERT inválido")

        values = []
        i = val_idx + 2
        while i < len(tokens) and tokens[i] != ")":
            if tokens[i] == ",":
                i += 1
                continue
            values.append(self._parse_value(tokens[i]))
            i += 1

        return {
            "type": "INSERT",
            "table": table,
            "values": values
        }

    # -----------------------------
    # CREATE TABLE
    # -----------------------------
    def _parse_create(self, tokens):
        # CREATE TABLE name ( col type [INDEX tech], ... ) [FROM FILE path]
        if len(tokens) < 4 or tokens[1] != "TABLE":
            raise Exception("CREATE inválido")

        table = tokens[2]
        if tokens[3] != "(":
            raise Exception("CREATE inválido")

        columns = []
        i = 4
        while i < len(tokens) and tokens[i] != ")":
            if tokens[i] == ",":
                i += 1
                continue

            col_name = tokens[i]
            col_type = tokens[i + 1]
            i += 2

            col_index = None
            if i < len(tokens) and tokens[i] == "INDEX":
                col_index = tokens[i + 1]
                i += 2

            columns.append({
                "name": col_name,
                "type": col_type,
                "index": col_index
            })

        from_file = None
        if "FROM" in tokens and "FILE" in tokens:
            from_idx = tokens.index("FROM")
            file_idx = tokens.index("FILE")
            if file_idx != from_idx + 1:
                raise Exception("CREATE inválido")
            from_file = tokens[file_idx + 1]

        return {
            "type": "CREATE_TABLE",
            "table": table,
            "columns": columns,
            "from_file": from_file
        }

    # -----------------------------
    # DELETE
    # -----------------------------
    def _parse_delete(self, tokens):
        # DELETE FROM table WHERE col = value
        if len(tokens) < 6 or tokens[1] != "FROM":
            raise Exception("DELETE inválido")

        table = tokens[2]
        if "WHERE" not in tokens or "=" not in tokens:
            raise Exception("DELETE inválido")

        where_idx = tokens.index("WHERE")
        col = tokens[where_idx + 1]
        eq_idx = tokens.index("=")
        value = self._parse_value(tokens[eq_idx + 1])

        return {
            "type": "DELETE",
            "table": table,
            "condition": {
                "type": "EQUAL",
                "column": col,
                "value": value
            }
        }

    # -----------------------------
    # HELPERS
    # -----------------------------
    def _parse_spatial_in(self, table, column, tokens):
        # WHERE col IN ( POINT ( x , y ) , RADIUS r )
        # WHERE col IN ( POINT ( x , y ) , K k )
        if "POINT" not in tokens:
            raise Exception("IN espacial inválido")

        p_idx = tokens.index("POINT")
        if tokens[p_idx + 1] != "(" or tokens[p_idx + 5] != ")":
            raise Exception("POINT inválido")

        x = self._parse_value(tokens[p_idx + 2])
        y = self._parse_value(tokens[p_idx + 4])

        if "RADIUS" in tokens:
            r_idx = tokens.index("RADIUS")
            radius = self._parse_value(tokens[r_idx + 1])
            return {
                "type": "SELECT",
                "table": table,
                "condition": {
                    "type": "IN_RADIUS",
                    "column": column,
                    "point": (x, y),
                    "radius": radius
                }
            }

        if "K" in tokens:
            k_idx = tokens.index("K")
            kval = self._parse_value(tokens[k_idx + 1])
            return {
                "type": "SELECT",
                "table": table,
                "condition": {
                    "type": "IN_KNN",
                    "column": column,
                    "point": (x, y),
                    "k": kval
                }
            }

        raise Exception("IN espacial inválido")

    def _parse_value(self, token):
        # intenta int, luego float, sino string
        try:
            return int(token)
        except ValueError:
            pass
        try:
            return float(token)
        except ValueError:
            return token
class Parser:

    def parse(self, tokens):
        if tokens[0] == "SELECT":
            return self._parse_select(tokens)
        elif tokens[0] == "INSERT":
            return self._parse_insert(tokens)
        else:
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

        # Caso 1: =
        if "=" in tokens:
            value = int(tokens[-1])
            return {
                "type": "SELECT",
                "table": table,
                "condition": {
                    "type": "EQUAL",
                    "value": value
                }
            }

        # Caso 2: BETWEEN
        if "BETWEEN" in tokens:
            idx = tokens.index("BETWEEN")
            v1 = int(tokens[idx + 1])
            v2 = int(tokens[idx + 3])

            return {
                "type": "SELECT",
                "table": table,
                "condition": {
                    "type": "BETWEEN",
                    "begin": v1,
                    "end": v2
                }
            }

        raise Exception("Condición no soportada")

    # -----------------------------
    # INSERT
    # -----------------------------
    def _parse_insert(self, tokens):
        # INSERT INTO table VALUES ( x )

        table = tokens[2]

        if "VALUES" not in tokens:
            raise Exception("INSERT inválido")

        val_idx = tokens.index("VALUES")
        value = int(tokens[val_idx + 2])  # ( 5 )

        return {
            "type": "INSERT",
            "table": table,
            "value": value
        }
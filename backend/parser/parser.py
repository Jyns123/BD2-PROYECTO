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
        # SELECT <projection> FROM <table-or-join> [WHERE ...] [GROUP BY col] [ORDER BY col [ASC|DESC]]
        if "FROM" not in tokens:
            raise Exception("SELECT sin FROM")

        from_idx = tokens.index("FROM")
        projection = self._parse_projection(tokens[1:from_idx])

        # Encontrar el final del FROM-clause: hasta WHERE / GROUP / ORDER / fin.
        end_kw_idx = len(tokens)
        for kw in ("WHERE", "GROUP", "ORDER"):
            if kw in tokens[from_idx + 1:]:
                idx = tokens.index(kw, from_idx + 1)
                end_kw_idx = min(end_kw_idx, idx)

        from_tokens = tokens[from_idx + 1:end_kw_idx]

        # Detectar JOIN
        join_info = None
        if "JOIN" in from_tokens:
            join_info = self._parse_join_clause(from_tokens)
            table = join_info["left"]
        else:
            if not from_tokens:
                raise Exception("FROM vacío")
            table = from_tokens[0]

        # GROUP BY
        group_col = None
        if "GROUP" in tokens:
            g_idx = tokens.index("GROUP")
            if g_idx + 2 >= len(tokens) or tokens[g_idx + 1] != "BY":
                raise Exception("GROUP BY mal formado")
            group_col = tokens[g_idx + 2]

        # ORDER BY
        order_col = None
        order_dir = "ASC"
        if "ORDER" in tokens:
            o_idx = tokens.index("ORDER")
            if o_idx + 2 >= len(tokens) or tokens[o_idx + 1] != "BY":
                raise Exception("ORDER BY mal formado")
            order_col = tokens[o_idx + 2]
            if o_idx + 3 < len(tokens) and tokens[o_idx + 3] in ("ASC", "DESC"):
                order_dir = tokens[o_idx + 3]

        # WHERE (sólo soportado en queries single-table sin GROUP/ORDER avanzados)
        where_cond = None
        if "WHERE" in tokens:
            w_idx = tokens.index("WHERE")
            where_cond = self._parse_where(table, tokens, w_idx)

        # ─────────────────────────────────────────────────────
        # Dispatch por tipo de SELECT
        # ─────────────────────────────────────────────────────

        # JOIN
        if join_info is not None:
            return {
                "type": "SELECT_JOIN",
                "left": join_info["left"],
                "right": join_info["right"],
                "on_left": join_info["on_left"],
                "on_right": join_info["on_right"],
                "projection": projection,
            }

        # GROUP BY
        if group_col is not None:
            aggregates = [item for item in projection["items"] if item["kind"] == "AGG"]
            if not aggregates:
                raise Exception("GROUP BY requiere al menos un agregado en SELECT")
            return {
                "type": "SELECT_GROUP",
                "table": table,
                "group_col": group_col,
                "projection": projection,
            }

        # Aggregate sin GROUP BY: SELECT COUNT(*) FROM t  (single-row global aggregate)
        aggregates = [item for item in projection["items"] if item["kind"] == "AGG"]
        if aggregates and join_info is None:
            return {
                "type": "SELECT_GROUP",
                "table": table,
                "group_col": None,
                "projection": projection,
            }

        # ORDER BY (External Merge Sort)
        if order_col is not None:
            return {
                "type": "SELECT_ORDER",
                "table": table,
                "order_col": order_col,
                "order_dir": order_dir,
                "projection": projection,
            }

        # WHERE existente
        if where_cond is not None:
            return {
                "type": "SELECT",
                "table": table,
                "condition": where_cond,
                "projection": projection,
            }

        # SELECT * FROM t
        return {"type": "SELECT_ALL", "table": table, "projection": projection}

    # -----------------------------
    # Projection
    # -----------------------------
    def _parse_projection(self, proj_tokens):
        # Formatos:
        #   *                     -> kind='STAR'
        #   col1, col2, ...       -> kind='COL' por cada item
        #   AGG ( col_or_* )      -> kind='AGG'
        #   AGG ( col_or_* ) AS alias
        if not proj_tokens:
            raise Exception("Proyección vacía")

        if proj_tokens == ["*"]:
            return {"items": [{"kind": "STAR"}]}

        items = []
        i = 0
        while i < len(proj_tokens):
            tok = proj_tokens[i]
            if tok == ",":
                i += 1
                continue

            # AGG ( col )
            if tok in ("COUNT", "SUM", "AVG", "MIN", "MAX"):
                if i + 3 >= len(proj_tokens) or proj_tokens[i + 1] != "(" or proj_tokens[i + 3] != ")":
                    raise Exception(f"Agregado {tok} mal formado")
                arg = proj_tokens[i + 2]
                alias = None
                next_i = i + 4
                if next_i < len(proj_tokens) and proj_tokens[next_i] == "AS":
                    if next_i + 1 >= len(proj_tokens):
                        raise Exception("AS sin alias")
                    alias = proj_tokens[next_i + 1]
                    next_i += 2
                items.append({"kind": "AGG", "op": tok, "arg": arg, "alias": alias})
                i = next_i
                continue

            # columna simple
            items.append({"kind": "COL", "name": tok})
            i += 1

        return {"items": items}

    # -----------------------------
    # JOIN clause
    # -----------------------------
    def _parse_join_clause(self, from_tokens):
        # Soporta: t1 [INNER] JOIN t2 ON t1.col = t2.col
        # o:       t1 [INNER] JOIN t2 ON col_l = col_r   (sin prefijo, asume left/right por orden)
        if not from_tokens or from_tokens[0] in ("JOIN", "INNER"):
            raise Exception("JOIN sin tabla izquierda")

        left = from_tokens[0]
        i = 1
        if i < len(from_tokens) and from_tokens[i] == "INNER":
            i += 1
        if i >= len(from_tokens) or from_tokens[i] != "JOIN":
            raise Exception("JOIN inválido")
        i += 1

        if i >= len(from_tokens):
            raise Exception("JOIN sin tabla derecha")
        right = from_tokens[i]
        i += 1

        if i >= len(from_tokens) or from_tokens[i] != "ON":
            raise Exception("JOIN sin ON")
        i += 1

        if i + 2 >= len(from_tokens) or from_tokens[i + 1] != "=":
            raise Exception("ON inválido")

        lhs, _, rhs = from_tokens[i], from_tokens[i + 1], from_tokens[i + 2]

        on_left = lhs.split(".")[1] if "." in lhs else lhs
        on_right = rhs.split(".")[1] if "." in rhs else rhs
        # Si los prefijos están invertidos (right.col = left.col), reordenar.
        if "." in lhs and lhs.split(".")[0] == right and "." in rhs and rhs.split(".")[0] == left:
            on_left, on_right = on_right, on_left

        return {"left": left, "right": right, "on_left": on_left, "on_right": on_right}

    # -----------------------------
    # WHERE
    # -----------------------------
    def _parse_where(self, table, tokens, where_idx):
        col = tokens[where_idx + 1]

        # Caso 1: =
        if "=" in tokens[where_idx + 1:]:
            eq_off = tokens[where_idx + 1:].index("=")
            eq_idx = where_idx + 1 + eq_off
            value = self._parse_value(tokens[eq_idx + 1])
            return {"type": "EQUAL", "column": col, "value": value}

        # Caso 2: BETWEEN
        if "BETWEEN" in tokens[where_idx + 1:]:
            bt_off = tokens[where_idx + 1:].index("BETWEEN")
            bt_idx = where_idx + 1 + bt_off
            v1 = self._parse_value(tokens[bt_idx + 1])
            v2 = self._parse_value(tokens[bt_idx + 3])
            return {"type": "BETWEEN", "column": col, "begin": v1, "end": v2}

        # Caso 3: IN espacial (POINT/RADIUS/K)
        if "IN" in tokens[where_idx + 1:]:
            return self._parse_spatial_in_cond(col, tokens)

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
    def _parse_spatial_in_cond(self, column, tokens):
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
                "type": "IN_RADIUS",
                "column": column,
                "point": (x, y),
                "radius": radius,
            }

        if "K" in tokens:
            k_idx = tokens.index("K")
            kval = self._parse_value(tokens[k_idx + 1])
            return {
                "type": "IN_KNN",
                "column": column,
                "point": (x, y),
                "k": kval,
            }

        raise Exception("IN espacial inválido")

    def _parse_value(self, token):
        try:
            return int(token)
        except ValueError:
            pass
        try:
            return float(token)
        except ValueError:
            return token

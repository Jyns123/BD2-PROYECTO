def tokenize(query: str):
    # Lexer simple para el subconjunto SQL del curso
    tokens = []
    i = 0
    n = len(query)

    keywords = {
        "SELECT", "FROM", "WHERE", "BETWEEN", "AND",
        "INSERT", "INTO", "VALUES", "CREATE", "TABLE",
        "INDEX", "BPLUSTREE", "HASH", "SEQUENTIAL", "HEAP", "RTREE",
        "DELETE", "FILE", "IN", "POINT", "RADIUS", "K",
        "INT", "FLOAT", "TEXT"
    }

    while i < n:
        ch = query[i]

        if ch.isspace():
            i += 1
            continue

        if ch in "(),;=":
            tokens.append(ch)
            i += 1
            continue

        # strings en comillas simples: 'hola mundo'
        if ch == "'":
            i += 1
            start = i
            while i < n and query[i] != "'":
                i += 1
            literal = query[start:i]
            tokens.append(literal)
            if i < n and query[i] == "'":
                i += 1
            continue

        # numeros (int/float) con signo opcional
        if ch.isdigit() or (ch in "+-" and i + 1 < n and query[i + 1].isdigit()):
            start = i
            i += 1
            dot = False
            while i < n and (query[i].isdigit() or (query[i] == "." and not dot)):
                if query[i] == ".":
                    dot = True
                i += 1
            tokens.append(query[start:i])
            continue

        # identificadores / keywords
        start = i
        while i < n and (query[i].isalnum() or query[i] == "_"):
            i += 1
        word = query[start:i]
        if not word:
            # caracter desconocido: saltar para evitar loop
            i += 1
            continue
        up = word.upper()
        tokens.append(up if up in keywords else word)

    return tokens

def tokenize(query: str):
    query = query.replace("(", " ( ").replace(")", " ) ")
    query = query.replace(",", " , ")
    tokens = query.strip().split()

    # normalizar
    return [t.upper() if t.upper() in {
        "SELECT", "FROM", "WHERE", "BETWEEN", "AND", "INSERT", "INTO", "VALUES"
    } else t for t in tokens]
def tokenize(query: str):
    query = query.replace(';', ' ').replace('(', ' ( ').replace(')', ' ) ')
    query = query.replace(',', ' , ')
    tokens = query.strip().split()

    # normalizar
    keywords = {
        'SELECT', 'FROM', 'WHERE', 'BETWEEN', 'AND', 'INSERT', 'INTO', 'VALUES',
        'CREATE', 'TABLE', 'INDEX', 'BPLUSTREE', 'HASH', 'SEQUENTIAL', 'HEAP', 'RTREE'
    }
    
    return [t.upper() if t.upper() in keywords else t for t in tokens]

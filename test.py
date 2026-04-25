import os
from index.bplustree import BPlusTree

# -----------------------------
# CONFIG
# -----------------------------
FILE = "bpt_test.bin"
RECORD_SIZE = 100


def key_fn(r):
    return int(r[:4].decode())


def make_record(i):
    return str(i).zfill(4).encode().ljust(RECORD_SIZE, b' ')


def reset():
    if os.path.exists(FILE):
        os.remove(FILE)


def assert_equal(a, b, msg):
    if a != b:
        raise Exception(f"[ERROR] {msg} -> esperado: {b}, obtenido: {a}")
    else:
        print(f"[OK] {msg}")


# -----------------------------
# TESTS
# -----------------------------

def test_basic_insert_search():
    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn, order=4)

    for i in range(10):
        tree.insert(make_record(i))

    res = tree.search(5)
    assert_equal(len(res), 1, "Search básico")

    tree.close()


def test_range():
    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn, order=4)

    for i in range(10):
        tree.insert(make_record(i))

    res = tree.range_search(3, 7)
    keys = [key_fn(r) for r in res]

    assert_equal(keys, [3, 4, 5, 6, 7], "Range search correcto")

    tree.close()


def test_duplicates():
    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn, order=4)

    for _ in range(5):
        tree.insert(make_record(5))

    res = tree.search(5)
    assert_equal(len(res), 5, "Duplicados soportados")

    tree.close()


def test_not_found():
    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn, order=4)

    for i in range(10):
        tree.insert(make_record(i))

    res = tree.search(999)
    assert_equal(res, [], "Búsqueda inexistente")

    tree.close()


def test_large_insert():
    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn, order=4)

    for i in range(500):
        tree.insert(make_record(i))

    res = tree.search(123)
    assert_equal(len(res), 1, "Inserción grande funciona")

    tree.close()


def test_range_empty():
    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn, order=4)

    for i in range(10):
        tree.insert(make_record(i))

    res = tree.range_search(1000, 2000)
    assert_equal(res, [], "Range vacío")

    tree.close()


# -----------------------------
# RUN
# -----------------------------
if __name__ == "__main__":
    print("\n--- TEST B+ TREE ---\n")

    test_basic_insert_search()
    test_range()
    test_duplicates()
    test_not_found()
    test_large_insert()
    test_range_empty()

    print("\nTODOS LOS TESTS PASARON\n")
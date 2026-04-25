import os
import time
from index.bplustree import BPlusTree

FILE = "test.db"
RECORD_SIZE = 100


# -------------------------
# helpers
# -------------------------

def reset():
    if os.path.exists(FILE):
        os.remove(FILE)


def make_record(key: int) -> bytes:
    # clave en los primeros 4 bytes (string padded)
    return str(key).zfill(4).encode().ljust(RECORD_SIZE, b' ')


def key_fn(record: bytes) -> int:
    return int(record[:4].decode())


def assert_equal(a, b, msg):
    if a != b:
        raise Exception(f"[ERROR] {msg} -> esperado: {b}, obtenido: {a}")
    print("[OK]", msg)


def assert_true(cond, msg):
    if not cond:
        raise Exception(f"[ERROR] {msg}")
    print("[OK]", msg)


# -------------------------
# TESTS
# -------------------------

def test_basic():
    print("\n--- TEST BÁSICO ---")

    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn)

    tree.insert(make_record(5))

    res = tree.search(5)
    assert_equal(len(res), 1, "Insert + search básico")

    tree.close()


def test_duplicates():
    print("\n--- TEST DUPLICADOS ---")

    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn)

    for _ in range(11):
        tree.insert(make_record(5))

    res = tree.search(5)

    print("Encontrados:", len(res))
    assert_equal(len(res), 11, "Duplicados exactos")

    tree.close()


def test_split_heavy_duplicates():
    print("\n--- TEST SPLIT CON DUPLICADOS ---")

    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn, order=4)

    for _ in range(50):
        tree.insert(make_record(5))

    res = tree.search(5)

    print("Encontrados:", len(res))
    assert_equal(len(res), 50, "Duplicados en múltiples hojas")

    tree.close()


def test_mixed_data():
    print("\n--- TEST DATOS MIXTOS ---")

    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn)

    for i in range(100):
        tree.insert(make_record(i))

    # duplicados de 5
    for _ in range(10):
        tree.insert(make_record(5))

    res = tree.search(5)
    assert_equal(len(res), 11, "Duplicados + únicos")

    tree.close()


def test_range():
    print("\n--- TEST RANGE ---")

    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn)

    for i in range(100):
        tree.insert(make_record(i))

    res = tree.range_search(10, 20)

    assert_equal(len(res), 11, "Range correcto")

    tree.close()


def test_persistence():
    print("\n--- TEST PERSISTENCIA ---")

    reset()

    tree = BPlusTree(FILE, RECORD_SIZE, key_fn)

    for i in range(100):
        tree.insert(make_record(i))

    for _ in range(10):
        tree.insert(make_record(5))

    tree.close()

    # reabrir
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn)

    res = tree.search(5)
    assert_equal(len(res), 11, "Persistencia de duplicados")

    tree.close()


def test_large():
    print("\n--- TEST GRANDE ---")

    reset()
    tree = BPlusTree(FILE, RECORD_SIZE, key_fn)

    n = 1000

    t0 = time.time()

    for i in range(n):
        tree.insert(make_record(i % 100))  # genera duplicados

    t1 = time.time()

    print("Insert tiempo (ms):", (t1 - t0) * 1000)

    res = tree.search(42)
    assert_true(len(res) > 0, "Search en dataset grande")

    tree.close()


# -------------------------
# RUN
# -------------------------

def run():
    print("\n--- FULL B+ TREE TEST ---")

    test_basic()
    test_duplicates()
    test_split_heavy_duplicates()
    test_mixed_data()
    test_range()
    test_persistence()
    test_large()

    print("\nTODOS LOS TESTS PASARON")


if __name__ == "__main__":
    run()
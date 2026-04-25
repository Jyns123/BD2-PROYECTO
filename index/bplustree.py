import pickle
from storage.disk_manager import DiskManager

PAGE_SIZE = 4096


# -----------------------------
# NODOS
# -----------------------------

class LeafNode:
    def __init__(self):
        self.is_leaf = True
        self.keys = []
        self.records = []
        self.next_leaf = -1


class InternalNode:
    def __init__(self):
        self.is_leaf = False
        self.keys = []
        self.children = []  # len = keys + 1


# -----------------------------
# B+ TREE
# -----------------------------

class BPlusTree:

    def __init__(self, path, record_size, key_fn, order=4):
        self.dm = DiskManager(path)
        self.record_size = record_size
        self.key_fn = key_fn
        self.order = order

        self.root = self._new_leaf()

    # -----------------------------
    # SERIALIZACIÓN
    # -----------------------------

    def _write_node(self, pid, node):
        data = pickle.dumps(node)
        if len(data) > PAGE_SIZE:
            raise ValueError("Nodo excede PAGE_SIZE")
        data = data.ljust(PAGE_SIZE, b'\x00')
        self.dm.write_page(pid, data)

    def _read_node(self, pid):
        raw = self.dm.read_page(pid)
        return pickle.loads(raw.rstrip(b'\x00'))

    # -----------------------------
    # CREACIÓN
    # -----------------------------

    def _new_leaf(self):
        pid = self.dm.allocate_page()
        self._write_node(pid, LeafNode())
        return pid

    def _new_internal(self):
        pid = self.dm.allocate_page()
        self._write_node(pid, InternalNode())
        return pid

    # -----------------------------
    # FIND LEAF (FIX CRÍTICO)
    # -----------------------------

    def _find_leaf(self, node_id, key):
        node = self._read_node(node_id)

        if node.is_leaf:
            return node_id

        i = 0
        # CAMBIO CLAVE: > en vez de >=
        while i < len(node.keys) and key > node.keys[i]:
            i += 1

        return self._find_leaf(node.children[i], key)

    # -----------------------------
    # SEARCH (soporta duplicados)
    # -----------------------------

    def search(self, key):
        leaf_id = self._find_leaf(self.root, key)
        results = []

        while leaf_id != -1:
            leaf = self._read_node(leaf_id)

            for k, r in zip(leaf.keys, leaf.records):
                if k == key:
                    results.append(r)
                elif k > key:
                    return results

            leaf_id = leaf.next_leaf

        return results

    # -----------------------------
    # RANGE SEARCH
    # -----------------------------

    def range_search(self, begin, end):
        leaf_id = self._find_leaf(self.root, begin)
        results = []

        while leaf_id != -1:
            leaf = self._read_node(leaf_id)

            for k, r in zip(leaf.keys, leaf.records):
                if begin <= k <= end:
                    results.append(r)
                elif k > end:
                    return results

            leaf_id = leaf.next_leaf

        return results

    # -----------------------------
    # INSERT
    # -----------------------------

    def insert(self, record):
        key = self.key_fn(record)

        split = self._insert_recursive(self.root, key, record)

        if split:
            new_root = self._new_internal()
            root_node = self._read_node(new_root)

            root_node.keys = [split[0]]
            root_node.children = [self.root, split[1]]

            self._write_node(new_root, root_node)
            self.root = new_root

    def _insert_recursive(self, node_id, key, record):
        node = self._read_node(node_id)

        # ---------------- LEAF ----------------
        if node.is_leaf:
            self._insert_in_leaf(node, key, record)

            if len(node.keys) < self.order:
                self._write_node(node_id, node)
                return None

            return self._split_leaf(node_id, node)

        # ---------------- INTERNAL ----------------
        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1

        child_id = node.children[i]
        split = self._insert_recursive(child_id, key, record)

        if not split:
            return None

        split_key, new_child = split

        node.keys.insert(i, split_key)
        node.children.insert(i + 1, new_child)

        if len(node.children) != len(node.keys) + 1:
            raise Exception("Invariante rota: children != keys + 1")

        if len(node.keys) < self.order:
            self._write_node(node_id, node)
            return None

        return self._split_internal(node_id, node)

    # -----------------------------
    # INSERT EN HOJA
    # -----------------------------

    def _insert_in_leaf(self, node, key, record):
        i = 0
        while i < len(node.keys) and node.keys[i] <= key:
            i += 1

        node.keys.insert(i, key)
        node.records.insert(i, record)

    # -----------------------------
    # SPLIT LEAF
    # -----------------------------

    def _split_leaf(self, node_id, node):
        mid = len(node.keys) // 2

        right = LeafNode()
        right.keys = node.keys[mid:]
        right.records = node.records[mid:]

        node.keys = node.keys[:mid]
        node.records = node.records[:mid]

        new_id = self._new_leaf()

        right.next_leaf = node.next_leaf
        node.next_leaf = new_id

        self._write_node(node_id, node)
        self._write_node(new_id, right)

        return right.keys[0], new_id

    # -----------------------------
    # SPLIT INTERNAL
    # -----------------------------

    def _split_internal(self, node_id, node):
        mid = len(node.keys) // 2

        split_key = node.keys[mid]

        right = InternalNode()
        right.keys = node.keys[mid + 1:]
        right.children = node.children[mid + 1:]

        node.keys = node.keys[:mid]
        node.children = node.children[:mid + 1]

        if len(node.children) != len(node.keys) + 1:
            raise Exception("Error split izquierda")

        if len(right.children) != len(right.keys) + 1:
            raise Exception("Error split derecha")

        new_id = self._new_internal()

        self._write_node(node_id, node)
        self._write_node(new_id, right)

        return split_key, new_id

    # -----------------------------
    # CLOSE
    # -----------------------------

    def close(self):
        self.dm.close()
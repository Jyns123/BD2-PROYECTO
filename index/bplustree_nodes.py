# index/bplustree_nodes.py

import struct


class LeafNode:
    def __init__(self, record_size):
        self.is_leaf = True          # FIX: bool consistente con bplustree.py (data[0] == 1)
        self.next = -1               # FIX: renombrado de next_leaf → next para coincidir con Node.next
        self.keys = []
        self.children = []           # FIX: renombrado de records → children para coincidir con Node.children
        self.record_size = record_size

    def insert(self, key, record):
        # FIX: inserción posicional O(n) en lugar de append+sort
        # Evita el bug de zip(*[]) cuando la lista está vacía
        i = 0
        while i < len(self.keys) and self.keys[i] < key:
            i += 1
        self.keys.insert(i, key)
        self.children.insert(i, record)

    def is_full(self, max_keys):
        # FIX: > en lugar de >= para respetar orden del árbol
        # Con order=4: splitear cuando keys > 4, no cuando keys >= 4
        return len(self.keys) > max_keys


class InternalNode:
    def __init__(self):
        self.is_leaf = False         # FIX: bool consistente
        self.next = -1               # FIX: campo next faltante (aunque no se usa en internos,
        self.keys = []               #      su ausencia causaría AttributeError si se accede)
        self.children = []

    def is_full(self, max_keys):
        # FIX: mismo fix que LeafNode
        return len(self.keys) > max_keys
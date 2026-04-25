# index/bplustree_nodes.py

import struct

class LeafNode:
    def __init__(self, record_size):
        self.is_leaf = 1
        self.next_leaf = -1
        self.keys = []
        self.records = []
        self.record_size = record_size

    def insert(self, key, record):
        self.keys.append(key)
        self.records.append(record)

        # mantener orden
        combined = list(zip(self.keys, self.records))
        combined.sort(key=lambda x: x[0])

        self.keys, self.records = zip(*combined)
        self.keys = list(self.keys)
        self.records = list(self.records)

    def is_full(self, max_keys):
        return len(self.keys) >= max_keys


class InternalNode:
    def __init__(self):
        self.is_leaf = 0
        self.keys = []
        self.children = []

    def is_full(self, max_keys):
        return len(self.keys) >= max_keys
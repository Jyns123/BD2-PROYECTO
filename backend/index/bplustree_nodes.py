# index/bplustree_nodes.py

import struct


class LeafNode:
    def __init__(self, record_size):
        self.is_leaf = True          
        self.next = -1              
        self.keys = []
        self.children = []           
        self.record_size = record_size

    def insert(self, key, record):

        i = 0
        while i < len(self.keys) and self.keys[i] < key:
            i += 1
        self.keys.insert(i, key)
        self.children.insert(i, record)

    def is_full(self, max_keys):

        return len(self.keys) > max_keys


class InternalNode:
    def __init__(self):
        self.is_leaf = False         
        self.next = -1               
        self.keys = []              
        self.children = []

    def is_full(self, max_keys):
        return len(self.keys) > max_keys
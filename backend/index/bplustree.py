import struct
from storage.disk_manager import DiskManager, PAGE_SIZE


class Node:
    def __init__(self, is_leaf):
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []
        self.next = -1


class BPlusTree:
    def __init__(self, file_path, record_size, key_extractor, order=4):
        self.dm = DiskManager(file_path)
        self.record_size = record_size
        self.key_extractor = key_extractor
        self.order = order
        self.cache = {}

        if self.dm._get_total_pages() == 1:
            self.root = self._new_leaf()
            self.dm.set_root(self.root)      # persistir root inicial
        else:
            self.root = self.dm.get_root()   # leer root real desde metadata

    # -------------------------
    # SERIALIZACIÓN
    # -------------------------

    def _serialize(self, node):
        data = bytearray(PAGE_SIZE)
        data[0] = 1 if node.is_leaf else 0
        data[1:5] = len(node.keys).to_bytes(4, 'big')

        offset = 5
        for k in node.keys:
            data[offset:offset+4] = int(k).to_bytes(4, 'big')
            offset += 4

        next_ptr = node.next if node.next >= 0 else 0
        data[offset:offset+4] = next_ptr.to_bytes(4, 'big')
        offset += 4

        if node.is_leaf:
            for rec in node.children:
                data[offset:offset+self.record_size] = rec
                offset += self.record_size
        else:
            for child in node.children:
                data[offset:offset+4] = int(child).to_bytes(4, 'big')
                offset += 4

        return bytes(data)

    def _deserialize(self, data):
        is_leaf = data[0] == 1
        n_keys = int.from_bytes(data[1:5], 'big')

        node = Node(is_leaf)
        offset = 5

        for _ in range(n_keys):
            k = int.from_bytes(data[offset:offset+4], 'big')
            node.keys.append(k)
            offset += 4

        node.next = int.from_bytes(data[offset:offset+4], 'big')
        if node.next == 0:
            node.next = -1
        offset += 4

        if is_leaf:
            for _ in range(n_keys):
                rec = data[offset:offset+self.record_size]
                node.children.append(rec)
                offset += self.record_size
        else:
            for _ in range(n_keys + 1):
                child = int.from_bytes(data[offset:offset+4], 'big')
                node.children.append(child)
                offset += 4

        return node

    # -------------------------
    # IO
    # -------------------------

    def _read_node(self, page_id):
        if page_id in self.cache:
            return self.cache[page_id]
        data = self.dm.read_page(page_id)
        node = self._deserialize(data)
        self.cache[page_id] = node
        return node

    def _write_node(self, page_id, node):
        self.cache[page_id] = node
        self.dm.write_page(page_id, self._serialize(node))

    def _new_leaf(self):
        node = Node(is_leaf=True)
        pid = self.dm.allocate_page()
        self._write_node(pid, node)
        return pid

    def _new_internal(self):
        node = Node(is_leaf=False)
        pid = self.dm.allocate_page()
        self._write_node(pid, node)
        return pid

    # -------------------------
    # INSERT
    # -------------------------

    def insert(self, record):
        self.cache.clear()                           # simular disco real
        key = self.key_extractor(record)
        split = self._insert_recursive(self.root, key, record)

        if split:
            new_root = Node(is_leaf=False)
            new_root.keys = [split[0]]
            new_root.children = [self.root, split[1]]

            root_id = self.dm.allocate_page()
            self._write_node(root_id, new_root)
            self.root = root_id
            self.dm.set_root(self.root)              # persistir nuevo root

    def _insert_recursive(self, node_id, key, record):
        node = self._read_node(node_id)

        if node.is_leaf:
            i = 0
            while i < len(node.keys) and node.keys[i] < key:
                i += 1
            node.keys.insert(i, key)
            node.children.insert(i, record)

            if len(node.keys) < self.order:
                self._write_node(node_id, node)
                return None
            return self._split_leaf(node_id, node)

        else:
            i = 0
            while i < len(node.keys) and key > node.keys[i]:
                i += 1
            split = self._insert_recursive(node.children[i], key, record)

            if not split:
                return None

            new_key, new_child = split
            node.keys.insert(i, new_key)
            node.children.insert(i + 1, new_child)

            if len(node.keys) < self.order:
                self._write_node(node_id, node)
                return None
            return self._split_internal(node_id, node)

    # -------------------------
    # SPLIT
    # -------------------------

    def _split_leaf(self, node_id, node):
        mid = len(node.keys) // 2

        left = Node(is_leaf=True)
        right = Node(is_leaf=True)

        left.keys = node.keys[:mid]
        left.children = node.children[:mid]
        right.keys = node.keys[mid:]
        right.children = node.children[mid:]
        right.next = node.next

        new_id = self.dm.allocate_page()
        left.next = new_id

        self._write_node(node_id, left)
        self._write_node(new_id, right)

        return right.keys[0], new_id

    def _split_internal(self, node_id, node):
        mid = len(node.keys) // 2
        promote = node.keys[mid]

        left = Node(is_leaf=False)
        right = Node(is_leaf=False)

        left.keys = node.keys[:mid]
        left.children = node.children[:mid + 1]
        right.keys = node.keys[mid + 1:]
        right.children = node.children[mid + 1:]

        new_id = self.dm.allocate_page()

        self._write_node(node_id, left)
        self._write_node(new_id, right)

        return promote, new_id

    # -------------------------
    # SEARCH
    # -------------------------

    def search(self, key):
        self.cache.clear()                           # simular disco real
        node_id = self.root

        while True:
            node = self._read_node(node_id)

            if node.is_leaf:
                res = []
                i = 0
                while i < len(node.keys):
                    if node.keys[i] == key:
                        res.append(node.children[i])
                    elif node.keys[i] > key:
                        return res
                    i += 1

                next_id = node.next
                while next_id != -1:
                    node = self._read_node(next_id)
                    for i in range(len(node.keys)):
                        if node.keys[i] == key:
                            res.append(node.children[i])
                        elif node.keys[i] > key:
                            return res
                    next_id = node.next

                return res

            else:
                i = 0
                while i < len(node.keys) and key > node.keys[i]:
                    i += 1
                node_id = node.children[i]

    # -------------------------
    # RANGE SEARCH
    # -------------------------

    def range_search(self, start, end):
        self.cache.clear()                           # simular disco real
        node_id = self.root

        while True:
            node = self._read_node(node_id)
            if node.is_leaf:
                break
            i = 0
            while i < len(node.keys) and start > node.keys[i]:
                i += 1
            node_id = node.children[i]

        res = []
        while node_id != -1:
            node = self._read_node(node_id)
            for i in range(len(node.keys)):
                k = node.keys[i]
                if start <= k <= end:
                    res.append(node.children[i])
                elif k > end:
                    return res
            node_id = node.next

        return res

    # -------------------------
    # CLOSE
    # -------------------------

    def close(self):
        self.dm.close()
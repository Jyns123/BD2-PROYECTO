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
            self.dm.set_root(self.root)      
        else:
            self.root = self.dm.get_root()   

    # SERIALIZACIÓN

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

    
    # IO

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

    # INSERT

    def insert(self, record):
        self.cache.clear()                          
        key = self.key_extractor(record)
        split = self._insert_recursive(self.root, key, record)

        if split:
            new_root = Node(is_leaf=False)
            new_root.keys = [split[0]]
            new_root.children = [self.root, split[1]]

            root_id = self.dm.allocate_page()
            self._write_node(root_id, new_root)
            self.root = root_id
            self.dm.set_root(self.root)              

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

    # SPLIT

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

    # SEARCH

    def search(self, key):
        self.cache.clear()                          
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

    # RANGE SEARCH

    def range_search(self, start, end):
        self.cache.clear()                          
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

    # SCAN

    def scan(self):
        self.cache.clear()
        node_id = self.root

        while True:
            node = self._read_node(node_id)
            if node.is_leaf:
                break
            node_id = node.children[0]

        res = []
        while node_id != -1:
            node = self._read_node(node_id)
            res.extend(node.children)
            node_id = node.next

        return res

    # REMOVE

    def remove(self, key):
        self.cache.clear()                          
        path = []
        leaf_id = self._find_leaf_with_path(key, path)
        removed = self._delete_from_leaf(leaf_id, key)
        if removed == 0:
            return 0

        self._rebalance_after_delete(leaf_id, path)
        return removed

    def _find_leaf_with_path(self, key, path):
        node_id = self.root
        while True:
            node = self._read_node(node_id)
            if node.is_leaf:
                return node_id
            i = 0
            while i < len(node.keys) and key > node.keys[i]:
                i += 1
            path.append((node_id, i))
            node_id = node.children[i]

    def _delete_from_leaf(self, leaf_id, key):
        node = self._read_node(leaf_id)
        if not node.is_leaf:
            return 0

        new_keys = []
        new_children = []
        removed = 0

        for k, rec in zip(node.keys, node.children):
            if k == key:
                removed += 1
            else:
                new_keys.append(k)
                new_children.append(rec)

        if removed > 0:
            node.keys = new_keys
            node.children = new_children
            self._write_node(leaf_id, node)

        return removed

    def _rebalance_after_delete(self, node_id, path):
        min_keys = max(1, self.order // 2)

        while True:
            node = self._read_node(node_id)

            if node_id == self.root:
                if not node.is_leaf and len(node.keys) == 0:
                    self.root = node.children[0]
                    self.dm.set_root(self.root)
                return

            if len(node.keys) >= min_keys:
                return

            if not path:
                return

            parent_id, idx = path.pop()
            parent = self._read_node(parent_id)

            left_id = parent.children[idx - 1] if idx > 0 else None
            right_id = parent.children[idx + 1] if idx + 1 < len(parent.children) else None

            if left_id is not None:
                left = self._read_node(left_id)
                if len(left.keys) > min_keys:
                    if node.is_leaf:
                        node.keys.insert(0, left.keys.pop())
                        node.children.insert(0, left.children.pop())
                        parent.keys[idx - 1] = node.keys[0]
                    else:
                        sep = parent.keys[idx - 1]
                        borrowed_key = left.keys.pop()
                        borrowed_child = left.children.pop()
                        node.keys.insert(0, sep)
                        node.children.insert(0, borrowed_child)
                        parent.keys[idx - 1] = borrowed_key

                    self._write_node(left_id, left)
                    self._write_node(node_id, node)
                    self._write_node(parent_id, parent)
                    return

            if right_id is not None:
                right = self._read_node(right_id)
                if len(right.keys) > min_keys:
                    if node.is_leaf:
                        node.keys.append(right.keys.pop(0))
                        node.children.append(right.children.pop(0))
                        parent.keys[idx] = right.keys[0]
                    else:
                        sep = parent.keys[idx]
                        borrowed_key = right.keys.pop(0)
                        borrowed_child = right.children.pop(0)
                        node.keys.append(sep)
                        node.children.append(borrowed_child)
                        parent.keys[idx] = borrowed_key

                    self._write_node(right_id, right)
                    self._write_node(node_id, node)
                    self._write_node(parent_id, parent)
                    return

            if left_id is not None:
                left = self._read_node(left_id)
                if node.is_leaf:
                    left.keys.extend(node.keys)
                    left.children.extend(node.children)
                    left.next = node.next
                else:
                    sep = parent.keys[idx - 1]
                    left.keys.append(sep)
                    left.keys.extend(node.keys)
                    left.children.extend(node.children)

                parent.keys.pop(idx - 1)
                parent.children.pop(idx)

                self._write_node(left_id, left)
                self._write_node(parent_id, parent)
                node_id = parent_id
                continue

            if right_id is not None:
                right = self._read_node(right_id)
                if node.is_leaf:
                    node.keys.extend(right.keys)
                    node.children.extend(right.children)
                    node.next = right.next
                else:
                    sep = parent.keys[idx]
                    node.keys.append(sep)
                    node.keys.extend(right.keys)
                    node.children.extend(right.children)

                parent.keys.pop(idx)
                parent.children.pop(idx + 1)

                self._write_node(node_id, node)
                self._write_node(parent_id, parent)
                node_id = parent_id
                continue

    # CLOSE

    def close(self):
        self.dm.close()
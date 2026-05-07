"""
index/rtree.py

R-Tree persistente en páginas de disco.

Estructura:
- Cada nodo ocupa exactamente una página (PAGE_SIZE bytes).
- Nodos hoja: almacenan puntos (x, y) + registro.
- Nodos internos: almacenan MBRs (bounding boxes) + punteros a hijos.
- Orden M = máximo de entradas por nodo (calculado según PAGE_SIZE y record_size).

Operaciones:
- insert(record)           — inserta un punto
- range_search(cx, cy, r)  — todos los puntos dentro del radio r del centro (cx, cy)
- knn(cx, cy, k)           — k vecinos más cercanos al punto (cx, cy)
- get_mbrs()               — extrae todos los MBRs por nivel (para visualización)

Serialización de una página:
  Byte 0:     is_leaf (1 = hoja, 0 = interno)
  Bytes 1-4:  n_entries (int big-endian)
  Bytes 5+:   entradas

  Entrada en nodo interno:
    [min_x: f32][min_y: f32][max_x: f32][max_y: f32][child_page_id: i32]
    → 4+4+4+4+4 = 20 bytes

  Entrada en nodo hoja:
    [x: f32][y: f32][record: bytes]
"""

import struct
import math
import heapq
from storage.disk_manager import DiskManager, PAGE_SIZE

# -------------------------------------------------------
# LAYOUT DE SERIALIZACIÓN
# -------------------------------------------------------
INTERNAL_ENTRY_SIZE = 4 * 4 + 4   # 20 bytes
LEAF_COORD_SIZE     = 4 * 2        # 8 bytes
PAGE_HEADER         = 5            # is_leaf(1) + n_entries(4)


# -------------------------------------------------------
# MBR (Minimum Bounding Rectangle)
# -------------------------------------------------------
class MBR:
    __slots__ = ["min_x", "min_y", "max_x", "max_y"]

    def __init__(self, min_x, min_y, max_x, max_y):
        self.min_x = min_x
        self.min_y = min_y
        self.max_x = max_x
        self.max_y = max_y

    @classmethod
    def from_point(cls, x, y):
        return cls(x, y, x, y)

    def expand(self, other):
        return MBR(
            min(self.min_x, other.min_x),
            min(self.min_y, other.min_y),
            max(self.max_x, other.max_x),
            max(self.max_y, other.max_y),
        )

    def area(self):
        return (self.max_x - self.min_x) * (self.max_y - self.min_y)

    def area_enlargement(self, other):
        return self.expand(other).area() - self.area()

    def min_dist_to_point(self, x, y):
        dx = max(self.min_x - x, 0, x - self.max_x)
        dy = max(self.min_y - y, 0, y - self.max_y)
        return math.sqrt(dx * dx + dy * dy)

    def intersects_circle(self, cx, cy, r):
        return self.min_dist_to_point(cx, cy) <= r

    def pack(self):
        return struct.pack(">ffff", self.min_x, self.min_y, self.max_x, self.max_y)

    @classmethod
    def unpack(cls, data, offset=0):
        min_x, min_y, max_x, max_y = struct.unpack_from(">ffff", data, offset)
        return cls(min_x, min_y, max_x, max_y)


# -------------------------------------------------------
# NODE (en memoria)
# -------------------------------------------------------
class RNode:
    def __init__(self, is_leaf):
        self.is_leaf = is_leaf
        # hoja:    entries = [ (x, y, record_bytes), ... ]
        # interno: entries = [ (MBR, child_page_id), ... ]
        self.entries = []


# -------------------------------------------------------
# R-TREE
# -------------------------------------------------------
class RTree:
    def __init__(self, file_path: str, record_size: int, point_extractor):
        """
        file_path:        archivo .db
        record_size:      bytes por registro completo
        point_extractor:  funcion record_bytes -> (x, y)
        """
        self.dm = DiskManager(file_path)
        self.record_size = record_size
        self.point_extractor = point_extractor

        avail = PAGE_SIZE - PAGE_HEADER
        self.M_leaf     = avail // (LEAF_COORD_SIZE + record_size)
        self.M_internal = avail // INTERNAL_ENTRY_SIZE
        self.m_leaf     = max(1, self.M_leaf // 2)
        self.m_internal = max(1, self.M_internal // 2)

        if self.dm._get_total_pages() == 1:
            root_pid = self._new_leaf()
            self.root = root_pid
            self.dm.set_root(root_pid)
        else:
            self.root = self.dm.get_root()

    # -------------------------------------------------------
    # SERIALIZACIÓN
    # -------------------------------------------------------

    def _serialize(self, node: RNode) -> bytes:
        data = bytearray(PAGE_SIZE)
        data[0] = 1 if node.is_leaf else 0
        data[1:5] = len(node.entries).to_bytes(4, "big")
        offset = PAGE_HEADER

        if node.is_leaf:
            for (x, y, rec) in node.entries:
                struct.pack_into(">ff", data, offset, x, y)
                offset += 8
                data[offset:offset + self.record_size] = rec
                offset += self.record_size
        else:
            for (mbr, child_id) in node.entries:
                data[offset:offset + 16] = mbr.pack()
                offset += 16
                struct.pack_into(">i", data, offset, child_id)
                offset += 4

        return bytes(data)

    def _deserialize(self, data: bytes) -> RNode:
        is_leaf = data[0] == 1
        n = int.from_bytes(data[1:5], "big")
        node = RNode(is_leaf)
        offset = PAGE_HEADER

        if is_leaf:
            for _ in range(n):
                x, y = struct.unpack_from(">ff", data, offset)
                offset += 8
                rec = bytes(data[offset:offset + self.record_size])
                offset += self.record_size
                node.entries.append((x, y, rec))
        else:
            for _ in range(n):
                mbr = MBR.unpack(data, offset)
                offset += 16
                child_id = struct.unpack_from(">i", data, offset)[0]
                offset += 4
                node.entries.append((mbr, child_id))

        return node

    # -------------------------------------------------------
    # I/O
    # -------------------------------------------------------

    def _read_node(self, page_id: int) -> RNode:
        return self._deserialize(self.dm.read_page(page_id))

    def _write_node(self, page_id: int, node: RNode):
        self.dm.write_page(page_id, self._serialize(node))

    def _new_leaf(self) -> int:
        pid = self.dm.allocate_page()
        self._write_node(pid, RNode(is_leaf=True))
        return pid

    def _new_internal(self) -> int:
        pid = self.dm.allocate_page()
        self._write_node(pid, RNode(is_leaf=False))
        return pid

    # -------------------------------------------------------
    # MBR HELPERS
    # -------------------------------------------------------

    def _mbr_of_node(self, node: RNode) -> MBR:
        if node.is_leaf:
            xs = [e[0] for e in node.entries]
            ys = [e[1] for e in node.entries]
        else:
            xs = [e[0].min_x for e in node.entries] + [e[0].max_x for e in node.entries]
            ys = [e[0].min_y for e in node.entries] + [e[0].max_y for e in node.entries]
        return MBR(min(xs), min(ys), max(xs), max(ys))

    def _mbr_of_page(self, page_id: int) -> MBR:
        return self._mbr_of_node(self._read_node(page_id))

    # -------------------------------------------------------
    # INSERT
    # -------------------------------------------------------

    def insert(self, record: bytes):
        x, y = self.point_extractor(record)
        result = self._insert_recursive(self.root, x, y, record)

        if result is not None:
            new_mbr_left, new_mbr_right, right_pid = result
            new_root = RNode(is_leaf=False)
            new_root.entries = [
                (new_mbr_left,  self.root),
                (new_mbr_right, right_pid),
            ]
            new_root_pid = self.dm.allocate_page()
            self._write_node(new_root_pid, new_root)
            self.root = new_root_pid
            self.dm.set_root(self.root)

    def _insert_recursive(self, page_id, x, y, record):
        node = self._read_node(page_id)

        if node.is_leaf:
            node.entries.append((x, y, record))
            if len(node.entries) <= self.M_leaf:
                self._write_node(page_id, node)
                return None
            return self._split_leaf(page_id, node)
        else:
            best_i = self._choose_subtree(node, x, y)
            best_child_pid = node.entries[best_i][1]

            result = self._insert_recursive(best_child_pid, x, y, record)

            if result is None:
                new_child_mbr = self._mbr_of_page(best_child_pid)
                node.entries[best_i] = (new_child_mbr, best_child_pid)
                self._write_node(page_id, node)
                return None

            mbr_left, mbr_right, right_pid = result
            node.entries[best_i] = (mbr_left, best_child_pid)
            node.entries.append((mbr_right, right_pid))

            if len(node.entries) <= self.M_internal:
                self._write_node(page_id, node)
                return None

            return self._split_internal(page_id, node)

    def _choose_subtree(self, node: RNode, x, y) -> int:
        point_mbr = MBR.from_point(x, y)
        best_i, best_enlarge, best_area = 0, float("inf"), float("inf")
        for i, (mbr, _) in enumerate(node.entries):
            enlarge = mbr.area_enlargement(point_mbr)
            area    = mbr.area()
            if enlarge < best_enlarge or (enlarge == best_enlarge and area < best_area):
                best_enlarge, best_area, best_i = enlarge, area, i
        return best_i

    # -------------------------------------------------------
    # SPLIT
    # -------------------------------------------------------

    def _split_leaf(self, page_id, node: RNode):
        left_entries, right_entries = self._linear_split_entries(
            node.entries, key_fn=lambda e: (e[0], e[1])
        )
        left_node  = RNode(is_leaf=True);  left_node.entries  = left_entries
        right_node = RNode(is_leaf=True);  right_node.entries = right_entries

        right_pid = self.dm.allocate_page()
        self._write_node(page_id,  left_node)
        self._write_node(right_pid, right_node)

        return self._mbr_of_node(left_node), self._mbr_of_node(right_node), right_pid

    def _split_internal(self, page_id, node: RNode):
        left_entries, right_entries = self._linear_split_entries(
            node.entries,
            key_fn=lambda e: ((e[0].min_x + e[0].max_x) / 2,
                              (e[0].min_y + e[0].max_y) / 2),
        )
        left_node  = RNode(is_leaf=False); left_node.entries  = left_entries
        right_node = RNode(is_leaf=False); right_node.entries = right_entries

        right_pid = self.dm.allocate_page()
        self._write_node(page_id,  left_node)
        self._write_node(right_pid, right_node)

        return self._mbr_of_node(left_node), self._mbr_of_node(right_node), right_pid

    def _linear_split_entries(self, entries, key_fn):
        mid = len(entries) // 2
        sorted_e = sorted(entries, key=lambda e: key_fn(e)[0])
        return sorted_e[:mid], sorted_e[mid:]

    # -------------------------------------------------------
    # RANGE SEARCH
    # -------------------------------------------------------

    def range_search(self, cx: float, cy: float, r: float) -> list:
        results = []
        self._range_recursive(self.root, cx, cy, r, results)
        return results

    def _range_recursive(self, page_id, cx, cy, r, results):
        node = self._read_node(page_id)
        if node.is_leaf:
            for (x, y, rec) in node.entries:
                if math.sqrt((x - cx) ** 2 + (y - cy) ** 2) <= r:
                    results.append(rec)
        else:
            for (mbr, child_id) in node.entries:
                if mbr.intersects_circle(cx, cy, r):
                    self._range_recursive(child_id, cx, cy, r, results)

    # -------------------------------------------------------
    # SCAN (para SELECT *)
    # -------------------------------------------------------

    def scan(self) -> list:
        results = []
        self._scan_recursive(self.root, results)
        return results

    def _scan_recursive(self, page_id, results):
        node = self._read_node(page_id)
        if node.is_leaf:
            for (_x, _y, rec) in node.entries:
                results.append(rec)
        else:
            for (_mbr, child_id) in node.entries:
                self._scan_recursive(child_id, results)

    # -------------------------------------------------------
    # KNN
    # -------------------------------------------------------

    def knn(self, cx: float, cy: float, k: int) -> list:
        heap, results = [], []
        root_node = self._read_node(self.root)
        root_mbr  = self._mbr_of_node(root_node)
        heapq.heappush(heap, (root_mbr.min_dist_to_point(cx, cy), 0, self.root))

        while heap and len(results) < k:
            dist, entry_type, payload = heapq.heappop(heap)
            if entry_type == 1:
                results.append(payload)
                continue
            node = self._read_node(payload)
            if node.is_leaf:
                for (x, y, rec) in node.entries:
                    d = math.sqrt((x - cx) ** 2 + (y - cy) ** 2)
                    heapq.heappush(heap, (d, 1, rec))
            else:
                for (mbr, child_id) in node.entries:
                    heapq.heappush(heap, (mbr.min_dist_to_point(cx, cy), 0, child_id))

        return results

    # -------------------------------------------------------
    # VISUALIZACIÓN — get_mbrs
    # -------------------------------------------------------

    def get_mbrs(self) -> list:
        """
        Recorre el árbol completo y devuelve todos los MBRs con metadatos de nivel.

        Retorna lista de dicts:
          {
            "level":     int,   # 0 = raíz, aumenta hacia las hojas
            "is_leaf":   bool,
            "min_x":     float,
            "min_y":     float,
            "max_x":     float,
            "max_y":     float,
            "n_entries": int,   # entradas dentro de este nodo
            "page_id":   int,   # página en disco (útil para debug)
          }
        """
        result = []
        self._collect_mbrs(self.root, level=0, result=result)
        return result

    def _collect_mbrs(self, page_id: int, level: int, result: list):
        node = self._read_node(page_id)

        if node.is_leaf:
            if not node.entries:
                return
            xs = [e[0] for e in node.entries]
            ys = [e[1] for e in node.entries]
            result.append({
                "level":     level,
                "is_leaf":   True,
                "min_x":     float(min(xs)),
                "min_y":     float(min(ys)),
                "max_x":     float(max(xs)),
                "max_y":     float(max(ys)),
                "n_entries": len(node.entries),
                "page_id":   page_id,
            })
        else:
            if not node.entries:
                return
            result.append({
                "level":     level,
                "is_leaf":   False,
                "min_x":     float(min(e[0].min_x for e in node.entries)),
                "min_y":     float(min(e[0].min_y for e in node.entries)),
                "max_x":     float(max(e[0].max_x for e in node.entries)),
                "max_y":     float(max(e[0].max_y for e in node.entries)),
                "n_entries": len(node.entries),
                "page_id":   page_id,
            })
            for (mbr, child_id) in node.entries:
                self._collect_mbrs(child_id, level + 1, result)

    # -------------------------------------------------------
    # STATS / CLOSE
    # -------------------------------------------------------

    def get_stats(self):
        return self.dm.get_stats()

    def close(self):
        self.dm.close()
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

Serialización de una página:
  Byte 0:     is_leaf (1 = hoja, 0 = interno)
  Bytes 1-4:  n_entries (int big-endian)
  Bytes 5+:   entradas

  Entrada en nodo interno (28 bytes):
    [min_x: f64][min_y: f64][max_x: f64][max_y: f64][child_page_id: i32]
    → 8+8+8+8+4 = 36 bytes  → usamos 4 floats (f32) para ahorrar: 4+4+4+4+4 = 20 bytes

  Entrada en nodo hoja (8 + record_size bytes):
    [x: f32][y: f32][record: bytes]
"""

import struct
import math
import heapq
from storage.disk_manager import DiskManager, PAGE_SIZE

# -------------------------------------------------------
# LAYOUT DE SERIALIZACIÓN
# -------------------------------------------------------
# Nodo interno: cada entrada = min_x, min_y, max_x, max_y (4×f32) + child_id (i32)
INTERNAL_ENTRY_SIZE = 4 * 4 + 4   # 20 bytes

# Nodo hoja: cada entrada = x, y (2×f32) + record
LEAF_COORD_SIZE = 4 * 2            # 8 bytes

# Header de página: is_leaf (1) + n_entries (4) = 5 bytes
PAGE_HEADER = 5


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
        """Retorna nuevo MBR que contiene self y other."""
        return MBR(
            min(self.min_x, other.min_x),
            min(self.min_y, other.min_y),
            max(self.max_x, other.max_x),
            max(self.max_y, other.max_y),
        )

    def area(self):
        return (self.max_x - self.min_x) * (self.max_y - self.min_y)

    def area_enlargement(self, other):
        """Cuánto crece el área al incluir other."""
        expanded = self.expand(other)
        return expanded.area() - self.area()

    def min_dist_to_point(self, x, y):
        """Distancia mínima del punto (x,y) al MBR (0 si está dentro)."""
        dx = max(self.min_x - x, 0, x - self.max_x)
        dy = max(self.min_y - y, 0, y - self.max_y)
        return math.sqrt(dx * dx + dy * dy)

    def intersects_circle(self, cx, cy, r):
        return self.min_dist_to_point(cx, cy) <= r

    def pack(self):
        return struct.pack(">ffff",
            self.min_x, self.min_y,
            self.max_x, self.max_y
        )

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

        # Capacidad máxima por nodo
        avail = PAGE_SIZE - PAGE_HEADER
        self.M_leaf     = avail // (LEAF_COORD_SIZE + record_size)
        self.M_internal = avail // INTERNAL_ENTRY_SIZE
        self.m_leaf     = max(1, self.M_leaf // 2)      # mínimo (para split)
        self.m_internal = max(1, self.M_internal // 2)

        # Root
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
            # El root se splitió → crear nueva raíz
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
        """
        Retorna None si no hubo split.
        Retorna (mbr_left, mbr_right, right_page_id) si hubo split.
        """
        node = self._read_node(page_id)

        if node.is_leaf:
            node.entries.append((x, y, record))

            if len(node.entries) <= self.M_leaf:
                self._write_node(page_id, node)
                return None

            return self._split_leaf(page_id, node)

        else:
            # Elegir hijo con menor agrandamiento de MBR
            best_i = self._choose_subtree(node, x, y)
            best_child_pid = node.entries[best_i][1]

            result = self._insert_recursive(best_child_pid, x, y, record)

            if result is None:
                # Actualizar MBR del hijo en este nodo
                new_child_mbr = self._mbr_of_page(best_child_pid)
                node.entries[best_i] = (new_child_mbr, best_child_pid)
                self._write_node(page_id, node)
                return None

            # Hubo split en el hijo
            mbr_left, mbr_right, right_pid = result
            node.entries[best_i] = (mbr_left, best_child_pid)
            node.entries.append((mbr_right, right_pid))

            if len(node.entries) <= self.M_internal:
                self._write_node(page_id, node)
                return None

            return self._split_internal(page_id, node)

    def _choose_subtree(self, node: RNode, x, y) -> int:
        """Índice del hijo cuyo MBR crece menos al incluir (x, y)."""
        point_mbr = MBR.from_point(x, y)
        best_i = 0
        best_enlarge = float("inf")
        best_area = float("inf")

        for i, (mbr, _) in enumerate(node.entries):
            enlarge = mbr.area_enlargement(point_mbr)
            area = mbr.area()
            if enlarge < best_enlarge or (enlarge == best_enlarge and area < best_area):
                best_enlarge = enlarge
                best_area = area
                best_i = i

        return best_i

    # -------------------------------------------------------
    # SPLIT
    # -------------------------------------------------------

    def _split_leaf(self, page_id, node: RNode):
        left_entries, right_entries = self._linear_split_entries(
            node.entries,
            key_fn=lambda e: (e[0], e[1]),  # (x, y)
        )

        left_node = RNode(is_leaf=True)
        left_node.entries = left_entries
        right_node = RNode(is_leaf=True)
        right_node.entries = right_entries

        right_pid = self.dm.allocate_page()
        self._write_node(page_id, left_node)
        self._write_node(right_pid, right_node)

        mbr_left  = self._mbr_of_node(left_node)
        mbr_right = self._mbr_of_node(right_node)

        return mbr_left, mbr_right, right_pid

    def _split_internal(self, page_id, node: RNode):
        left_entries, right_entries = self._linear_split_entries(
            node.entries,
            key_fn=lambda e: ((e[0].min_x + e[0].max_x) / 2,
                              (e[0].min_y + e[0].max_y) / 2),
        )

        left_node = RNode(is_leaf=False)
        left_node.entries = left_entries
        right_node = RNode(is_leaf=False)
        right_node.entries = right_entries

        right_pid = self.dm.allocate_page()
        self._write_node(page_id, left_node)
        self._write_node(right_pid, right_node)

        mbr_left  = self._mbr_of_node(left_node)
        mbr_right = self._mbr_of_node(right_node)

        return mbr_left, mbr_right, right_pid

    def _linear_split_entries(self, entries, key_fn):
        """
        Linear Split (Guttman):
        - Encuentra las dos semillas más separadas en X o Y.
        - Distribuye el resto minimizando agrandamiento.
        """
        n = len(entries)
        mid = n // 2

        # Ordenar por coordenada X del centroide para el split
        sorted_entries = sorted(entries, key=lambda e: key_fn(e)[0])

        left  = sorted_entries[:mid]
        right = sorted_entries[mid:]

        return left, right

    # -------------------------------------------------------
    # RANGE SEARCH
    # -------------------------------------------------------

    def range_search(self, cx: float, cy: float, r: float) -> list:
        """
        Retorna todos los registros cuyo punto (x, y) está
        dentro del círculo de centro (cx, cy) y radio r.
        """
        results = []
        self._range_recursive(self.root, cx, cy, r, results)
        return results

    def _range_recursive(self, page_id, cx, cy, r, results):
        node = self._read_node(page_id)

        if node.is_leaf:
            for (x, y, rec) in node.entries:
                dist = math.sqrt((x - cx) ** 2 + (y - cy) ** 2)
                if dist <= r:
                    results.append(rec)
        else:
            for (mbr, child_id) in node.entries:
                if mbr.intersects_circle(cx, cy, r):
                    self._range_recursive(child_id, cx, cy, r, results)

    # -------------------------------------------------------
    # KNN
    # -------------------------------------------------------

    def knn(self, cx: float, cy: float, k: int) -> list:
        """
        Retorna los k registros más cercanos al punto (cx, cy).
        Usa Best-First Search con min-heap sobre distancia mínima al MBR.
        """
        # heap: (min_dist, is_leaf_flag, page_id_or_entry)
        # Para nodos: (dist_to_mbr, 0, page_id)
        # Para puntos: (dist_to_point, 1, record)

        heap = []
        results = []

        # distancia mínima del root al punto
        root_node = self._read_node(self.root)
        root_mbr  = self._mbr_of_node(root_node)
        heapq.heappush(heap, (root_mbr.min_dist_to_point(cx, cy), 0, self.root))

        while heap and len(results) < k:
            dist, entry_type, payload = heapq.heappop(heap)

            if entry_type == 1:
                # Es un registro (hoja)
                results.append(payload)
                continue

            # Es un nodo
            node = self._read_node(payload)

            if node.is_leaf:
                for (x, y, rec) in node.entries:
                    d = math.sqrt((x - cx) ** 2 + (y - cy) ** 2)
                    heapq.heappush(heap, (d, 1, rec))
            else:
                for (mbr, child_id) in node.entries:
                    d = mbr.min_dist_to_point(cx, cy)
                    heapq.heappush(heap, (d, 0, child_id))

        return results

    # -------------------------------------------------------
    # STATS
    # -------------------------------------------------------

    def get_stats(self):
        return self.dm.get_stats()

    # -------------------------------------------------------
    # CLOSE
    # -------------------------------------------------------

    def close(self):
        self.dm.close()
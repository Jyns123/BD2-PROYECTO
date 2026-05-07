import json
import os
import struct
import time
from typing import Dict, Optional
import threading

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from engine.engine import Engine
from parser.parser import Parser
from parser.tokenizer import tokenize
from index.bplustree import BPlusTree
from index.hash import ExtendibleHash
from index.sequential import SequentialFile
from index.rtree import RTree
from index.heap import HeapFile
from concurrency.lock_manager import LockManager

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
CATALOG_PATH = os.path.join(DATA_DIR, "catalog.json")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Esto le dice al navegador: "Sí, deja que Vite lea mis datos"
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
engine = Engine()
parser = Parser()
lock_manager = LockManager()


class QueryRequest(BaseModel):
    sql: str
    column_sizes: Optional[Dict[str, int]] = None
    base_path: Optional[str] = None


class Codec:
    def __init__(self, columns, column_sizes=None):
        self.columns = columns
        self.column_sizes = column_sizes or {}
        self.fields = []
        self.record_size = 0

        for col in columns:
            name = col.get("name")
            col_type = (col.get("type") or "TEXT").upper()
            size = self._size_for(col_type, name)
            self.fields.append((name, col_type, size, self.record_size))
            self.record_size += size

    def _size_for(self, col_type, name):
        if col_type == "INT":   return 4
        if col_type == "FLOAT": return 8
        return int(self.column_sizes.get(name, 32))

    def encode(self, values):
        if isinstance(values, (int, float, str)):
            values = [values]
        parts = []
        for i, field in enumerate(self.fields):
            col_type, size = field[1], field[2]
            raw = values[i] if i < len(values) else ""
            if col_type == "INT":
                parts.append(struct.pack(">i", int(raw) if raw != "" else 0))
            elif col_type == "FLOAT":
                parts.append(struct.pack(">d", float(raw) if raw != "" else 0.0))
            else:
                b = str(raw).encode("utf-8")[:size]
                parts.append(b.ljust(size, b"\x00"))
        return b"".join(parts)

    def decode(self, record):
        row = {}
        for name, col_type, size, offset in self.fields:
            chunk = record[offset:offset + size]
            if col_type == "INT":
                row[name] = struct.unpack(">i", chunk)[0]
            elif col_type == "FLOAT":
                row[name] = struct.unpack(">d", chunk)[0]
            else:
                row[name] = chunk.split(b"\x00", 1)[0].decode("utf-8")
        return row

    def key_extractor(self, key_col):
        idx = 0
        for i, (name, _, size, offset) in enumerate(self.fields):
            if name == key_col:
                idx = i
                break
        name, col_type, size, offset = self.fields[idx]

        def extractor(record):
            chunk = record[offset:offset + size]
            if col_type == "INT":   return struct.unpack(">i", chunk)[0]
            if col_type == "FLOAT": return struct.unpack(">d", chunk)[0]
            return chunk.split(b"\x00", 1)[0].decode("utf-8")

        return extractor

    def point_extractor(self):
            lat_idx = -1
            lon_idx = -1

            for i, col in enumerate(self.columns): 
                col_name = col['name'].lower()
                if col_name in ['lat', 'latitude', 'latitud']:
                    lat_idx = i
                elif col_name in ['lon', 'lng', 'longitude', 'longitud']:
                    lon_idx = i

            if lat_idx == -1 or lon_idx == -1:
                raise Exception("Error: La tabla espacial necesita columnas llamadas 'latitude' y 'longitude'.")

            (_, t_lat, s_lat, o_lat) = self.fields[lat_idx]
            (_, t_lon, s_lon, o_lon) = self.fields[lon_idx]

            def read_num(col_type, chunk):
                if col_type == "INT":   return float(struct.unpack(">i", chunk)[0])
                if col_type == "FLOAT": return float(struct.unpack(">d", chunk)[0])
                raise Exception("RTREE requiere columnas numericas")

            def extractor(record):
                val_lat = read_num(t_lat, record[o_lat : o_lat + s_lat])
                val_lon = read_num(t_lon, record[o_lon : o_lon + s_lon])
                return val_lon, val_lat  

            return extractor


def _load_catalog():
    if not os.path.exists(CATALOG_PATH):
        return {}
    with open(CATALOG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_catalog(catalog):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2)


def _get_key_column(columns):
    for col in columns:
        if col.get("index"):
            return col.get("name")
    return columns[0].get("name") if columns else None


def _get_or_open_table(name, meta):
    if name in engine.tables:
        return

    columns      = meta.get("columns", [])
    index_type   = (meta.get("index") or "HEAP").upper()
    column_sizes = meta.get("column_sizes") or {}
    codec        = Codec(columns, column_sizes)
    key_col      = meta.get("key_column")
    main_path    = os.path.join(DATA_DIR, f"{name}.db")

    if index_type == "SEQUENTIAL":
        overflow_path = os.path.join(DATA_DIR, f"{name}_overflow.db")
        index = SequentialFile(main_path, overflow_path, codec.record_size, codec.key_extractor(key_col))
    elif index_type == "HASH":
        index = ExtendibleHash(main_path, codec.record_size, codec.key_extractor(key_col))
    elif index_type == "RTREE":
        index = RTree(main_path, codec.record_size, codec.point_extractor())
    elif index_type == "HEAP":
        index = HeapFile(main_path, codec.record_size, codec.key_extractor(key_col))
    else:
        index = BPlusTree(main_path, codec.record_size, codec.key_extractor(key_col))

    engine.create_table(name, index, columns=columns, index_type=index_type)


class InferCsvRequest(BaseModel):
    path: str
    sample_rows: Optional[int] = 50


def _infer_type(values):
    if not values:
        return "TEXT"
    is_int = is_float = True
    saw_value = False
    for v in values:
        s = (v or "").strip()
        if not s:
            continue
        saw_value = True
        try:    int(s)
        except: is_int = False
        try:    float(s)
        except: is_float = False
        if not is_int and not is_float:
            break
    if not saw_value: return "TEXT"
    if is_int:        return "INT"
    if is_float:      return "FLOAT"
    return "TEXT"


def _sanitize_col_name(name: str) -> str:
    out = []
    for ch in (name or "").strip():
        out.append(ch if (ch.isalnum() or ch == "_") else "_")
    cleaned = "".join(out).strip("_") or "col"
    if cleaned[0].isdigit():
        cleaned = "c_" + cleaned
    return cleaned


# ───────────────────────────────────────────────────────
# ENDPOINTS
# ───────────────────────────────────────────────────────

@app.post("/infer-csv")
def infer_csv(req: InferCsvRequest):
    raw = req.path
    candidates = [raw]
    if not os.path.isabs(raw):
        candidates += [os.path.join(DATA_DIR, raw), os.path.join(DATA_DIR, os.path.basename(raw))]

    file_path = next((c for c in candidates if os.path.exists(c)), None)
    if not file_path:
        raise HTTPException(status_code=404, detail=f"CSV no encontrado: {raw}")

    import csv as _csv
    columns = []
    try:
        with open(file_path, mode="r", encoding="utf-8") as f:
            reader = _csv.reader(f)
            header = next(reader, None)
            if not header:
                raise HTTPException(status_code=400, detail="CSV vacío o sin header")

            samples  = [[] for _ in header]
            max_len  = [0 for _ in header]
            n_rows   = 0
            limit    = max(1, int(req.sample_rows or 50))
            for row in reader:
                for i in range(len(header)):
                    val = row[i] if i < len(row) else ""
                    samples[i].append(val)
                    if len(val) > max_len[i]: max_len[i] = len(val)
                n_rows += 1
                if n_rows >= limit: break

            seen_names = set()
            for i, raw_name in enumerate(header):
                col_type = _infer_type(samples[i])
                base = _sanitize_col_name(raw_name)
                name, suffix = base, 2
                while name in seen_names:
                    name = f"{base}_{suffix}"; suffix += 1
                seen_names.add(name)
                col = {"name": name, "type": col_type}
                if name != (raw_name or "").strip(): col["original"] = raw_name
                if col_type == "TEXT": col["size"] = max(16, ((max_len[i] + 7) // 8) * 8)
                columns.append(col)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error leyendo CSV: {e}")

    return {"columns": columns, "sampled": n_rows}


@app.get("/tables")
def list_tables():
    return {"tables": list(_load_catalog().keys())}


@app.get("/tables/{name}")
def get_table(name: str):
    catalog = _load_catalog()
    if name not in catalog:
        raise HTTPException(status_code=404, detail="Tabla no existe")
    return catalog[name]


@app.delete("/tables/{name}")
def drop_table(name: str):
    catalog = _load_catalog()
    if name not in catalog:
        raise HTTPException(status_code=404, detail="Tabla no existe")

    tx_id = threading.get_ident()
    lock_manager.acquire_exclusive(name, tx_id)
    try:
        meta = catalog[name]
        try:   _get_or_open_table(name, meta)
        except: pass
        try:
            engine.drop_table(name, base_path=DATA_DIR)
        except:
            for fname in (f"{name}.db", f"{name}_overflow.db", f"{name}.db.dir"):
                fpath = os.path.join(DATA_DIR, fname)
                if os.path.exists(fpath): os.remove(fpath)
        del catalog[name]
        _save_catalog(catalog)
        return {"ok": True, "table": name}
    finally:
        lock_manager.release_all(tx_id)


# ───────────────────────────────────────────────────────
# R-TREE MBR VISUALIZATION ENDPOINT  ← NUEVO
# ───────────────────────────────────────────────────────

@app.get("/rtree-mbrs/{table}")
def get_rtree_mbrs(table: str):
    """
    Lee el R-Tree de la tabla indicada desde disco y devuelve todos los MBRs
    con nivel, tipo de nodo y coordenadas. Usado por el visualizador QGIS.

    Respuesta:
    {
      "table":       "places",
      "total_nodes": 42,
      "max_level":   3,
      "n_points":    1000,
      "mbrs": [
        { "level": 0, "is_leaf": false, "min_x": ..., "min_y": ...,
          "max_x": ..., "max_y": ..., "n_entries": 4, "page_id": 1 },
        ...
      ]
    }
    """
    catalog = _load_catalog()

    if table not in catalog:
        raise HTTPException(status_code=404, detail=f"Tabla '{table}' no existe")

    meta       = catalog[table]
    index_type = (meta.get("index") or "").upper()

    if index_type != "RTREE":
        raise HTTPException(
            status_code=400,
            detail=f"La tabla '{table}' usa índice {index_type}, no RTREE"
        )

    db_path = os.path.join(DATA_DIR, f"{table}.db")
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail=f"Archivo {db_path} no encontrado")

    columns      = meta.get("columns", [])
    column_sizes = meta.get("column_sizes") or {}
    codec        = Codec(columns, column_sizes)

    try:
        rtree    = RTree(db_path, codec.record_size, codec.point_extractor())
        mbrs     = rtree.get_mbrs()
        n_points = len(rtree.scan())
        rtree.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error leyendo R-Tree: {e}")

    max_level = max((m["level"] for m in mbrs), default=0)

    return {
        "table":       table,
        "total_nodes": len(mbrs),
        "max_level":   max_level,
        "n_points":    n_points,
        "mbrs":        mbrs,
    }


# ───────────────────────────────────────────────────────
# QUERY
# ───────────────────────────────────────────────────────

@app.post("/query")
def run_query(req: QueryRequest):
    sql       = req.sql
    base_path = req.base_path or DATA_DIR
    catalog   = _load_catalog()

    try:
        qd = parser.parse(tokenize(sql))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    qtype = qd.get("type")

    if qtype == "CREATE_TABLE":
        columns      = qd.get("columns", [])
        key_col      = _get_key_column(columns)
        codec        = Codec(columns, req.column_sizes)
        declared_index = next((col["index"] for col in columns if col.get("index")), None)

        meta = {
            "columns":     columns,
            "index":       declared_index or "HEAP",
            "record_size": codec.record_size,
            "key_column":  key_col,
            "column_sizes": req.column_sizes or {},
        }

        t0    = time.perf_counter()
        tx_id = threading.get_ident()
        lock_manager.acquire_exclusive(qd["table"], tx_id)
        try:
            engine.execute(
                qd, codec.encode,
                record_size    = codec.record_size,
                key_extractor  = codec.key_extractor(key_col),
                point_extractor= codec.point_extractor() if (meta["index"] or "").upper() == "RTREE" else None,
                base_path      = base_path,
            )
            t1 = time.perf_counter()
            catalog[qd["table"]] = meta
            _save_catalog(catalog)
            return {"ok": True, "table": qd["table"], "time_ms": (t1 - t0) * 1000}
        finally:
            lock_manager.release_all(tx_id)

    if qtype in ("SELECT", "SELECT_ALL", "INSERT", "DELETE"):
        table = qd.get("table")
        if table not in catalog:
            raise HTTPException(status_code=404, detail="Tabla no existe")

        meta         = catalog[table]
        columns      = meta.get("columns", [])
        codec        = Codec(columns, meta.get("column_sizes") or {})
        mode         = "X" if qtype in ("INSERT", "DELETE") else "S"
        tx_id        = threading.get_ident()

        if mode == "S": lock_manager.acquire_shared(table, tx_id)
        else:           lock_manager.acquire_exclusive(table, tx_id)

        try:
            _get_or_open_table(table, meta)
            t0 = time.perf_counter()
            results, stats = engine.execute(qd, codec.encode, decode=codec.decode)
            t1 = time.perf_counter()

            if results is None:
                return {"ok": True, "rows": [], "stats": {**stats, "time_ms": (t1 - t0) * 1000}}

            rows = [codec.decode(r) for r in results]
            return {"ok": True, "rows": rows, "stats": {**stats, "time_ms": (t1 - t0) * 1000}}
        finally:
            lock_manager.release_all(tx_id)

    raise HTTPException(status_code=400, detail="Query no soportada")
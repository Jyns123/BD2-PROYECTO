import json
import os
import struct
import time
from typing import Any, Dict, List, Optional
import threading

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from engine.engine import Engine
from parser.parser import Parser
from parser.tokenizer import tokenize
from index.bplustree import BPlusTree
from index.hash import ExtendibleHash
from index.sequential import SequentialFile
from index.rtree import RTree
from concurrency.lock_manager import LockManager

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
CATALOG_PATH = os.path.join(DATA_DIR, "catalog.json")

app = FastAPI()
engine = Engine()
parser = Parser()
# Lock manager para coordinar peticiones HTTP concurrentes (locks por tabla)
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
        if col_type == "INT":
            return 4
        if col_type == "FLOAT":
            return 8
        if col_type == "TEXT":
            return int(self.column_sizes.get(name, 32))
        return int(self.column_sizes.get(name, 32))

    def encode(self, values):
        if isinstance(values, (int, float, str)):
            values = [values]
        parts = []
        for i, (name, col_type, size, _) in enumerate(self.fields):
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
            if col_type == "INT":
                return struct.unpack(">i", chunk)[0]
            if col_type == "FLOAT":
                return struct.unpack(">d", chunk)[0]
            return chunk.split(b"\x00", 1)[0].decode("utf-8")

        return extractor

    def point_extractor(self):
        if len(self.fields) < 2:
            raise Exception("RTREE requiere al menos 2 columnas numericas")

        (n1, t1, s1, o1) = self.fields[0]
        (n2, t2, s2, o2) = self.fields[1]

        def read_num(col_type, chunk):
            if col_type == "INT":
                return float(struct.unpack(">i", chunk)[0])
            if col_type == "FLOAT":
                return float(struct.unpack(">d", chunk)[0])
            raise Exception("RTREE requiere columnas numericas")

        def extractor(record):
            c1 = record[o1:o1 + s1]
            c2 = record[o2:o2 + s2]
            return read_num(t1, c1), read_num(t2, c2)

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

    columns = meta.get("columns", [])
    index_type = (meta.get("index") or "BPLUSTREE").upper()
    column_sizes = meta.get("column_sizes") or {}
    codec = Codec(columns, column_sizes)
    key_col = meta.get("key_column")

    main_path = os.path.join(meta.get("base_path", DATA_DIR), f"{name}.db")

    if index_type == "SEQUENTIAL":
        overflow_path = os.path.join(meta.get("base_path", DATA_DIR), f"{name}_overflow.db")
        index = SequentialFile(main_path, overflow_path, codec.record_size, codec.key_extractor(key_col))
    elif index_type == "HASH":
        index = ExtendibleHash(main_path, codec.record_size, codec.key_extractor(key_col))
    elif index_type == "RTREE":
        index = RTree(main_path, codec.record_size, codec.point_extractor())
    else:
        index = BPlusTree(main_path, codec.record_size, codec.key_extractor(key_col))

    engine.create_table(name, index, columns=columns, index_type=index_type)


@app.get("/tables")
def list_tables():
    catalog = _load_catalog()
    return {"tables": list(catalog.keys())}


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

    meta = catalog[name]
    # Adquirir lock exclusivo para borrar la tabla
    tx_id = threading.get_ident()
    lock_manager.acquire_exclusive(name, tx_id)
    try:
        engine.drop_table(name, base_path=meta.get("base_path", DATA_DIR))
        del catalog[name]
        _save_catalog(catalog)
        return {"ok": True, "table": name}
    finally:
        lock_manager.release_all(tx_id)


@app.post("/query")
def run_query(req: QueryRequest):
    sql = req.sql
    base_path = req.base_path or DATA_DIR
    catalog = _load_catalog()

    try:
        qd = parser.parse(tokenize(sql))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    qtype = qd.get("type")

    if qtype == "CREATE_TABLE":
        columns = qd.get("columns", [])
        key_col = _get_key_column(columns)
        codec = Codec(columns, req.column_sizes)

        meta = {
            "columns": columns,
            "index": (columns[0].get("index") if columns else "BPLUSTREE"),
            "record_size": codec.record_size,
            "key_column": key_col,
            "column_sizes": req.column_sizes or {},
            "base_path": base_path,
        }

        t0 = time.perf_counter()
        # Adquirir lock exclusivo sobre la tabla a crear para evitar races
        tx_id = threading.get_ident()
        lock_manager.acquire_exclusive(qd["table"], tx_id)
        try:
            engine.execute(
                qd,
                codec.encode,
                record_size=codec.record_size,
                key_extractor=codec.key_extractor(key_col),
                point_extractor=codec.point_extractor() if (meta["index"] or "").upper() == "RTREE" else None,
                base_path=base_path,
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

        meta = catalog[table]
        columns = meta.get("columns", [])
        codec = Codec(columns, meta.get("column_sizes") or {})

        # Determinar modo de lock: lectura compartida para SELECTs, exclusivo para escrituras
        mode = "S"
        if qtype in ("INSERT", "DELETE"):
            mode = "X"

        tx_id = threading.get_ident()
        if mode == "S":
            lock_manager.acquire_shared(table, tx_id)
        else:
            lock_manager.acquire_exclusive(table, tx_id)

        try:
            # Abrir tabla (si no está en memoria) *con* el lock ya adquirido
            _get_or_open_table(table, meta)

            t0 = time.perf_counter()
            results, stats = engine.execute(qd, codec.encode)
            t1 = time.perf_counter()

            if results is None:
                return {"ok": True, "rows": [], "stats": {**stats, "time_ms": (t1 - t0) * 1000}}

            rows = [codec.decode(r) for r in results]
            return {"ok": True, "rows": rows, "stats": {**stats, "time_ms": (t1 - t0) * 1000}}
        finally:
            lock_manager.release_all(tx_id)

    raise HTTPException(status_code=400, detail="Query no soportada")

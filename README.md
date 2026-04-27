# Simulador de Gestor de Base de Datos
### Base de Datos 2 · UTEC · Ciclo 2026-1

Sistema de indexación sobre memoria secundaria que implementa desde cero las estructuras clásicas de organización de archivos, un parser SQL, simulador de concurrencia e interfaz gráfica. :D

---

## Integrantes

| Nombre | GitHub |
|--------|--------|
| Jyns Ordóñez | — |
| — | — |
| — | — |

---

## Estructuras implementadas

| Estructura | insert | search | range_search | remove |
|------------|--------|--------|--------------|--------|
| Sequential File | O(1) aux / O(n) rebuild | O(log n) + O(n) overflow | O(log n + k) | — |
| Extendible Hashing | O(1) amortizado | O(1) | No soportado | — |
| B+ Tree | O(log n) | O(log n) | O(log n + k) | — |
| R-Tree | — | — | O(log n + k) | — |

---

## Requisitos

```bash
Python 3.10+
```

Sin dependencias externas para el backend. Para el benchmark y reporte:

```bash
pip install matplotlib numpy
```


## Instalación



Con Docker (recomendado):


Sin Docker:


---

## Estructura del proyecto

```
BD2-PROYECTO/
│
├── storage/
│   ├── disk_manager.py     # Acceso a disco por páginas (PAGE_SIZE = 4096)
│   └── page.py             # Manejo de registros dentro de una página
│
├── index/
│   ├── heap.py             # Heap File (sin orden, base de Sequential)
│   ├── sequential.py       # Archivo ordenado con overflow
│   ├── hash.py             # Extendible Hashing
│   ├── bplustree.py        # B+ Tree persistente
│   └── rtree.py            # R-Tree espacial
│
├── parser/
│   ├── tokenizer.py        # Tokenizador SQL
│   └── parser.py           # Parser → dict estructurado
│
├── engine/
│   ├── engine.py           # Coordinador de tablas e índices
│   └── executor.py         # Despacho de queries
│
├── concurrency/
│   └── simulator.py        # Simulador de transacciones concurrentes
│
├── frontend/               # Interfaz gráfica o web
│
├── utils/
│   ├── csv_loader.py       # Carga de datos desde CSV
│   ├── metrics.py          # MetricsLogger — captura I/O y tiempo
│   ├── benchmark.py        # Experimentos automáticos n=1k/10k/100k
│   └── report_generator.py # Genera informe Markdown + gráficos PNG
│
├── test_all.py             # Suite de 72 tests
├── main.py                 # Punto de entrada
├── docker-compose.yml
└── README.md
```

---

## Uso rápido

### Desde Python

```python
from index.bplustree import BPlusTree
import struct

RECORD_SIZE = 40

def make_record(key):
    return struct.pack(">I", key) + b"\x00" * (RECORD_SIZE - 4)

def key_extractor(record):
    return struct.unpack(">I", record[:4])[0]

tree = BPlusTree("students.db", RECORD_SIZE, key_extractor, order=50)
tree.insert(make_record(101))
results = tree.search(101)
results = tree.range_search(100, 200)
tree.close()
```

### Con SQL

```sql
CREATE TABLE students (id INT INDEX bplustree) FROM FILE 'data/students.csv';

SELECT * FROM students WHERE id = 101;
SELECT * FROM students WHERE id BETWEEN 100 AND 200;
INSERT INTO students VALUES (101, 'Ana', 20);
DELETE FROM students WHERE id = 101;
```

---

## Tests

```bash
# Con pytest
python -m pytest test_all.py -v

# Sin pytest (compatible Windows)
python test_all.py

# Solo una suite
python -m pytest test_all.py::TestBPlusTree -v
```

Resultado esperado: **72/72 tests pasando**.

| Suite | Tests |
|-------|-------|
| TestDiskManager | 12 |
| TestHeapFile | 8 |
| TestBPlusTree | 17 |
| TestExtendibleHash | 8 |
| TestSequentialFile | 10 |
| TestEngine | 12 |
| TestMetricsLogger | 5 |

---

## Experimentos y reporte

```bash
# 1. Correr benchmark (genera utils/experiment_log.json)
python -m utils.benchmark

# 2. Generar informe con gráficos
python -m utils.report_generator

# Resultado: informe_experimental.md + graficos/
```

El benchmark mide **accesos a disco** (páginas leídas + escritas) y **tiempo en ms** para insert, search y range_search con n = 1 000, 10 000 y 100 000 registros en cada estructura.

---

## Métricas por operación

Cada operación reporta:

```python
{
  "reads":  4,      # páginas leídas
  "writes": 2,      # páginas escritas
  "time_ms": 0.31   # tiempo de reloj
}
```

El Engine resetea los contadores antes de cada operación para que las métricas sean por operación y no acumuladas.

---

## Decisiones de diseño

**Página 0 como metadata** — Los bytes 0-3 guardan `total_pages` y los bytes 4-7 guardan `root_page_id` del B+ Tree. Sin esto, al reabrir el archivo la raíz apuntaría a la página 1 hardcodeada, que puede ser incorrecta tras múltiples inserciones.

**Cache limpiable en B+ Tree** — El cache en memoria se limpia al inicio de cada operación medida (`cache.clear()`) para simular accesos reales a disco y obtener métricas válidas. En producción el cache puede mantenerse activo para mejor rendimiento.

**_UnifiedDM en SequentialFile** — Como Sequential usa dos archivos (main + overflow), se expone un DiskManager virtual que suma los contadores de ambos, manteniendo la interfaz uniforme que espera el Engine.

**Profundidades locales en ExtendibleHash** — El directorio clásico de Extendible Hashing requiere conocer la profundidad local de cada bucket para decidir si duplicar el directorio global o solo redirigir punteros. Sin este atributo los splits son incorrectos.

---

## Docker

```bash
docker compose up --build
```

El `docker-compose.yml` levanta el backend y el frontend en contenedores separados con volúmenes para persistencia de los archivos `.db`.

---

## Licencia

Proyecto académico — UTEC 2026-1.

# Simulador de Gestor de Base de Datos
### Base de Datos 2 - UTEC - Ciclo 2026-1

Este proyecto es un mini-SGBD hecho desde cero para el curso de BD2. Implementa estructuras de indexacion sobre disco, un parser SQL propio, algoritmos de procesamiento externo (sort, hashing, join), un simulador de concurrencia y una interfaz web con React. Todo el acceso al disco ocurre a nivel de pagina de 4 KB - no se carga ningun archivo completo en memoria en ningun momento. :D

---

## Integrantes

| Nombre | GitHub |
|--------|--------|
| Jyns Ordóñez | [@Jyns123](https://github.com/Jyns123) |
| Denzel Bautista | - |
| Gian Aedo | - |

---

## Que implementa este proyecto?

En términos generales, el sistema tiene siete partes que trabajan juntas:

- **Capa de almacenamiento** - maneja páginas de 4 KB en disco, asigna páginas nuevas y guarda metadatos persistentes
- **Estructuras de indexacion** - Sequential File, Extendible Hashing, B+ Tree, R-Tree y Heap File, todas operando sobre disco
- **Parser SQL** - tokenizador + parser que convierte una query en un diccionario que el motor puede ejecutar
- **Motor de consultas** - coordina qué tabla va a qué indice y recolecta las métricas de I/O
- **Algoritmos externos** - External Sort para ORDER BY, External Hashing para GROUP BY, Hash Join para INNER JOIN
- **Control de concurrencia** - Lock Manager con bloqueos Shared/Exclusive y un simulador de transacciones
- **Frontend** - aplicacion web con editor SQL, tabla de resultados, panel de metricas y visualizador del R-Tree

---

## Estructura del proyecto

```
BD2-PROYECTO/
  backend/
    storage/
      disk_manager.py      -- acceso a disco por paginas (PAGE_SIZE = 4096 bytes)
      page.py              -- gestion de registros de tamano fijo dentro de una pagina

    index/
      heap.py              -- Heap File, inserciones sin orden y escaneo lineal
      sequential.py        -- archivo ordenado + desbordamiento
      hash.py              -- Extendible Hashing, directorio dinamico
      bplustree.py         -- B+ Tree persistente con hojas enlazadas
      rtree.py             -- R-Tree espacial (puntos 2D, MBRs, kNN)

    parser/
      tokenizer.py         -- lexer: keywords, literales, operadores
      parser.py            -- produce un dict estructurado por tipo de query

    engine/
      engine.py            -- coordinador de tablas, catalogo y despacho de queries

    algoritmos/
      external_sort.py     -- merge sort externo de dos fases (ORDER BY)
      external_hashing.py  -- hashing externo con agregados (GROUP BY)
      hash_join.py         -- hash join de dos fases (INNER JOIN)

    concurrency/
      lock_manager.py      -- bloqueos S/X con colas de espera thread-safe
      simulator.py         -- simulador de transacciones y deteccion de conflictos

    utils/
      metrics.py           -- MetricsLogger, captura lecturas escrituras y tiempo
      csv_loader.py        -- carga de datasets CSV
      benchmark.py         -- experimentos automaticos n=1k/10k/100k
      report_generator.py  -- genera informe Markdown + graficos PNG

    data/
      chipotle.csv
      chipotle_stores.csv  -- tiene longitud y latitud, se usa con el R-Tree
      liga1.csv
      students.csv
      catalog.json         -- catalogo persistente de tablas entre sesiones

    api.py                 -- API REST con FastAPI (el backend principal)
    test.py                -- suite de 72 tests correctamente pasados :DD
    requirements.txt
    Dockerfile

  frontend/
    src/
      components/
        QueryEditor.jsx       -- editor SQL con resaltado sintactico (CodeMirror)
        ResultsPanel.jsx      -- tabla de resultados con paginacion
        StatsBar.jsx          -- metricas de I/O en tiempo real
        Sidebar.jsx           -- lista de tablas, crear y eliminar
        TableInspector.jsx    -- ver esquema y datos de una tabla
        CreateTableModal.jsx  -- cargar CSV e inferir columnas automaticamente
      services/
        api.js               -- cliente HTTP para el backend
      App.jsx                -- layout principal + visualizador del R-Tree
      main.jsx
      index.css

    package.json             -- React 19
    vite.config.js
    Dockerfile

  docker-compose.yml         -- levanta backend en :8000 y frontend en :80
  README.md
```

---

## Estructuras de indexacion

Todas operan sobre paginas de 4 KB. Los conteos de I/O son exactos por operacion.

### Sequential File

Tiene un archivo principal ordenado por clave y un archivo de desbordamiento para las inserciones intermedias. Cuando el overflow acumula mas de 200 registros, se reconstruye el principal con una fusion ordenada. La busqueda usa busqueda binaria sobre el principal y luego escanea el overflow. Como siempre, como en la mayoría de casos del sequential.

| Operacion | Complejidad |
|-----------|-------------|
| insert | O(1) en overflow / O(n) en reconstruccion |
| search | O(log n) + O(n) overflow |
| range_search | O(log n + k) |
| scan | O(n) |

### Extendible Hashing

Directorio dinamico que se duplica cuando un bucket se desborda y su profundidad local iguala la global. O(1) amortizado para insercion y busqueda. No soporta rangos. El directorio se persiste en un archivo `.db.dir` separado del archivo de datos.

| Operacion | Complejidad |
|-----------|-------------|
| insert | O(1) amortizado |
| search | O(1) |
| range_search | no soportado |

### B+ Tree

Arbol balanceado con nodos serializados a paginas de tamano fijo. Las hojas estan enlazadas para recorridos por rango sin volver a la raiz. Soporta claves numericas (float64, 8 bytes) y claves de texto (UTF-8 con padding). El page_id de la raiz se guarda en los metadatos del DiskManager para no perderla al cerrar.

| Operacion | Complejidad |
|-----------|-------------|
| insert | O(log n) |
| search | O(log n) |
| range_search | O(log n + k) |

### R-Tree (espacial)

Indice espacial para puntos 2D, cada nodo interno guarda MBRs (Minimum Bounding Rectangles) de sus hijos. Soporta busqueda por radio circular y knn. La API expone los MBRs de todos los niveles para que el frontend los pueda dibujar.

| Operacion | Que hace |
|-----------|----------|
| insert(record) | inserta un punto |
| range_search(cx, cy, r) | todos los puntos dentro del circulo |
| knn(cx, cy, k) | los k puntos mas cercanos (distancia euclidiana) |
| get_mbrs() | retorna todos los MBRs por nivel para el visualizador |

---

## Parser SQL

El tokenizador reconoce keywords, identificadores, literales numericos y de texto, y operadores. El parser convierte los tokens en un diccionario que el motor puede ejecutar directamente.

Sentencias soportadas implementadas:

```sql
-- crear tabla cargando desde CSV
CREATE TABLE estudiantes (
    id INT INDEX bplustree,
    nombre TEXT,
    edad INT
) FROM FILE 'data/students.csv';

-- busqueda puntual
SELECT * FROM estudiantes WHERE id = 101;

-- busqueda por rango
SELECT * FROM estudiantes WHERE id BETWEEN 100 AND 200;

-- busqueda espacial por radio
SELECT * FROM tiendas WHERE ubicacion IN (POINT(-77.05, -12.07), RADIUS 5.0);

-- k vecinos mas cercanos
SELECT * FROM tiendas WHERE ubicacion IN (POINT(-77.05, -12.07), K 10);

-- insercion y eliminacion
INSERT INTO estudiantes VALUES (202, 'Ana', 21);
DELETE FROM estudiantes WHERE id = 202;

-- order by con sort externo
SELECT * FROM estudiantes ORDER BY edad;

-- group by con agregados
SELECT edad, COUNT(*) FROM estudiantes GROUP BY edad;
SELECT ciudad, AVG(nota) FROM estudiantes GROUP BY ciudad;

-- inner join
SELECT * FROM estudiantes INNER JOIN cursos ON estudiantes.id = cursos.id_estudiante;
```

Los tipos de indice que acepta `CREATE TABLE` son: `bplustree`, `hash`, `sequential`, `heap`, `rtree`.

---

## Algoritmos de procesamiento externo

Cuando los datos no caben en memoria, el sistema usa algoritmos que trabajan directamente sobre paginas en disco.

**External Sort** (ORDER BY) - merge sort externo de dos fases. Primero genera runs ordenados en archivos temporales, despues hace una fusion k-vias sobre esos runs.

**External Hashing** (GROUP BY) - hashing externo de dos fases. Particiona los registros en buckets en disco segun la clave de agrupamiento, luego carga cada bucket y calcula el agregado. Soporta COUNT, SUM, AVG, MIN y MAX.

**Hash Join** (INNER JOIN) - hash join de dos fases. Particiona ambas relaciones en los mismos buckets por clave de join, luego construye una tabla hash con el bucket izquierdo y lo sondea con los del derecho.

---

## Control de concurrencia

Implementa **2PL (Two-Phase Locking)** a nivel de tabla.

- **Bloqueos S (shared):** los adquieren los SELECT. Varios lectores pueden coexistir.
- **Bloqueos X (exclusive):** los adquieren INSERT y DELETE. Bloquean a todos los demas.
- Las colas de espera se manejan con `threading.Condition`.
- El simulador registra todas las operaciones con timestamp, detecta conflictos W-W, R-W y W-R, y permite ver el log de ejecucion.

---

## Interfaz grafica (Frontend)

React 19 y Tailwind CSS consumiendo la API REST del backend.

| Componente | Para que sirve |
|------------|----------------|
| QueryEditor | editor SQL con resaltado (CodeMirror, tema One Dark), ejecutar con Ctrl +Enter |
| ResultsPanel | tabla paginada con los registros retornados |
| StatsBar | muestra lecturas, escrituras y tiempo en ms de la ultima query |
| Sidebar | lista las tablas, permite crearlas con CSV y eliminarlas |
| Visualizador R-Tree | canvas con pan (clic+ arrastre) y zoom (ruedita), dibuja MBRs por nivel con colores distintos |

---

## Como ejecutar el proyecto

### Con Docker


```bash
git clone https://github.com/<usuario>/BD2-PROYECTO.git
cd BD2-PROYECTO

docker compose up --build
```

Eso levanta:
- backend en `http://localhost:8000`
- frontend en `http://localhost:80`

Los archivos `.db` del backend se persisten en `./backend/data` mediante un volumen, asi que los datos sobreviven entre reinicios. Para detener:

```bash
docker compose down
```

---


### Primeros pasos

Una vez que todo este corriendo:

1. Abrir `http://localhost` (Docker) o `http://localhost:5173` (manual).
2. Hacer clic en "Nueva tabla" en el sidebar e importar uno de los CSV de `backend/data/`.
3. Escribir una query SQL en el editor y ejecutarla con Ctrl+Enter.
4. Ver los resultados en el panel inferior y las metricas de I/O arriba.
5. Para tablas con indice R-Tree, abrir el visualizador de MBRs desde el icono de mapa.

---

## API REST

| Método | Endpoint | Que hace |
|--------|----------|----------|
| GET | /tables | lista todas las tablas |
| GET | /tables/{nombre} | esquema y metadatos de una tabla |
| DELETE | /tables/{nombre} | elimina una tabla y su .db |
| POST | /infer-csv | infiere columnas y tipos desde un CSV |
| POST | /query | ejecuta una o mas sentencias SQL separadas por `;` |
| GET | /rtree-mbrs/{tabla} | MBRs del R-Tree para el visualizador |

Ejemplo de request a `/query`:

```json
{
  "sql": "SELECT * FROM students WHERE id BETWEEN 100 AND 200;",
  "column_sizes": { "nombre": 30 },
  "base_path": "data"
}
```

Respuesta:

```json
{
  "results": [
    { "id": 101, "nombre": "Ana Garcia", "edad": 20 }
  ],
  "stats": {
    "reads": 4,
    "writes": 0,
    "time_ms": 0.85
  },
  "error": null
}
```

---

## Tests

```bash
# desde backend/
cd backend

# con pytest
python -m pytest test.py -v

# solo una suite
python -m pytest test.py::TestBPlusTree -v

# sin pytest (funciona en Windows sin nada extra)
python test.py
```

Resultado esperado: **72/72 tests pasadoss**.

| Suite | Tests | Que cubre |
|-------|-------|-----------|
| TestDiskManager | 12 | lectura/escritura de paginas, asignacion, metadatos |
| TestHeapFile | 8 | insercion, escaneo, espacio por pagina |
| TestBPlusTree | 17 | insercion, busqueda, rango, splits, persistencia |
| TestExtendibleHash | 8 | insercion, busqueda, duplicacion de directorio |
| TestSequentialFile | 10 | insercion ordenada, busqueda binaria, desbordamiento |
| TestEngine | 12 | CREATE, INSERT, SELECT, DELETE y estadisticas |
| TestMetricsLogger | 5 | exactitud de las mediciones de I/O y tiempo |

---

## Experimentos y benchmarks

```bash
cd backend

python -m utils.benchmark

python -m utils.report_generator
```

Esto produce `informe_experimental.md` y una carpeta `graficos/` con imagenes PNG comparando las estructuras. El benchmark mide accesos a disco y tiempo en ms para insercion, busqueda y rango, con n = 1.000, 10.000 y 100.000 registros.

---

## Metricas por operacion

Cada operacion retorna algo como esto:

```python
{
  "reads":   4,      # paginas leidas desde disco
  "writes":  2,      # paginas escritas a disco
  "time_ms": 0.31    # tiempo de reloj en milisegundos
}
```

El motor resetea los contadores antes de cada operacion para que los numeros sean por operacion y no acumulados desde que arranco el servidor.

---

## Datasets incluidos

| Archivo | Contenido | Buen indice para usar |
|---------|-----------|----------------------|
| students.csv | estudiantes, id, nombre, edad, nota | Sequential File, B+ Tree |
| chipotle.csv | transacciones de pedidos, item, precio | Extendible Hashing, B+ Tree |
| chipotle_stores.csv | tiendas con longitud y latitud | R-Tree |
| liga1.csv | equipos, puntos, goles | Sequential File, B+ Tree |

---

## Decisiones de diseño

**Pagina 0 como metadatos** - los primeros 8 bytes de cada `.db` guardan `total_pages` y `root_page_id`. Sin esto, al reabrir el archivo el arbol no sabe donde esta su raiz actual, porque puede haber migrado tras multiples splits.

**Cache limpiable en B+ Tree** - el cache en memoria se borra al inicio de cada operacion medida para simular accesos reales y obtener metricas validas. En produccion se puede dejar activo para mejor rendimiento.

**_UnifiedDM en Sequential File** - como Sequential usa dos archivos (main + overflow), se expone un DiskManager virtual que suma los contadores de ambos. Eso mantiene la interfaz uniforme que espera el motor.

**Profundidades locales en Extendible Hashing** - el directorio clasico necesita saber la profundidad local de cada bucket para decidir si duplicar el directorio global o solo redirigir punteros. Sin ese atributo los splits son incorrectos y se producen colisiones infinitas.

**2PL a nivel de tabla** - la granularidad elegida es la tabla completa, no la pagina individual. Simplifica la implementacion y evita deadlocks intra-tabla, a costa de menos concurrencia bajo carga alta. Para el alcance del proyecto esta bien.

**Codec de tipos en la API** - el Codec maneja tres tipos: INT (4 bytes, big-endian), FLOAT (8 bytes, IEEE 754) y TEXT (N bytes, UTF-8 con padding de ceros). Los tamanos de columnas TEXT son configurables en el request de `/query` para adaptarse a distintos datasets.

---

Proyecto academico - UTEC 2026-1.

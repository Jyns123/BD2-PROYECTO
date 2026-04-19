# Anexo Técnico: Definición de Interfaces y Responsabilidades por Módulo

El presente anexo define, de manera precisa, las responsabilidades de cada componente del sistema, así como las funciones principales que deberán implementarse. El objetivo es establecer una base común para el desarrollo, evitando ambigüedades en la implementación.

---

# I. Módulo `storage/`

Este módulo abstrae completamente el acceso a disco. Ningún otro componente debe interactuar directamente con archivos.

## I.I Archivo: `disk_manager.py`

### Responsabilidad

Gestionar lectura/escritura de páginas en disco y contabilizar accesos.

### Funciones

#### `read_page(page_id: int) -> bytes`

* **Entrada:**

  * `page_id`: identificador de la página a leer
* **Salida:**

  * `bytes` de tamaño `PAGE_SIZE`
* **Descripción:**

  * Posiciona el puntero en `page_id * PAGE_SIZE`
  * Lee exactamente una página
  * Incrementa contador de lecturas

---

#### `write_page(page_id: int, data: bytes) -> None`

* **Entrada:**

  * `page_id`: identificador de la página
  * `data`: bytes de tamaño `PAGE_SIZE`
* **Salida:** ninguna
* **Descripción:**

  * Escribe la página en la posición correspondiente
  * Incrementa contador de escrituras

---

#### `allocate_page() -> int`

* **Salida:**

  * nuevo `page_id`
* **Descripción:**

  * Retorna la siguiente página libre
  * No inicializa contenido necesariamente

---

#### `get_stats() -> dict`

* **Salida:**

  * `{reads: int, writes: int}`
* **Descripción:**

  * Retorna métricas acumuladas

---

## I.II Archivo: `page.py`

### Responsabilidad

Representar una página en memoria y manejar registros dentro de ella.

### Atributos esperados

* `data: bytearray`
* `free_space_offset: int`

---

### Funciones

#### `insert_record(record: bytes) -> bool`

* **Entrada:**

  * `record`: datos serializados
* **Salida:**

  * `True` si se insertó, `False` si no hay espacio
* **Descripción:**

  * Inserta el registro si hay espacio disponible

---

#### `read_records() -> list`

* **Salida:**

  * lista de registros
* **Descripción:**

  * Itera sobre la página y reconstruye registros

---

#### `has_space(size: int) -> bool`

* **Entrada:**

  * tamaño del registro
* **Salida:**

  * booleano

---

# II. Módulo `index/`

Contiene las estructuras de organización de datos sobre disco.

---

## II.I Archivo: `heap.py`

### Responsabilidad

Implementar almacenamiento sin orden.

---

### Funciones

#### `insert(record: bytes) -> None`

* Busca una página con espacio
* Si no existe, crea nueva página

---

#### `search(predicate) -> list`

* **Entrada:**

  * función o condición
* **Salida:**

  * registros que cumplen condición
* **Descripción:**

  * Scan completo

---

#### `delete(predicate) -> int`

* **Salida:**

  * cantidad de registros eliminados

---

---

## II.II Archivo: `sequential.py`

### Responsabilidad

Archivo ordenado con overflow.

---

### Funciones

#### `insert(key, record: bytes)`

* Inserta en overflow
* Mantiene archivo principal intacto

---

#### `search(key) -> record`

* Búsqueda binaria en archivo principal
* Búsqueda lineal en overflow

---

#### `range_search(begin, end) -> list`

* Iteración ordenada

---

#### `rebuild() -> None`

* Merge entre archivo principal y overflow
* Reorganiza datos

---

---

## II.III Archivo: `hash.py`

### Responsabilidad

Extendible hashing.

---

### Estructuras internas

* `directory: list[int]`
* `global_depth: int`

---

### Funciones

#### `hash(key) -> int`

* Función hash base

---

#### `insert(key, record)`

* Determina bucket
* Maneja split si overflow

---

#### `search(key)`

* Acceso directo a bucket

---

#### `split(bucket_id)`

* Duplica entradas de directorio si necesario

---

---

## II.IV Archivo: `bplustree.py`

### Responsabilidad

Árbol B+ persistente en páginas.

---

### Estructuras

* Nodo interno
* Nodo hoja
* Punteros a páginas

---

### Funciones

#### `search(key)`

* Navega desde raíz hasta hoja

---

#### `insert(key, record)`

* Inserta en hoja
* Maneja split si es necesario

---

#### `range_search(begin, end)`

* Recorre hojas enlazadas

---

#### `split(node)`

* Divide nodo en dos
* Propaga clave al padre

---

---

## II.V Archivo: `rtree.py`

### Responsabilidad

Indexación espacial.

---

### Funciones

#### `range_search(point, radius)`

* Retorna objetos dentro del rango

---

#### `knn(point, k)`

* Retorna k vecinos más cercanos

---

---

# III. Módulo `parser/`

---

## III.I Archivo: `tokenizer.py`

### Función

#### `tokenize(query: str) -> list`

* Divide la consulta en tokens

---

## III.II Archivo: `parser.py`

### Función

#### `parse(tokens: list) -> dict`

* **Salida:**

  * representación estructurada:

```json
{
  "type": "SELECT",
  "table": "students",
  "condition": {...}
}
```

---

---

# IV. Módulo `engine/`

---

## IV.I Archivo: `executor.py`

### Función principal

#### `execute(query_dict: dict)`

* Recibe salida del parser
* Llama al índice correspondiente

---

### Funciones auxiliares

#### `handle_select(...)`

#### `handle_insert(...)`

#### `handle_delete(...)`

---

---

# V. Módulo `concurrency/`

---

## V.I Archivo: `simulator.py`

### Funciones

#### `run_transaction(operations: list)`

* Ejecuta operaciones secuencialmente

---

#### `run_concurrent(transactions: list)`

* Ejecuta múltiples transacciones

---

#### `log_operation(op)`

* Registra orden de ejecución

---

---

# VI. Módulo `frontend/`

### Responsabilidad

Interfaz de interacción con el usuario.

---

### Funciones esperadas

* Envío de consultas al backend
* Renderizado de resultados
* Visualización de métricas

---

---

# VII. Módulo `utils/`

---

## VII.I `csv_loader.py`

#### `load_csv(path: str) -> list`

* Convierte CSV a registros

---

## VII.II `metrics.py`

#### `measure_time(func)`

* Wrapper para medir tiempo

---

---

# VIII. Archivo `main.py`

### Responsabilidad

Punto de entrada del sistema.

---

### Funciones

#### `main()`

* Inicializa módulos
* Loop de ejecución
* Conecta parser + engine + frontend

---

---

# IX. Consideraciones Generales

* Todas las estructuras deben operar sobre páginas.
* Ninguna estructura debe acceder directamente a archivos.
* Las funciones deben ser independientes y testeables.
* Se prioriza claridad en implementación sobre optimización extrema.
* Toda operación debe reflejar su costo en accesos a disco.

---



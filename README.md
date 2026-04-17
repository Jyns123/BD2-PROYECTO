# Simulador de Gestor de Base de Datos

**Curso:** Base de Datos II – UTEC
**Proyecto 1 (2026-1)**

---

# 1. Descripción General

El presente proyecto tiene como finalidad el diseño e implementación de un simulador de Sistema Gestor de Base de Datos (SGBD), enfocado en el manejo eficiente de datos en memoria secundaria. A diferencia de sistemas comerciales, este proyecto prioriza la comprensión detallada del costo de las operaciones en disco, mediante el desarrollo desde cero de estructuras de almacenamiento e indexación.

El sistema permitirá gestionar datos mediante archivos organizados en páginas de tamaño fijo, ejecutar consultas sobre distintas estructuras de índice, y evaluar el rendimiento de dichas operaciones en términos de accesos a disco y tiempo de ejecución.

---

# 2. Objetivo

El objetivo principal es construir un sistema que permita analizar, de manera controlada, el comportamiento de diferentes técnicas de organización de archivos y estructuras de indexación sobre memoria secundaria.

De manera específica, se busca:

* Implementar acceso a disco a nivel de página.
* Desarrollar múltiples estructuras de indexación.
* Integrar un mecanismo de consulta basado en SQL.
* Simular ejecución concurrente de operaciones.
* Medir métricas de desempeño y compararlas experimentalmente.

---

# 3. Arquitectura del Sistema

El sistema se estructura en módulos independientes, cada uno con responsabilidades claramente definidas. Esta separación permite mantener el control de la complejidad y facilita el trabajo colaborativo.

```
project/
│
├── storage/        # Gestión de páginas y acceso a disco
├── index/          # Estructuras de indexación
├── parser/         # Procesamiento de consultas SQL
├── engine/         # Ejecución de operaciones
├── concurrency/    # Simulación de transacciones
├── frontend/       # Interfaz de usuario
├── utils/          # Funcionalidades auxiliares
└── main.py
```

---

# 4. Módulo de Almacenamiento (storage)

Este módulo constituye la base del sistema. Todas las operaciones deben realizarse a través de este componente, garantizando que el acceso a disco se realice exclusivamente a nivel de página.

## 4.1 Modelo de Página

* Tamaño fijo: 4096 bytes.
* Unidad mínima de lectura y escritura.
* Los archivos se interpretan como secuencias de páginas.

## 4.2 Componentes

### DiskManager

Responsable de:

* Leer páginas desde disco.
* Escribir páginas en disco.
* Contabilizar accesos a disco (lecturas y escrituras).

### Page

Representa una página en memoria:

* Contiene un arreglo de bytes de tamaño fijo.
* Permite almacenar y recuperar registros.
* Puede incluir metadata (por ejemplo, espacio libre disponible).

## 4.3 Consideraciones

* No se permite cargar archivos completos en memoria.
* Toda operación debe traducirse en accesos explícitos a páginas.
* El uso de posicionamiento en archivo (`seek`) es obligatorio.

---

# 5. Módulo de Indexación (index)

Este módulo contiene las estructuras encargadas de organizar los registros en disco para optimizar operaciones de consulta e inserción.

## 5.1 Heap File

* Organización sin orden.
* Inserción eficiente mediante append.
* Búsqueda lineal.

Se utiliza como base para comprender el comportamiento sin indexación.

---

## 5.2 Sequential File

* Archivo principal ordenado por clave.
* Archivo auxiliar de desbordamiento (overflow).
* Reconstrucción del archivo cuando el overflow alcanza un umbral K.

Operaciones:

* Inserción
* Búsqueda puntual
* Búsqueda por rango
* Eliminación

---

## 5.3 Extendible Hashing

* Uso de un directorio dinámico en memoria.
* Buckets almacenados en páginas.
* División de buckets cuando se produce desbordamiento.

Operaciones:

* Inserción
* Búsqueda puntual
* Eliminación

No soporta búsquedas por rango.

---

## 5.4 B+ Tree

* Estructura balanceada basada en páginas.
* Nodos internos para direccionamiento.
* Nodos hoja enlazados para recorridos secuenciales.

Operaciones:

* Inserción
* Búsqueda puntual
* Búsqueda por rango
* Eliminación

Consideraciones:

* Tamaño de nodo limitado por el tamaño de página.
* División de nodos en inserciones.
* Navegación eficiente entre hojas para consultas por rango.

---

## 5.5 R-Tree

* Estructura para indexación espacial.
* Manejo de datos multidimensionales (por ejemplo, coordenadas).
* Soporte para consultas por rango espacial y vecinos más cercanos.

Operaciones:

* Búsqueda por rango (radio)
* k vecinos más cercanos (kNN)

Se permite adaptar una implementación base, integrándola al modelo de páginas del sistema.

---

# 6. Módulo de Parser (parser)

Este módulo es responsable de interpretar consultas en un subconjunto de SQL y traducirlas a operaciones ejecutables por el sistema.

## 6.1 Funcionalidad

* Análisis léxico (tokenización).
* Análisis sintáctico básico.
* Traducción de consultas a llamadas a funciones del motor.

## 6.2 Alcance

El parser soporta:

* CREATE TABLE
* SELECT (búsqueda puntual y por rango)
* INSERT
* DELETE

## 6.3 Consideraciones

* No se requiere un parser completo de SQL.
* Se prioriza claridad y funcionalidad sobre complejidad formal.
* Se pueden utilizar expresiones regulares y parsing manual.

---

# 7. Módulo de Ejecución (engine)

Este módulo actúa como intermediario entre el parser y las estructuras de indexación.

## Responsabilidades

* Recibir instrucciones del parser.
* Identificar la estructura de índice asociada a la tabla.
* Ejecutar la operación correspondiente.
* Retornar resultados al frontend.

---

# 8. Módulo de Concurrencia (concurrency)

Este componente simula la ejecución concurrente de transacciones.

## Funcionalidad mínima

* Ejecución de múltiples operaciones simultáneas.
* Registro de operaciones en un log.
* Visualización del orden de ejecución.

## Extensión opcional

* Implementación de mecanismos de bloqueo (shared/exclusive).
* Detección de interbloqueos.

---

# 9. Interfaz de Usuario (frontend)

La interfaz permite interactuar con el sistema de manera visual.

## Funcionalidades

* Editor de consultas SQL.
* Visualización de resultados en formato tabular.
* Panel de métricas:

  * Número de accesos a disco.
  * Tiempo de ejecución.
* Visualización gráfica para consultas espaciales (R-Tree).

---

# 10. Métricas y Evaluación

El sistema debe medir:

## Accesos a disco

* Número de páginas leídas.
* Número de páginas escritas.

## Tiempo de ejecución

* Medido en milisegundos.
* Calculado por operación.

Estas métricas permitirán comparar el rendimiento de las distintas estructuras de indexación.

---

# 11. Consideraciones de Implementación

* El backend debe desarrollarse exclusivamente en Python.
* Se debe utilizar programación orientada a objetos.
* Las estructuras deben ser genéricas respecto al tipo de clave.
* No se permite el uso de motores de bases de datos externos.
* El acceso a disco debe ser explícito y controlado.

---

# 12. Estrategia de Desarrollo

Se recomienda el siguiente orden de implementación:

1. Módulo de almacenamiento (páginas y acceso a disco).
2. Heap File.
3. Sequential File.
4. Extendible Hashing.
5. B+ Tree.
6. Parser SQL.
7. Motor de ejecución.
8. Concurrencia.
9. Interfaz gráfica.

Cada módulo debe ser validado antes de avanzar al siguiente.

---

# 13. Observaciones Finales

El proyecto prioriza la comprensión de los costos reales de las operaciones sobre memoria secundaria. Por ello, la correcta implementación del módulo de almacenamiento es crítica, ya que todos los demás componentes dependen directamente de él.

La complejidad del sistema debe ser gestionada cuidadosamente, privilegiando implementaciones claras y funcionales sobre soluciones excesivamente sofisticadas.

"""
PAGE

Este módulo maneja registros dentro de una página en memoria.

Qué hace:
- Organiza datos dentro de una página de 4096 bytes.
- Permite insertar y leer registros.
- Convierte entre bytes (disco) y registros (lógico).

Modelo:
- Registros de tamaño fijo (record_size).
- Estructura simple:
    [HEADER][RECORDS...]

Header:
- record_count: número de registros almacenados.

Cómo funciona:
- Los registros se guardan secuencialmente.
- Cada registro ocupa exactamente record_size bytes.
- El acceso es por índice (offset directo).

Funciones principales:
- insert_record → inserta si hay espacio
- read_records → devuelve todos los registros
- read_record → acceso directo por índice
- has_space → verifica capacidad
- to_bytes / from_bytes → serialización

Qué garantiza:
- Lectura consistente de registros
- Sin fragmentación (por longitud fija)
- Acceso rápido O(1)

Qué NO hace:
- No elimina registros
- No maneja espacio libre interno
- No soporta longitud variable

Relación con DiskManager:
    DiskManager maneja bytes
    Page maneja registros

Flujo típico:
    leer bytes → Page → modificar → bytes → guardar
"""

PAGE_SIZE = 4096
HEADER_SIZE = 4  # record_count


class Page:
    def __init__(self, record_size: int, data: bytes = None):
        # -----------------------------
        # VALIDACIONES
        # -----------------------------
        if not isinstance(record_size, int) or record_size <= 0:
            raise ValueError("record_size debe ser un entero positivo")

        if record_size > PAGE_SIZE - HEADER_SIZE:
            raise ValueError("record_size demasiado grande para la página")

        self.record_size = record_size
        self.max_records = (PAGE_SIZE - HEADER_SIZE) // record_size

        # -----------------------------
        # INICIALIZACIÓN
        # -----------------------------
        if data is None:
            self.data = bytearray(PAGE_SIZE)
            self._set_record_count(0)
        else:
            if not isinstance(data, (bytes, bytearray)):
                raise ValueError("data debe ser bytes o bytearray")

            if len(data) != PAGE_SIZE:
                raise ValueError(f"data debe tener tamaño exacto {PAGE_SIZE}")

            self.data = bytearray(data)

            # validar consistencia
            count = self._get_record_count()
            if count < 0 or count > self.max_records:
                raise ValueError("Página corrupta: record_count inválido")

    # -----------------------------
    # HEADER
    # -----------------------------

    def _get_record_count(self) -> int:
        return int.from_bytes(self.data[0:4], 'big')

    def _set_record_count(self, value: int):
        if value < 0 or value > self.max_records:
            raise ValueError("record_count fuera de rango")

        self.data[0:4] = value.to_bytes(4, 'big')

    # -----------------------------
    # SPACE CHECK
    # -----------------------------

    def has_space(self) -> bool:
        return self._get_record_count() < self.max_records

    # -----------------------------
    # INSERT
    # -----------------------------

    def insert_record(self, record: bytes) -> bool:
        if not isinstance(record, (bytes, bytearray)):
            raise ValueError("record debe ser bytes")

        if len(record) != self.record_size:
            raise ValueError(
                f"El registro debe tener tamaño {self.record_size} bytes"
            )

        if not self.has_space():
            return False

        try:
            index = self._get_record_count()
            offset = HEADER_SIZE + index * self.record_size

            self.data[offset:offset + self.record_size] = record

            self._set_record_count(index + 1)

            return True

        except Exception as e:
            raise IOError(f"Error insertando registro: {e}")

    # -----------------------------
    # READ
    # -----------------------------

    def read_records(self) -> list:
        try:
            records = []
            count = self._get_record_count()

            for i in range(count):
                offset = HEADER_SIZE + i * self.record_size
                record = bytes(
                    self.data[offset:offset + self.record_size]
                )
                records.append(record)

            return records

        except Exception as e:
            raise IOError(f"Error leyendo registros: {e}")

    # -----------------------------
    # READ SINGLE (útil para índices)
    # -----------------------------

    def read_record(self, index: int) -> bytes:
        if not isinstance(index, int) or index < 0:
            raise ValueError("index debe ser >= 0")

        count = self._get_record_count()

        if index >= count:
            raise IndexError("index fuera de rango")

        try:
            offset = HEADER_SIZE + index * self.record_size
            return bytes(self.data[offset:offset + self.record_size])

        except Exception as e:
            raise IOError(f"Error leyendo registro {index}: {e}")

    # -----------------------------
    # SERIALIZATION
    # -----------------------------

    def to_bytes(self) -> bytes:
        return bytes(self.data)

    @staticmethod
    def from_bytes(data: bytes, record_size: int):
        return Page(record_size, data)

    # -----------------------------
    # DEBUG / INFO
    # -----------------------------

    def get_record_count(self) -> int:
        return self._get_record_count()

    def get_free_slots(self) -> int:
        return self.max_records - self._get_record_count()
import os

PAGE_SIZE = 4096
META_PAGE_ID = 0

"""Hola chicos, ojo que la pagina 0 será solo metadata, ya el resto contendrá datos. ademas
quise tener los try y except en caso de errores para debug

DISK MANAGER

Este módulo maneja TODO el acceso a disco del sistema.
Es la única capa que puede leer o escribir en el archivo.

Qué hace:
- Trabaja con páginas de tamaño fijo (4096 bytes).
- Permite leer y escribir páginas usando page_id.
- Asigna nuevas páginas (allocate_page).
- Lleva conteo de accesos a disco (reads/writes).

Cómo funciona:
- Cada página está en: offset = page_id * PAGE_SIZE
- La página 0 se usa como metadata (guarda total_pages).
- No se carga el archivo completo en memoria.

Qué garantiza:
- Persistencia correcta en disco.
- Acceso controlado y medible.
- Independencia del sistema operativo.

Qué NO hace:
- No maneja registros (eso lo hace Page).
- No reutiliza páginas eliminadas.
- No tiene buffer ni caché.

Uso típico:
    read_page → modificar → write_page

IMPORTANTE:
Todo el sistema (Heap, Hash, B+Tree, etc.) debe usar este módulo
para acceder a disco.
"""
class DiskManager:
    def __init__(self, file_path: str):
        if not isinstance(file_path, str) or len(file_path.strip()) == 0:
            raise ValueError("file_path debe ser un string válido")

        self.file_path = file_path

        new_file = not os.path.exists(file_path)

        try:
            self.file = open(file_path, "r+b" if not new_file else "w+b")
        except Exception as e:
            raise IOError(f"No se pudo abrir el archivo: {e}")

        self.read_count = 0
        self.write_count = 0

        if new_file:
            self._init_meta_page()
        else:
            # validar metadata existente
            total = self._get_total_pages()
            if total < 1:
                raise ValueError("Archivo corrupto: total_pages inválido")

    # -----------------------------
    # META PAGE
    # -----------------------------

    def _init_meta_page(self):
        try:
            data = bytearray(PAGE_SIZE)
            data[0:4] = (1).to_bytes(4, 'big')  # solo metadata

            self.file.seek(0)
            self.file.write(data)
            self.file.flush()
        except Exception as e:
            raise IOError(f"Error inicializando metadata: {e}")

    def _get_total_pages(self) -> int:
        try:
            self.file.seek(0)
            data = self.file.read(4)

            if len(data) < 4:
                raise ValueError("Metadata incompleta")

            total = int.from_bytes(data, 'big')

            if total < 1:
                raise ValueError("total_pages inválido")

            return total
        except Exception as e:
            raise IOError(f"Error leyendo metadata: {e}")

    def _set_total_pages(self, value: int):
        if value < 1:
            raise ValueError("total_pages debe ser >= 1")

        try:
            self.file.seek(0)
            self.file.write(value.to_bytes(4, 'big'))
            self.file.flush()
        except Exception as e:
            raise IOError(f"Error escribiendo metadata: {e}")

    # -----------------------------
    # READ
    # -----------------------------

    def read_page(self, page_id: int) -> bytes:
        if not isinstance(page_id, int) or page_id < 0:
            raise ValueError("page_id debe ser un entero >= 0")

        try:
            offset = page_id * PAGE_SIZE
            self.file.seek(offset)

            data = self.file.read(PAGE_SIZE)

            if len(data) < PAGE_SIZE:
                data += b'\x00' * (PAGE_SIZE - len(data))

            self.read_count += 1
            return data

        except Exception as e:
            raise IOError(f"Error leyendo página {page_id}: {e}")

    # -----------------------------
    # WRITE
    # -----------------------------

    def write_page(self, page_id: int, data: bytes) -> None:
        if not isinstance(page_id, int) or page_id < 0:
            raise ValueError("page_id debe ser un entero >= 0")

        if not isinstance(data, (bytes, bytearray)):
            raise ValueError("data debe ser bytes o bytearray")

        if len(data) != PAGE_SIZE:
            raise ValueError(f"La página debe tener exactamente {PAGE_SIZE} bytes")

        try:
            offset = page_id * PAGE_SIZE
            self.file.seek(offset)

            self.file.write(data)
            self.file.flush()

            self.write_count += 1

        except Exception as e:
            raise IOError(f"Error escribiendo página {page_id}: {e}")

    # -----------------------------
    # ALLOCATE
    # -----------------------------

    def allocate_page(self) -> int:
        try:
            total_pages = self._get_total_pages()
            new_page_id = total_pages

            # escribir página vacía
            self.write_page(new_page_id, b'\x00' * PAGE_SIZE)

            # actualizar metadata
            self._set_total_pages(total_pages + 1)

            return new_page_id

        except Exception as e:
            raise IOError(f"Error asignando nueva página: {e}")

    # -----------------------------
    # STATS
    # -----------------------------

    def get_stats(self) -> dict:
        return {
            "reads": self.read_count,
            "writes": self.write_count
        }

    # -----------------------------
    # CLOSE
    # -----------------------------

    def close(self):
        try:
            if self.file and not self.file.closed:
                self.file.close()
        except Exception as e:
            raise IOError(f"Error cerrando archivo: {e}")
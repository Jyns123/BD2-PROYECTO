import csv
import os


def _sanitize(name: str) -> str:
    # Mismo criterio que api._sanitize_col_name para que los headers del CSV
    # coincidan con los nombres de columna sanitizados de la tabla.
    out = []
    for ch in (name or "").strip():
        if ch.isalnum() or ch == "_":
            out.append(ch)
        else:
            out.append("_")
    cleaned = "".join(out).strip("_")
    if not cleaned:
        cleaned = "col"
    if cleaned[0].isdigit():
        cleaned = "c_" + cleaned
    return cleaned


class CSVLoader:
    """Clase para cargar datos masivamente desde un archivo CSV al motor de BD."""

    @staticmethod
    def load(file_path: str, table_name: str, engine,
             key_parser=int, row_parser=None, make_record=None,
             batch_size=1000, column_names=None):
        """
        column_names: lista de nombres de columnas de la tabla (en orden).
        Si el CSV tiene header con esos mismos nombres, las filas se reordenan
        por nombre. Si no, se usa orden posicional (compatibilidad).
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")

        inserted = 0
        with open(file_path, mode='r', encoding='utf-8') as f:
            # Detectar header: leer primera línea sin avanzar el stream principal
            sniff = csv.reader(f)
            try:
                first_row = next(sniff)
            except StopIteration:
                return 0

            use_dict = False
            header_map = None
            if column_names:
                # Sanitizar también el header del CSV para que coincida con
                # los nombres de columna sanitizados de la tabla
                header_sanitized = [_sanitize(h) for h in first_row]
                if set(header_sanitized) >= set(column_names):
                    use_dict = True
                    header_map = header_sanitized

            if use_dict:
                # Reabrir como DictReader con headers sanitizados y mapear por nombre
                f.seek(0)
                dict_reader = csv.DictReader(f, fieldnames=header_map)
                next(dict_reader, None)  # saltar la fila de header original
                for drow in dict_reader:
                    if not drow:
                        continue
                    try:
                        ordered = [drow.get(c, "") for c in column_names]
                        payload = row_parser(ordered) if row_parser else key_parser(ordered[0])
                        record = make_record(payload) if make_record else payload
                        engine.insert(table_name, record)
                        inserted += 1
                    except Exception as e:
                        print(f"Error cargando fila {drow}: {e}")
            else:
                # Fallback posicional: reabrir y procesar todas las filas
                f.seek(0)
                reader = csv.reader(f)
                for row in reader:
                    if not row:
                        continue
                    try:
                        if row_parser:
                            payload = row_parser(row)
                        else:
                            payload = key_parser(row[0])
                        record = make_record(payload) if make_record else payload
                        engine.insert(table_name, record)
                        inserted += 1
                    except Exception as e:
                        print(f"Error cargando fila {row}: {e}")

        return inserted

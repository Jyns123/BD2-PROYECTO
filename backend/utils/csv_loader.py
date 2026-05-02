import csv
import os

class CSVLoader:
    """Clase para cargar datos masivamente desde un archivo CSV al motor de BD."""

    @staticmethod
    def load(file_path: str, table_name: str, engine,
             key_parser=int, row_parser=None, make_record=None,
             batch_size=1000):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")

        inserted = 0
        with open(file_path, mode='r', encoding='utf-8') as f:
            reader = csv.reader(f)
            # Asumimos que la primera columna o fila puede ser el ID o key
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

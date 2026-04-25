from engine.executor import Engine
import os

FILE = "students.db"
RECORD_SIZE = 100

def make_record(k):
    return str(k).zfill(4).encode().ljust(RECORD_SIZE, b' ')

def key_fn(r):
    return int(r[:4].decode())

if os.path.exists(FILE):
    os.remove(FILE)

engine = Engine()

engine.create_table("students", FILE, RECORD_SIZE, key_fn)

# inserts
for i in range(100):
    engine.insert("students", make_record(i))

# duplicados
for _ in range(10):
    engine.insert("students", make_record(5))

print(engine.search("students", 5))
print(engine.range_search("students", 10, 20))

engine.close()
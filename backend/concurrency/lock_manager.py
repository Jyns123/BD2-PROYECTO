import threading
import time


class LockManager:
    """
    Soporta locks compartidos (S) y exclusivos (X). Las adquisiciones
    bloquean hasta que el lock puede ser otorgado. Provee `release_all`
    para liberar todos los locks de una transacción.
    """

    def __init__(self):
        self._locks = {}
        self._global_lock = threading.Lock()

    def _get_entry(self, resource):
        with self._global_lock:
            entry = self._locks.get(resource)
            if entry is None:
                entry = {"mode": "NONE", "holders": set(), "cond": threading.Condition()}
                self._locks[resource] = entry
            return entry

    def acquire_shared(self, resource, tx_id, timeout=None):
        entry = self._get_entry(resource)
        cond = entry["cond"]
        with cond:
            start = time.time()
            while entry["mode"] == "X" and (tx_id not in entry["holders"]):
                if timeout is not None:
                    remaining = timeout - (time.time() - start)
                    if remaining <= 0:
                        return False
                    cond.wait(timeout=remaining)
                else:
                    cond.wait()
            entry["holders"].add(tx_id)
            entry["mode"] = "S"
            return True

    def acquire_exclusive(self, resource, tx_id, timeout=None):
        entry = self._get_entry(resource)
        cond = entry["cond"]
        with cond:
            start = time.time()
            while True:
                if entry["mode"] == "NONE":
                    entry["mode"] = "X"
                    entry["holders"] = {tx_id}
                    return True

                if entry["mode"] == "S" and entry["holders"] == {tx_id}:
                    entry["mode"] = "X"
                    entry["holders"] = {tx_id}
                    return True

                if entry["mode"] == "X" and entry["holders"] == {tx_id}:
                    return True

                if timeout is not None:
                    remaining = timeout - (time.time() - start)
                    if remaining <= 0:
                        return False
                    cond.wait(timeout=remaining)
                else:
                    cond.wait()

    def release(self, resource, tx_id):
        entry = self._get_entry(resource)
        cond = entry["cond"]
        with cond:
            if tx_id in entry["holders"]:
                entry["holders"].remove(tx_id)
            if not entry["holders"]:
                entry["mode"] = "NONE"
            cond.notify_all()

    def release_all(self, tx_id):
        with self._global_lock:
            resources = list(self._locks.keys())
        for r in resources:
            try:
                self.release(r, tx_id)
            except Exception:
                pass

    def status(self):
        with self._global_lock:
            return {r: {"mode": e["mode"], "holders": set(e["holders"])} for r, e in self._locks.items()}

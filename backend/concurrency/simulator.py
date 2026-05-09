"""
concurrency/simulator.py

Simulador de Control de Concurrencia
Implementa ejecución concurrente de transacciones sobre estructuras de índice,
con:
  - Bloqueos Compartidos (S) y Exclusivos (X) que implementó denzel en el lock manager a nivel de recurso
  - Log de operaciones con timestamps y orden real de ejecución
  - Detección de conflictos: W-W, R-W, W-R
  - Detección de deadlocks por timeout
  - Wait-for graph para identificar ciclos de espera

"""

import threading
import time
import uuid
from enum import Enum
from concurrency.lock_manager import LockManager



class OpType(Enum):
    READ   = "READ"
    WRITE  = "WRITE"
    DELETE = "DELETE"
    COMMIT = "COMMIT"
    ABORT  = "ABORT"



class LogEntry:
    """Una línea del log de operaciones."""

    def __init__(self, tx_id, tx_name, op_type, resource, status):
        self.tx_id     = tx_id
        self.tx_name   = tx_name
        self.op_type   = op_type        
        self.resource  = resource       
        self.status    = status         
        self.timestamp = time.time()

    def __str__(self):
        t  = time.strftime("%H:%M:%S", time.localtime(self.timestamp))
        ms = int((self.timestamp % 1) * 1000)
        return (
            f"[{t}.{ms:03d}] "
            f"{self.tx_name:<10} | "
            f"{self.op_type.value:<8} | "
            f"{str(self.resource):<24} | "
            f"{self.status}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# CONFLICT ENTRY
# ─────────────────────────────────────────────────────────────────────────────

class ConflictEntry:

    DESCRIPTIONS = {
        "W-W": "Escritura-Escritura: ambas transacciones intentan modificar el mismo recurso",
        "R-W": "Lectura-Escritura: TX quiere leer un recurso que otra está escribiendo",
        "W-R": "Escritura-Lectura: TX quiere escribir un recurso que otra está leyendo",
    }

    def __init__(self, tx_requesting, tx_holding, resource, conflict_type):
        self.tx_requesting = tx_requesting   # nombre TX que pide el lock
        self.tx_holding    = tx_holding      # nombre TX que tiene el lock
        self.resource      = resource
        self.conflict_type = conflict_type   # "W-W" | "R-W" | "W-R"
        self.timestamp     = time.time()

    def __str__(self):
        desc = self.DESCRIPTIONS.get(self.conflict_type, "")
        return (
            f"⚠  CONFLICTO [{self.conflict_type}] "
            f"{self.tx_requesting} → {self.tx_holding} "
            f"en '{self.resource}' — {desc}"
        )



class Transaction:


    def __init__(self, tx_id=None, name=None):
        self.tx_id   = tx_id or str(uuid.uuid4())
        self.name    = name or f"TX-{self.tx_id[:6]}"
        self.ops     = []       
        self.results = {}     
        self.status  = "PENDING"  

    def add_read(self, resource: str, fn):
        """Agrega una operación de lectura (adquiere lock S)."""
        self.ops.append({"op": OpType.READ, "resource": resource, "fn": fn})

    def add_write(self, resource: str, fn):
        """Agrega una operación de escritura (adquiere lock X)."""
        self.ops.append({"op": OpType.WRITE, "resource": resource, "fn": fn})

    def add_delete(self, resource: str, fn):
        """Agrega una operación de eliminación (adquiere lock X)."""
        self.ops.append({"op": OpType.DELETE, "resource": resource, "fn": fn})

    def __repr__(self):
        return f"Transaction(name={self.name!r}, ops={len(self.ops)}, status={self.status!r})"



class ConcurrencySimulator:


    LOCK_TIMEOUT = 2.0  # segundos antes de abortar por deadlock/timeout

    def __init__(self):
        self.lock_manager    = LockManager()

        self._log            = []          
        self._conflicts      = []          
        self._log_lock       = threading.Lock()
        self._conflict_lock  = threading.Lock()

        self._active_locks   = {}
        self._active_lock    = threading.Lock()

        self._wait_for       = {}
        self._wait_for_lock  = threading.Lock()



    def _log_op(self, tx: Transaction, op_type: OpType, resource: str, status: str):
        entry = LogEntry(tx.tx_id, tx.name, op_type, resource, status)
        with self._log_lock:
            self._log.append(entry)

    def _log_conflict(self, tx_req: str, tx_hold: str, resource: str, ctype: str):
        entry = ConflictEntry(tx_req, tx_hold, resource, ctype)
        with self._conflict_lock:
            self._conflicts.append(entry)



    def _register_lock(self, resource: str, tx_name: str, mode: str):
        with self._active_lock:
            self._active_locks.setdefault(resource, {})[tx_name] = mode

    def _unregister_lock(self, resource: str, tx_name: str):
        with self._active_lock:
            if resource in self._active_locks:
                self._active_locks[resource].pop(tx_name, None)

    def _unregister_all_locks(self, tx_name: str):
        with self._active_lock:
            for holders in self._active_locks.values():
                holders.pop(tx_name, None)


    def _add_waiting(self, waiter: str, resource: str):
        """Registra que `waiter` está esperando a los holders actuales de `resource`."""
        with self._active_lock:
            holders = set(self._active_locks.get(resource, {}).keys())
        blockers = holders - {waiter}
        if blockers:
            with self._wait_for_lock:
                self._wait_for.setdefault(waiter, set()).update(blockers)

    def _remove_waiting(self, waiter: str):
        with self._wait_for_lock:
            self._wait_for.pop(waiter, None)

    def _has_cycle(self) -> bool:
        """Detecta ciclos en el wait-for graph (deadlock real)."""
        with self._wait_for_lock:
            graph = {k: set(v) for k, v in self._wait_for.items()}

        visited, in_stack = set(), set()

        def dfs(node):
            visited.add(node)
            in_stack.add(node)
            for neighbour in graph.get(node, []):
                if neighbour not in visited:
                    if dfs(neighbour):
                        return True
                elif neighbour in in_stack:
                    return True
            in_stack.discard(node)
            return False

        for node in list(graph.keys()):
            if node not in visited:
                if dfs(node):
                    return True
        return False



    def _detect_conflicts(self, resource: str, tx: Transaction, requested_mode: str):
        """
        Detecta conflictos con holders actuales del recurso y los registra.
        Tipos:
          W-W : yo escribo, él también escribe  (X vs X)
          W-R : yo escribo, él lee              (X vs S)
          R-W : yo leo,     él escribe          (S vs X)
        """
        with self._active_lock:
            holders = dict(self._active_locks.get(resource, {}))

        for holder_name, holder_mode in holders.items():
            if holder_name == tx.name:
                continue

            if requested_mode == "X" and holder_mode == "X":
                self._log_conflict(tx.name, holder_name, resource, "W-W")
            elif requested_mode == "X" and holder_mode == "S":
                self._log_conflict(tx.name, holder_name, resource, "W-R")
            elif requested_mode == "S" and holder_mode == "X":
                self._log_conflict(tx.name, holder_name, resource, "R-W")



    def _acquire(self, tx: Transaction, resource: str, mode: str) -> bool:
        """
        Intenta adquirir lock S (lectura) o X (escritura).
        Registra conflictos si los hay, actualiza wait-for graph.
        Retorna True si se adquirió, False si timeout (deadlock).
        """
        self._detect_conflicts(resource, tx, mode)
        self._add_waiting(tx.name, resource)
        self._log_op(tx, OpType.READ if mode == "S" else OpType.WRITE,
                     resource, "REQUESTING")

        if mode == "S":
            ok = self.lock_manager.acquire_shared(
                resource, tx.tx_id, timeout=self.LOCK_TIMEOUT
            )
        else:
            ok = self.lock_manager.acquire_exclusive(
                resource, tx.tx_id, timeout=self.LOCK_TIMEOUT
            )

        self._remove_waiting(tx.name)

        if ok:
            self._register_lock(resource, tx.name, mode)
            self._log_op(tx, OpType.READ if mode == "S" else OpType.WRITE,
                         resource, "LOCK-ACQUIRED")
        else:
            cycle = self._has_cycle()
            reason = "DEADLOCK-ABORT" if cycle else "TIMEOUT-ABORT"
            self._log_op(tx, OpType.READ if mode == "S" else OpType.WRITE,
                         resource, reason)

        return ok

    def _release_all(self, tx: Transaction):
        self.lock_manager.release_all(tx.tx_id)
        self._unregister_all_locks(tx.name)
        self._remove_waiting(tx.name)



    def _run_transaction(self, tx: Transaction, delay_between_ops: float):
        tx.status = "RUNNING"
        self._log_op(tx, OpType.READ, "—", "STARTED")

        try:
            for step_idx, op in enumerate(tx.ops):
                op_type  = op["op"]
                resource = op["resource"]
                fn       = op["fn"]

                mode = "X" if op_type in (OpType.WRITE, OpType.DELETE) else "S"

                ok = self._acquire(tx, resource, mode)
                if not ok:
                    tx.status = "ABORTED"
                    self._release_all(tx)
                    self._log_op(tx, OpType.ABORT, "ALL", "ROLLED-BACK")
                    return

                # Ejecutar la operación real
                try:
                    result = fn()
                    tx.results[f"step_{step_idx}_{op_type.value}"] = result
                    self._log_op(tx, op_type, resource, "OK")
                except Exception as e:
                    self._log_op(tx, op_type, resource, f"ERROR: {e}")
                    tx.status = "ABORTED"
                    self._release_all(tx)
                    self._log_op(tx, OpType.ABORT, "ALL", "ROLLED-BACK-ON-ERROR")
                    return

                if delay_between_ops > 0:
                    time.sleep(delay_between_ops)

            # Todo OK → COMMIT
            tx.status = "COMMITTED"
            self._release_all(tx)
            self._log_op(tx, OpType.COMMIT, "ALL", "COMMITTED ✓")

        except Exception as e:
            tx.status = "ABORTED"
            self._release_all(tx)
            self._log_op(tx, OpType.ABORT, "ALL", f"ABORTED-UNEXPECTED: {e}")


    def run(self, transactions: list, delay_between_ops: float = 0.05) -> dict:
        """
        Ejecuta las transacciones en paralelo.

        Args:
            transactions:       List[Transaction] a ejecutar concurrentemente.
            delay_between_ops:  Pausa en segundos entre operaciones de cada TX
                                (simula carga real y maximiza entrelazo observable).

        Returns:
            Reporte con log, conflictos, estado de cada TX y tiempo total.
        """
        # Limpiar estado previo
        self._log.clear()
        self._conflicts.clear()
        self._active_locks.clear()
        self._wait_for.clear()
        for tx in transactions:
            tx.status  = "PENDING"
            tx.results = {}

        threads = [
            threading.Thread(
                target=self._run_transaction,
                args=(tx, delay_between_ops),
                name=tx.name,
                daemon=True,
            )
            for tx in transactions
        ]

        start = time.time()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        elapsed = time.time() - start

        return self._build_report(transactions, elapsed)

    # ─────────────────────────────────────────────────────────────────────────
    # REPORTE
    # ─────────────────────────────────────────────────────────────────────────

    def _build_report(self, transactions: list, elapsed_s: float) -> dict:
        deadlock_detected = any(
            "DEADLOCK-ABORT" in e.status or "TIMEOUT-ABORT" in e.status
            for e in self._log
        )
        return {
            "elapsed_ms":        round(elapsed_s * 1000, 2),
            "deadlock_detected": deadlock_detected,
            "conflict_count":    len(self._conflicts),
            "transactions": [
                {
                    "id":      tx.tx_id,
                    "name":    tx.name,
                    "status":  tx.status,
                    "ops":     len(tx.ops),
                    "results": tx.results,
                }
                for tx in transactions
            ],
            "conflicts": [str(c) for c in self._conflicts],
            "log":       [str(e) for e in self._log],
        }



    def make_transaction(self, name: str = None) -> Transaction:
        """Crea una nueva transacción lista para recibir operaciones."""
        return Transaction(name=name)

    def get_log_text(self) -> str:
        """Devuelve el log completo como string legible."""
        with self._log_lock:
            lines = [str(e) for e in self._log]
        return "\n".join(lines)

    def get_conflicts_text(self) -> str:
        """Devuelve todos los conflictos detectados como string legible."""
        with self._conflict_lock:
            lines = [str(c) for c in self._conflicts]
        if not lines:
            return "Sin conflictos detectados."
        return "\n".join(lines)

    def get_lock_status(self) -> dict:
        """Estado actual de todos los locks (útil para debug)."""
        return self.lock_manager.status()


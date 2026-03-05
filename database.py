import sqlite3
import time
import uuid
from datetime import datetime

MAX_RETRIES = 5
BACKOFF = [0.2, 0.4, 0.8, 1.6, 3.2]


class Database:

    def __init__(self, path="database.db"):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._configure()

    def _configure(self):
        self.execute("PRAGMA journal_mode=WAL;")
        self.execute("PRAGMA foreign_keys=ON;")
        self.execute("PRAGMA synchronous=NORMAL;")
        self.execute("PRAGMA temp_store=MEMORY;")
        self.execute("PRAGMA cache_size=-20000;")
        self.execute("PRAGMA optimize;")

    def execute(self, query, params=()):
        return self._retry(lambda: self.conn.execute(query, params))

    def fetchone(self, query, params=()):
        return self._retry(lambda: self.conn.execute(query, params).fetchone())

    def fetchall(self, query, params=()):
        return self._retry(lambda: self.conn.execute(query, params).fetchall())

    def transaction(self, name, operation_id=None):
        return TransactionContext(self.conn, name, operation_id)

    def _log_concurrency_event(
        self,
        operation_id,
        operation_type,
        branch_id,
        retries,
        duration_ms,
        final_status,
    ):
        try:
            self.conn.execute(
                """
                INSERT INTO concurrency_events(
                    id, operation_id, operation_type, branch_id,
                    retries, duration_ms, final_status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    operation_id,
                    operation_type,
                    branch_id,
                    retries,
                    duration_ms,
                    final_status,
                    datetime.utcnow().isoformat(),
                ),
            )
            self.conn.commit()
        except Exception:
            pass

    def _retry(self, fn, operation_id=None, operation_type="DB_OP", branch_id=None):
        _op_id = operation_id or str(uuid.uuid4())
        t_start = time.monotonic()
        last_exc = None
        for attempt in range(MAX_RETRIES):
            try:
                result = fn()
                if attempt > 0:
                    duration_ms = int((time.monotonic() - t_start) * 1000)
                    self._log_concurrency_event(
                        _op_id, operation_type, branch_id,
                        attempt, duration_ms, "SUCCESS"
                    )
                return result
            except sqlite3.OperationalError as e:
                last_exc = e
                if "locked" in str(e).lower() and attempt < MAX_RETRIES - 1:
                    duration_ms = int((time.monotonic() - t_start) * 1000)
                    self._log_concurrency_event(
                        _op_id, operation_type, branch_id,
                        attempt + 1, duration_ms, "RETRYING"
                    )
                    time.sleep(BACKOFF[attempt])
                    continue
                duration_ms = int((time.monotonic() - t_start) * 1000)
                self._log_concurrency_event(
                    _op_id, operation_type, branch_id,
                    attempt + 1, duration_ms, "FAILED"
                )
                raise
        duration_ms = int((time.monotonic() - t_start) * 1000)
        self._log_concurrency_event(
            _op_id, operation_type, branch_id,
            MAX_RETRIES, duration_ms, "FAILED"
        )
        raise last_exc


class TransactionContext:

    def __init__(self, conn, name, operation_id):
        self.conn = conn
        self.name = name
        self.operation_id = operation_id or str(uuid.uuid4())

    def __enter__(self):
        self.conn.execute("BEGIN IMMEDIATE;")
        return self.operation_id

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()

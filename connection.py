# core/db/connection.py
# Gestor de conexiones SQLite enterprise - WAL, FK, thread-safe
# Compatible con PyInstaller frozen + desarrollo normal
from __future__ import annotations
import sqlite3
import os
import sys
import threading
import logging
from contextlib import contextmanager
from typing import Optional

logger = logging.getLogger("spj.db")

# ── Resolución de ruta robusta ────────────────────────────────────────────────
def _base_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BASE_DIR = _base_dir()
DB_PATH  = os.path.join(BASE_DIR, "data", "punto_venta.db")


# ── Pool de conexiones por hilo ───────────────────────────────────────────────
_local = threading.local()


def _configure(conn: sqlite3.Connection) -> None:
    """Aplica configuración obligatoria a cada nueva conexión."""
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")   # ~32 MB
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA mmap_size=268435456") # 256 MB mmap
    conn.execute("PRAGMA wal_autocheckpoint=1000")


def get_connection() -> sqlite3.Connection:
    """
    Devuelve la conexión del hilo actual (1 conexión por hilo).
    Crea la conexión si no existe para este hilo.
    NUNCA usar check_same_thread=False con PyQt — cada widget en su hilo.
    """
    if not hasattr(_local, "conn") or _local.conn is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        conn = sqlite3.connect(DB_PATH, check_same_thread=True, timeout=30)
        _configure(conn)
        _local.conn = conn
        logger.debug("Nueva conexión SQLite para hilo %s", threading.current_thread().name)
    return _local.conn


def close_connection() -> None:
    """Cierra la conexión del hilo actual (llamar en closeEvent de ventanas)."""
    if hasattr(_local, "conn") and _local.conn:
        try:
            _local.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            _local.conn.close()
        except Exception:
            pass
        _local.conn = None


@contextmanager
def transaction(conn: Optional[sqlite3.Connection] = None):
    """
    Context manager para transacciones con SAVEPOINT (nesteable).
    Uso:
        with transaction(conn) as c:
            c.execute(...)
    En error → ROLLBACK automático y re-raise.
    """
    c = conn or get_connection()
    sp = f"sp_{id(threading.current_thread())}"
    c.execute(f"SAVEPOINT {sp}")
    try:
        yield c
        c.execute(f"RELEASE {sp}")
    except Exception:
        try:
            c.execute(f"ROLLBACK TO {sp}")
            c.execute(f"RELEASE {sp}")
        except Exception:
            pass
        raise

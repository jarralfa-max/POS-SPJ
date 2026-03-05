# migrations/v13_v8_industrial.py
# ── MIGRACIÓN v13 — v8 INDUSTRIAL PRODUCTION READY ──────────────────────────
# A. chicken_batches ← root_batch_id  (raíz matemática del árbol de lote)
# B. event_log       ← origin_device_id, device_version  (sync distribuido)
# C. detalles_venta  ← costo_unitario_real, margen_real   (audit FIFO)
# D. Índices enterprise:
#      branch_inventory_batches(root_batch_id)
#      event_log(origin_device_id, device_version)
#      chicken_batches(root_batch_id)
#      batch_movements(producto_id, fecha DESC)
# Idempotente. Seguro correr múltiples veces.
from __future__ import annotations
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    # ── A. chicken_batches.root_batch_id ─────────────────────────────────────
    # root_batch_id: apunta al ancestro raíz del árbol (NULL = es raíz).
    # Para lotes originales se auto-asigna en recepcionar_lote().
    # Para sub-lotes derivados se hereda del padre.
    _add_col(conn, "chicken_batches", "root_batch_id",
             "INTEGER REFERENCES chicken_batches(id)")

    # Backfill: lotes sin parent_batch_id son raíces — root = self
    try:
        conn.execute("""
            UPDATE chicken_batches
            SET root_batch_id = id
            WHERE parent_batch_id IS NULL
              AND root_batch_id IS NULL
        """)
    except Exception:
        pass

    # Backfill: sub-lotes — propagar root del padre (hasta 5 niveles de profundidad)
    for _ in range(5):
        try:
            conn.execute("""
                UPDATE chicken_batches
                SET root_batch_id = (
                    SELECT COALESCE(p.root_batch_id, p.id)
                    FROM chicken_batches p
                    WHERE p.id = chicken_batches.parent_batch_id
                )
                WHERE parent_batch_id IS NOT NULL
                  AND root_batch_id IS NULL
            """)
        except Exception:
            break

    # ── B. event_log: origin_device_id + device_version ─────────────────────
    # origin_device_id: uuid del dispositivo que originó el evento
    # device_version  : contador incremental por dispositivo (para LWWT)
    _add_col(conn, "event_log", "origin_device_id", "TEXT DEFAULT ''")
    _add_col(conn, "event_log", "device_version",   "INTEGER DEFAULT 0")

    # ── C. detalles_venta: costo y margen real por ítem ──────────────────────
    _add_col(conn, "detalles_venta", "costo_unitario_real", "REAL DEFAULT 0")
    _add_col(conn, "detalles_venta", "margen_real",         "REAL DEFAULT 0")

    # ── D. Índices enterprise ─────────────────────────────────────────────────
    _add_idx(conn, "chicken_batches",          "idx_cb_root",
             "root_batch_id")
    _add_idx(conn, "branch_inventory_batches", "idx_bib_root",
             "batch_id")          # joineo frecuente root_batch_id vía cb
    _add_idx(conn, "event_log",                "idx_el_device_ver",
             "origin_device_id, device_version")
    _add_idx(conn, "event_log",                "idx_el_hash",
             "payload_hash")      # detección de duplicados
    _add_idx(conn, "batch_movements",          "idx_bm_prod_fecha",
             "producto_id, fecha DESC")
    _add_idx(conn, "detalles_venta",           "idx_dv_margen",
             "costo_unitario_real, margen_real")


def _add_col(conn: sqlite3.Connection, table: str, col: str, defn: str) -> None:
    try:
        existing = [r[1] for r in conn.execute(
            f"PRAGMA table_info({table})"
        ).fetchall()]
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
    except Exception:
        pass


def _add_idx(
    conn: sqlite3.Connection, table: str, idx: str, cols: str
) -> None:
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if table in tables:
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {idx} ON {table}({cols})"
            )
    except Exception:
        pass

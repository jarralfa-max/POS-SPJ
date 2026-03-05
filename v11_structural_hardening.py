# migrations/v11_structural_hardening.py
# ── MIGRACIÓN v11 — ENDURECIMIENTO ESTRUCTURAL SPJ Enterprise v6 ──────────────
# Cambios:
#   A. chicken_batches ← parent_batch_id + transformation_id
#      (trazabilidad árbol base→derivado a nivel de lote, no solo BIB)
#   B. Tabla conciliation_runs — registro histórico de conciliaciones
#   C. Índices enterprise faltantes:
#      - batch_movements(created_at)
#      - branch_inventory_batches(branch_id, producto_id)
#      - event_log(synced, tipo)
#      - conciliation_runs(executed_at)
#   D. Campo event_version en event_log (versionado de payload para sync)
# Idempotente — seguro correr múltiples veces.
from __future__ import annotations
import sqlite3


def up(conn: sqlite3.Connection) -> None:

    # ── A. Columnas en chicken_batches ────────────────────────────────────────
    # parent_batch_id: id del batch del que se derivó (NULL para originales)
    # transformation_id: uuid de la sesión de transformación (une todos sus sub-lotes)
    _add_col(conn, "chicken_batches", "parent_batch_id",  "INTEGER REFERENCES chicken_batches(id)")
    _add_col(conn, "chicken_batches", "transformation_id", "TEXT")

    # ── B. Tabla conciliation_runs ────────────────────────────────────────────
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS conciliation_runs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid            TEXT    NOT NULL UNIQUE DEFAULT (lower(hex(randomblob(16)))),
            branch_id       INTEGER NOT NULL,
            usuario         TEXT    NOT NULL DEFAULT 'Sistema',
            tolerancia_kg   REAL    NOT NULL DEFAULT 0.05,
            total_batches   INTEGER DEFAULT 0,
            batches_ok      INTEGER DEFAULT 0,
            batches_diff    INTEGER DEFAULT 0,
            diferencia_kg   REAL    DEFAULT 0,
            ajustes_count   INTEGER DEFAULT 0,
            estado          TEXT    NOT NULL DEFAULT 'completado'
                            CHECK(estado IN ('completado','parcial','error')),
            detalle_json    TEXT,   -- BatchDiff[] serializado
            ejecutado_en    DATETIME DEFAULT (datetime('now')),
            duracion_ms     INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_conciliation_runs_branch
            ON conciliation_runs(branch_id, ejecutado_en DESC);

        CREATE INDEX IF NOT EXISTS idx_conciliation_runs_fecha
            ON conciliation_runs(ejecutado_en DESC);
    """)

    # ── C. Índices enterprise faltantes ───────────────────────────────────────
    _add_idx(conn, "batch_movements",          "idx_bm_created_at",   "fecha DESC")
    _add_idx(conn, "batch_movements",          "idx_bm_batch_tipo",   "batch_id, tipo")
    _add_idx(conn, "branch_inventory_batches", "idx_bib_branch_prod", "branch_id, producto_id")
    _add_idx(conn, "branch_inventory_batches", "idx_bib_batch_branch","batch_id, branch_id")
    _add_idx(conn, "event_log",                "idx_el_synced_tipo",  "synced, tipo")
    _add_idx(conn, "chicken_batches",          "idx_cb_parent",       "parent_batch_id")
    _add_idx(conn, "chicken_batches",          "idx_cb_transform",    "transformation_id")

    # ── D. Campo event_version en event_log ───────────────────────────────────
    _add_col(conn, "event_log", "event_version", "INTEGER NOT NULL DEFAULT 1")
    _add_col(conn, "event_log", "payload_hash",  "TEXT")  # SHA256 del payload


def _add_col(conn: sqlite3.Connection, table: str, col: str, defn: str) -> None:
    """Agrega columna si no existe. Silencioso si tabla/columna ya existe."""
    try:
        existing = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
    except Exception:
        pass


def _add_idx(conn: sqlite3.Connection, table: str, idx: str, cols: str) -> None:
    """Crea índice solo si tabla existe."""
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if table in tables:
            conn.execute(f"CREATE INDEX IF NOT EXISTS {idx} ON {table}({cols})")
    except Exception:
        pass

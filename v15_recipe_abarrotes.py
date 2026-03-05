# migrations/v15_recipe_abarrotes.py
# ── MIGRACIÓN v15 — Recetas de ingredientes para productos abarrotes ──────────
# Agrega tabla product_recipes_abarrotes para manejar consumo automático
# de ingredientes en productos compuestos (no-pollo).
#
# Ejemplo: "Torta de jamón" consume:
#   - pan_torta: ratio=1, merma=5%
#   - jamon:     ratio=0.1kg, merma=2%
#   - queso:     ratio=0.05kg, merma=0%
#
# Idempotente — seguro correr múltiples veces.
from __future__ import annotations
import sqlite3


def _add_col(conn: sqlite3.Connection, table: str, col: str, defn: str) -> None:
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
    except sqlite3.OperationalError:
        pass


def up(conn: sqlite3.Connection) -> None:
    # ── Tabla product_recipes_abarrotes ───────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS product_recipes_abarrotes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_id     INTEGER NOT NULL REFERENCES productos(id),
            ingrediente_id  INTEGER NOT NULL REFERENCES productos(id),
            ratio           REAL    NOT NULL DEFAULT 1.0
                                CHECK(ratio > 0),
            merma           REAL    NOT NULL DEFAULT 0.0
                                CHECK(merma >= 0 AND merma < 100),
            unidad          TEXT    NOT NULL DEFAULT 'pza',
            activo          INTEGER NOT NULL DEFAULT 1,
            notas           TEXT,
            creado_en       DATETIME DEFAULT (datetime('now')),
            UNIQUE(producto_id, ingrediente_id)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pra_producto
            ON product_recipes_abarrotes(producto_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_pra_ingrediente
            ON product_recipes_abarrotes(ingrediente_id)
    """)

    # ── Agregar columnas extras a movimientos_inventario si no existen ────────
    _add_col(conn, "movimientos_inventario", "referencia_id",   "INTEGER")
    _add_col(conn, "movimientos_inventario", "referencia_tipo",  "TEXT")
    _add_col(conn, "movimientos_inventario", "costo_unitario",   "REAL DEFAULT 0")
    _add_col(conn, "movimientos_inventario", "uuid",             "TEXT")

    conn.commit()


def down(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("DROP TABLE IF EXISTS product_recipes_abarrotes")
        conn.commit()
    except Exception:
        pass

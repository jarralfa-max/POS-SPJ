# migrations/v12_operativo_pollo.py
# ── MIGRACIÓN v12 — TABLAS OPERATIVAS POLLO Enterprise ────────────────────────
# Agrega capa operativa liviana sobre el FIFO industrial:
#   A. inventario_global    — stock administrativo global por producto
#   B. inventario_sucursal  — stock operativo por sucursal/producto (vista materializada)
#   C. recepciones_pollo    — recepciones registradas por vendedores en sucursal
# Las tablas FIFO (chicken_batches, branch_inventory_batches) NO se tocan.
# Estas tablas complementan, no reemplazan.
from __future__ import annotations
import sqlite3


def up(conn: sqlite3.Connection) -> None:

    conn.executescript("""
        -- ══════════════════════════════════════════════════════════════════
        -- A. INVENTARIO GLOBAL (nivel administrativo)
        -- Refleja las compras registradas por el admin.
        -- Cada compra crea o actualiza un registro aquí.
        -- ══════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS inventario_global (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_id     INTEGER NOT NULL REFERENCES productos(id) ON DELETE RESTRICT,
            peso_kg         REAL    NOT NULL DEFAULT 0 CHECK(peso_kg >= 0),
            costo_promedio  REAL    DEFAULT 0,
            costo_total     REAL    DEFAULT 0,
            ultima_compra   DATETIME,
            updated_at      DATETIME DEFAULT (datetime('now')),
            UNIQUE(producto_id)
        );

        CREATE INDEX IF NOT EXISTS idx_inv_global_prod
            ON inventario_global(producto_id);

        -- ══════════════════════════════════════════════════════════════════
        -- B. INVENTARIO SUCURSAL (nivel operativo por sucursal)
        -- Se actualiza en cada recepción, venta y traspaso.
        -- Es la fuente rápida de stock local sin ir a BIB.
        -- ══════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS inventario_sucursal (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            sucursal_id     INTEGER NOT NULL DEFAULT 1,
            producto_id     INTEGER NOT NULL REFERENCES productos(id) ON DELETE RESTRICT,
            peso_kg         REAL    NOT NULL DEFAULT 0 CHECK(peso_kg >= 0),
            updated_at      DATETIME DEFAULT (datetime('now')),
            UNIQUE(sucursal_id, producto_id)
        );

        CREATE INDEX IF NOT EXISTS idx_inv_sucursal_suc_prod
            ON inventario_sucursal(sucursal_id, producto_id);

        -- ══════════════════════════════════════════════════════════════════
        -- C. RECEPCIONES_POLLO (recepciones operativas por vendedor)
        -- Cada recepción aumenta inventario_sucursal y descuenta
        -- inventario_global.
        -- ══════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS recepciones_pollo (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid            TEXT    NOT NULL UNIQUE DEFAULT (lower(hex(randomblob(16)))),
            sucursal_id     INTEGER NOT NULL DEFAULT 1,
            producto_id     INTEGER NOT NULL REFERENCES productos(id) ON DELETE RESTRICT,
            peso_kg         REAL    NOT NULL CHECK(peso_kg > 0),
            costo_kg        REAL    DEFAULT 0,
            costo_total     REAL    DEFAULT 0,
            usuario         TEXT    NOT NULL DEFAULT 'Sistema',
            notas           TEXT,
            estado          TEXT    NOT NULL DEFAULT 'confirmada'
                            CHECK(estado IN ('confirmada', 'anulada')),
            -- Referencia opcional al batch FIFO creado en paralelo
            batch_id        INTEGER,
            fecha           DATETIME DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_recepciones_suc
            ON recepciones_pollo(sucursal_id, fecha DESC);

        CREATE INDEX IF NOT EXISTS idx_recepciones_prod
            ON recepciones_pollo(producto_id);
    """)


def _add_col(conn: sqlite3.Connection, table: str, col: str, defn: str) -> None:
    try:
        existing = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
    except Exception:
        pass

# migrations/v12_inventario_enterprise.py
# ── MIGRACIÓN v12 — INVENTARIO ENTERPRISE POLLERÍA ────────────────────────────
# Tablas:
#   A. inventario_global      — stock administrativo por producto
#   B. inventario_sucursal    — stock operativo por sucursal/producto
#   C. recepciones_pollo      — recepciones operativas de vendedores
#   D. recetas_consumo        — recetas de venta con rendimiento proporcional
#   E. recetas_consumo_detalle — líneas de materia prima por receta
#   F. traspasos_pollo        — movimientos inter-sucursal formales
# Idempotente. No borra tablas existentes.
from __future__ import annotations
import sqlite3


def up(conn: sqlite3.Connection) -> None:

    conn.executescript("""
        -- ══════════════════════════════════════════════════════════════════
        -- A. INVENTARIO GLOBAL (administrativo)
        -- ══════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS inventario_global (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_id     INTEGER NOT NULL REFERENCES productos(id),
            peso_kg         REAL    NOT NULL DEFAULT 0
                            CHECK(peso_kg >= 0),
            costo_total     REAL    DEFAULT 0,
            costo_por_kg    REAL    DEFAULT 0,
            compra_ref_id   INTEGER,       -- referencia opcional a compras_pollo_global
            fecha           DATETIME DEFAULT (datetime('now')),
            usuario         TEXT    DEFAULT 'Sistema',
            notas           TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_ig_producto
            ON inventario_global(producto_id);

        -- ══════════════════════════════════════════════════════════════════
        -- B. INVENTARIO SUCURSAL (operativo, fuente autoritativa local)
        -- ══════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS inventario_sucursal (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            sucursal_id     INTEGER NOT NULL,
            producto_id     INTEGER NOT NULL REFERENCES productos(id),
            peso_kg         REAL    NOT NULL DEFAULT 0
                            CHECK(peso_kg >= 0),
            fecha_actualizacion DATETIME DEFAULT (datetime('now')),
            UNIQUE(sucursal_id, producto_id)
        );
        CREATE INDEX IF NOT EXISTS idx_is_suc_prod
            ON inventario_sucursal(sucursal_id, producto_id);

        -- ══════════════════════════════════════════════════════════════════
        -- C. RECEPCIONES OPERATIVAS (vendedor registra lo recibido)
        -- ══════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS recepciones_pollo (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            sucursal_id     INTEGER NOT NULL,
            producto_id     INTEGER NOT NULL REFERENCES productos(id),
            peso_kg         REAL    NOT NULL CHECK(peso_kg > 0),
            costo_kg        REAL    DEFAULT 0,
            proveedor       TEXT    DEFAULT '',
            lote_ref        TEXT    DEFAULT '',
            compra_global_id INTEGER,      -- enlace a compra global si aplica
            usuario_id      TEXT    NOT NULL DEFAULT 'Sistema',
            fecha           DATETIME DEFAULT (datetime('now')),
            estado          TEXT    DEFAULT 'confirmada'
                            CHECK(estado IN ('confirmada','anulada')),
            notas           TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_rec_pollo_suc
            ON recepciones_pollo(sucursal_id, fecha DESC);
        CREATE INDEX IF NOT EXISTS idx_rec_pollo_fecha
            ON recepciones_pollo(fecha DESC);

        -- ══════════════════════════════════════════════════════════════════
        -- D. RECETAS DE CONSUMO ENTERPRISE
        -- (Vincula producto venta → materias primas con % rendimiento)
        -- ══════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS recetas_consumo (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_venta_id INTEGER NOT NULL REFERENCES productos(id),
            nombre          TEXT    NOT NULL DEFAULT '',
            activo          INTEGER NOT NULL DEFAULT 1,
            creado_por      TEXT    DEFAULT 'Sistema',
            creado_en       DATETIME DEFAULT (datetime('now')),
            actualizado_en  DATETIME DEFAULT (datetime('now')),
            notas           TEXT,
            UNIQUE(producto_venta_id)
        );
        CREATE INDEX IF NOT EXISTS idx_rc_producto
            ON recetas_consumo(producto_venta_id, activo);

        -- ══════════════════════════════════════════════════════════════════
        -- E. DETALLE DE RECETA DE CONSUMO
        -- ══════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS recetas_consumo_detalle (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            receta_id       INTEGER NOT NULL
                            REFERENCES recetas_consumo(id) ON DELETE CASCADE,
            materia_prima_id INTEGER NOT NULL REFERENCES productos(id),
            porcentaje      REAL    NOT NULL CHECK(porcentaje > 0 AND porcentaje <= 100),
            nombre_mp       TEXT    DEFAULT '',   -- desnormalizado para performance
            orden           INTEGER DEFAULT 0,
            UNIQUE(receta_id, materia_prima_id)
        );
        CREATE INDEX IF NOT EXISTS idx_rcd_receta
            ON recetas_consumo_detalle(receta_id);

        -- ══════════════════════════════════════════════════════════════════
        -- F. TRASPASOS INTER-SUCURSAL
        -- ══════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS traspasos_pollo (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid            TEXT    NOT NULL UNIQUE DEFAULT (lower(hex(randomblob(16)))),
            sucursal_origen_id  INTEGER NOT NULL,
            sucursal_destino_id INTEGER NOT NULL,
            producto_id     INTEGER NOT NULL REFERENCES productos(id),
            peso_kg         REAL    NOT NULL CHECK(peso_kg > 0),
            estado          TEXT    NOT NULL DEFAULT 'pendiente'
                            CHECK(estado IN ('pendiente','confirmado','anulado')),
            usuario_origen  TEXT    NOT NULL DEFAULT 'Sistema',
            usuario_destino TEXT,
            observaciones   TEXT,
            fecha_solicitud DATETIME DEFAULT (datetime('now')),
            fecha_confirmacion DATETIME
        );
        CREATE INDEX IF NOT EXISTS idx_tp_origen
            ON traspasos_pollo(sucursal_origen_id, estado);
        CREATE INDEX IF NOT EXISTS idx_tp_destino
            ON traspasos_pollo(sucursal_destino_id, estado);
    """)


def _add_col(conn: sqlite3.Connection, table: str, col: str, defn: str) -> None:
    try:
        existing = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
    except Exception:
        pass

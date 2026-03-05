# migrations/v10_product_recipes.py
# Recetas de consumo de producto (surtidos, retazos, combos, bandejas).
# Permite que un producto vendido por peso descuente piezas individuales
# de forma proporcional y automática, con trazabilidad completa por lote.
from __future__ import annotations
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    """
    Crea product_recipes e índices.
    Idempotente — seguro correr múltiples veces.
    """
    conn.executescript("""
        -- ══════════════════════════════════════════════════════════════════
        -- TABLA PRINCIPAL: receta de consumo de un producto
        -- ══════════════════════════════════════════════════════════════════
        CREATE TABLE IF NOT EXISTS product_recipes (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,

            -- Producto padre que se vende por peso (ej: "Surtido", "Retazo")
            product_id        INTEGER NOT NULL REFERENCES productos(id) ON DELETE CASCADE,

            -- Pieza interna que se descuenta del inventario FIFO
            piece_product_id  INTEGER NOT NULL REFERENCES productos(id) ON DELETE RESTRICT,

            -- Porcentaje del peso vendido que corresponde a esta pieza.
            -- Ejemplo: Surtido = 40% pierna + 35% pechuga + 25% espinazo
            -- La suma de todos los porcentajes de un product_id DEBE ser 100.
            percentage        REAL    NOT NULL CHECK(percentage > 0 AND percentage <= 100),

            -- Nombre descriptivo del componente (precalculado para UI)
            piece_name        TEXT    NOT NULL DEFAULT '',

            -- Orden de presentación en UI
            orden             INTEGER DEFAULT 0,

            -- Activo/inactivo para versionado suave
            active            INTEGER NOT NULL DEFAULT 1,

            created_at        DATETIME DEFAULT (datetime('now')),
            updated_at        DATETIME DEFAULT (datetime('now')),

            UNIQUE(product_id, piece_product_id, active)
        );

        -- ══════════════════════════════════════════════════════════════════
        -- ÍNDICES
        -- ══════════════════════════════════════════════════════════════════
        CREATE INDEX IF NOT EXISTS idx_product_recipes_product
            ON product_recipes(product_id, active);

        CREATE INDEX IF NOT EXISTS idx_product_recipes_piece
            ON product_recipes(piece_product_id);
    """)


def _add_idx(conn: sqlite3.Connection, table: str, idx_name: str, columns: str) -> None:
    """Crea índice solo si la tabla existe."""
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if table in tables:
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns})"
            )
    except Exception:
        pass

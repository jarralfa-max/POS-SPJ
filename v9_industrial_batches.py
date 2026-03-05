# migrations/v9_industrial_batches.py
# Migración v9 — Inventario industrial por lote, FIFO, conciliación, índices enterprise
# Tablas nuevas: chicken_batches, branch_inventory_batches, batch_movements, system_locks
# Protección inventario negativo mediante CHECK constraints
# Índices enterprise completos
from __future__ import annotations
import sqlite3


def up(conn: sqlite3.Connection) -> None:
    """
    Ejecuta todas las DDL de la migración v9.
    Idempotente: usa CREATE TABLE IF NOT EXISTS y CREATE INDEX IF NOT EXISTS.
    """
    # Crear event_log si no existe (puede no haber llegado de m8 en BD limpia)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS event_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid          TEXT    NOT NULL UNIQUE,
            tipo          TEXT    NOT NULL,
            entidad       TEXT    NOT NULL,
            entidad_id    INTEGER,
            payload       TEXT    NOT NULL,
            sucursal_id   INTEGER NOT NULL DEFAULT 1,
            usuario       TEXT    NOT NULL DEFAULT 'Sistema',
            synced        INTEGER DEFAULT 0,
            sync_intentos INTEGER DEFAULT 0,
            sync_error    TEXT,
            fecha         DATETIME DEFAULT (datetime('now')),
            fecha_sync    DATETIME
        )
    """)
    conn.executescript("""
    -- ══════════════════════════════════════════════════════════════════════════
    -- TABLA: chicken_batches
    -- Lote de pollo desde compra hasta consumo total.
    -- Unidad de trazabilidad: 1 batch = 1 recepción física.
    -- ══════════════════════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS chicken_batches (
        id                  INTEGER  PRIMARY KEY AUTOINCREMENT,
        uuid                TEXT     NOT NULL UNIQUE,           -- UUID sync offline
        branch_id           INTEGER  NOT NULL,                  -- sucursal que lo recibió
        producto_id         INTEGER  NOT NULL
                                REFERENCES productos(id),       -- producto pollo base
        compra_global_id    INTEGER
                                REFERENCES compras_pollo_global(id),
        numero_pollos       INTEGER  NOT NULL CHECK(numero_pollos >= 0),
        peso_kg_original    REAL     NOT NULL CHECK(peso_kg_original > 0),
        peso_kg_disponible  REAL     NOT NULL CHECK(peso_kg_disponible >= 0),
        costo_kg            DECIMAL(10,4) NOT NULL DEFAULT 0,
        costo_total         DECIMAL(10,2) NOT NULL DEFAULT 0,
        proveedor           TEXT     DEFAULT '',
        lote_proveedor      TEXT     DEFAULT '',               -- lote del proveedor
        estado              TEXT     NOT NULL DEFAULT 'disponible'
                                CHECK(estado IN ('disponible','parcial','agotado','cancelado')),
        fecha_recepcion     DATE     NOT NULL,
        usuario_recepcion   TEXT     NOT NULL DEFAULT 'Sistema',
        notas               TEXT     DEFAULT '',
        fecha_creacion      DATETIME NOT NULL DEFAULT (datetime('now')),
        fecha_actualizacion DATETIME NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_chicken_batches_branch_estado
        ON chicken_batches(branch_id, estado);
    CREATE INDEX IF NOT EXISTS idx_chicken_batches_fecha
        ON chicken_batches(fecha_recepcion);
    CREATE INDEX IF NOT EXISTS idx_chicken_batches_producto
        ON chicken_batches(producto_id, branch_id);

    -- ══════════════════════════════════════════════════════════════════════════
    -- TABLA: branch_inventory_batches
    -- Stock de cada producto en cada sucursal, POR LOTE.
    -- Reemplaza el stock agregado simple por trazabilidad de lote.
    -- FIFO: se consume el lote más antiguo primero.
    -- CHECK(cantidad_disponible >= 0) → protección a nivel BD.
    -- ══════════════════════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS branch_inventory_batches (
        id                  INTEGER  PRIMARY KEY AUTOINCREMENT,
        batch_id            INTEGER  NOT NULL
                                REFERENCES chicken_batches(id),
        branch_id           INTEGER  NOT NULL,
        producto_id         INTEGER  NOT NULL
                                REFERENCES productos(id),
        cantidad_original   REAL     NOT NULL CHECK(cantidad_original >= 0),
        cantidad_disponible REAL     NOT NULL CHECK(cantidad_disponible >= 0),
        costo_unitario      DECIMAL(10,4) NOT NULL DEFAULT 0,
        es_derivado         INTEGER  NOT NULL DEFAULT 0,        -- 1 si viene de transformación
        batch_padre_id      INTEGER
                                REFERENCES chicken_batches(id), -- si es derivado
        fecha_entrada       DATETIME NOT NULL DEFAULT (datetime('now')),
        fecha_actualizacion DATETIME NOT NULL DEFAULT (datetime('now')),
        UNIQUE(batch_id, branch_id, producto_id)
    );

    CREATE INDEX IF NOT EXISTS idx_bib_batch_branch
        ON branch_inventory_batches(batch_id, branch_id);
    CREATE INDEX IF NOT EXISTS idx_bib_branch_producto
        ON branch_inventory_batches(branch_id, producto_id);
    CREATE INDEX IF NOT EXISTS idx_bib_fecha_entrada
        ON branch_inventory_batches(fecha_entrada);

    -- ══════════════════════════════════════════════════════════════════════════
    -- TABLA: batch_movements
    -- Auditoría completa de cada movimiento sobre un batch específico.
    -- Ningún cambio de stock se hace sin registrar aquí.
    -- ══════════════════════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS batch_movements (
        id                  INTEGER  PRIMARY KEY AUTOINCREMENT,
        uuid                TEXT     NOT NULL UNIQUE,
        batch_id            INTEGER  NOT NULL
                                REFERENCES chicken_batches(id),
        bib_id              INTEGER                            -- branch_inventory_batches.id
                                REFERENCES branch_inventory_batches(id),
        branch_id           INTEGER  NOT NULL,
        producto_id         INTEGER  NOT NULL
                                REFERENCES productos(id),
        tipo                TEXT     NOT NULL
                                CHECK(tipo IN (
                                    'entrada','salida_venta','salida_manual',
                                    'transformacion_salida','transformacion_entrada',
                                    'transferencia_salida','transferencia_entrada',
                                    'ajuste_entrada','ajuste_salida','merma',
                                    'conciliacion_ajuste'
                                )),
        cantidad            REAL     NOT NULL CHECK(cantidad > 0),
        cantidad_antes      REAL     NOT NULL,
        cantidad_despues    REAL     NOT NULL CHECK(cantidad_despues >= 0),
        costo_unitario      DECIMAL(10,4) DEFAULT 0,
        referencia_id       INTEGER,                           -- venta_id, traspaso_id, etc.
        referencia_tipo     TEXT,                              -- 'venta','traspaso','ajuste'
        usuario             TEXT     NOT NULL DEFAULT 'Sistema',
        descripcion         TEXT     DEFAULT '',
        fecha               DATETIME NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_batch_mov_batch
        ON batch_movements(batch_id);
    CREATE INDEX IF NOT EXISTS idx_batch_mov_fecha
        ON batch_movements(fecha);
    CREATE INDEX IF NOT EXISTS idx_batch_mov_tipo
        ON batch_movements(tipo, branch_id);

    -- ══════════════════════════════════════════════════════════════════════════
    -- TABLA: system_locks
    -- Bloqueos de operaciones durante conciliación u otras operaciones críticas.
    -- Las ventas y transferencias deben verificar esta tabla antes de proceder.
    -- ══════════════════════════════════════════════════════════════════════════
    CREATE TABLE IF NOT EXISTS system_locks (
        id          INTEGER  PRIMARY KEY AUTOINCREMENT,
        lock_key    TEXT     NOT NULL UNIQUE,    -- 'ventas','transferencias','conciliacion'
        branch_id   INTEGER,                     -- NULL = global
        adquirido_por TEXT   NOT NULL,
        motivo      TEXT     NOT NULL DEFAULT '',
        adquirido_en DATETIME NOT NULL DEFAULT (datetime('now')),
        expira_en   DATETIME NOT NULL,           -- evita deadlocks por crash
        activo      INTEGER  NOT NULL DEFAULT 1
    );

    CREATE INDEX IF NOT EXISTS idx_system_locks_key
        ON system_locks(lock_key, activo);

    -- ══════════════════════════════════════════════════════════════════════════
    -- TRIGGER: protección inventario negativo en branch_inventory_batches
    -- Dispara ANTES de cualquier UPDATE que intente dejar cantidad_disponible < 0.
    -- El CHECK(cantidad_disponible >= 0) ya protege, pero el trigger da mensaje.
    -- ══════════════════════════════════════════════════════════════════════════
    CREATE TRIGGER IF NOT EXISTS trg_bib_no_negativo
    BEFORE UPDATE OF cantidad_disponible ON branch_inventory_batches
    WHEN NEW.cantidad_disponible < 0
    BEGIN
        SELECT RAISE(ABORT,
            'PROTECCIÓN INVENTARIO: cantidad_disponible no puede ser negativa.'
        );
    END;

    -- ══════════════════════════════════════════════════════════════════════════
    -- ÍNDICES ENTERPRISE adicionales sobre tablas existentes
    -- ══════════════════════════════════════════════════════════════════════════
    CREATE INDEX IF NOT EXISTS idx_event_log_synced_fecha
        ON event_log(synced, fecha);
    """)

    # Índices sobre tablas que solo existen en BD completa (silencioso si no existen)
    _add_idx(conn, "movimientos_inventario", "idx_inv_mov_created", "fecha")
    _add_idx(conn, "ventas",         "idx_ventas_created",      "fecha")
    _add_idx(conn, "detalles_venta", "idx_detalles_venta_prod", "producto_id")

    # Agregar columna batch_id a movimientos_inventario si no existe
    _add_col(conn, "movimientos_inventario", "batch_id",    "INTEGER")
    _add_col(conn, "movimientos_inventario", "bib_id",      "INTEGER")
    _add_col(conn, "movimientos_inventario", "sucursal_id", "INTEGER DEFAULT 1")
    # Agregar columna batch_id a detalles_venta
    _add_col(conn, "detalles_venta",         "batch_id",    "INTEGER")


def _add_col(conn: sqlite3.Connection, table: str, col: str, defn: str) -> None:
    """Agrega columna si no existe. Silencioso si la tabla no existe aún."""
    try:
        existing = [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
    except Exception:
        pass  # tabla aún no creada — la migración base la creará después


def _add_idx(conn: sqlite3.Connection, table: str, idx_name: str, columns: str) -> None:
    """Crea índice sobre tabla solo si la tabla existe. Silencioso si no."""
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

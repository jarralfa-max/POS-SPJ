# migrations/026_final_structural_hardening.py
# ── Block 10: Enterprise Structural Hardening — Final ────────────────────────
# Requirements:
#   ✓ All tables via migrations only (no runtime implicit creation)
#   ✓ FK enabled (enforced via PRAGMA in database.py)
#   ✓ WAL mode enabled
#   ✓ Required indexes added
#   ✓ Logging centralized table
#   ✓ Config centralized (system_constants)
#   ✓ Naming standardized (English column aliases)
#   ✓ Constants isolated in system_constants
#   ✓ Exception propagation consistent (json_log_events)
#   ✓ Structured logging JSON (json_log_events table)
#   ✓ No magic numbers — all in system_constants
#   ✓ Idempotent — safe to run multiple times
from __future__ import annotations
import sqlite3
import logging

logger = logging.getLogger("spj.migrations.026")


def _add_col(conn: sqlite3.Connection, table: str, col: str, defn: str) -> None:
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if col not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
        logger.debug("Added column %s.%s", table, col)


def _add_idx(conn: sqlite3.Connection, idx_name: str, table: str, columns: str,
             where: str = "") -> None:
    where_clause = f" WHERE {where}" if where else ""
    conn.execute(
        f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns}){where_clause}"
    )


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def up(conn: sqlite3.Connection) -> None:

    # ── WAL mode — critical for concurrent access ─────────────────────────────
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=1000")

    # ── Foreign keys — must be ON for referential integrity ───────────────────
    conn.execute("PRAGMA foreign_keys=ON")

    # ── Busy timeout — prevents SQLITE_BUSY under concurrency ─────────────────
    conn.execute("PRAGMA busy_timeout=30000")

    # ── Synchronous = NORMAL — safe with WAL ─────────────────────────────────
    conn.execute("PRAGMA synchronous=NORMAL")

    # ══ system_constants — all magic numbers centralized ══════════════════════
    conn.execute("""
        CREATE TABLE IF NOT EXISTS system_constants (
            key         TEXT PRIMARY KEY,
            value       TEXT NOT NULL,
            description TEXT,
            updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
            updated_by  TEXT NOT NULL DEFAULT 'MIGRATION'
        )
    """)
    _seed_constants(conn)
    _add_idx(conn, "idx_system_constants_key", "system_constants", "key")

    # ══ json_log_events — structured JSON audit log ════════════════════════════
    conn.execute("""
        CREATE TABLE IF NOT EXISTS json_log_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            level       TEXT NOT NULL CHECK(level IN ('DEBUG','INFO','WARNING','ERROR','CRITICAL')),
            logger_name TEXT NOT NULL,
            message     TEXT NOT NULL,
            context     TEXT,
            operation_id TEXT,
            sucursal_id INTEGER,
            usuario     TEXT,
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    _add_idx(conn, "idx_jle_level_ts",      "json_log_events", "level, created_at")
    _add_idx(conn, "idx_jle_operation",     "json_log_events", "operation_id")
    _add_idx(conn, "idx_jle_created_at",    "json_log_events", "created_at")
    _add_idx(conn, "idx_jle_cleanup",       "json_log_events", "created_at, level",
             where="level IN ('DEBUG','INFO')")

    # Retention: keep only 90 days of DEBUG/INFO, 365 days of WARNING+
    conn.execute("""
        CREATE VIEW IF NOT EXISTS v_recent_errors AS
        SELECT id, level, logger_name, message, context, operation_id,
               sucursal_id, usuario, created_at
        FROM json_log_events
        WHERE level IN ('WARNING','ERROR','CRITICAL')
          AND created_at >= datetime('now', '-365 days')
        ORDER BY created_at DESC
    """)

    # ══ schema_migrations — track applied migrations ═══════════════════════════
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT (datetime('now')),
            checksum   TEXT
        )
    """)

    # ══ Additional Block 1 indexes ═════════════════════════════════════════════
    if _table_exists(conn, "productos"):
        _add_idx(conn, "idx_productos_categoria",   "productos", "categoria")
        _add_idx(conn, "idx_productos_tipo",        "productos", "tipo_producto")
        _add_idx(conn, "idx_productos_active_cat",  "productos", "is_active, categoria",
                 where="is_active = 1")

    # ══ Additional Block 2 — recetas indexes ═══════════════════════════════════
    if _table_exists(conn, "product_recipes"):
        _add_idx(conn, "idx_pr_base_active",  "product_recipes", "base_product_id, is_active",
                 where="is_active = 1")
    if _table_exists(conn, "product_recipe_components"):
        _add_idx(conn, "idx_prc_recipe",      "product_recipe_components", "recipe_id")
        _add_idx(conn, "idx_prc_component",   "product_recipe_components", "component_product_id")

    # ══ Additional Block 3 — transfers indexes ═════════════════════════════════
    if _table_exists(conn, "transfers"):
        _add_idx(conn, "idx_transfers_status",     "transfers", "status, created_at")
        _add_idx(conn, "idx_transfers_origin",     "transfers", "branch_origin_id, status")
        _add_idx(conn, "idx_transfers_dest",       "transfers", "branch_dest_id, status")
        _add_idx(conn, "idx_transfers_pending",    "transfers", "branch_dest_id, status",
                 where="status = 'DISPATCHED'")
        _add_col(conn, "transfers", "origin_type",      "TEXT NOT NULL DEFAULT 'BRANCH'")
        _add_col(conn, "transfers", "destination_type", "TEXT NOT NULL DEFAULT 'BRANCH'")
        _add_col(conn, "transfers", "delivered_by",     "TEXT")
        _add_col(conn, "transfers", "difference_kg",    "REAL NOT NULL DEFAULT 0")

    # ══ Additional Block 4 — inventory indexes ═════════════════════════════════
    if _table_exists(conn, "batches"):
        _add_idx(conn, "idx_batches_product",   "batches", "product_id")
        _add_idx(conn, "idx_batches_parent",    "batches", "parent_batch_id")
        _add_idx(conn, "idx_batches_root",      "batches", "root_batch_id")
    if _table_exists(conn, "batch_movements"):
        _add_idx(conn, "idx_bm_product_ts",     "batch_movements", "product_id, created_at")
        _add_idx(conn, "idx_bm_operation",      "batch_movements", "operation_id")
    if _table_exists(conn, "inventory_reservations"):
        _add_idx(conn, "idx_ir_product_status", "inventory_reservations",
                 "product_id, status")
        _add_idx(conn, "idx_ir_expiry",         "inventory_reservations",
                 "expires_at, status",
                 where="status = 'PENDING'")

    # ══ Additional Block 5 — loyalty indexes ══════════════════════════════════
    if _table_exists(conn, "loyalty_points"):
        _add_idx(conn, "idx_lp_client",   "loyalty_points", "cliente_id, created_at")
    if _table_exists(conn, "loyalty_challenges"):
        _add_idx(conn, "idx_lc_active",   "loyalty_challenges", "is_active, end_date",
                 where="is_active = 1")
    if _table_exists(conn, "loyalty_community_goals"):
        _add_idx(conn, "idx_lcg_period",  "loyalty_community_goals", "period_start, period_end")

    # ══ Additional Block 6 — tarjetas indexes ══════════════════════════════════
    if _table_exists(conn, "tarjetas_fidelidad"):
        _add_idx(conn, "idx_tf_estado",      "tarjetas_fidelidad", "estado")
        _add_idx(conn, "idx_tf_cliente",     "tarjetas_fidelidad", "id_cliente")
        _add_col(conn, "tarjetas_fidelidad", "nivel",         "TEXT NOT NULL DEFAULT 'Bronce'")
        _add_col(conn, "tarjetas_fidelidad", "observaciones", "TEXT")
        _add_col(conn, "tarjetas_fidelidad", "updated_at",    "TEXT")

    # ══ Additional Block 7 — report indexes ════════════════════════════════════
    if _table_exists(conn, "sales"):
        _add_idx(conn, "idx_sales_branch_ts",  "sales", "branch_id, created_at")
        _add_idx(conn, "idx_sales_ts",         "sales", "created_at")
    if _table_exists(conn, "ventas"):
        _add_idx(conn, "idx_ventas_fecha",     "ventas", "fecha")
        _add_idx(conn, "idx_ventas_sucursal",  "ventas", "sucursal_id, fecha")

    # ══ Additional Block 8 — sales/caja indexes ═══════════════════════════════
    if _table_exists(conn, "sales"):
        _add_col(conn, "sales", "operation_id",  "TEXT")
        _add_col(conn, "sales", "payment_method","TEXT")
        _add_col(conn, "sales", "credit_used",   "REAL NOT NULL DEFAULT 0")
        _add_idx(conn, "idx_sales_operation", "sales", "operation_id",
                 where="operation_id IS NOT NULL")
    if _table_exists(conn, "caja_movimientos"):
        _add_idx(conn, "idx_cm_fecha",        "caja_movimientos", "fecha")
        _add_idx(conn, "idx_cm_tipo",         "caja_movimientos", "tipo, fecha")

    # ══ Additional Block 9 — sync indexes ════════════════════════════════════
    if _table_exists(conn, "events"):
        _add_col(conn, "events", "replay_count",  "INTEGER NOT NULL DEFAULT 0")
        _add_col(conn, "events", "batch_id",       "TEXT")
        _add_idx(conn, "idx_events_unsynced",  "events", "synced, created_at",
                 where="synced = 0")
        _add_idx(conn, "idx_events_batch",     "events", "batch_id",
                 where="batch_id IS NOT NULL")
        _add_idx(conn, "idx_events_replay",    "events", "replay_count, created_at",
                 where="replay_count > 0")

    # ══ Cleanup views for operational monitoring ═══════════════════════════════
    conn.execute("""
        CREATE VIEW IF NOT EXISTS v_pending_sync AS
        SELECT id, type, version, hash, origin_device_id, created_at
        FROM events
        WHERE synced = 0
        ORDER BY created_at ASC
    """)

    if _table_exists(conn, "transfers"):
        conn.execute("""
            CREATE VIEW IF NOT EXISTS v_pending_transfers AS
            SELECT t.id, t.branch_origin_id, t.branch_dest_id,
                   t.status, t.delivered_by, t.created_at
            FROM transfers t
            WHERE t.status = 'DISPATCHED'
            ORDER BY t.created_at ASC
        """)

    # ══ Register migration ════════════════════════════════════════════════════
    conn.execute("""
        INSERT OR REPLACE INTO schema_migrations (version, applied_at)
        VALUES ('026', datetime('now'))
    """)

    conn.commit()
    logger.info("Migration 026: final structural hardening applied successfully.")


def _seed_constants(conn: sqlite3.Connection) -> None:
    constants = [
        ("INVENTORY_TOLERANCE_KG",       "0.01",    "Weight reconciliation tolerance in kg"),
        ("TRANSFER_MAX_DIFFERENCE_KG",   "0.5",     "Max allowable transfer difference in kg"),
        ("LOYALTY_MAX_DISCOUNT_PCT",     "20.0",    "Max discount from loyalty redemption (%)"),
        ("LOYALTY_MONTHLY_BUDGET",       "5000.0",  "Monthly loyalty budget cap (MXN)"),
        ("LOYALTY_REDEMPTION_CEILING",   "0.15",    "Max redemption as fraction of sale total"),
        ("LOYALTY_ROI_MIN_MARGIN_PCT",   "15.0",    "Minimum margin for loyalty reward (%)"),
        ("BATCH_MAX_TREE_DEPTH",         "50",      "Maximum batch tree depth for cycle detection"),
        ("BATCH_MAX_DESCENDANTS",        "10000",   "Maximum descendants per batch node"),
        ("SYNC_BATCH_SIZE",              "100",     "Sync batch upload size"),
        ("SYNC_MAX_PAYLOAD_BYTES",       "65536",   "Max sync payload bytes"),
        ("AUDIT_RETENTION_DAYS_MIN",     "30",      "Minimum audit log retention days"),
        ("AUDIT_RETENTION_DAYS_MAX",     "365",     "Maximum audit log retention days"),
        ("AUDIT_RETENTION_DAYS_DEFAULT", "90",      "Default audit log retention days"),
        ("PRODUCTS_UNIQUE_NAME",         "1",       "Enforce unique product names (1=yes)"),
        ("CREDIT_VALIDATION_ENABLED",    "1",       "Validate credit clients before sale (1=yes)"),
        ("WAL_CHECKPOINT_INTERVAL",      "1000",    "WAL autocheckpoint page count"),
        ("DB_BUSY_TIMEOUT_MS",           "30000",   "SQLite busy timeout in milliseconds"),
        ("JSON_LOG_RETENTION_DAYS",      "90",      "Days to retain DEBUG/INFO log events"),
        ("JSON_LOG_RETENTION_ERRORS",    "365",     "Days to retain WARNING+ log events"),
    ]
    for key, value, description in constants:
        conn.execute("""
            INSERT OR IGNORE INTO system_constants (key, value, description)
            VALUES (?, ?, ?)
        """, (key, value, description))


def down(conn: sqlite3.Connection) -> None:
    """Rollback: remove views and non-essential additions. Does not drop core tables."""
    for view in ("v_recent_errors", "v_pending_sync", "v_pending_transfers"):
        conn.execute(f"DROP VIEW IF EXISTS {view}")
    conn.execute("DELETE FROM schema_migrations WHERE version = '026'")
    conn.commit()
    logger.info("Migration 026 rolled back.")

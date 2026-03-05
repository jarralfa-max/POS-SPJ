# migrations/024_enterprise_blocks_5_8.py
# ── Enterprise Migration — Blocks 5, 8, 10 supplementary ───────────────────
# Block 5: loyalty_challenges, loyalty_community_goals, loyalty_budget_caps,
#          loyalty_challenge_progress, loyalty_roi_tracking,
#          loyalty_multiplier_rules, loyalty_redemption_limits
# Block 8: ventas operation_id, credit_approved, caja_operations table
# Block 10: json_log_events, missing indexes, system_constants additions
from __future__ import annotations
import sqlite3


def _add_col(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _add_idx(conn: sqlite3.Connection, table: str, idx_name: str, columns: str) -> None:
    conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns})")


def up(conn: sqlite3.Connection) -> None:

    # ══ BLOCK 5 — FIDELIDAD ENTERPRISE TABLES ═══════════════════════════════

    # Layer 2: Levels / Status history
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_level_history (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id    INTEGER NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
            level_before  TEXT    NOT NULL,
            level_after   TEXT    NOT NULL,
            reason        TEXT,
            created_at    DATETIME DEFAULT (datetime('now'))
        )
    """)
    _add_idx(conn, "loyalty_level_history", "idx_llh_client", "cliente_id")

    # Layer 3: Challenges (Gamification)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_challenges (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT    NOT NULL,
            description     TEXT,
            challenge_type  TEXT    NOT NULL DEFAULT 'PURCHASES',
            target_value    REAL    NOT NULL DEFAULT 0,
            points_reward   INTEGER NOT NULL DEFAULT 0,
            start_date      DATE    NOT NULL,
            end_date        DATE    NOT NULL,
            is_active       INTEGER NOT NULL DEFAULT 1,
            branch_id       INTEGER,
            min_level       TEXT,
            created_at      DATETIME DEFAULT (datetime('now'))
        )
    """)
    _add_idx(conn, "loyalty_challenges", "idx_lc_active", "is_active, end_date")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_challenge_progress (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            challenge_id    INTEGER NOT NULL REFERENCES loyalty_challenges(id) ON DELETE CASCADE,
            cliente_id      INTEGER NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
            current_value   REAL    NOT NULL DEFAULT 0,
            completed       INTEGER NOT NULL DEFAULT 0,
            completed_at    DATETIME,
            points_awarded  INTEGER NOT NULL DEFAULT 0,
            updated_at      DATETIME DEFAULT (datetime('now')),
            UNIQUE(challenge_id, cliente_id)
        )
    """)
    _add_idx(conn, "loyalty_challenge_progress", "idx_lcp_client", "cliente_id, completed")
    _add_idx(conn, "loyalty_challenge_progress", "idx_lcp_challenge", "challenge_id")

    # Layer 4: Community Goals
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_community_goals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT    NOT NULL,
            description     TEXT,
            target_value    REAL    NOT NULL DEFAULT 0,
            current_value   REAL    NOT NULL DEFAULT 0,
            goal_type       TEXT    NOT NULL DEFAULT 'TOTAL_PURCHASES',
            reward_type     TEXT    NOT NULL DEFAULT 'MULTIPLIER',
            reward_value    REAL    NOT NULL DEFAULT 1.5,
            start_date      DATE    NOT NULL,
            end_date        DATE    NOT NULL,
            is_active       INTEGER NOT NULL DEFAULT 1,
            branch_id       INTEGER,
            achieved        INTEGER NOT NULL DEFAULT 0,
            achieved_at     DATETIME,
            created_at      DATETIME DEFAULT (datetime('now'))
        )
    """)
    _add_idx(conn, "loyalty_community_goals", "idx_lcg_active", "is_active, end_date")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_community_contributions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            goal_id         INTEGER NOT NULL REFERENCES loyalty_community_goals(id) ON DELETE CASCADE,
            cliente_id      INTEGER NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
            venta_id        INTEGER,
            contribution    REAL    NOT NULL DEFAULT 0,
            created_at      DATETIME DEFAULT (datetime('now'))
        )
    """)
    _add_idx(conn, "loyalty_community_contributions", "idx_lcc_goal", "goal_id")
    _add_idx(conn, "loyalty_community_contributions", "idx_lcc_client", "cliente_id")

    # Financial guards: budget caps per month per branch
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_budget_caps (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id       INTEGER NOT NULL,
            year_month      TEXT    NOT NULL,
            budget_limit    REAL    NOT NULL DEFAULT 0,
            points_issued   INTEGER NOT NULL DEFAULT 0,
            value_issued    REAL    NOT NULL DEFAULT 0,
            updated_at      DATETIME DEFAULT (datetime('now')),
            UNIQUE(branch_id, year_month)
        )
    """)
    _add_idx(conn, "loyalty_budget_caps", "idx_lbc_branch_month", "branch_id, year_month")

    # Multiplier rules
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_multiplier_rules (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_type       TEXT    NOT NULL,
            condition_value TEXT    NOT NULL,
            multiplier      REAL    NOT NULL DEFAULT 1.0,
            priority        INTEGER NOT NULL DEFAULT 0,
            is_active       INTEGER NOT NULL DEFAULT 1,
            created_at      DATETIME DEFAULT (datetime('now'))
        )
    """)

    # Redemption limits per branch
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_redemption_limits (
            branch_id           INTEGER PRIMARY KEY,
            max_pct_per_sale    REAL    NOT NULL DEFAULT 30.0,
            max_pts_per_sale    INTEGER NOT NULL DEFAULT 500,
            max_monthly_pts     INTEGER NOT NULL DEFAULT 5000,
            min_purchase_for_points REAL NOT NULL DEFAULT 50.0,
            updated_at          DATETIME DEFAULT (datetime('now'))
        )
    """)

    # ROI tracking
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_roi_tracking (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id       INTEGER NOT NULL,
            year_month      TEXT    NOT NULL,
            revenue_from_loyal_customers REAL NOT NULL DEFAULT 0,
            cost_of_rewards REAL    NOT NULL DEFAULT 0,
            roi_pct         REAL    NOT NULL DEFAULT 0,
            computed_at     DATETIME DEFAULT (datetime('now')),
            UNIQUE(branch_id, year_month)
        )
    """)

    # Points transaction log
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_points_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id      INTEGER NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
            venta_id        INTEGER,
            points_delta    INTEGER NOT NULL DEFAULT 0,
            operation_type  TEXT    NOT NULL DEFAULT 'EARN',
            multiplier      REAL    NOT NULL DEFAULT 1.0,
            balance_after   INTEGER NOT NULL DEFAULT 0,
            branch_id       INTEGER,
            usuario         TEXT,
            notes           TEXT,
            created_at      DATETIME DEFAULT (datetime('now'))
        )
    """)
    _add_idx(conn, "loyalty_points_log", "idx_lpl_client", "cliente_id, created_at DESC")
    _add_idx(conn, "loyalty_points_log", "idx_lpl_venta", "venta_id")

    # loyalty_scores UPSERT target
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_scores (
            cliente_id      INTEGER PRIMARY KEY REFERENCES clientes(id) ON DELETE CASCADE,
            score_total     REAL    NOT NULL DEFAULT 0,
            nivel           TEXT    NOT NULL DEFAULT 'Bronce',
            score_frecuencia REAL   NOT NULL DEFAULT 0,
            score_volumen   REAL    NOT NULL DEFAULT 0,
            score_margen    REAL    NOT NULL DEFAULT 0,
            score_comunidad REAL    NOT NULL DEFAULT 0,
            updated_at      DATETIME DEFAULT (datetime('now'))
        )
    """)

    # clientes loyalty columns
    _add_col(conn, "clientes", "nivel_fidelidad",   "TEXT    NOT NULL DEFAULT 'Bronce'")
    _add_col(conn, "clientes", "puntos",            "INTEGER NOT NULL DEFAULT 0")
    _add_col(conn, "clientes", "allows_credit",     "INTEGER NOT NULL DEFAULT 0")
    _add_col(conn, "clientes", "credit_limit",      "REAL    NOT NULL DEFAULT 0")
    _add_col(conn, "clientes", "credit_balance",    "REAL    NOT NULL DEFAULT 0")
    _add_col(conn, "clientes", "fecha_registro",    "DATETIME DEFAULT (datetime('now'))")

    # system_constants for loyalty
    conn.execute("""
        CREATE TABLE IF NOT EXISTS system_constants (
            key     TEXT PRIMARY KEY,
            value   TEXT NOT NULL,
            updated_at DATETIME DEFAULT (datetime('now'))
        )
    """)

    defaults = [
        ("LOYALTY_MARGIN_FLOOR",       "0.15"),
        ("LOYALTY_POINTS_PER_PESO",    "1.0"),
        ("LOYALTY_MAX_DISCOUNT_PCT",   "30.0"),
        ("LOYALTY_MONTHLY_BUDGET_CAP", "5000.0"),
        ("LOYALTY_ROI_FLOOR",          "1.2"),
        ("TRANSFER_MAX_DIFFERENCE_KG", "0.5"),
        ("CREDIT_VALIDATION_ENABLED",  "1"),
        ("SYNC_BATCH_SIZE",            "100"),
    ]
    for k, v in defaults:
        conn.execute(
            "INSERT OR IGNORE INTO system_constants(key, value) VALUES (?,?)",
            (k, v)
        )

    # ══ BLOCK 8 — VENTAS Y CAJA ══════════════════════════════════════════════

    # ventas enterprise columns
    _add_col(conn, "ventas", "operation_id",    "TEXT")
    _add_col(conn, "ventas", "credit_approved", "INTEGER NOT NULL DEFAULT 1")
    _add_col(conn, "ventas", "loyalty_points",  "INTEGER NOT NULL DEFAULT 0")
    _add_col(conn, "ventas", "margin_pct",      "REAL    NOT NULL DEFAULT 0")
    _add_col(conn, "ventas", "observations",    "TEXT")

    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ventas_operation_id
        ON ventas(operation_id) WHERE operation_id IS NOT NULL
    """)

    # caja_operations — enterprise atomic caja table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS caja_operations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id       INTEGER NOT NULL,
            operation_id    TEXT    NOT NULL UNIQUE,
            operation_type  TEXT    NOT NULL CHECK(operation_type IN ('INGRESO','EGRESO','APERTURA','CIERRE','AJUSTE')),
            amount          REAL    NOT NULL DEFAULT 0 CHECK(amount >= 0),
            usuario         TEXT    NOT NULL,
            reference       TEXT,
            forma_pago      TEXT,
            venta_id        INTEGER,
            notes           TEXT,
            created_at      DATETIME DEFAULT (datetime('now'))
        )
    """)
    _add_idx(conn, "caja_operations", "idx_co_branch_date",   "branch_id, created_at DESC")
    _add_idx(conn, "caja_operations", "idx_co_operation_type","operation_type")
    _add_idx(conn, "caja_operations", "idx_co_venta",         "venta_id")

    # movimientos_caja (legacy table — add missing columns)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS movimientos_caja (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo        TEXT    NOT NULL,
            monto       REAL    NOT NULL DEFAULT 0,
            descripcion TEXT,
            forma_pago  TEXT,
            usuario     TEXT,
            sucursal_id INTEGER,
            fecha       DATETIME DEFAULT (datetime('now'))
        )
    """)
    _add_col(conn, "movimientos_caja", "sucursal_id", "INTEGER")
    _add_col(conn, "movimientos_caja", "operation_id", "TEXT")
    _add_idx(conn, "movimientos_caja", "idx_mc_fecha",    "fecha DESC")
    _add_idx(conn, "movimientos_caja", "idx_mc_sucursal", "sucursal_id, fecha DESC")

    # ══ BLOCK 10 — STRUCTURAL HARDENING ══════════════════════════════════════

    # JSON structured log events
    conn.execute("""
        CREATE TABLE IF NOT EXISTS json_log_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            level       TEXT    NOT NULL DEFAULT 'INFO',
            logger      TEXT    NOT NULL,
            message     TEXT    NOT NULL,
            payload     TEXT,
            branch_id   INTEGER,
            usuario     TEXT,
            created_at  DATETIME DEFAULT (datetime('now'))
        )
    """)
    _add_idx(conn, "json_log_events", "idx_jle_level_date", "level, created_at DESC")

    # Report cache / KPI snapshots
    conn.execute("""
        CREATE TABLE IF NOT EXISTS kpi_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id   INTEGER NOT NULL,
            date_from   DATE    NOT NULL,
            date_to     DATE    NOT NULL,
            snapshot    TEXT    NOT NULL,
            generated_at DATETIME DEFAULT (datetime('now')),
            UNIQUE(branch_id, date_from, date_to)
        )
    """)

    # Additional performance indexes
    _add_idx(conn, "ventas", "idx_ventas_fecha_suc",  "sucursal_id, fecha DESC")
    _add_idx(conn, "ventas", "idx_ventas_operation",  "operation_id")
    _add_idx(conn, "ventas", "idx_ventas_cliente",    "cliente_id")

    existing_tables = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "detalles_venta" in existing_tables:
        _add_idx(conn, "detalles_venta", "idx_dv_venta",   "venta_id")
        _add_idx(conn, "detalles_venta", "idx_dv_producto", "producto_id")

    conn.commit()


def down(conn: sqlite3.Connection) -> None:
    # Non-destructive: only drop tables added in this migration
    for table in [
        "loyalty_community_contributions",
        "loyalty_challenge_progress",
        "loyalty_community_goals",
        "loyalty_challenges",
        "loyalty_budget_caps",
        "loyalty_multiplier_rules",
        "loyalty_redemption_limits",
        "loyalty_roi_tracking",
        "loyalty_points_log",
        "loyalty_level_history",
        "loyalty_scores",
        "caja_operations",
        "json_log_events",
        "kpi_snapshots",
    ]:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.commit()

# migrations/023_enterprise_upgrade.py
# ── Enterprise Upgrade Migration — SPJ v11 ───────────────────────────────────
# Blocks 1-10 structural changes:
#   Block 1: productos soft-delete, UNIQUE constraints, is_active/deleted_at
#   Block 2: recetas FK constraints, ON DELETE RESTRICT, cycle prevention columns
#   Block 3: transferencias two-phase (dispatch + reception)
#   Block 4: inventory_reservations, reservation_id column
#   Block 5: loyalty_enterprise (challenges, community_goals, budget_caps)
#   Block 6: tarjetas persistence fix, config_diseno_tarjetas migration
#   Block 7: report_cache, kpi_snapshots
#   Block 8: sales operation_id, credit_validations
#   Block 9: sync_batch_log, sync_version_history
#   Block 10: additional indexes, json_log_events, constants table
from __future__ import annotations
import sqlite3


def _add_col(conn, table, column, definition):
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def _add_idx(conn, table, idx_name, columns):
    conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns})")


def up(conn: sqlite3.Connection) -> None:

    # ══ BLOCK 1 — PRODUCTOS ══════════════════════════════════════════════════

    # Soft delete columns on productos
    _add_col(conn, "productos", "is_active",   "INTEGER NOT NULL DEFAULT 1")
    _add_col(conn, "productos", "deleted_at",  "DATETIME")
    _add_col(conn, "productos", "deleted_by",  "TEXT")

    # Migrate existing oculto=1 rows to is_active=0
    conn.execute("""
        UPDATE productos SET is_active = 0
        WHERE oculto = 1 AND is_active = 1
    """)

    # UNIQUE constraint on productos.nombre (case-insensitive via normalised shadow column)
    _add_col(conn, "productos", "nombre_normalizado", "TEXT")
    conn.execute("UPDATE productos SET nombre_normalizado = LOWER(TRIM(nombre)) WHERE nombre_normalizado IS NULL")

    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_productos_nombre_normalizado
        ON productos(nombre_normalizado)
        WHERE is_active = 1
    """)

    # UNIQUE constraint on sku if column exists
    prod_cols = {r[1] for r in conn.execute("PRAGMA table_info(productos)").fetchall()}
    if "sku" in prod_cols:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_productos_sku
            ON productos(sku)
            WHERE sku IS NOT NULL AND is_active = 1
        """)

    # Protection flags table for deletion guards
    conn.execute("""
        CREATE TABLE IF NOT EXISTS productos_deletion_guard (
            producto_id INTEGER PRIMARY KEY REFERENCES productos(id) ON DELETE RESTRICT,
            has_sales   INTEGER NOT NULL DEFAULT 0,
            has_movements INTEGER NOT NULL DEFAULT 0,
            has_recipes INTEGER NOT NULL DEFAULT 0,
            last_checked DATETIME DEFAULT (datetime('now'))
        )
    """)

    _add_idx(conn, "productos", "idx_productos_is_active", "is_active")
    _add_idx(conn, "productos", "idx_productos_nombre_norm", "nombre_normalizado")

    # ══ BLOCK 2 — RECETAS ═══════════════════════════════════════════════════

    # product_recipes enterprise columns
    _add_col(conn, "product_recipes", "is_active",        "INTEGER NOT NULL DEFAULT 1")
    _add_col(conn, "product_recipes", "validates_at",     "DATETIME")
    _add_col(conn, "product_recipes", "total_rendimiento","REAL DEFAULT 0")
    _add_col(conn, "product_recipes", "total_merma",      "REAL DEFAULT 0")

    # Ensure UNIQUE on product_recipes(base_product_id) — one recipe per base product
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_product_recipes_base
        ON product_recipes(base_product_id)
        WHERE is_active = 1
    """)

    # Recipe components: ON DELETE RESTRICT already enforced at app layer;
    # add referential columns
    _add_col(conn, "product_recipe_components", "merma_pct",   "REAL NOT NULL DEFAULT 0")
    _add_col(conn, "product_recipe_components", "orden",       "INTEGER NOT NULL DEFAULT 0")
    _add_col(conn, "product_recipe_components", "descripcion", "TEXT")

    # Cycle detection support
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recipe_dependency_graph (
            parent_recipe_id INTEGER NOT NULL REFERENCES product_recipes(id),
            child_product_id INTEGER NOT NULL REFERENCES productos(id),
            depth            INTEGER NOT NULL DEFAULT 1,
            PRIMARY KEY (parent_recipe_id, child_product_id)
        )
    """)

    # ══ BLOCK 3 — TRANSFERENCIAS ════════════════════════════════════════════

    conn.execute("""
        CREATE TABLE IF NOT EXISTS transfers (
            id                  TEXT PRIMARY KEY,
            branch_origin_id    INTEGER NOT NULL,
            branch_dest_id      INTEGER NOT NULL,
            origin_type         TEXT NOT NULL DEFAULT 'BRANCH'
                                CHECK(origin_type IN ('BRANCH','GLOBAL')),
            destination_type    TEXT NOT NULL DEFAULT 'BRANCH'
                                CHECK(destination_type IN ('BRANCH','GLOBAL')),
            status              TEXT NOT NULL DEFAULT 'PENDING'
                                CHECK(status IN ('PENDING','DISPATCHED','RECEIVED','CANCELLED')),
            dispatched_by       TEXT NOT NULL,
            dispatched_at       DATETIME,
            received_by         TEXT,
            received_at         DATETIME,
            observations        TEXT,
            difference_kg       REAL DEFAULT 0,
            operation_id        TEXT UNIQUE,
            created_at          DATETIME NOT NULL DEFAULT (datetime('now')),
            _sync_version       INTEGER NOT NULL DEFAULT 0
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS transfer_items (
            id              TEXT PRIMARY KEY,
            transfer_id     TEXT NOT NULL REFERENCES transfers(id) ON DELETE RESTRICT,
            product_id      INTEGER NOT NULL REFERENCES productos(id) ON DELETE RESTRICT,
            quantity_sent   REAL NOT NULL CHECK(quantity_sent > 0),
            unit            TEXT NOT NULL DEFAULT 'kg',
            quantity_received REAL,
            difference      REAL GENERATED ALWAYS AS
                                (COALESCE(quantity_received,0) - quantity_sent) VIRTUAL,
            batch_id        TEXT,
            notes           TEXT
        )
    """)

    _add_idx(conn, "transfers",      "idx_transfers_status",     "status")
    _add_idx(conn, "transfers",      "idx_transfers_origin",     "branch_origin_id, status")
    _add_idx(conn, "transfers",      "idx_transfers_dest",       "branch_dest_id, status")
    _add_idx(conn, "transfer_items", "idx_transfer_items_xfer",  "transfer_id")
    _add_idx(conn, "transfer_items", "idx_transfer_items_prod",  "product_id")

    # Prevent receiving more than sent via trigger
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_transfer_items_received_check
        BEFORE UPDATE OF quantity_received ON transfer_items
        FOR EACH ROW
        WHEN NEW.quantity_received > OLD.quantity_sent
        BEGIN
            SELECT RAISE(ABORT, 'RECEIVED_EXCEEDS_SENT');
        END
    """)

    # Prevent editing after receipt
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_transfer_edit_after_receipt
        BEFORE UPDATE ON transfers
        FOR EACH ROW
        WHEN OLD.status = 'RECEIVED' AND NEW.status != OLD.status
        BEGIN
            SELECT RAISE(ABORT, 'TRANSFER_ALREADY_RECEIVED');
        END
    """)

    # ══ BLOCK 4 — INVENTARIO ════════════════════════════════════════════════

    conn.execute("""
        CREATE TABLE IF NOT EXISTS inventory_reservations (
            id              TEXT PRIMARY KEY,
            branch_id       INTEGER NOT NULL,
            product_id      INTEGER NOT NULL REFERENCES productos(id) ON DELETE RESTRICT,
            reserved_qty    REAL NOT NULL CHECK(reserved_qty > 0),
            operation_id    TEXT NOT NULL,
            operation_type  TEXT NOT NULL,
            expires_at      DATETIME NOT NULL,
            released        INTEGER NOT NULL DEFAULT 0,
            created_at      DATETIME NOT NULL DEFAULT (datetime('now'))
        )
    """)

    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_reservation_op
        ON inventory_reservations(operation_id, product_id, branch_id)
        WHERE released = 0
    """)
    _add_idx(conn, "inventory_reservations", "idx_reservation_branch_prod",
             "branch_id, product_id, released")

    # Add reservation_id FK to batch_movements
    _add_col(conn, "batch_movements", "reservation_id", "TEXT")
    _add_col(conn, "batch_movements", "operation_id",   "TEXT")

    # Zero-negative constraint enforcement view
    conn.execute("""
        CREATE VIEW IF NOT EXISTS v_negative_inventory AS
        SELECT branch_id, product_id, quantity
        FROM branch_inventory
        WHERE quantity < 0
    """)

    # ══ BLOCK 5 — FIDELIDAD ENTERPRISE ══════════════════════════════════════

    # Challenges (gamification)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_challenges (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL,
            description     TEXT,
            challenge_type  TEXT NOT NULL
                            CHECK(challenge_type IN ('PURCHASE_COUNT','AMOUNT','PRODUCT','REFERRAL','COMMUNITY')),
            target_value    REAL NOT NULL,
            reward_points   INTEGER NOT NULL,
            start_date      DATE NOT NULL,
            end_date        DATE NOT NULL,
            is_active       INTEGER NOT NULL DEFAULT 1,
            branch_id       INTEGER,
            created_at      DATETIME NOT NULL DEFAULT (datetime('now'))
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_challenge_progress (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            challenge_id    INTEGER NOT NULL REFERENCES loyalty_challenges(id),
            cliente_id      INTEGER NOT NULL REFERENCES clientes(id),
            current_value   REAL NOT NULL DEFAULT 0,
            completed       INTEGER NOT NULL DEFAULT 0,
            completed_at    DATETIME,
            reward_granted  INTEGER NOT NULL DEFAULT 0,
            UNIQUE(challenge_id, cliente_id)
        )
    """)

    # Community goals
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_community_goals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT NOT NULL,
            description     TEXT,
            target_value    REAL NOT NULL,
            current_value   REAL NOT NULL DEFAULT 0,
            reward_type     TEXT NOT NULL DEFAULT 'MULTIPLIER',
            reward_value    REAL NOT NULL DEFAULT 1.5,
            start_date      DATE NOT NULL,
            end_date        DATE NOT NULL,
            completed       INTEGER NOT NULL DEFAULT 0,
            completed_at    DATETIME,
            is_active       INTEGER NOT NULL DEFAULT 1,
            branch_id       INTEGER
        )
    """)

    # Monthly budget cap for loyalty rewards
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_budget_caps (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id       INTEGER NOT NULL,
            year_month      TEXT NOT NULL,
            budget_limit    REAL NOT NULL,
            spent_amount    REAL NOT NULL DEFAULT 0,
            UNIQUE(branch_id, year_month)
        )
    """)

    # Dynamic multiplier rules
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_multiplier_rules (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_name       TEXT NOT NULL,
            rule_type       TEXT NOT NULL
                            CHECK(rule_type IN ('DAY_OF_WEEK','HOUR_RANGE','PRODUCT_CATEGORY','LEVEL','CHALLENGE')),
            condition_value TEXT NOT NULL,
            multiplier      REAL NOT NULL CHECK(multiplier > 0 AND multiplier <= 10),
            is_active       INTEGER NOT NULL DEFAULT 1,
            priority        INTEGER NOT NULL DEFAULT 0
        )
    """)

    # Redemption ceiling table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_redemption_limits (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id       INTEGER NOT NULL,
            max_pct_per_sale REAL NOT NULL DEFAULT 30.0,
            max_pts_per_sale INTEGER NOT NULL DEFAULT 500,
            max_monthly_pts INTEGER NOT NULL DEFAULT 5000,
            min_purchase_for_points REAL NOT NULL DEFAULT 50.0,
            UNIQUE(branch_id)
        )
    """)

    # ROI tracking
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_roi_tracking (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id       INTEGER NOT NULL,
            year_month      TEXT NOT NULL,
            points_issued   INTEGER NOT NULL DEFAULT 0,
            points_redeemed INTEGER NOT NULL DEFAULT 0,
            cost_of_rewards REAL NOT NULL DEFAULT 0,
            revenue_from_loyal_customers REAL NOT NULL DEFAULT 0,
            roi_pct         REAL,
            computed_at     DATETIME NOT NULL DEFAULT (datetime('now')),
            UNIQUE(branch_id, year_month)
        )
    """)

    # Ticket engagement messages
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_ticket_messages (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            message_type    TEXT NOT NULL
                            CHECK(message_type IN ('COMMUNITY','LEVEL','CHALLENGE','GENERIC')),
            message_template TEXT NOT NULL,
            is_active       INTEGER NOT NULL DEFAULT 1,
            priority        INTEGER NOT NULL DEFAULT 0
        )
    """)

    # Seed default messages
    conn.executemany("""
        INSERT OR IGNORE INTO loyalty_ticket_messages (message_type, message_template, priority)
        VALUES (?,?,?)
    """, [
        ("COMMUNITY",  "¡La comunidad SPJ lleva {community_pct:.0f}% hacia la meta! Compra y suma.", 10),
        ("LEVEL",      "Estás en nivel {level}. Te faltan {pts_next} pts para {next_level}.", 10),
        ("CHALLENGE",  "Reto activo: {challenge_name} — {progress:.0f}% completado.", 10),
        ("GENERIC",    "¡Gracias por tu compra! Acumulaste {pts_earned} puntos. Total: {pts_total}.", 5),
    ])

    _add_idx(conn, "loyalty_challenges",         "idx_challenges_active",  "is_active, end_date")
    _add_idx(conn, "loyalty_challenge_progress", "idx_challenge_prog_clt", "cliente_id, completed")
    _add_idx(conn, "loyalty_community_goals",    "idx_community_active",   "is_active, end_date")

    # ══ BLOCK 6 — TARJETAS ══════════════════════════════════════════════════

    # Ensure tarjetas_fidelidad table exists with all required columns
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tarjetas_fidelidad (
            id              INTEGER PRIMARY KEY,
            codigo_qr       TEXT NOT NULL,
            id_cliente      INTEGER REFERENCES clientes(id),
            estado          TEXT NOT NULL DEFAULT 'disponible'
                            CHECK(estado IN ('disponible','asignada','activa','inactiva','perdida','bloqueada')),
            puntos_iniciales INTEGER NOT NULL DEFAULT 0,
            puntos_actuales  INTEGER NOT NULL DEFAULT 0,
            es_pregenerada  INTEGER NOT NULL DEFAULT 0,
            fecha_creacion  DATETIME NOT NULL DEFAULT (datetime('now')),
            fecha_asignacion DATETIME,
            observaciones   TEXT,
            design_config   TEXT,
            _sync_version   INTEGER NOT NULL DEFAULT 0
        )
    """)

    # Ensure historico_tarjetas exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS historico_tarjetas (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            id_tarjeta  INTEGER NOT NULL REFERENCES tarjetas_fidelidad(id),
            tipo_cambio TEXT NOT NULL,
            valor_anterior TEXT,
            nuevo_valor TEXT,
            usuario     TEXT,
            fecha       DATETIME NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # Ensure config_diseno_tarjetas table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS config_diseno_tarjetas (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            clave   TEXT NOT NULL UNIQUE,
            valor   TEXT
        )
    """)

    _add_idx(conn, "tarjetas_fidelidad", "idx_tarjetas_cliente", "id_cliente")
    _add_idx(conn, "tarjetas_fidelidad", "idx_tarjetas_estado",  "estado")

    # ══ BLOCK 7 — REPORTES CEO LEVEL ════════════════════════════════════════

    conn.execute("""
        CREATE TABLE IF NOT EXISTS kpi_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id       INTEGER NOT NULL,
            snapshot_date   DATE NOT NULL,
            total_revenue   REAL NOT NULL DEFAULT 0,
            total_cost      REAL NOT NULL DEFAULT 0,
            gross_margin    REAL NOT NULL DEFAULT 0,
            gross_margin_pct REAL NOT NULL DEFAULT 0,
            ticket_count    INTEGER NOT NULL DEFAULT 0,
            avg_ticket      REAL NOT NULL DEFAULT 0,
            inventory_value REAL NOT NULL DEFAULT 0,
            active_clients  INTEGER NOT NULL DEFAULT 0,
            new_clients     INTEGER NOT NULL DEFAULT 0,
            points_issued   INTEGER NOT NULL DEFAULT 0,
            computed_at     DATETIME NOT NULL DEFAULT (datetime('now')),
            UNIQUE(branch_id, snapshot_date)
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS report_export_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            report_type TEXT NOT NULL,
            format      TEXT NOT NULL CHECK(format IN ('PDF','EXCEL','CSV')),
            branch_id   INTEGER,
            date_from   DATE,
            date_to     DATE,
            exported_by TEXT NOT NULL,
            file_path   TEXT,
            row_count   INTEGER,
            exported_at DATETIME NOT NULL DEFAULT (datetime('now'))
        )
    """)

    _add_idx(conn, "kpi_snapshots", "idx_kpi_branch_date", "branch_id, snapshot_date DESC")

    # ══ BLOCK 8 — VENTAS / CAJA ══════════════════════════════════════════════

    # operation_id on ventas for idempotency
    _add_col(conn, "ventas", "operation_id",    "TEXT")
    _add_col(conn, "ventas", "credit_approved", "INTEGER NOT NULL DEFAULT 1")
    _add_col(conn, "ventas", "credit_limit_used","REAL DEFAULT 0")

    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ventas_operation_id
        ON ventas(operation_id)
        WHERE operation_id IS NOT NULL
    """)

    # Credit limit tracking on clients
    _add_col(conn, "clientes", "credit_limit",   "REAL DEFAULT 0")
    _add_col(conn, "clientes", "credit_balance", "REAL DEFAULT 0")
    _add_col(conn, "clientes", "allows_credit",  "INTEGER NOT NULL DEFAULT 0")

    # Caja operation log for concurrency safety
    conn.execute("""
        CREATE TABLE IF NOT EXISTS caja_operations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_id       INTEGER NOT NULL,
            operation_id    TEXT NOT NULL UNIQUE,
            operation_type  TEXT NOT NULL
                            CHECK(operation_type IN ('APERTURA','CIERRE','INGRESO','EGRESO','AJUSTE')),
            amount          REAL NOT NULL,
            accumulated_cash REAL,
            usuario         TEXT NOT NULL,
            reference       TEXT,
            created_at      DATETIME NOT NULL DEFAULT (datetime('now'))
        )
    """)

    _add_idx(conn, "caja_operations", "idx_caja_ops_branch",   "branch_id, created_at DESC")
    _add_idx(conn, "ventas",          "idx_ventas_operation",  "operation_id")

    # ══ BLOCK 9 — SINCRONIZACIÓN ════════════════════════════════════════════

    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_batch_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id        TEXT NOT NULL UNIQUE,
            device_id       TEXT NOT NULL,
            event_count     INTEGER NOT NULL,
            compressed_size INTEGER,
            status          TEXT NOT NULL DEFAULT 'PENDING'
                            CHECK(status IN ('PENDING','SENT','CONFIRMED','FAILED')),
            created_at      DATETIME NOT NULL DEFAULT (datetime('now')),
            sent_at         DATETIME,
            confirmed_at    DATETIME,
            error_message   TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_version_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id        TEXT NOT NULL,
            version         INTEGER NOT NULL,
            hash            TEXT NOT NULL,
            device_id       TEXT NOT NULL,
            recorded_at     DATETIME NOT NULL DEFAULT (datetime('now')),
            UNIQUE(event_id, version)
        )
    """)

    _add_idx(conn, "sync_batch_log",      "idx_sync_batch_status", "status, created_at")
    _add_idx(conn, "sync_version_history","idx_sync_ver_event",    "event_id")

    # ══ BLOCK 10 — STRUCTURAL HARDENING ════════════════════════════════════

    # Constants / config table expansion
    conn.execute("""
        CREATE TABLE IF NOT EXISTS system_constants (
            key     TEXT PRIMARY KEY,
            value   TEXT NOT NULL,
            type    TEXT NOT NULL DEFAULT 'STRING'
                    CHECK(type IN ('STRING','INTEGER','REAL','BOOLEAN','JSON')),
            module  TEXT,
            locked  INTEGER NOT NULL DEFAULT 0,
            description TEXT
        )
    """)

    # Insert core constants
    conn.executemany("""
        INSERT OR IGNORE INTO system_constants (key, value, type, module, description)
        VALUES (?,?,?,?,?)
    """, [
        ("WEIGHT_TOLERANCE_KG",     "0.01",  "REAL",    "inventory", "Tolerancia peso batch kg"),
        ("MAX_BATCH_DEPTH",         "50",    "INTEGER", "inventory", "Profundidad máxima árbol batch"),
        ("SYNC_BATCH_SIZE",         "100",   "INTEGER", "sync",      "Eventos por lote sync"),
        ("MAX_PAYLOAD_BYTES",       "1048576","INTEGER","sync",      "Tamaño máximo payload JSON"),
        ("LOYALTY_POINTS_PER_PESO", "1.0",   "REAL",    "loyalty",   "Puntos por peso comprado"),
        ("LOYALTY_MAX_DISCOUNT_PCT","30.0",  "REAL",    "loyalty",   "Descuento máximo por puntos %"),
        ("LOYALTY_MARGIN_FLOOR",    "0.15",  "REAL",    "loyalty",   "Margen mínimo para otorgar puntos"),
        ("CREDIT_VALIDATION_ENABLED","1",   "BOOLEAN", "ventas",    "Validar crédito en ventas"),
        ("TRANSFER_MAX_DIFFERENCE_KG","0.5","REAL",    "transfers", "Diferencia máxima tolerada en traspaso"),
        ("REPORT_EXPORT_PATH",      "exports/","STRING","reports",   "Directorio exportaciones"),
    ])

    # JSON structured audit log
    conn.execute("""
        CREATE TABLE IF NOT EXISTS json_audit_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type  TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id   TEXT,
            payload     TEXT NOT NULL,
            usuario     TEXT NOT NULL,
            branch_id   INTEGER,
            ip_address  TEXT,
            created_at  DATETIME NOT NULL DEFAULT (datetime('now'))
        )
    """)

    _add_idx(conn, "json_audit_log", "idx_audit_event",   "event_type, created_at DESC")
    _add_idx(conn, "json_audit_log", "idx_audit_entity",  "entity_type, entity_id")

    # Critical compound indexes
    _add_idx(conn, "ventas",            "idx_ventas_fecha_suc",    "sucursal_id, fecha DESC")
    _add_idx(conn, "detalles_venta",    "idx_dv_venta",            "venta_id")
    _add_idx(conn, "productos",         "idx_prod_categoria",      "categoria, is_active")
    _add_idx(conn, "clientes",          "idx_clientes_activo",     "activo")

    conn.commit()

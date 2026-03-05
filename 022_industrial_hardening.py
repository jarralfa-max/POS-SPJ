def upgrade(conn):
    # ── Extend concurrency_events with required columns if missing ─────────
    existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(concurrency_events)").fetchall()
    }

    if "branch_id" not in existing:
        conn.execute(
            "ALTER TABLE concurrency_events ADD COLUMN branch_id TEXT"
        )
    if "duration_ms" not in existing:
        conn.execute(
            "ALTER TABLE concurrency_events ADD COLUMN duration_ms INTEGER"
        )
    if "operation_type" not in existing:
        conn.execute(
            "ALTER TABLE concurrency_events ADD COLUMN operation_type TEXT NOT NULL DEFAULT 'UNKNOWN'"
        )

    # ── sync_conflicts table ───────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_conflicts(
            id             TEXT PRIMARY KEY,
            event_id       TEXT NOT NULL,
            conflict_type  TEXT NOT NULL,
            local_version  INTEGER,
            remote_version INTEGER,
            remote_hash    TEXT,
            computed_hash  TEXT,
            resolved       INTEGER NOT NULL DEFAULT 0,
            created_at     TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_sync_conflicts_event
        ON sync_conflicts(event_id, created_at)
    """)

    # ── Extend batch_tree_audits with has_cycle and passed if missing ─────
    ba_existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(batch_tree_audits)").fetchall()
    }
    if "has_cycle" not in ba_existing:
        conn.execute(
            "ALTER TABLE batch_tree_audits ADD COLUMN has_cycle INTEGER NOT NULL DEFAULT 0"
        )
    if "passed" not in ba_existing:
        conn.execute(
            "ALTER TABLE batch_tree_audits ADD COLUMN passed INTEGER NOT NULL DEFAULT 0"
        )
    if "audit_uuid" not in ba_existing:
        conn.execute(
            "ALTER TABLE batch_tree_audits ADD COLUMN audit_uuid TEXT"
        )

    # ── margin_anomalies: add product_id and week_label columns if missing ─
    ma_existing = {
        row[1]
        for row in conn.execute("PRAGMA table_info(margin_anomalies)").fetchall()
    }
    if "product_id" not in ma_existing:
        conn.execute(
            "ALTER TABLE margin_anomalies ADD COLUMN product_id TEXT"
        )
    if "week_label" not in ma_existing:
        conn.execute(
            "ALTER TABLE margin_anomalies ADD COLUMN week_label TEXT"
        )

    # ── system_integrity_reports table ─────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS system_integrity_reports(
            id          TEXT PRIMARY KEY,
            report_type TEXT NOT NULL,
            passed      INTEGER NOT NULL DEFAULT 0,
            details     TEXT,
            created_at  TEXT NOT NULL
        )
    """)

    # ── Hardened indexes (all idempotent) ──────────────────────────────────
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_branch_inventory_branch_product
        ON branch_inventory(branch_id, product_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_batches_parent
        ON batches(parent_batch_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_batches_root
        ON batches(root_batch_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_hash_version
        ON events(hash, version)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_batch_movements_operation
        ON batch_movements(branch_id, operation_type, created_at)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_concurrency_branch_created
        ON concurrency_events(branch_id, created_at)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_batch_tree_audits_created
        ON batch_tree_audits(root_batch_id, created_at)
    """)

    # ── Config defaults ────────────────────────────────────────────────────
    conn.execute("""
        INSERT OR IGNORE INTO configuracion(clave, valor, descripcion, categoria)
        VALUES
            ('integrity_tolerance_kg', '0.01', 'Tolerancia en kg para integridad de lotes', 'Integrity'),
            ('integrity_max_depth', '50', 'Profundidad máxima de árbol de lotes', 'Integrity'),
            ('sync_max_payload_bytes', '1048576', 'Tamaño máximo de payload sync en bytes', 'Sync'),
            ('audit_retention_concurrency_days', '30', 'Retención de concurrency_events en días', 'Cleanup'),
            ('audit_retention_batch_tree_days', '90', 'Retención de batch_tree_audits en días', 'Cleanup'),
            ('audit_retention_integrity_reports_days', '180', 'Retención de system_integrity_reports en días', 'Cleanup')
    """)

    conn.commit()


def downgrade(conn):
    conn.execute("DROP INDEX IF EXISTS idx_branch_inventory_branch_product")
    conn.execute("DROP INDEX IF EXISTS idx_batches_parent")
    conn.execute("DROP INDEX IF EXISTS idx_batches_root")
    conn.execute("DROP INDEX IF EXISTS idx_events_hash_version")
    conn.execute("DROP INDEX IF EXISTS idx_batch_movements_operation")
    conn.execute("DROP INDEX IF EXISTS idx_concurrency_branch_created")
    conn.execute("DROP INDEX IF EXISTS idx_batch_tree_audits_created")
    conn.execute("DROP INDEX IF EXISTS idx_sync_conflicts_event")
    conn.execute("DROP TABLE IF EXISTS sync_conflicts")
    conn.execute("DROP TABLE IF EXISTS system_integrity_reports")
    conn.commit()

def upgrade(conn):
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_branch_inventory_product
        ON branch_inventory(branch_id, product_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_batch_movements_operation
        ON batch_movements(branch_id, operation_type, created_at)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_hash
        ON events(hash)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_sync_created
        ON events(synced, created_at)
    """)
    conn.commit()


def downgrade(conn):
    conn.execute("DROP INDEX IF EXISTS idx_branch_inventory_product")
    conn.execute("DROP INDEX IF EXISTS idx_batch_movements_operation")
    conn.execute("DROP INDEX IF EXISTS idx_events_hash")
    conn.execute("DROP INDEX IF EXISTS idx_events_sync_created")
    conn.commit()

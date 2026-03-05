# migrations/025_sync_batch_log.py
# Block 9 supplement: sync_batch_log, sync_version_history
from __future__ import annotations
import sqlite3


def _add_idx(conn, table, idx_name, columns):
    conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({columns})")


def up(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_batch_log (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id     TEXT    NOT NULL UNIQUE,
            event_count  INTEGER NOT NULL DEFAULT 0,
            status       TEXT    NOT NULL DEFAULT 'SENT',
            created_at   DATETIME DEFAULT (datetime('now')),
            confirmed_at DATETIME
        )
    """)
    _add_idx(conn, "sync_batch_log", "idx_sbl_status", "status, created_at DESC")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_version_history (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id     TEXT    NOT NULL,
            version      INTEGER NOT NULL,
            hash         TEXT    NOT NULL,
            recorded_at  DATETIME DEFAULT (datetime('now'))
        )
    """)
    _add_idx(conn, "sync_version_history", "idx_svh_event", "event_id, version")

    # Add columns to sync_events if missing
    existing = {r[1] for r in conn.execute("PRAGMA table_info(sync_events)").fetchall()}
    if "retry_count" not in existing:
        conn.execute("ALTER TABLE sync_events ADD COLUMN retry_count INTEGER NOT NULL DEFAULT 0")
    if "last_attempt" not in existing:
        conn.execute("ALTER TABLE sync_events ADD COLUMN last_attempt DATETIME")
    if "synced_at" not in existing:
        conn.execute("ALTER TABLE sync_events ADD COLUMN synced_at DATETIME")

    conn.commit()


def down(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS sync_batch_log")
    conn.execute("DROP TABLE IF EXISTS sync_version_history")
    conn.commit()

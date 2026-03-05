# sync/sync_worker.py — Block 9 enterprise supplement
# This file supplements the existing sync/worker.py and sync/sync_engine.py
# Adds: batch_log, unsynced monitoring, duplicate replay prevention.
# Already in worker.py (from security patches):
#   ✓ SQL injection protection (_SYNC_ALLOWED_TABLES whitelist)
#   ✓ Socket resource cleanup
#   ✓ Conflict resolver externalized (sync/conflict_resolver.py)
# New additions here: SyncMonitor for unsynced event tracking
from __future__ import annotations
import logging
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional
logger = logging.getLogger("spj.sync.monitor")


class SyncMonitor:
    """
    Block 9: Unsynced event monitoring.
    Tracks pending events, duplicate replay prevention, and batch log.
    """

    def __init__(self, db):
        self.db = db

    def _now(self) -> str:
        return datetime.utcnow().isoformat()

    def get_unsynced_count(self) -> int:
        """Return count of events pending sync."""
        row = self.db.fetchone(
            "SELECT COUNT(*) AS c FROM sync_events WHERE synced = 0 AND retry_count < 10"
        )
        return int(row["c"] if row else 0)

    def get_unsynced_events(self, limit: int = 200) -> List[Dict]:
        """Return oldest unsynced events sorted by created_at."""
        rows = self.db.fetchall("""
            SELECT id, event_type, payload_json, version, created_at, retry_count
            FROM sync_events
            WHERE synced = 0 AND retry_count < 10
            ORDER BY created_at ASC
            LIMIT ?
        """, (limit,))
        return [dict(r) for r in rows]

    def mark_batch_sent(self, event_ids: List[str], batch_id: str) -> None:
        """Record that a batch was dispatched to the server."""
        with self.db.transaction("SYNC_BATCH_SENT"):
            for eid in event_ids:
                self.db.execute("""
                    UPDATE sync_events SET
                        last_attempt = ?,
                        retry_count  = retry_count + 1
                    WHERE id = ?
                """, (self._now(), eid))
            # Log the batch dispatch
            self.db.execute("""
                INSERT OR IGNORE INTO sync_batch_log(
                    batch_id, event_count, status, created_at
                ) VALUES (?,?,?,?)
            """, (batch_id, len(event_ids), "SENT", self._now()))

    def mark_batch_confirmed(self, event_ids: List[str], batch_id: str) -> None:
        """Mark events as synced after server confirmation."""
        with self.db.transaction("SYNC_BATCH_CONFIRM"):
            for eid in event_ids:
                self.db.execute(
                    "UPDATE sync_events SET synced=1, synced_at=? WHERE id=?",
                    (self._now(), eid)
                )
            self.db.execute(
                "UPDATE sync_batch_log SET status='CONFIRMED', confirmed_at=? WHERE batch_id=?",
                (self._now(), batch_id)
            )

    def is_event_already_applied(self, event_id: str) -> bool:
        """Duplicate replay prevention: returns True if already applied."""
        row = self.db.fetchone(
            "SELECT id FROM sync_events WHERE id=? AND synced=1", (event_id,)
        )
        return row is not None

    def get_sync_status(self) -> Dict:
        """Returns comprehensive sync health status."""
        unsynced = self.get_unsynced_count()
        oldest_row = self.db.fetchone("""
            SELECT MIN(created_at) AS oldest FROM sync_events WHERE synced=0
        """)
        oldest = (oldest_row["oldest"] if oldest_row else None) or ""
        conflicts_row = self.db.fetchone(
            "SELECT COUNT(*) AS c FROM sync_conflicts WHERE resolved=0"
        ) if self._table_exists("sync_conflicts") else None
        conflicts = int(conflicts_row["c"] if conflicts_row else 0)
        return {
            "unsynced_count": unsynced,
            "oldest_unsynced": oldest[:19] if oldest else "",
            "unresolved_conflicts": conflicts,
            "is_healthy": unsynced < 1000 and conflicts < 100,
        }

    def _table_exists(self, table: str) -> bool:
        row = self.db.fetchone(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        return row is not None

    def purge_old_synced(self, days_to_keep: int = 30) -> int:
        """Remove old confirmed events to prevent table bloat."""
        if days_to_keep < 7:
            raise ValueError("days_to_keep must be at least 7")
        cutoff = (datetime.utcnow() - timedelta(days=days_to_keep)).isoformat()
        with self.db.transaction("SYNC_PURGE"):
            cursor = self.db.execute("""
                DELETE FROM sync_events
                WHERE synced = 1 AND synced_at < ?
            """, (cutoff,))
        try:
            return cursor.rowcount
        except Exception:
            return 0

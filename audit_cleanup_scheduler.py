import logging
import sqlite3
import threading
import time
from datetime import datetime

logger = logging.getLogger("spj.audit_cleanup")

DEFAULT_INTERVAL_SECONDS = 86400  # 24 hours


class AuditCleanupScheduler:

    def __init__(self, db_path, interval_seconds=DEFAULT_INTERVAL_SECONDS):
        self.db_path = db_path
        self.interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name="AuditCleanupScheduler",
            daemon=True,
        )
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)

    def run_once(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            self._cleanup_concurrency_events(conn)
            self._cleanup_batch_tree_audits(conn)
            self._cleanup_integrity_reports(conn)
        finally:
            conn.close()

    def _loop(self):
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception as exc:
                logger.error("AuditCleanupScheduler error: %s", exc)
            self._stop_event.wait(timeout=self.interval_seconds)

    def _get_retention_days(self, conn, key, default):
        try:
            row = conn.execute(
                "SELECT valor FROM configuracion WHERE clave = ?", (key,)
            ).fetchone()
            if row and row["valor"]:
                return int(row["valor"])
        except Exception:
            pass
        return default

    def _cleanup_concurrency_events(self, conn):
        days = self._get_retention_days(conn, "audit_retention_concurrency_days", 30)
        try:
            cursor = conn.execute("""
                DELETE FROM concurrency_events
                WHERE created_at < datetime('now', ?)
                  AND final_status IN ('SUCCESS', 'FAILED')
            """, (f"-{days} days",))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(
                    "Cleaned %d concurrency_events older than %d days",
                    cursor.rowcount, days
                )
        except sqlite3.Error as exc:
            logger.error("Failed to clean concurrency_events: %s", exc)
            conn.rollback()

    def _cleanup_batch_tree_audits(self, conn):
        days = self._get_retention_days(conn, "audit_retention_batch_tree_days", 90)
        try:
            cursor = conn.execute("""
                DELETE FROM batch_tree_audits
                WHERE created_at < datetime('now', ?)
                  AND passed = 1
            """, (f"-{days} days",))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(
                    "Cleaned %d batch_tree_audits older than %d days",
                    cursor.rowcount, days
                )
        except sqlite3.Error as exc:
            logger.error("Failed to clean batch_tree_audits: %s", exc)
            conn.rollback()

    def _cleanup_integrity_reports(self, conn):
        days = self._get_retention_days(
            conn, "audit_retention_integrity_reports_days", 180
        )
        try:
            cursor = conn.execute("""
                DELETE FROM system_integrity_reports
                WHERE created_at < datetime('now', ?)
            """, (f"-{days} days",))
            conn.commit()
            if cursor.rowcount > 0:
                logger.info(
                    "Cleaned %d system_integrity_reports older than %d days",
                    cursor.rowcount, days
                )
        except sqlite3.Error as exc:
            logger.error("Failed to clean system_integrity_reports: %s", exc)
            conn.rollback()

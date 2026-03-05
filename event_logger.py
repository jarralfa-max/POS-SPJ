# sync/event_logger.py
# ── EVENT LOGGER v8 — SPJ Enterprise ─────────────────────────────────────────
# Registro offline-first con:
#   - SHA256 del payload (idempotencia en servidor)
#   - origin_device_id (UUID fijo por instalación)
#   - device_version   (contador incremental por dispositivo)
#   - event_version    (versión del schema del payload)
from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
import uuid
from pathlib import Path

logger = logging.getLogger("spj.events")


# ── Device ID: UUID fijo por instalación ─────────────────────────────────────
_DEVICE_ID_FILE = Path(
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data",
        ".device_id",
    )
)
_device_id_lock = threading.Lock()
_DEVICE_ID: str = ""


def _get_device_id() -> str:
    global _DEVICE_ID
    if _DEVICE_ID:
        return _DEVICE_ID
    with _device_id_lock:
        if _DEVICE_ID:
            return _DEVICE_ID
        try:
            _DEVICE_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
            if _DEVICE_ID_FILE.exists():
                _DEVICE_ID = _DEVICE_ID_FILE.read_text().strip()
            if not _DEVICE_ID:
                _DEVICE_ID = str(uuid.uuid4())
                _DEVICE_ID_FILE.write_text(_DEVICE_ID)
        except Exception as exc:
            logger.warning("No se pudo leer/crear .device_id: %s", exc)
            _DEVICE_ID = "unknown-" + str(uuid.uuid4())[:8]
    return _DEVICE_ID


def _sha256_payload(payload: object) -> str:
    raw = (
        payload
        if isinstance(payload, str)
        else json.dumps(payload, sort_keys=True, ensure_ascii=False)
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class EventLogger:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn       = conn
        self._device_id = _get_device_id()
        self._ensure_table()

    def _ensure_table(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS event_log (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid             TEXT    NOT NULL UNIQUE,
                tipo             TEXT    NOT NULL,
                entidad          TEXT    NOT NULL,
                entidad_id       INTEGER,
                payload          TEXT    NOT NULL,
                payload_hash     TEXT,
                sucursal_id      INTEGER NOT NULL DEFAULT 1,
                usuario          TEXT    NOT NULL,
                origin_device_id TEXT    DEFAULT '',
                device_version   INTEGER DEFAULT 0,
                event_version    INTEGER NOT NULL DEFAULT 1,
                synced           INTEGER DEFAULT 0,
                sync_intentos    INTEGER DEFAULT 0,
                sync_error       TEXT,
                fecha            DATETIME DEFAULT CURRENT_TIMESTAMP,
                fecha_sync       DATETIME
            )
        """)
        for idx_sql in [
            "CREATE INDEX IF NOT EXISTS idx_el_synced      ON event_log(synced)",
            "CREATE INDEX IF NOT EXISTS idx_el_tipo        ON event_log(tipo)",
            "CREATE INDEX IF NOT EXISTS idx_el_synced_tipo ON event_log(synced, tipo)",
            "CREATE INDEX IF NOT EXISTS idx_el_hash        ON event_log(payload_hash)",
            "CREATE INDEX IF NOT EXISTS idx_el_device_ver  ON event_log(origin_device_id, device_version)",
        ]:
            try:
                self.conn.execute(idx_sql)
            except Exception:
                pass

    def _next_device_version(self, tipo: str) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(MAX(device_version), 0) + 1 FROM event_log "
            "WHERE origin_device_id = ? AND tipo = ?",
            (self._device_id, tipo),
        ).fetchone()
        return int(row[0]) if row else 1

    def registrar(
        self,
        tipo:          str,
        entidad:       str,
        entidad_id:    int   = None,
        payload:       dict  = None,
        sucursal_id:   int   = 1,
        usuario:       str   = "Sistema",
        event_version: int   = 1,
    ) -> int:
        try:
            event_uuid   = str(uuid.uuid4())
            payload_str  = json.dumps(payload or {}, ensure_ascii=False)
            payload_hash = _sha256_payload(payload_str)
            device_ver   = self._next_device_version(tipo)
            cur = self.conn.execute(
                """
                INSERT INTO event_log
                    (uuid, tipo, entidad, entidad_id, payload, payload_hash,
                     sucursal_id, usuario, origin_device_id, device_version,
                     event_version)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    event_uuid, tipo, entidad, entidad_id,
                    payload_str, payload_hash,
                    sucursal_id, usuario,
                    self._device_id, device_ver,
                    event_version,
                ),
            )
            logger.debug(
                "Evento [%s] %s#%s uuid=%s dev_ver=%d hash=%s",
                tipo, entidad, entidad_id, event_uuid, device_ver, payload_hash[:8],
            )
            return cur.lastrowid
        except Exception as exc:
            logger.warning("EventLogger.registrar falló silenciosamente: %s", exc)
            return -1

    def pendientes(self, limit: int = 100) -> list:
        return self.conn.execute(
            """
            SELECT id, uuid, tipo, entidad, entidad_id, payload,
                   payload_hash, sucursal_id, usuario,
                   origin_device_id, device_version, event_version, fecha
            FROM event_log
            WHERE synced = 0
            ORDER BY fecha ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    def marcar_sincronizado(self, event_id: int) -> None:
        self.conn.execute(
            "UPDATE event_log SET synced=1, fecha_sync=datetime('now') WHERE id=?",
            (event_id,),
        )

    def marcar_sincronizado_por_hash(self, payload_hash: str) -> None:
        """Idempotencia: hash ya existe en servidor → marcar synced sin duplicar."""
        self.conn.execute(
            "UPDATE event_log SET synced=1, fecha_sync=datetime('now') "
            "WHERE payload_hash=? AND synced=0",
            (payload_hash,),
        )

    def marcar_error(self, event_id: int, error: str) -> None:
        self.conn.execute(
            "UPDATE event_log SET sync_intentos=sync_intentos+1, sync_error=? WHERE id=?",
            (str(error)[:500], event_id),
        )

    def contar_pendientes(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) FROM event_log WHERE synced=0"
        ).fetchone()
        return row[0] if row else 0

    def device_id(self) -> str:
        return self._device_id
    


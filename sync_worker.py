# sync/sync_worker.py
# ── SYNC WORKER DISTRIBUIDO — SPJ Enterprise v3.2 ────────────────────────────
# QThread background que envía eventos al servidor central.
# - Reintento exponencial con jitter
# - Backoff progresivo hasta max_backoff
# - marcar synced=1 SOLO si servidor confirma HTTP 200/201
# - Manejo de conflictos HTTP 409
# - Arranca automáticamente al inicio de la app
# - No bloquea UI
from __future__ import annotations

import hashlib
import json
import logging
import random
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, List, Optional

logger = logging.getLogger("spj.sync_worker")


# ── Configuración ─────────────────────────────────────────────────────────────

@dataclass
class SyncConfig:
    url_servidor:      str   = ""
    sucursal_id:       int   = 1
    api_key:           str   = ""
    intervalo_seg:     int   = 60
    batch_size:        int   = 100
    timeout_http:      int   = 30
    max_backoff_seg:   int   = 600
    retry_base_seg:    float = 5.0
    max_reintentos:    int   = 10
    # Política de resolución de conflictos:
    #   SERVER_AUTHORITATIVE — el servidor siempre gana (default seguro)
    #   LAST_WRITE_WINS      — el evento más reciente por device_version gana
    conflict_policy:   str   = "SERVER_AUTHORITATIVE"


# ── Resultado de envío ────────────────────────────────────────────────────────

@dataclass
class SyncResult:
    enviados:    int = 0
    confirmados: int = 0
    conflictos:  int = 0
    errores:     int = 0
    pendientes:  int = 0


# ══════════════════════════════════════════════════════════════════════════════
# INTENTO: importar QThread de PyQt5. Si no disponible, usa threading.Thread.
# ══════════════════════════════════════════════════════════════════════════════

try:
    from PyQt5.QtCore import QThread, pyqtSignal, QObject
    _USE_QTHREAD = True
except ImportError:
    _USE_QTHREAD = False


if _USE_QTHREAD:

    class _SyncWorkerBase(QThread):
        """Base QThread con señales para integración con UI PyQt5."""
        sync_completado  = pyqtSignal(int, int)    # enviados, errores
        sync_error       = pyqtSignal(str)
        sync_progreso    = pyqtSignal(int, int)     # actual, total
        conectividad_ok  = pyqtSignal(bool)

        def __init__(self, config: SyncConfig, conn_factory: Callable, parent=None):
            super().__init__(parent)
            self._config      = config
            self._conn_factory = conn_factory
            self._stop_flag   = threading.Event()
            self._force_now   = threading.Event()
            self._backoff     = 0.0
            self.setDaemon(True)

        def run(self):
            """Loop principal en el QThread."""
            logger.info("SyncWorker QThread arrancado (sucursal=%d)", self._config.sucursal_id)
            while not self._stop_flag.is_set():
                try:
                    result = self._ciclo_sync()
                    self.sync_completado.emit(result.confirmados, result.errores)
                    if result.errores == 0:
                        self._backoff = 0.0
                        self.conectividad_ok.emit(True)
                    else:
                        self._incrementar_backoff()
                        self.conectividad_ok.emit(False)
                except Exception as exc:
                    logger.error("SyncWorker ciclo falló: %s", exc, exc_info=True)
                    self.sync_error.emit(str(exc))
                    self._incrementar_backoff()

                # Esperar intervalo o force_now
                wait = min(
                    self._config.intervalo_seg + self._backoff,
                    self._config.max_backoff_seg,
                )
                self._force_now.wait(timeout=wait)
                self._force_now.clear()

            logger.info("SyncWorker QThread detenido")

        def forzar_sync(self):
            """Activa sync inmediato (llaman desde UI)."""
            self._force_now.set()

        def detener(self):
            self._stop_flag.set()
            self._force_now.set()
            self.wait(3000)

else:
    class _SyncWorkerBase:   # type: ignore
        """Base threading.Thread cuando PyQt5 no está disponible."""

        def __init__(self, config: SyncConfig, conn_factory: Callable, parent=None):
            self._config       = config
            self._conn_factory = conn_factory
            self._stop_flag    = threading.Event()
            self._force_now    = threading.Event()
            self._backoff      = 0.0
            self._thread: Optional[threading.Thread] = None
            # Callbacks en lugar de señales
            self.on_sync_completado: Optional[Callable[[int, int], None]] = None
            self.on_sync_error:      Optional[Callable[[str], None]] = None
            self.on_conectividad:    Optional[Callable[[bool], None]] = None

        def start(self):
            self._stop_flag.clear()
            self._thread = threading.Thread(
                target=self._loop, name="SyncWorker", daemon=True
            )
            self._thread.start()

        def _loop(self):
            logger.info("SyncWorker Thread arrancado (sucursal=%d)", self._config.sucursal_id)
            while not self._stop_flag.is_set():
                try:
                    result = self._ciclo_sync()
                    if self.on_sync_completado:
                        self.on_sync_completado(result.confirmados, result.errores)
                    if result.errores == 0:
                        self._backoff = 0.0
                        if self.on_conectividad:
                            self.on_conectividad(True)
                    else:
                        self._incrementar_backoff()
                        if self.on_conectividad:
                            self.on_conectividad(False)
                except Exception as exc:
                    logger.error("SyncWorker ciclo falló: %s", exc, exc_info=True)
                    if self.on_sync_error:
                        self.on_sync_error(str(exc))
                    self._incrementar_backoff()

                wait = min(
                    self._config.intervalo_seg + self._backoff,
                    self._config.max_backoff_seg,
                )
                self._force_now.wait(timeout=wait)
                self._force_now.clear()

            logger.info("SyncWorker Thread detenido")

        def forzar_sync(self):
            self._force_now.set()

        def detener(self):
            self._stop_flag.set()
            self._force_now.set()
            if self._thread:
                self._thread.join(timeout=5)


# ══════════════════════════════════════════════════════════════════════════════
# SYNC WORKER REAL — lógica de envío HTTP
# ══════════════════════════════════════════════════════════════════════════════

class SyncWorker(_SyncWorkerBase):
    """
    Worker de sincronización distribuida offline-first.

    Protocolo esperado del servidor:
        POST /sync   JSON body: { "sucursal_id": int, "events": [...] }
        Respuestas:
            200/201  { "confirmados": [uuid1, uuid2, ...] }
            409      { "conflictos": [{"uuid": ..., "razon": ...}], "confirmados": [...] }
            4xx/5xx  Error — retry con backoff

    Uso (PyQt5):
        worker = SyncWorker(config, conn_factory=get_connection)
        worker.sync_completado.connect(mi_slot)
        worker.start()

    Uso (threading):
        worker = SyncWorker(config, conn_factory=get_connection)
        worker.on_sync_completado = lambda e, err: print(e, err)
        worker.start()
    """

    def __init__(
        self,
        config:        SyncConfig,
        conn_factory:  Callable[[], sqlite3.Connection],
        parent=None,
    ):
        super().__init__(config, conn_factory, parent)

    # ── Ciclo de sync ─────────────────────────────────────────────────────────

    def _ciclo_sync(self) -> SyncResult:
        """
        Recolecta eventos pendientes, los envía al servidor, confirma los exitosos.
        Maneja idempotencia por hash y resolución de conflictos por política.
        """
        if not self._config.url_servidor:
            return SyncResult()

        conn = self._conn_factory()
        pendientes = self._obtener_pendientes(conn)

        if not pendientes:
            return SyncResult()

        result = SyncResult(
            enviados=len(pendientes),
            pendientes=self._contar_pendientes(conn),
        )

        for i in range(0, len(pendientes), self._config.batch_size):
            lote = pendientes[i: i + self._config.batch_size]
            try:
                resp = self._enviar_lote(lote)
                confirmados   = resp.get("confirmados",   [])
                conflictos    = resp.get("conflictos",    [])
                ya_existentes = resp.get("ya_existentes", [])  # hashes idempotentes

                # Marcar ya_existentes como synced (idempotencia por hash)
                if ya_existentes:
                    for h in ya_existentes:
                        try:
                            conn.execute(
                                "UPDATE event_log SET synced=1, fecha_sync=datetime('now') "
                                "WHERE payload_hash=? AND synced=0",
                                (h,),
                            )
                        except Exception:
                            pass
                    result.confirmados += len(ya_existentes)

                if confirmados:
                    self._marcar_sincronizados(conn, confirmados)
                    result.confirmados += len(confirmados)

                if conflictos:
                    self._resolver_conflictos(conn, conflictos)
                    result.conflictos += len(conflictos)

                conn.commit()
                logger.info(
                    "Sync lote %d-%d: +%d conf, %d conflictos, %d idempotentes",
                    i, i + len(lote), len(confirmados),
                    len(conflictos), len(ya_existentes),
                )

            except _ConflictError as ce:
                conflict_data = ce.args[0] if ce.args else {}
                conflictos_d  = conflict_data.get("conflictos", [])
                logger.warning(
                    "Conflicto sync lote %d: %d conflictos — UUIDs: %s",
                    i, len(conflictos_d),
                    [c.get("uuid", "?")[:8] for c in conflictos_d[:5]],
                )
                if conflictos_d:
                    self._resolver_conflictos(conn, conflictos_d)
                    conn.commit()
                result.conflictos += len(conflictos_d) or 1

            except Exception as exc:
                logger.error("Error sync lote %d: %s", i, exc)
                result.errores += 1
                self._marcar_errores_lote(conn, lote, str(exc))
                conn.commit()

        return result

    # ── HTTP ──────────────────────────────────────────────────────────────────

    def _enviar_lote(self, eventos: list) -> dict:
        """
        POST JSON al servidor. Retorna dict con confirmados/conflictos/ya_existentes.
        Lanza _ConflictError en 409, urllib.error.URLError en red.
        Incluye payload_hash para idempotencia y origin_device_id para trazabilidad.
        """
        payload = json.dumps({
            "sucursal_id":      self._config.sucursal_id,
            "timestamp":        datetime.now().isoformat(),
            "conflict_policy":  self._config.conflict_policy,
            "events": [
                {
                    "uuid":             e["uuid"],
                    "tipo":             e["tipo"],
                    "entidad":          e["entidad"],
                    "entidad_id":       e["entidad_id"],
                    "payload":          json.loads(e["payload"]) if isinstance(e["payload"], str) else e["payload"],
                    "payload_hash":     e.get("payload_hash") or _sha256(e["payload"]),
                    "event_version":    e.get("event_version", 1),
                    "origin_device_id": e.get("origin_device_id", ""),
                    "device_version":   e.get("device_version", 0),
                    "usuario":          e["usuario"],
                    "fecha":            e["fecha"],
                }
                for e in eventos
            ],
        }, ensure_ascii=False).encode("utf-8")

        headers = {
            "Content-Type":   "application/json",
            "X-SPJ-API-Key":  self._config.api_key,
            "X-Sucursal-ID":  str(self._config.sucursal_id),
        }

        req = urllib.request.Request(
            self._config.url_servidor,
            data=payload,
            headers=headers,
            method="POST",
        )

        try:
            with urllib.request.urlopen(req, timeout=self._config.timeout_http) as resp:
                body = resp.read().decode("utf-8")
                data = json.loads(body) if body else {}
                return data

        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8")
            except Exception:
                pass

            if exc.code == 409:
                data = json.loads(body) if body else {}
                raise _ConflictError(data) from exc
            raise

    # ── Acceso a BD ───────────────────────────────────────────────────────────

    def _obtener_pendientes(self, conn: sqlite3.Connection) -> List[dict]:
        rows = conn.execute(
            """
            SELECT id, uuid, tipo, entidad, entidad_id, payload,
                   usuario, fecha,
                   COALESCE(payload_hash,   '')  AS payload_hash,
                   COALESCE(event_version,  1)   AS event_version,
                   COALESCE(origin_device_id,'') AS origin_device_id,
                   COALESCE(device_version, 0)   AS device_version
            FROM event_log
            WHERE synced = 0
              AND sync_intentos < ?
            ORDER BY fecha ASC
            LIMIT ?
            """,
            (self._config.max_reintentos, self._config.batch_size),
        ).fetchall()
        return [
            {
                "id":               r[0],
                "uuid":             r[1],
                "tipo":             r[2],
                "entidad":          r[3],
                "entidad_id":       r[4],
                "payload":          r[5],
                "usuario":          r[6],
                "fecha":            r[7],
                "payload_hash":     r[8],
                "event_version":    r[9],
                "origin_device_id": r[10],
                "device_version":   r[11],
            }
            for r in rows
        ]

    def _contar_pendientes(self, conn: sqlite3.Connection) -> int:
        row = conn.execute(
            "SELECT COUNT(*) FROM event_log WHERE synced=0"
        ).fetchone()
        return row[0] if row else 0

    def _marcar_sincronizados(self, conn: sqlite3.Connection, uuids: list) -> None:
        if not uuids:
            return
        placeholders = ",".join("?" * len(uuids))
        conn.execute(
            f"UPDATE event_log SET synced=1, fecha_sync=datetime('now') "
            f"WHERE uuid IN ({placeholders})",
            uuids,
        )

    def _marcar_errores_lote(
        self, conn: sqlite3.Connection, eventos: list, error: str
    ) -> None:
        ids = [e["id"] for e in eventos]
        if not ids:
            return
        placeholders = ",".join("?" * len(ids))
        error_trunc = error[:500]
        conn.execute(
            f"UPDATE event_log SET sync_intentos=sync_intentos+1, sync_error=? "
            f"WHERE id IN ({placeholders})",
            [error_trunc] + ids,
        )

    def _registrar_conflictos(
        self, conn: sqlite3.Connection, conflictos: list
    ) -> None:
        for c in conflictos:
            uuid_ev = c.get("uuid", "")
            razon   = c.get("razon", "conflicto")
            conn.execute(
                "UPDATE event_log SET sync_error=?, sync_intentos=sync_intentos+1 "
                "WHERE uuid=?",
                (f"CONFLICTO: {razon}", uuid_ev),
            )
            logger.warning("Conflicto evento %s: %s", uuid_ev, razon)

    def _resolver_conflictos(
        self, conn: sqlite3.Connection, conflictos: list
    ) -> None:
        """
        Aplica la política de resolución configurada:

        SERVER_AUTHORITATIVE (default):
            El servidor gana siempre. El evento local queda marcado con
            sync_error='CONFLICTO:…' y sync_intentos incrementado.
            No se reenvía automáticamente.

        LAST_WRITE_WINS:
            Compara device_version local vs remote_version del conflicto.
            Si local > remote → reenqueue (synced=0, incrementa event_version).
            Si local <= remote → servidor gana (marcar sync_error).
        """
        policy = self._config.conflict_policy

        for c in conflictos:
            uuid_ev        = c.get("uuid", "")
            razon          = c.get("razon", "conflicto")
            remote_version = int(c.get("remote_version", 0))

            if policy == "LAST_WRITE_WINS":
                row = conn.execute(
                    "SELECT device_version FROM event_log WHERE uuid=?",
                    (uuid_ev,),
                ).fetchone()
                local_ver = int(row[0]) if row else 0

                if local_ver > remote_version:
                    # Local más reciente — reenqueue con event_version incrementado
                    conn.execute(
                        "UPDATE event_log SET synced=0, sync_error=NULL, "
                        "event_version=event_version+1, "
                        "sync_intentos=CASE WHEN sync_intentos>0 THEN sync_intentos-1 ELSE 0 END "
                        "WHERE uuid=?",
                        (uuid_ev,),
                    )
                    logger.info(
                        "LWW: evento %s reenqueued (local_ver=%d > remote=%d)",
                        uuid_ev[:8], local_ver, remote_version,
                    )
                    continue

            # SERVER_AUTHORITATIVE o LWW donde servidor gana
            conn.execute(
                "UPDATE event_log SET sync_error=?, sync_intentos=sync_intentos+1 "
                "WHERE uuid=?",
                (f"CONFLICTO[{policy}]: {razon}", uuid_ev),
            )
            logger.warning(
                "Conflicto [%s] evento %s: %s", policy, uuid_ev[:8], razon
            )

    # ── Backoff exponencial ───────────────────────────────────────────────────

    def _incrementar_backoff(self) -> None:
        """Backoff exponencial con jitter: base * 2^n + random(0, base)."""
        if self._backoff == 0:
            self._backoff = self._config.retry_base_seg
        else:
            self._backoff = min(
                self._backoff * 2 + random.uniform(0, self._config.retry_base_seg),
                self._config.max_backoff_seg,
            )
        logger.debug("SyncWorker backoff: %.1f s", self._backoff)

    # ── Diagnóstico ───────────────────────────────────────────────────────────

    def status(self) -> dict:
        try:
            conn = self._conn_factory()
            pendientes = self._contar_pendientes(conn)
            row = conn.execute(
                "SELECT COUNT(*) FROM event_log WHERE synced=1"
            ).fetchone()
            sincronizados = row[0] if row else 0
        except Exception:
            pendientes = sincronizados = -1

        return {
            "activo":           not self._stop_flag.is_set(),
            "backoff_seg":      round(self._backoff, 1),
            "pendientes":       pendientes,
            "sincronizados":    sincronizados,
            "url_configurada":  bool(self._config.url_servidor),
            "sucursal_id":      self._config.sucursal_id,
        }


# ── Excepción interna ─────────────────────────────────────────────────────────

def _sha256(payload: object) -> str:
    """SHA-256 del payload serializado — permite detección de duplicados en servidor."""
    raw = payload if isinstance(payload, str) else json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class _ConflictError(Exception):
    def __init__(self, data: dict):
        self.data = data
        super().__init__(str(data))


# ── Factory helper para integración en main.py ───────────────────────────────

def crear_sync_worker(
    conn_factory: Callable[[], sqlite3.Connection],
    sucursal_id: int = 1,
    url: str = "",
    api_key: str = "",
    intervalo_seg: int = 60,
) -> SyncWorker:
    """
    Crea y configura un SyncWorker listo para arrancar.

    Uso en main.py:
        from sync.sync_worker import crear_sync_worker
        from core.db.connection import get_connection

        worker = crear_sync_worker(get_connection, sucursal_id=1,
                                   url=config.SYNC_URL, api_key=config.SYNC_KEY)
        worker.start()
        # Conectar señales PyQt si se usa QThread:
        worker.sync_completado.connect(lambda e, err: ...)
    """
    config = SyncConfig(
        url_servidor=url,
        sucursal_id=sucursal_id,
        api_key=api_key,
        intervalo_seg=intervalo_seg,
    )
    return SyncWorker(config, conn_factory)

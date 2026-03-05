# sync/worker.py
# Motor de sincronización offline-first para multi-sucursal
# Arquitectura: SQLite local → PostgreSQL central (o HTTP API)
# - Detecta conectividad antes de intentar sync
# - Retry exponencial con jitter
# - Registro de conflictos para resolución manual
# - Thread-safe, no bloquea UI
from __future__ import annotations
import sqlite3
import json
import logging
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Callable, Optional
from dataclasses import dataclass

logger = logging.getLogger("spj.sync")


# ── Configuración ─────────────────────────────────────────────────────────────

@dataclass
class SyncConfig:
    url_servidor:       str   = "https://api.spj.example.com/sync"
    sucursal_id:        int   = 1
    api_key:            str   = ""
    intervalo_seg:      int   = 60      # segundos entre ciclos de sync
    max_intentos:       int   = 10
    timeout_http:       int   = 30
    batch_size:         int   = 100     # eventos por lote
    retry_base_seg:     int   = 5       # base para backoff exponencial


# ── Excepciones ───────────────────────────────────────────────────────────────

class SyncError(Exception):
    pass

class ConflictoError(SyncError):
    pass


# ── Worker principal ──────────────────────────────────────────────────────────

class SyncWorker:
    """
    Worker de sincronización que corre en un hilo separado.
    Recolecta eventos locales pendientes y los envía al servidor central.
    Recibe cambios del servidor y los aplica localmente.

    Uso:
        worker = SyncWorker(config, conn_factory=get_connection)
        worker.start()                # arranca hilo background
        worker.sync_now()             # fuerza sync inmediato
        worker.stop()                 # detiene el hilo
    """

    def __init__(
        self,
        config:       SyncConfig,
        conn_factory: Callable[[], sqlite3.Connection],
    ):
        self.config       = config
        self.conn_factory = conn_factory
        self._stop        = threading.Event()
        self._thread:     Optional[threading.Thread] = None
        self._sync_now    = threading.Event()
        self.on_sync_ok:  Optional[Callable[[int], None]] = None   # callback: n eventos enviados
        self.on_sync_err: Optional[Callable[[str], None]] = None   # callback: mensaje error

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="SyncWorker", daemon=True
        )
        self._thread.start()
        logger.info("SyncWorker iniciado (intervalo=%ds)", self.config.intervalo_seg)

    def stop(self) -> None:
        self._stop.set()
        self._sync_now.set()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("SyncWorker detenido")

    def sync_now(self) -> None:
        """Fuerza un ciclo de sync inmediato (no espera al intervalo)."""
        self._sync_now.set()

    # ── Loop principal ────────────────────────────────────────────────────────

    def _loop(self) -> None:
        while not self._stop.is_set():
            # Espera hasta el intervalo O hasta que alguien llame sync_now()
            triggered = self._sync_now.wait(timeout=self.config.intervalo_seg)
            self._sync_now.clear()

            if self._stop.is_set():
                break

            try:
                self._ciclo_sync()
            except Exception as e:
                logger.error("Error en ciclo sync: %s", e, exc_info=True)
                if self.on_sync_err:
                    self.on_sync_err(str(e))

    def _ciclo_sync(self) -> None:
        if not self._hay_conexion():
            logger.debug("Sin conexión a internet. Sync diferido.")
            return

        conn = self.conn_factory()
        enviados = self._enviar_pendientes(conn)

        if enviados > 0:
            logger.info("Sync: %d eventos enviados al servidor", enviados)
            if self.on_sync_ok:
                self.on_sync_ok(enviados)

        # Recibir cambios del servidor
        self._recibir_cambios(conn)

    # ── Envío de eventos locales ──────────────────────────────────────────────

    def _enviar_pendientes(self, conn: sqlite3.Connection) -> int:
        """Lee eventos no enviados y los manda al servidor por lotes."""
        pendientes = conn.execute("""
            SELECT se.id, se.uuid, se.tabla, se.operacion,
                   se.registro_id, se.registro_uuid, se.payload,
                   se.usuario, se.creado_en, se.intentos
            FROM sync_eventos se
            JOIN sync_cola sc ON sc.evento_uuid = se.uuid
            WHERE se.enviado = 0
              AND sc.bloqueado = 0
              AND sc.proximo_intento <= datetime('now')
            ORDER BY se.creado_en
            LIMIT ?
        """, (self.config.batch_size,)).fetchall()

        if not pendientes:
            return 0

        # Construir payload del lote
        lote = [
            {
                "uuid":         row["uuid"],
                "tabla":        row["tabla"],
                "operacion":    row["operacion"],
                "registro_id":  row["registro_id"],
                "registro_uuid": row["registro_uuid"],
                "payload":      json.loads(row["payload"]) if row["payload"] else None,
                "usuario":      row["usuario"],
                "creado_en":    row["creado_en"],
                "sucursal_id":  self.config.sucursal_id,
            }
            for row in pendientes
        ]

        try:
            respuesta = self._http_post("/eventos/batch", {"eventos": lote})
        except Exception as e:
            # Registrar fallo y programar retry con backoff exponencial
            for row in pendientes:
                intentos = (row["intentos"] or 0) + 1
                delay    = min(self.config.retry_base_seg * (2 ** intentos) + self._jitter(), 3600)
                proximo  = datetime.now() + timedelta(seconds=delay)
                conn.execute("""
                    UPDATE sync_eventos SET intentos=? WHERE uuid=?
                """, (intentos, row["uuid"]))
                conn.execute("""
                    UPDATE sync_cola
                    SET bloqueado = CASE WHEN ? >= ? THEN 1 ELSE 0 END,
                        proximo_intento = ?
                    WHERE evento_uuid = ?
                """, (intentos, self.config.max_intentos, proximo.isoformat(), row["uuid"]))
            conn.commit()
            raise SyncError(f"Fallo HTTP al enviar lote: {e}") from e

        # Marcar como enviados
        uuids_ok = {item["uuid"] for item in respuesta.get("aceptados", [])}
        uuids_conflicto = {c["uuid"] for c in respuesta.get("conflictos", [])}

        for row in pendientes:
            if row["uuid"] in uuids_ok:
                conn.execute(
                    "UPDATE sync_eventos SET enviado=1, enviado_en=datetime('now') WHERE uuid=?",
                    (row["uuid"],)
                )
            elif row["uuid"] in uuids_conflicto:
                conflicto = next(c for c in respuesta["conflictos"] if c["uuid"] == row["uuid"])
                self._registrar_conflicto(conn, row, conflicto.get("payload_remoto"))
        conn.commit()
        return len(uuids_ok)

    # ── Recepción de cambios del servidor ────────────────────────────────────

    def _recibir_cambios(self, conn: sqlite3.Connection) -> None:
        """Descarga cambios del servidor y aplica localmente (last-write-wins)."""
        try:
            ultimo = conn.execute(
                "SELECT COALESCE(MAX(enviado_en), '2000-01-01') FROM sync_eventos WHERE enviado=1"
            ).fetchone()[0]

            respuesta = self._http_get(f"/cambios?sucursal={self.config.sucursal_id}&desde={ultimo}")
            cambios   = respuesta.get("cambios", [])

            for cambio in cambios:
                try:
                    self._aplicar_cambio(conn, cambio)
                except Exception as e:
                    logger.warning("Cambio remoto ignorado (error): %s | %s", cambio.get("uuid"), e)

            if cambios:
                conn.commit()
                logger.info("Sync: %d cambios recibidos del servidor", len(cambios))

        except Exception as e:
            logger.warning("No se pudieron recibir cambios: %s", e)

    def _aplicar_cambio(self, conn: sqlite3.Connection, cambio: dict) -> None:
        """
        Aplica un cambio remoto usando last-write-wins basado en _sync_version.
        Si la versión local es mayor → conflicto (no sobreescribir).
        """
        tabla    = cambio.get("tabla")
        op       = cambio.get("operacion")
        payload  = cambio.get("payload", {})
        uuid_reg = cambio.get("registro_uuid")

        if not tabla or not payload or not uuid_reg:
            return

        # Verificar si existe localmente
        local = conn.execute(
            f"SELECT _sync_version FROM {tabla} WHERE uuid=?", (uuid_reg,)
        ).fetchone()

        version_remota = payload.get("_sync_version", 0)
        version_local  = local[0] if local else -1

        if version_local > version_remota:
            # Local tiene versión más nueva → conflicto
            self._registrar_conflicto(conn, {"uuid": uuid_reg, "tabla": tabla}, payload)
            return

        if op == "DELETE":
            conn.execute(f"UPDATE {tabla} SET _deleted=1 WHERE uuid=?", (uuid_reg,))
        elif op == "INSERT" and not local:
            cols   = ", ".join(payload.keys())
            places = ", ".join("?" * len(payload))
            conn.execute(
                f"INSERT OR IGNORE INTO {tabla} ({cols}) VALUES ({places})",
                list(payload.values())
            )
        elif op == "UPDATE" and local:
            sets   = ", ".join(f"{k}=?" for k in payload if k != "uuid")
            values = [v for k, v in payload.items() if k != "uuid"]
            values.append(uuid_reg)
            conn.execute(f"UPDATE {tabla} SET {sets} WHERE uuid=?", values)

    # ── Captura de eventos locales (triggers) ─────────────────────────────────

    @staticmethod
    def registrar_evento(
        conn:        sqlite3.Connection,
        tabla:       str,
        operacion:   str,
        registro_id: int,
        payload:     dict,
        usuario:     str,
        sucursal_id: int = 1,
    ) -> None:
        """
        Registra un evento de cambio local en sync_eventos + sync_cola.
        Llamar desde repositories DESPUÉS de cada INSERT/UPDATE/DELETE crítico.
        """
        ev_uuid = str(uuid.uuid4())
        try:
            reg_uuid = payload.get("uuid", "")
            conn.execute("""
                INSERT INTO sync_eventos
                    (uuid, tabla, operacion, registro_id, registro_uuid,
                     payload, sucursal_id, usuario)
                VALUES (?,?,?,?,?,?,?,?)
            """, (
                ev_uuid, tabla, operacion, registro_id, reg_uuid,
                json.dumps(payload, default=str),
                sucursal_id, usuario
            ))
            conn.execute("""
                INSERT INTO sync_cola (evento_uuid, prioridad)
                VALUES (?, ?)
            """, (ev_uuid, 5 if operacion == "INSERT" else 3))
        except Exception as e:
            logger.warning("No se pudo registrar evento sync: %s", e)

    # ── Conflictos ────────────────────────────────────────────────────────────

    def _registrar_conflicto(
        self,
        conn:          sqlite3.Connection,
        evento:        dict,
        payload_remoto: dict = None,
    ) -> None:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO sync_conflictos
                    (tabla, registro_uuid, payload_remoto)
                VALUES (?,?,?)
            """, (
                evento.get("tabla"),
                evento.get("uuid") or evento.get("registro_uuid"),
                json.dumps(payload_remoto, default=str) if payload_remoto else None
            ))
            logger.warning("Conflicto registrado: tabla=%s uuid=%s",
                           evento.get("tabla"), evento.get("uuid"))
        except Exception as e:
            logger.error("Error registrando conflicto: %s", e)

    # ── HTTP helpers (stub — reemplazar con requests/httpx en producción) ─────

    def _http_post(self, path: str, data: dict) -> dict:
        """
        Envía datos al servidor.
        En producción reemplazar con:
            import requests
            resp = requests.post(
                self.config.url_servidor + path,
                json=data,
                headers={"Authorization": f"Bearer {self.config.api_key}"},
                timeout=self.config.timeout_http
            )
            resp.raise_for_status()
            return resp.json()
        """
        raise NotImplementedError("Implementar HTTP client en producción")

    def _http_get(self, path: str) -> dict:
        """
        Obtiene datos del servidor.
        Ver _http_post para implementación.
        """
        raise NotImplementedError("Implementar HTTP client en producción")

    def _hay_conexion(self) -> bool:
        """Verificación rápida de conectividad."""
        import socket
        try:
            socket.setdefaulttimeout(3)
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect(("8.8.8.8", 53))
            return True
        except socket.error:
            return False

    def _jitter(self) -> float:
        """Añade aleatoriedad al retry para evitar thundering herd."""
        import random
        return random.uniform(0, 5)


# ── Registro automático de eventos mediante decorador ────────────────────────

def auto_sync_evento(tabla: str, operacion: str):
    """
    Decorador para repositories: registra automáticamente evento sync
    después de cada operación de escritura.

    Uso:
        @auto_sync_evento("ventas", "INSERT")
        def crear_venta(self, datos: dict) -> int:
            ...
    """
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            resultado = func(self, *args, **kwargs)
            # El repository debe exponer self.conn y self.usuario
            try:
                payload = kwargs.get("datos") or (args[0] if args else {})
                if isinstance(payload, dict):
                    SyncWorker.registrar_evento(
                        conn=self.conn,
                        tabla=tabla,
                        operacion=operacion,
                        registro_id=resultado if isinstance(resultado, int) else 0,
                        payload=payload,
                        usuario=getattr(self, "usuario", "Sistema"),
                        sucursal_id=getattr(self, "sucursal_id", 1),
                    )
            except Exception as e:
                logger.warning("auto_sync_evento falló: %s", e)
            return resultado
        return wrapper
    return decorator

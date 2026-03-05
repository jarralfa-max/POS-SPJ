# core/services/scheduler_service.py
# ── SCHEDULER SERVICE — SPJ Enterprise v9 ────────────────────────────────────
# Programador interno de tareas periódicas:
#   - Sync de eventos pendientes (cada N segundos)
#   - Conciliación automática global vs local (cada hora)
#   - Forecast de demanda (diario, off-peak)
#   - Snapshot de fidelidad (incremental, cada 30 min)
#
# DISEÑO:
#   - Un hilo daemon por tarea — no bloquean el hilo principal
#   - Cada tarea define su propio intervalo y ventana horaria (off-peak)
#   - Errores en una tarea no afectan las demás
#   - Arranque y parada controlados desde main.py o inicializar_sistema()
from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dtime
from typing import Callable, Dict, List, Optional

logger = logging.getLogger("spj.scheduler")


# ── Tarea programada ──────────────────────────────────────────────────────────

@dataclass
class TareaProgramada:
    nombre:        str
    funcion:       Callable[[], None]
    intervalo_seg: int
    solo_offpeak:  bool          = False   # si True, solo corre entre 22:00–06:00
    ultimo_run:    Optional[float] = field(default=None, repr=False)
    errores_cons:  int            = 0      # errores consecutivos
    max_errores:   int            = 5      # tras estos errores, pausa larga

    def esta_en_offpeak(self) -> bool:
        h = datetime.now().hour
        return h >= 22 or h < 6

    def debe_ejecutar(self) -> bool:
        if self.solo_offpeak and not self.esta_en_offpeak():
            return False
        if self.ultimo_run is None:
            return True
        return (time.monotonic() - self.ultimo_run) >= self.intervalo_seg


# ── Scheduler ────────────────────────────────────────────────────────────────

class SchedulerService:
    """
    Programador interno de tareas periódicas.

    Uso en main.py / inicializar_sistema():

        sched = SchedulerService(conn_factory, sucursal_id=1, usuario="Sistema")
        sched.registrar_defaults()   # sync + conciliación + forecast + snapshot
        sched.start()
        # ... app corre ...
        sched.stop()

    Se puede agregar una tarea custom:
        sched.registrar("mi_tarea", mi_funcion, intervalo_seg=600)
    """

    def __init__(
        self,
        conn_factory:  Callable[[], sqlite3.Connection],
        sucursal_id:   int = 1,
        usuario:       str = "Sistema",
    ) -> None:
        self._conn_factory = conn_factory
        self._sucursal_id  = sucursal_id
        self._usuario      = usuario
        self._tareas:      Dict[str, TareaProgramada] = {}
        self._stop_event   = threading.Event()
        self._thread:      Optional[threading.Thread] = None
        self._lock         = threading.Lock()

    # ── Registro de tareas ────────────────────────────────────────────────────

    def registrar(
        self,
        nombre:        str,
        funcion:       Callable[[], None],
        intervalo_seg: int,
        solo_offpeak:  bool = False,
    ) -> None:
        with self._lock:
            self._tareas[nombre] = TareaProgramada(
                nombre=nombre,
                funcion=funcion,
                intervalo_seg=intervalo_seg,
                solo_offpeak=solo_offpeak,
            )
        logger.info(
            "Scheduler: tarea '%s' registrada (intervalo=%ds offpeak=%s)",
            nombre, intervalo_seg, solo_offpeak,
        )

    def registrar_defaults(self) -> None:
        """
        Registra las 4 tareas estándar del sistema.
        Requiere que los engines estén disponibles en el entorno.
        """
        self.registrar(
            "conciliacion",
            self._task_conciliacion,
            intervalo_seg=3600,   # cada hora
        )
        self.registrar(
            "loyalty_snapshot",
            self._task_loyalty_snapshot,
            intervalo_seg=1800,   # cada 30 min
        )
        self.registrar(
            "forecast",
            self._task_forecast,
            intervalo_seg=86400,  # diario
            solo_offpeak=True,    # solo 22:00–06:00
        )
        self.registrar(
            "limpieza_event_log",
            self._task_limpieza_event_log,
            intervalo_seg=86400,
            solo_offpeak=True,
        )

    # ── Control ───────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            logger.debug("Scheduler ya activo.")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="SchedulerService", daemon=True
        )
        self._thread.start()
        logger.info("SchedulerService iniciado con %d tareas.", len(self._tareas))

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("SchedulerService detenido.")

    def ejecutar_ahora(self, nombre: str) -> bool:
        """Fuerza ejecución inmediata de una tarea por nombre. Retorna True si existía."""
        with self._lock:
            tarea = self._tareas.get(nombre)
        if not tarea:
            return False
        self._ejecutar_tarea(tarea)
        return True

    def status(self) -> List[dict]:
        with self._lock:
            return [
                {
                    "nombre":        t.nombre,
                    "intervalo_seg": t.intervalo_seg,
                    "solo_offpeak":  t.solo_offpeak,
                    "ultimo_run":    t.ultimo_run,
                    "errores_cons":  t.errores_cons,
                }
                for t in self._tareas.values()
            ]

    # ── Loop interno ──────────────────────────────────────────────────────────

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            with self._lock:
                tareas = list(self._tareas.values())

            for tarea in tareas:
                if self._stop_event.is_set():
                    break
                if tarea.debe_ejecutar():
                    self._ejecutar_tarea(tarea)

            self._stop_event.wait(timeout=30)  # tick cada 30s

    def _ejecutar_tarea(self, tarea: TareaProgramada) -> None:
        inicio = time.monotonic()
        try:
            tarea.funcion()
            tarea.ultimo_run  = time.monotonic()
            tarea.errores_cons = 0
            duracion = round(time.monotonic() - inicio, 2)
            logger.info("Scheduler '%s' OK (%.2fs)", tarea.nombre, duracion)
        except Exception as exc:
            tarea.errores_cons += 1
            tarea.ultimo_run   = time.monotonic()  # resetear timer aun en error
            logger.error(
                "Scheduler '%s' FALLÓ (err_consecutivos=%d): %s",
                tarea.nombre, tarea.errores_cons, exc, exc_info=False,
            )
            # Backoff: si hay demasiados errores consecutivos, duplicar el intervalo
            if tarea.errores_cons >= tarea.max_errores:
                tarea.intervalo_seg = min(tarea.intervalo_seg * 2, 86400)
                logger.warning(
                    "Scheduler '%s': demasiados errores, intervalo ampliado a %ds",
                    tarea.nombre, tarea.intervalo_seg,
                )

    # ── Implementaciones de tareas ────────────────────────────────────────────

    def _task_conciliacion(self) -> None:
        """Conciliación automática global vs local — fix #9."""
        from core.services.distribution_engine import DistributionEngine
        conn = self._conn_factory()
        eng  = DistributionEngine(conn, sucursal_id=self._sucursal_id, usuario="Scheduler")
        result = eng.conciliar()
        if result.alerta:
            logger.warning(
                "Scheduler conciliación ALERTA: diferencia=%.3f sucursal=%d",
                result.diferencia, result.sucursal_id,
            )

    def _task_loyalty_snapshot(self) -> None:
        """Snapshot incremental de fidelidad — fix #10."""
        conn = self._conn_factory()
        self._recalcular_loyalty_snapshots(conn)

    def _task_forecast(self) -> None:
        """Pronóstico de demanda — off-peak."""
        try:
            from core.services.forecast_engine import ForecastEngine
            conn = self._conn_factory()
            eng  = ForecastEngine(conn, sucursal_id=self._sucursal_id)
            eng.generar_forecast_diario()
        except ImportError:
            logger.debug("Scheduler forecast: ForecastEngine no disponible, omitido.")

    def _task_limpieza_event_log(self) -> None:
        """Limpia event_log synced con más de 90 días — mantiene la tabla manejable."""
        conn = self._conn_factory()
        cur  = conn.execute(
            "DELETE FROM event_log WHERE synced=1 AND fecha < date('now', '-90 days')"
        )
        eliminados = cur.rowcount
        conn.commit()
        if eliminados > 0:
            logger.info("Scheduler limpieza event_log: %d filas eliminadas", eliminados)

    def _recalcular_loyalty_snapshots(self, conn: sqlite3.Connection) -> None:
        """
        Recalculo incremental de snapshots de fidelidad.
        Solo procesa clientes que tienen eventos NUEVOS desde su último snapshot.
        Fix #10: evita recalcular toda la historia — solo desde ultimo_evento_id.
        """
        # Asegurar tabla snapshot
        conn.execute("""
            CREATE TABLE IF NOT EXISTS loyalty_snapshots (
                cliente_id        INTEGER PRIMARY KEY REFERENCES clientes(id),
                puntos_actuales   INTEGER NOT NULL DEFAULT 0,
                nivel             TEXT    NOT NULL DEFAULT 'Bronce',
                visitas           INTEGER NOT NULL DEFAULT 0,
                importe_total     REAL    NOT NULL DEFAULT 0,
                ultimo_evento_id  INTEGER,
                fecha_snapshot    DATETIME DEFAULT (datetime('now'))
            )
        """)
        conn.commit()

        # Clientes con eventos nuevos desde su último snapshot
        clientes_dirty = conn.execute("""
            SELECT DISTINCT hp.cliente_id
            FROM historico_puntos hp
            LEFT JOIN loyalty_snapshots ls ON ls.cliente_id = hp.cliente_id
            WHERE ls.cliente_id IS NULL
               OR hp.id > COALESCE(ls.ultimo_evento_id, 0)
        """).fetchall()

        actualizados = 0
        for (cliente_id,) in clientes_dirty:
            self._recalcular_snapshot_cliente(conn, cliente_id)
            actualizados += 1

        if actualizados > 0:
            conn.commit()
            logger.info(
                "Scheduler loyalty_snapshot: %d clientes actualizados.", actualizados
            )

    def _recalcular_snapshot_cliente(
        self, conn: sqlite3.Connection, cliente_id: int
    ) -> None:
        """Recalcula snapshot de un cliente específico desde su ultimo_evento_id."""
        from core.domain.models import LoyaltySnapshot

        # Obtener checkpoint anterior
        snap_row = conn.execute(
            "SELECT puntos_actuales, visitas, importe_total, ultimo_evento_id "
            "FROM loyalty_snapshots WHERE cliente_id=?",
            (cliente_id,),
        ).fetchone()

        base_puntos     = int(snap_row[0]) if snap_row else 0
        base_visitas    = int(snap_row[1]) if snap_row else 0
        base_importe    = float(snap_row[2]) if snap_row else 0.0
        ultimo_ev_id    = int(snap_row[3]) if snap_row and snap_row[3] else 0

        # Agregar solo eventos NUEVOS (mayor que el checkpoint)
        eventos = conn.execute(
            """
            SELECT id, puntos, tipo_movimiento, importe
            FROM historico_puntos
            WHERE cliente_id = ? AND id > ?
            ORDER BY id ASC
            """,
            (cliente_id, ultimo_ev_id),
        ).fetchall()

        if not eventos:
            return

        for row in eventos:
            ev_id, puntos_delta, tipo, importe = row
            base_puntos  += int(puntos_delta or 0)
            base_importe += float(importe or 0)
            if tipo in ("venta", "ganancia"):
                base_visitas += 1
            ultimo_ev_id = ev_id

        nivel = LoyaltySnapshot.calcular_nivel(base_puntos)

        conn.execute(
            """
            INSERT INTO loyalty_snapshots
                (cliente_id, puntos_actuales, nivel, visitas, importe_total,
                 ultimo_evento_id, fecha_snapshot)
            VALUES (?,?,?,?,?,?,datetime('now'))
            ON CONFLICT(cliente_id) DO UPDATE SET
                puntos_actuales  = excluded.puntos_actuales,
                nivel            = excluded.nivel,
                visitas          = excluded.visitas,
                importe_total    = excluded.importe_total,
                ultimo_evento_id = excluded.ultimo_evento_id,
                fecha_snapshot   = excluded.fecha_snapshot
            """,
            (cliente_id, base_puntos, nivel, base_visitas, base_importe, ultimo_ev_id),
        )

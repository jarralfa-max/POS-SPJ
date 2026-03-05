# core/events/event_bus.py
# ── EVENT BUS LIGERO — SPJ Enterprise v9 ──────────────────────────────────────
# Bus de eventos thread-safe para orquestación entre motores sin acoplamiento.
#
# REGLAS:
#   1. Handlers síncronos: operaciones rápidas (< 50 ms)
#   2. Handlers asíncronos (async_=True): impresión, sync remota, forecast
#   3. Fallo de un handler NO cancela los demás
#   4. Errores siempre se loguean — nunca se tragan silenciosamente
#   5. Suscribirse después de publicar NO recibe eventos pasados
#
# EVENTOS ESTÁNDAR DEL SISTEMA:
#   VENTA_COMPLETADA      — SalesEngine → RecipeEngine, LoyaltyEngine, TicketEngine,
#                                         HardwareManager, ForecastEngine
#   COMPRA_REGISTRADA     — PurchaseEngine → EventLogger
#   RECEPCION_CONFIRMADA  — DistributionEngine → EventLogger
#   TRASPASO_INICIADO     — DistributionEngine → EventLogger
#   TRASPASO_CONFIRMADO   — DistributionEngine → EventLogger
#   STOCK_BAJO_MINIMO     — InventoryEngine → ForecastEngine, ModuloReportes
#   CONCILIACION_DIFERENCIA — ConciliationEngine → ModuloReportes
#   TARJETA_ESCANEADA     — ModuloVentas → CardBatchEngine, LoyaltyEngine
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("spj.event_bus")

# ── Tipos ─────────────────────────────────────────────────────────────────────
Handler = Callable[[dict], None]

# ── Constantes de eventos ─────────────────────────────────────────────────────
VENTA_COMPLETADA        = "VENTA_COMPLETADA"
COMPRA_REGISTRADA       = "COMPRA_REGISTRADA"
RECEPCION_CONFIRMADA    = "RECEPCION_CONFIRMADA"
TRASPASO_INICIADO       = "TRASPASO_INICIADO"
TRASPASO_CONFIRMADO     = "TRASPASO_CONFIRMADO"
STOCK_BAJO_MINIMO       = "STOCK_BAJO_MINIMO"
CONCILIACION_DIFERENCIA = "CONCILIACION_DIFERENCIA"
TARJETA_ESCANEADA       = "TARJETA_ESCANEADA"
AJUSTE_INVENTARIO       = "AJUSTE_INVENTARIO"
SESION_INICIADA         = "SESION_INICIADA"


class EventBus:
    """
    Bus de eventos singleton thread-safe.

    Uso típico:
        # Registrar handler al iniciar sistema
        EventBus.subscribe("VENTA_COMPLETADA", recipe_engine.handle)

        # Publicar desde engine
        EventBus.publish("VENTA_COMPLETADA", {"venta_id": 5, ...})

        # Publicar sin bloquear UI (impresión, sync)
        EventBus.publish("VENTA_COMPLETADA", payload, async_=True)
    """

    _instance:  Optional["EventBus"] = None
    _inst_lock: threading.Lock       = threading.Lock()

    def __new__(cls) -> "EventBus":
        if cls._instance is None:
            with cls._inst_lock:
                if cls._instance is None:
                    obj = super().__new__(cls)
                    obj._handlers: Dict[str, List[Tuple[int, str, Handler]]] = {}
                    obj._lock = threading.RLock()
                    obj._executor = ThreadPoolExecutor(
                        max_workers=4, thread_name_prefix="spj_event"
                    )
                    cls._instance = obj
        return cls._instance

    # ── API pública ───────────────────────────────────────────────────────────

    def subscribe(
        self,
        event_type: str,
        handler:    Handler,
        priority:   int = 0,
        label:      str = "",
    ) -> None:
        """
        Registra un handler para un tipo de evento.

        Args:
            event_type: Nombre del evento (usar constantes de este módulo).
            handler:    Callable que recibe (payload: dict) → None.
            priority:   Mayor valor = ejecuta primero. Default 0.
            label:      Nombre descriptivo para logs (se infiere si vacío).
        """
        if not callable(handler):
            raise TypeError(f"handler debe ser callable, recibido: {type(handler)}")
        label = label or getattr(handler, "__qualname__", repr(handler))
        with self._lock:
            bucket = self._handlers.setdefault(event_type, [])
            # Evitar duplicados del mismo handler
            for _, lbl, h in bucket:
                if h is handler:
                    logger.debug("Handler '%s' ya registrado para '%s' — ignorado.", lbl, event_type)
                    return
            bucket.append((priority, label, handler))
            bucket.sort(key=lambda t: -t[0])  # orden descendente por prioridad
        logger.debug("Suscrito [%s] → %s (prio=%d)", event_type, label, priority)

    def unsubscribe(self, event_type: str, handler: Handler) -> bool:
        """Elimina un handler. Retorna True si existía."""
        with self._lock:
            bucket = self._handlers.get(event_type, [])
            before = len(bucket)
            self._handlers[event_type] = [(p, l, h) for p, l, h in bucket if h is not handler]
            return len(self._handlers[event_type]) < before

    def publish(
        self,
        event_type: str,
        payload:    dict,
        async_:     bool = False,
    ) -> None:
        """
        Publica un evento a todos los handlers suscritos.

        Args:
            event_type: Tipo del evento.
            payload:    Datos del evento. Debe ser un dict serializable.
            async_:     Si True, handlers se ejecutan en ThreadPoolExecutor.
                        Usar para impresión, sync remota, forecast (operaciones lentas).
        """
        with self._lock:
            handlers = list(self._handlers.get(event_type, []))

        if not handlers:
            logger.debug("Evento '%s' publicado sin handlers registrados.", event_type)
            return

        if async_:
            self._executor.submit(self._dispatch, event_type, payload, handlers)
        else:
            self._dispatch(event_type, payload, handlers)

    def clear_handlers(self, event_type: Optional[str] = None) -> None:
        """Elimina handlers. Sin argumento, limpia todo (útil para tests)."""
        with self._lock:
            if event_type:
                self._handlers.pop(event_type, None)
            else:
                self._handlers.clear()

    def handler_count(self, event_type: str) -> int:
        with self._lock:
            return len(self._handlers.get(event_type, []))

    def registered_events(self) -> List[str]:
        with self._lock:
            return [e for e, hs in self._handlers.items() if hs]

    # ── Internos ──────────────────────────────────────────────────────────────

    def _dispatch(
        self,
        event_type: str,
        payload:    dict,
        handlers:   List[Tuple[int, str, Handler]],
    ) -> None:
        """Ejecuta todos los handlers en orden de prioridad. Errores no cancela el resto."""
        logger.debug("Despachando '%s' a %d handler(s).", event_type, len(handlers))
        for priority, label, handler in handlers:
            try:
                handler(payload)
                logger.debug("Handler OK: [%s] → %s", event_type, label)
            except Exception as exc:
                logger.error(
                    "Handler FALLÓ [%s] → %s: %s",
                    event_type, label, exc, exc_info=True,
                )
                # El fallo de un handler NO detiene los demás.


# ── Acceso global (singleton) ─────────────────────────────────────────────────
_bus: Optional[EventBus] = None


def get_bus() -> EventBus:
    """Retorna la instancia global del EventBus (crea si no existe)."""
    global _bus
    if _bus is None:
        _bus = EventBus()
    return _bus

# core/services/ventas_facade.py — SPJ Enterprise v9.1
# Fix #6: Separación UI / Service / Engine
#
# Capas:
#   UI         →  VentasFacade  (sólo métodos nombrados por intención)
#   Facade     →  SalesEngine   (orquestación de la venta)
#   SalesEngine→  InventoryEngine, FidelidadEngine, EventLogger, EventBus
#
# La UI no importa engines ni accede a BD directamente.
# Cualquier cambio en la lógica de venta sólo toca SalesEngine.
from __future__ import annotations

import logging
import sqlite3
from typing import Callable, List, Optional

logger = logging.getLogger("spj.ventas_facade")


class VentasFacade:
    """
    Punto de entrada único que la UI usa para operaciones de venta.
    No contiene lógica de negocio — sólo delega y adapta señales.

    Uso en ventas.py:
        facade = VentasFacade(self.conexion, sucursal_id=1, usuario="cajero")
        facade.on_venta_ok = lambda r: self._mostrar_ticket(r.ticket_data)
        facade.on_error    = lambda e: self._mostrar_error(str(e))
        facade.procesar(items, datos_pago)
    """

    def __init__(
        self,
        conn:        sqlite3.Connection,
        sucursal_id: int = 1,
        usuario:     str = "cajero",
    ) -> None:
        self.conn        = conn
        self.sucursal_id = sucursal_id
        self.usuario     = usuario

        # Callbacks (la UI asigna sus handlers antes de llamar a procesar())
        self.on_venta_ok:     Optional[Callable] = None  # (ResultadoVenta) → None
        self.on_error:        Optional[Callable] = None  # (Exception) → None
        self.on_stock_alerta: Optional[Callable] = None  # (producto_id, stock) → None

    # ── API pública (la UI sólo llama estos métodos) ──────────────────────────

    def procesar(self, items, datos_pago, **kwargs) -> None:
        """
        Procesa la venta de forma completamente asíncrona respecto a la UI.
        El resultado llega por callbacks on_venta_ok / on_error.
        """
        try:
            resultado = self._engine().procesar_venta(
                items=items,
                datos_pago=datos_pago,
                usuario=self.usuario,
                **kwargs,
            )
            if self.on_venta_ok:
                self.on_venta_ok(resultado)
        except Exception as exc:
            logger.error("VentasFacade.procesar: %s", exc)
            if self.on_error:
                self.on_error(exc)

    def verificar_stock(self, producto_id: int, cantidad: float) -> float:
        """Retorna stock disponible sin modificar nada. La UI decide si mostrar alerta."""
        from core.database import Connection
        from core.services.inventory_engine import InventoryEngine
        inv = InventoryEngine(Connection(self.conn), branch_id=self.sucursal_id)
        return inv.get_stock(producto_id)

    def buscar_cliente(self, termino: str) -> list:
        """Búsqueda de cliente por nombre/teléfono/tarjeta. Retorna lista de dicts."""
        rows = self.conn.execute(
            """
            SELECT c.id, c.nombre, COALESCE(c.apellido,'') as apellido,
                   COALESCE(c.telefono,'') as telefono,
                   COALESCE(c.puntos, 0) as puntos
            FROM clientes c
            WHERE c.nombre  LIKE ?
               OR c.telefono LIKE ?
               OR EXISTS (
                   SELECT 1 FROM tarjetas_fidelidad t
                   WHERE t.id_cliente = c.id
                     AND (t.codigo = ? OR t.codigo_qr = ?)
               )
            LIMIT 10
            """,
            (f"%{termino}%", f"%{termino}%", termino, termino),
        ).fetchall()
        return [dict(r) for r in rows]

    def anular_venta(self, venta_id: int, motivo: str = "") -> None:
        """Anula venta y revierte inventario. La UI sólo necesita el ID."""
        try:
            engine = self._engine()
            engine.anular_venta(venta_id=venta_id, motivo=motivo)
            logger.info("Venta #%d anulada por %s", venta_id, self.usuario)
        except Exception as exc:
            logger.error("VentasFacade.anular_venta: %s", exc)
            if self.on_error:
                self.on_error(exc)

    # ── Interno ───────────────────────────────────────────────────────────────

    def _engine(self):
        """Lazy-crea SalesEngine. La UI nunca lo toca directamente."""
        from core.database import Connection
        from core.services.sales_engine import SalesEngine
        from core.database import get_db
        db = Connection(self.conn)
        return SalesEngine(db=db, usuario=self.usuario, branch_id=self.sucursal_id)

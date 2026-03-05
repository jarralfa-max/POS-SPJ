# core/services/sales_service.py
# SalesService — Motor transaccional de ventas enterprise
# Toda la venta (header + detalles + inventario + caja + puntos) en UNA transacción.
# En caso de cualquier error → ROLLBACK completo, cero corrupción.
from __future__ import annotations
import sqlite3
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from core.services.inventory_service import InventoryService, StockInsuficienteError

logger = logging.getLogger("spj.sales")


# ── Excepciones de dominio ────────────────────────────────────────────────────

class VentaError(Exception):
    pass

class CarritoVacioError(VentaError):
    pass

class PagoInsuficienteError(VentaError):
    pass


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ItemVenta:
    producto_id:     int
    nombre:          str
    cantidad:        float
    precio_unitario: float
    descuento:       float = 0.0
    unidad:          str   = "pza"
    comentarios:     str   = ""

    @property
    def subtotal(self) -> float:
        return round(self.cantidad * self.precio_unitario - self.descuento, 2)


@dataclass
class DatosPago:
    forma_pago:       str           # 'Efectivo','Tarjeta','Crédito','Transferencia'
    efectivo_recibido: float = 0.0
    cambio:           float  = 0.0
    saldo_credito:    float  = 0.0  # Monto que queda a crédito


@dataclass
class ResultadoVenta:
    venta_id:       int
    folio:          str
    subtotal:       float
    descuento:      float
    iva:            float
    total:          float
    cambio:         float
    puntos_ganados: int
    ticket_data:    dict = field(default_factory=dict)


# ── Servicio de ventas ────────────────────────────────────────────────────────

class SalesService:
    """
    Motor transaccional de ventas.
    Patrón de uso desde UI:

        svc = SalesService(conn)
        resultado = svc.procesar_venta(items, datos_pago, usuario="cajero1")
        imprimir_ticket(resultado.ticket_data)
    """

    PUNTOS_POR_PESO = 1  # 1 punto por peso MXN (configurable)

    def __init__(self, conn: sqlite3.Connection, sucursal_id: int = 1):
        self.conn        = conn
        self.sucursal_id = sucursal_id

    def procesar_venta(
        self,
        items:             List[ItemVenta],
        datos_pago:        DatosPago,
        usuario:           str,
        cliente_id:        Optional[int] = None,
        descuento_global:  float = 0.0,   # fracción: 0.10 = 10%
        iva_rate:          float = 0.0,
        folio:             Optional[str] = None,
        puntos_a_canjear:  int   = 0,
        valor_punto:       float = 0.10,  # $0.10 por punto
    ) -> ResultadoVenta:
        """
        Procesa una venta de forma COMPLETAMENTE ATÓMICA.
        Pasos en una sola transacción:
            1. Crea registro ventas
            2. Crea detalles_venta
            3. Descuenta inventario (InventoryService)
            4. Registra movimiento_caja
            5. Aplica puntos de fidelidad
            6. Registra evento sync

        En caso de error en CUALQUIER paso → ROLLBACK total.
        """
        if not items:
            raise CarritoVacioError("La venta no tiene productos.")

        # ── Cálculos sin tocar BD ─────────────────────────────────────────────
        bruto       = sum(i.subtotal for i in items)
        desc_pesos  = round(bruto * descuento_global, 2)
        desc_puntos = round(puntos_a_canjear * valor_punto, 2) if puntos_a_canjear > 0 else 0.0
        subtotal    = round(bruto - desc_pesos - desc_puntos, 2)
        iva_monto   = round(subtotal * iva_rate, 2)
        total       = round(subtotal + iva_monto, 2)

        if total < 0:
            raise VentaError(f"Total negativo ({total:.2f}). Revise descuentos.")

        if datos_pago.forma_pago == "Efectivo":
            if datos_pago.efectivo_recibido < total - 0.009:
                raise PagoInsuficienteError(
                    f"Efectivo insuficiente: recibido=${datos_pago.efectivo_recibido:.2f}, "
                    f"total=${total:.2f}"
                )
            datos_pago.cambio = round(datos_pago.efectivo_recibido - total, 2)

        puntos_ganados = int(total * self.PUNTOS_POR_PESO) if cliente_id else 0
        folio = folio or self._generar_folio()

        # ── TRANSACCIÓN ATÓMICA ───────────────────────────────────────────────
        sp = f"venta_{folio.replace('-','_').replace(' ','_')}"
        try:
            self.conn.execute(f"SAVEPOINT {sp}")

            # 1. Venta header
            cur = self.conn.execute("""
                INSERT INTO ventas
                    (folio, sucursal_id, usuario, cliente_id,
                     subtotal, descuento, iva, total,
                     forma_pago, efectivo_recibido, cambio,
                     puntos_ganados, puntos_usados, descuento_puntos,
                     estado)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'completada')
            """, (
                folio, self.sucursal_id, usuario, cliente_id,
                subtotal, round(desc_pesos + desc_puntos, 2),
                iva_monto, total,
                datos_pago.forma_pago,
                datos_pago.efectivo_recibido,
                datos_pago.cambio,
                puntos_ganados, puntos_a_canjear, desc_puntos,
            ))
            venta_id = cur.lastrowid

            # 2. Detalles + 3. Inventario
            inv = InventoryService(self.conn, usuario=usuario, sucursal_id=self.sucursal_id)
            for item in items:
                self.conn.execute("""
                    INSERT INTO detalles_venta
                        (venta_id, producto_id, cantidad, precio_unitario,
                         descuento, subtotal, unidad, comentarios)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (
                    venta_id, item.producto_id, item.cantidad,
                    item.precio_unitario, item.descuento, item.subtotal,
                    item.unidad, item.comentarios,
                ))
                # Lanza StockInsuficienteError si no hay stock → ROLLBACK
                inv.registrar_salida_venta(item.producto_id, item.cantidad, venta_id)

            # 4. Movimiento de caja
            self.conn.execute("""
                INSERT INTO movimientos_caja
                    (tipo, monto, descripcion, usuario, venta_id, forma_pago)
                VALUES ('INGRESO',?,?,?,?,?)
            """, (total, f"Venta #{folio}", usuario, venta_id, datos_pago.forma_pago))

            # 5a. Crédito del cliente
            if datos_pago.forma_pago == "Crédito" and cliente_id and datos_pago.saldo_credito > 0:
                self.conn.execute(
                    "UPDATE clientes SET saldo=saldo+? WHERE id=?",
                    (datos_pago.saldo_credito, cliente_id)
                )

            # 5b. Puntos — canjeo de puntos
            if cliente_id and puntos_a_canjear > 0:
                self.conn.execute(
                    "UPDATE clientes SET puntos=puntos-? WHERE id=?",
                    (puntos_a_canjear, cliente_id)
                )
                saldo = self._puntos_cliente(cliente_id)
                self.conn.execute("""
                    INSERT INTO historico_puntos
                        (cliente_id, tipo, puntos, descripcion, saldo_actual, usuario, venta_id)
                    VALUES (?,'REDENCION',?,?,?,?,?)
                """, (
                    cliente_id, puntos_a_canjear,
                    f"Canje en venta {folio}", saldo,
                    usuario, venta_id
                ))

            # 5c. Puntos — acumulación
            if cliente_id and puntos_ganados > 0:
                self.conn.execute(
                    "UPDATE clientes SET puntos=puntos+?, fecha_ultima_compra=datetime('now') WHERE id=?",
                    (puntos_ganados, cliente_id)
                )
                saldo = self._puntos_cliente(cliente_id)
                self.conn.execute("""
                    INSERT INTO historico_puntos
                        (cliente_id, tipo, puntos, descripcion, saldo_actual, usuario, venta_id)
                    VALUES (?,'COMPRA',?,?,?,?,?)
                """, (
                    cliente_id, puntos_ganados,
                    f"Venta {folio}", saldo,
                    usuario, venta_id
                ))

            # 6. Evento sync (para sincronización offline)
            self._registrar_evento_sync("ventas", "INSERT", venta_id, folio, usuario)

            self.conn.execute(f"RELEASE {sp}")
            logger.info("Venta %s (#%d) procesada. Total=$%.2f Puntos+%d",
                        folio, venta_id, total, puntos_ganados)

            return ResultadoVenta(
                venta_id=venta_id,
                folio=folio,
                subtotal=subtotal,
                descuento=round(desc_pesos + desc_puntos, 2),
                iva=iva_monto,
                total=total,
                cambio=datos_pago.cambio,
                puntos_ganados=puntos_ganados,
                ticket_data=self._construir_ticket(
                    venta_id, folio, items, datos_pago,
                    subtotal, iva_monto, total,
                    desc_pesos + desc_puntos,
                    usuario, cliente_id, puntos_ganados
                ),
            )

        except Exception as exc:
            try:
                self.conn.execute(f"ROLLBACK TO {sp}")
                self.conn.execute(f"RELEASE {sp}")
            except Exception:
                pass
            logger.error("ROLLBACK venta %s: %s", folio, exc, exc_info=True)
            if isinstance(exc, (VentaError, StockInsuficienteError)):
                raise
            raise VentaError(f"Error inesperado al procesar venta: {exc}") from exc

    def cancelar_venta(
        self,
        venta_id: int,
        motivo:   str,
        usuario:  str,
    ) -> None:
        """
        Cancela una venta y REVIERTE el inventario.
        Solo cancela ventas 'completada'. No elimina registros.
        """
        sp = f"cancel_{venta_id}"
        try:
            self.conn.execute(f"SAVEPOINT {sp}")

            row = self.conn.execute(
                "SELECT estado FROM ventas WHERE id=?", (venta_id,)
            ).fetchone()
            if not row:
                raise VentaError(f"Venta #{venta_id} no encontrada.")
            if row["estado"] != "completada":
                raise VentaError(f"Solo se pueden cancelar ventas en estado 'completada'.")

            # Marcar como cancelada
            self.conn.execute(
                "UPDATE ventas SET estado='cancelada' WHERE id=?", (venta_id,)
            )

            # Revertir inventario (devolución de cada producto)
            detalles = self.conn.execute(
                "SELECT producto_id, cantidad FROM detalles_venta WHERE venta_id=?",
                (venta_id,)
            ).fetchall()
            inv = InventoryService(self.conn, usuario=usuario, sucursal_id=self.sucursal_id)
            for d in detalles:
                inv.registrar_entrada(
                    producto_id=d["producto_id"],
                    cantidad=d["cantidad"],
                    descripcion=f"Devolución por cancelación venta #{venta_id}: {motivo}",
                    referencia=str(venta_id),
                )

            # Movimiento de caja negativo
            venta = self.conn.execute(
                "SELECT total, forma_pago FROM ventas WHERE id=?", (venta_id,)
            ).fetchone()
            self.conn.execute("""
                INSERT INTO movimientos_caja
                    (tipo, monto, descripcion, usuario, venta_id, forma_pago)
                VALUES ('EGRESO',?,?,?,?,?)
            """, (
                venta["total"],
                f"Cancelación venta #{venta_id}: {motivo}",
                usuario, venta_id, venta["forma_pago"]
            ))

            self.conn.execute(f"RELEASE {sp}")
            logger.info("Venta #%d cancelada por %s: %s", venta_id, usuario, motivo)

        except Exception as exc:
            try:
                self.conn.execute(f"ROLLBACK TO {sp}")
                self.conn.execute(f"RELEASE {sp}")
            except Exception:
                pass
            if isinstance(exc, VentaError):
                raise
            raise VentaError(f"Error al cancelar venta: {exc}") from exc

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _generar_folio(self) -> str:
        """
        Folio = V{YYYYMMDD}-{secuencia_diaria_4dig}
        Evita COUNT(*) que puede colisionar en concurrencia;
        usa MAX para obtener el folio más alto del día.
        """
        hoy = datetime.now().strftime("%Y%m%d")
        prefix = f"V{hoy}-"
        row = self.conn.execute(
            "SELECT MAX(CAST(SUBSTR(folio, ?) AS INTEGER)) FROM ventas WHERE folio LIKE ?",
            (len(prefix) + 1, f"{prefix}%")
        ).fetchone()
        n = (row[0] or 0) + 1
        return f"{prefix}{n:04d}"

    def _puntos_cliente(self, cliente_id: int) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(puntos,0) FROM clientes WHERE id=?", (cliente_id,)
        ).fetchone()
        return int(row[0]) if row else 0

    def _nombre_cliente(self, cliente_id: int) -> str:
        row = self.conn.execute(
            "SELECT nombre, COALESCE(apellido,'') FROM clientes WHERE id=?",
            (cliente_id,)
        ).fetchone()
        if row:
            return f"{row[0]} {row[1]}".strip()
        return "Público General"

    def _registrar_evento_sync(
        self,
        tabla: str,
        operacion: str,
        registro_id: int,
        referencia: str,
        usuario: str,
    ) -> None:
        """Registra evento para sincronización offline. Fallo no crítico."""
        try:
            self.conn.execute("""
                INSERT INTO sync_eventos
                    (tabla, operacion, registro_id, sucursal_id, usuario)
                VALUES (?,?,?,?,?)
            """, (tabla, operacion, registro_id, self.sucursal_id, usuario))
        except Exception as e:
            logger.warning("No se pudo registrar evento sync: %s", e)

    def _construir_ticket(
        self, venta_id, folio, items, datos_pago,
        subtotal, iva, total, descuento,
        usuario, cliente_id, puntos_ganados
    ) -> dict:
        return {
            "venta_id":    venta_id,
            "folio":       folio,
            "fecha":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cajero":      usuario,
            "cliente":     self._nombre_cliente(cliente_id) if cliente_id else "Público General",
            "items": [
                {
                    "nombre":          i.nombre,
                    "cantidad":        i.cantidad,
                    "precio_unitario": i.precio_unitario,
                    "descuento":       i.descuento,
                    "subtotal":        i.subtotal,
                    "unidad":          i.unidad,
                }
                for i in items
            ],
            "totales": {
                "subtotal":    subtotal,
                "descuento":   descuento,
                "iva":         iva,
                "total":       total,
            },
            "pago": {
                "forma_pago":        datos_pago.forma_pago,
                "efectivo_recibido": datos_pago.efectivo_recibido,
                "cambio":            datos_pago.cambio,
            },
            "puntos_ganados": puntos_ganados,
        }

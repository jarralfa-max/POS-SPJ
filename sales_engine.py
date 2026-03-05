# core/services/sales_engine.py
# ── SALES ENGINE FIFO — SPJ Enterprise v3.2 ───────────────────────────────────
# Venta atómica: header + detalles + FIFO inventario + caja + puntos + evento.
# Un SAVEPOINT único cubre todo. Rollback total en cualquier fallo.
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional
from core.services.margin_audit_engine import MarginAuditEngine

from core.database import Connection, get_db
from core.services.inventory_engine import (
    InventoryEngine,
    StockInsuficienteError,
    LockActivoError,
    RecetaNoEncontradaError,
    MixConsumptionResult,
)

logger = logging.getLogger("spj.sales_engine")


# ── Excepciones ───────────────────────────────────────────────────────────────

class VentaError(Exception):
    pass

class PagoInsuficienteError(VentaError):
    pass

class CarritoVacioError(VentaError):
    pass


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ItemVenta:
    producto_id:      int
    nombre:           str
    cantidad:         float
    precio_unitario:  float
    descuento:        float = 0.0
    unidad:           str   = "pza"
    comentarios:      str   = ""

    @property
    def subtotal(self) -> float:
        return round(self.cantidad * self.precio_unitario - self.descuento, 2)


@dataclass
class DatosPago:
    forma_pago:        str
    efectivo_recibido: float = 0.0
    cambio:            float = 0.0
    saldo_credito:     float = 0.0


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
    batch_allocations: List[dict] = field(default_factory=list)  # detalle FIFO
    ticket_data:    dict          = field(default_factory=dict)


# ══════════════════════════════════════════════════════════════════════════════

class SalesEngine:
    """
    Motor de ventas FIFO con transacción única.

    Garantías:
    - Inventario descontado POR LOTE (FIFO más antiguo primero)
    - Si falla cualquier paso → ROLLBACK total (sin stock fantasma)
    - System lock 'ventas' verificado antes de procesar
    - Evento registrado en event_log para sync
    """

    PUNTOS_POR_PESO = 1

    def __init__(
        self,
        db:        Connection,
        branch_id: int = 1,
    ):
        self.db        = db
        self.branch_id = branch_id

    def procesar_venta(
        self,
        items:            List[ItemVenta],
        datos_pago:       DatosPago,
        usuario:          str,
        cliente_id:       int   = None,
        descuento_global: float = 0.0,
        iva_rate:         float = 0.0,
        folio:            str   = None,
    ) -> ResultadoVenta:
        """
        Procesa venta de forma completamente atómica.
        Descuenta inventario por FIFO de lote más antiguo.
        Lanza VentaError, StockInsuficienteError, LockActivoError.
        """
        if not items:
            raise CarritoVacioError("La venta no tiene productos.")

        # ── Pre-cálculos (sin tocar BD) ───────────────────────────────────────
        bruto      = sum(i.subtotal for i in items)
        desc_total = round(bruto * descuento_global, 2)
        subtotal   = round(bruto - desc_total, 2)
        iva        = round(subtotal * iva_rate, 2)
        total      = round(subtotal + iva, 2)

        if total < 0:
            raise VentaError(f"Total negativo ({total}). Revise descuentos.")

        if datos_pago.forma_pago == "Efectivo":
            if datos_pago.efectivo_recibido < total - 0.01:
                raise PagoInsuficienteError(
                    f"Efectivo insuficiente: recibido=${datos_pago.efectivo_recibido:.2f}"
                    f" total=${total:.2f}"
                )
            datos_pago.cambio = round(datos_pago.efectivo_recibido - total, 2)

        puntos = int(total * self.PUNTOS_POR_PESO) if cliente_id else 0
        if not folio:
            folio = self._generar_folio()

        inv = InventoryEngine(self.db, usuario=usuario, branch_id=self.branch_id)

        # ── Verificar lock antes de abrir transacción ─────────────────────────
        inv._check_lock("ventas")

        # ── TRANSACCIÓN ÚNICA ─────────────────────────────────────────────────
        with self.db.transaction(mode="IMMEDIATE"):
            # 1. Insertar header venta
            _, venta_id = self.db.execute_returning(
                """
                INSERT INTO ventas
                    (folio, usuario, cliente_id, subtotal, descuento, iva, total,
                     forma_pago, efectivo_recibido, cambio, puntos_ganados, estado,
                     fecha)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,'completada',datetime('now'))
                """,
                (folio, usuario, cliente_id, subtotal, desc_total, iva, total,
                 datos_pago.forma_pago, datos_pago.efectivo_recibido,
                 datos_pago.cambio, puntos),
            )

            # 2. Detalles + FIFO inventario
            batch_allocations = []
            for item in items:
                # ── Cálculo de costo real FIFO pre-descuento ─────────────────
                # Lee el costo_unitario del lote FIFO que se va a consumir
                # (sin modificar aún — solo lectura para conocer el costo)
                costo_real_item = self._calcular_costo_fifo_item(item)

                # Insertar detalle con costo real
                margen_real_item = round(item.precio_unitario - costo_real_item, 4)
                self.db.execute(
                    """
                    INSERT INTO detalles_venta
                        (venta_id, producto_id, cantidad, precio_unitario,
                         descuento, subtotal, unidad, comentarios,
                         costo_unitario_real, margen_real)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (venta_id, item.producto_id, item.cantidad,
                     item.precio_unitario, item.descuento, item.subtotal,
                     item.unidad, item.comentarios,
                     costo_real_item, margen_real_item),
                )

                # ── Descuento de inventario: receta mixta vs. FIFO directo ──
                from core.services.product_recipe_repository import ProductRecipeRepository
                repo = ProductRecipeRepository(self.db)

                if repo.has_recipe(item.producto_id):
                    # Producto compuesto (surtido/retazo): descontar piezas
                    # proporcionalmente según su receta de consumo.
                    mix_result = inv.consume_product_mix(
                        product_id=item.producto_id,
                        weight=item.cantidad,
                        sale_id=venta_id,
                        descripcion=f"Venta #{folio} — {item.nombre} (mix)",
                    )
                    for b in mix_result.breakdown:
                        for batch_id, bib_id, qty in b["allocations"]:
                            batch_allocations.append({
                                "producto_id": b["piece_product_id"],
                                "nombre":      b["piece_name"],
                                "batch_id":    batch_id,
                                "qty":         qty,
                                "via_mix":     True,
                                "mix_product": item.nombre,
                            })
                else:
                    # Producto simple: descontar FIFO directo (comportamiento actual)
                    alloc = inv.descontar_fifo(
                        producto_id=item.producto_id,
                        cantidad=item.cantidad,
                        venta_id=venta_id,
                        tipo="salida_venta",
                        descripcion=f"Venta #{folio} — {item.nombre}",
                    )
                    for batch_id, bib_id, qty in alloc.allocations:
                        batch_allocations.append({
                            "producto_id": item.producto_id,
                            "nombre":      item.nombre,
                            "batch_id":    batch_id,
                            "qty":         qty,
                            "via_mix":     False,
                        })

            # 3. Movimiento de caja
            self.db.execute(
                """
                INSERT INTO movimientos_caja
                    (tipo, monto, descripcion, usuario, venta_id, forma_pago)
                VALUES ('INGRESO',?,?,?,?,?)
                """,
                (total, f"Venta #{folio}", usuario, venta_id, datos_pago.forma_pago),
            )

            # 4. Crédito si aplica
            if datos_pago.forma_pago == "Crédito" and cliente_id and datos_pago.saldo_credito > 0:
                self.db.execute(
                    "UPDATE clientes SET saldo=saldo+? WHERE id=?",
                    (datos_pago.saldo_credito, cliente_id),
                )

            # 5. Puntos de fidelidad
            if cliente_id and puntos > 0:
                self.db.execute(
                    "UPDATE clientes SET puntos=puntos+?, "
                    "fecha_ultima_compra=datetime('now') WHERE id=?",
                    (puntos, cliente_id),
                )
                saldo = self.db.fetchscalar(
                    "SELECT puntos FROM clientes WHERE id=?",
                    (cliente_id,), default=puntos,
                )
                self.db.execute(
                    """
                    INSERT INTO historico_puntos
                        (cliente_id, tipo, puntos, descripcion, saldo_actual, usuario, venta_id)
                    VALUES (?,'COMPRA',?,?,?,?,?)
                    """,
                    (cliente_id, puntos, f"Venta {folio}", saldo, usuario, venta_id),
                )

            # 6. Evento offline-first
            try:
                from sync.event_logger import EventLogger
                EventLogger(self.db.raw).registrar(
                    tipo="venta",
                    entidad="ventas",
                    entidad_id=venta_id,
                    payload={
                        "folio":      folio,
                        "total":      total,
                        "items":      len(items),
                        "forma_pago": datos_pago.forma_pago,
                        "puntos":     puntos,
                        "batches":    len(batch_allocations),
                    },
                    sucursal_id=self.branch_id,
                    usuario=usuario,
                )
            except Exception as ev_exc:
                logger.warning("EventLogger ventas falló (no crítico): %s", ev_exc)

            # 7. VENTA_COMPLETADA → EventBus (async para no bloquear UI)
            try:
                from core.events.event_bus import get_bus, VENTA_COMPLETADA
                get_bus().publish(VENTA_COMPLETADA, {
                    "venta_id":    venta_id,
                    "folio":       folio,
                    "sucursal_id": self.branch_id,
                    "usuario":     usuario,
                    "cliente_id":  cliente_id,
                    "total":       total,
                    "forma_pago":  datos_pago.forma_pago,
                    "puntos":      puntos,
                    "items": [
                        {
                            "producto_id":     it.producto_id,
                            "nombre":          it.nombre,
                            "cantidad":        it.cantidad,
                            "precio_unitario": it.precio_unitario,
                            "subtotal":        it.subtotal,
                        }
                        for it in items
                    ],
                }, async_=True)
            except Exception as bus_exc:
                logger.warning("EventBus VENTA_COMPLETADA falló (no crítico): %s", bus_exc)

        logger.info("Venta %s #%d OK | total=$%.2f | batches=%d | puntos=%d",
                    folio, venta_id, total, len(batch_allocations), puntos)

        return ResultadoVenta(
            venta_id=venta_id,
            folio=folio,
            subtotal=subtotal,
            descuento=desc_total,
            iva=iva,
            total=total,
            cambio=datos_pago.cambio,
            puntos_ganados=puntos,
            batch_allocations=batch_allocations,
            ticket_data=self._ticket_data(
                venta_id, folio, items, datos_pago,
                subtotal, iva, total, usuario, cliente_id,
                batch_allocations,
            ),
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _calcular_costo_fifo_item(self, item: ItemVenta) -> float:
        """
        Calcula el costo_unitario_real promedio ponderado por FIFO
        para un ítem de venta, antes de modificar el inventario.
        Retorna costo por kg/unidad.
        """
        try:
            from core.services.product_recipe_repository import ProductRecipeRepository
            repo   = ProductRecipeRepository(self.db)
            recipe = repo.get_recipe(item.producto_id)

            if recipe:
                # Producto mixto: costo = suma ponderada de piezas
                costo_total = 0.0
                for comp in recipe.items:
                    kg_pieza = item.cantidad * (comp.percentage / 100.0)
                    costo_pieza = self.db.fetchscalar(
                        """
                        SELECT COALESCE(
                            SUM(bib.costo_unitario * bib.cantidad_disponible)
                            / NULLIF(SUM(bib.cantidad_disponible), 0),
                            0
                        )
                        FROM branch_inventory_batches bib
                        JOIN chicken_batches cb ON cb.id = bib.batch_id
                        WHERE bib.branch_id = ?
                          AND bib.producto_id = ?
                          AND bib.cantidad_disponible > 0
                          AND cb.estado NOT IN ('agotado','cancelado')
                        """,
                        (self.branch_id, comp.piece_product_id),
                        default=0.0,
                    )
                    costo_total += float(costo_pieza) * (comp.percentage / 100.0)
                return round(costo_total, 4)
            else:
                # Producto simple: costo FIFO del lote más antiguo
                row = self.db.fetchone(
                    """
                    SELECT COALESCE(bib.costo_unitario, 0)
                    FROM branch_inventory_batches bib
                    JOIN chicken_batches cb ON cb.id = bib.batch_id
                    WHERE bib.branch_id = ?
                      AND bib.producto_id = ?
                      AND bib.cantidad_disponible > 0
                      AND cb.estado NOT IN ('agotado','cancelado')
                    ORDER BY bib.fecha_entrada ASC, bib.id ASC
                    LIMIT 1
                    """,
                    (self.branch_id, item.producto_id),
                )
                return float(row[0]) if row else 0.0
        except Exception:
            return 0.0

    def _generar_folio(self) -> str:
        n = self.db.fetchscalar("SELECT COUNT(*) FROM ventas", default=0)
        return f"V{datetime.now().strftime('%Y%m%d')}-{(n or 0) + 1:04d}"

    def _ticket_data(
        self,
        venta_id, folio, items, datos_pago,
        subtotal, iva, total, usuario, cliente_id,
        batch_allocations,
    ) -> dict:
        cliente_nombre = "Público General"
        if cliente_id:
            row = self.db.fetchone(
                "SELECT nombre, COALESCE(apellido_paterno,'') FROM clientes WHERE id=?",
                (cliente_id,),
            )
            if row:
                cliente_nombre = f"{row[0]} {row[1]}".strip()

        return {
            "venta_id": venta_id,
            "folio":    folio,
            "fecha":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cajero":   usuario,
            "cliente":  cliente_nombre,
            "sucursal": self.branch_id,
            "items": [
                {
                    "nombre":          i.nombre,
                    "cantidad":        i.cantidad,
                    "precio_unitario": i.precio_unitario,
                    "total":           i.subtotal,
                    "unidad":          i.unidad,
                }
                for i in items
            ],
            "totales": {
                "subtotal":    subtotal,
                "impuestos":   iva,
                "total_final": total,
            },
            "pago": {
                "forma_pago":        datos_pago.forma_pago,
                "efectivo_recibido": datos_pago.efectivo_recibido,
                "cambio":            datos_pago.cambio,
            },
            "batch_detail": batch_allocations,
        }


class SalesEngine:

    def __init__(self, db, branch_id, user_role):
        self.db = db
        self.branch_id = branch_id
        self.user_role = user_role
        self.margin_engine = MarginAuditEngine(db)

    def process_sale(self, sale_data):
        operation_id = str(uuid.uuid4())
        start_time = datetime.utcnow()

        with self.db.transaction("SALES_PROCESS", operation_id=operation_id):

            total_amount = 0
            total_cost = 0

            sale_id = self.db.execute("""
                INSERT INTO sales(branch_id, created_at, total_amount, total_cost)
                VALUES (?, ?, 0, 0)
            """, (
                self.branch_id,
                datetime.utcnow().isoformat()
            ))

            for item in sale_data["items"]:

                total_amount += item["price"] * item["quantity"]
                total_cost += item["cost"] * item["quantity"]

                self.db.execute("""
                    INSERT INTO sales_details(
                        sale_id,
                        product_id,
                        quantity,
                        unit_price,
                        total_price,
                        batch_id
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    sale_id,
                    item["product_id"],
                    item["quantity"],
                    item["price"],
                    item["price"] * item["quantity"],
                    item["batch_id"]
                ))

            if total_amount == 0:
                raise Exception("INVALID_SALE_TOTAL")

            margin_real = (total_amount - total_cost) / total_amount

            if margin_real < 0 and self.user_role != "admin":
                raise Exception("ADMIN_OVERRIDE_REQUIRED")

            self.db.execute("""
                UPDATE sales
                SET total_amount = ?, total_cost = ?
                WHERE id = ?
            """, (
                total_amount,
                total_cost,
                sale_id
            ))

            week_label = datetime.utcnow().strftime("%Y-%W")
            self.margin_engine.detect_negative_margin(
                self.branch_id,
                week_label
            )

        return sale_id
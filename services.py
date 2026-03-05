# services.py  ── Motores transaccionales SPJ
# Compatible con el esquema REAL de tablas del proyecto.
# No rompe ningún módulo existente — ventas.py llama a SalesEngine.procesar_venta()
from __future__ import annotations
import sqlite3
import logging
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger("spj.services")


# ── Excepciones de dominio ────────────────────────────────────────────────────

class InventarioError(Exception):
    pass

class StockInsuficienteError(InventarioError):
    def __init__(self, producto_id: int, nombre: str, disponible: float, requerido: float):
        self.producto_id = producto_id
        self.nombre = nombre
        self.disponible = disponible
        self.requerido = requerido
        super().__init__(
            f"Stock insuficiente: '{nombre}' — disponible: {disponible:.3f}, requerido: {requerido:.3f}"
        )

class VentaError(Exception):
    pass


# ── InventoryEngine ───────────────────────────────────────────────────────────

class InventoryEngine:
    """
    Motor de inventario con auditoría completa.
    TODOS los cambios de stock pasan por aquí y quedan en movimientos_inventario.
    Compatible con el esquema movimientos_inventario del proyecto.
    """

    def __init__(self, conn: sqlite3.Connection, usuario: str = "Sistema"):
        self.conn = conn
        self.usuario = usuario or "Sistema"

    # ── API pública ───────────────────────────────────────────────────────────

    def get_stock(self, producto_id: int) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(existencia,0) FROM productos WHERE id=?", (producto_id,)
        ).fetchone()
        return float(row[0]) if row else 0.0

    def get_nombre(self, producto_id: int) -> str:
        row = self.conn.execute(
            "SELECT nombre FROM productos WHERE id=?", (producto_id,)
        ).fetchone()
        return row[0] if row else f"Producto#{producto_id}"

    def registrar_entrada(self, producto_id: int, cantidad: float,
                          descripcion: str = "Entrada", referencia: str = None,
                          costo_unitario: float = 0.0) -> None:
        if cantidad <= 0:
            raise InventarioError(f"Cantidad debe ser >0: {cantidad}")
        self._aplicar(producto_id, +cantidad, "entrada", descripcion, referencia, costo_unitario)

    def registrar_salida_venta(self, producto_id: int, cantidad: float, venta_id: int) -> None:
        """Descuenta stock por venta — valida suficiencia ANTES."""
        if cantidad <= 0:
            raise InventarioError(f"Cantidad debe ser >0: {cantidad}")
        stock = self.get_stock(producto_id)
        if stock < cantidad:
            raise StockInsuficienteError(
                producto_id, self.get_nombre(producto_id), stock, cantidad
            )
        self._aplicar(producto_id, -cantidad, "salida",
                      f"Venta #{venta_id}", str(venta_id))

    def ajustar_stock(self, producto_id: int, cantidad_nueva: float,
                      motivo: str = "Ajuste manual") -> None:
        diff = cantidad_nueva - self.get_stock(producto_id)
        if abs(diff) < 0.001:
            return
        self._aplicar(producto_id, diff, "ajuste", motivo)

    def transformar_pollo(self, producto_base_id: int, kg_descontar: float,
                          piezas: list) -> None:
        """
        Descuenta pollo entero y acredita cortes.
        piezas = [{'producto_id': int, 'kg': float, 'descripcion': str}, ...]
        """
        if kg_descontar <= 0:
            raise InventarioError("kg_descontar debe ser >0")
        stock = self.get_stock(producto_base_id)
        if stock < kg_descontar:
            raise StockInsuficienteError(
                producto_base_id, self.get_nombre(producto_base_id), stock, kg_descontar
            )
        total_piezas = sum(p["kg"] for p in piezas)
        if total_piezas > kg_descontar * 1.05:
            raise InventarioError(
                f"Piezas ({total_piezas:.3f}kg) exceden insumo ({kg_descontar:.3f}kg)"
            )
        self._aplicar(producto_base_id, -kg_descontar, "transformacion_salida",
                      f"Transformación → {len(piezas)} cortes")
        for p in piezas:
            self._aplicar(p["producto_id"], p["kg"], "transformacion_entrada",
                          p.get("descripcion", "Corte de pollo"))

    # ── Interno ───────────────────────────────────────────────────────────────

    def _aplicar(self, producto_id: int, delta: float, tipo: str,
                 descripcion: str = "", referencia: str = None,
                 costo_unitario: float = 0.0) -> None:
        stock_antes = self.get_stock(producto_id)
        stock_despues = round(stock_antes + delta, 4)
        if stock_despues < -0.001:
            raise StockInsuficienteError(
                producto_id, self.get_nombre(producto_id), stock_antes, abs(delta)
            )
        self.conn.execute(
            "UPDATE productos SET existencia=? WHERE id=?",
            (max(stock_despues, 0), producto_id)
        )
        # Registrar en movimientos_inventario — cubre ambos esquemas (tipo y tipo_movimiento)
        self.conn.execute("""
            INSERT INTO movimientos_inventario
                (producto_id, tipo, tipo_movimiento, cantidad,
                 existencia_anterior, existencia_nueva,
                 costo_unitario, costo_total,
                 descripcion, referencia, usuario, fecha)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
        """, (
            producto_id, tipo, tipo, abs(delta),
            stock_antes, max(stock_despues, 0),
            costo_unitario, round(abs(delta) * costo_unitario, 4),
            descripcion, referencia, self.usuario
        ))
        logger.debug("Inventario p#%s %s %.3f | %.3f→%.3f",
                     producto_id, tipo, delta, stock_antes, stock_despues)


# ── SalesEngine ───────────────────────────────────────────────────────────────

@dataclass
class ItemVenta:
    producto_id: int
    nombre: str
    cantidad: float
    precio_unitario: float
    descuento: float = 0.0
    unidad: str = "pza"
    comentarios: str = ""

    @property
    def subtotal(self) -> float:
        return round(self.cantidad * self.precio_unitario - self.descuento, 2)


@dataclass
class DatosPago:
    forma_pago: str        # 'Efectivo', 'Tarjeta', 'Crédito', 'Transferencia'
    efectivo_recibido: float = 0.0
    cambio: float = 0.0
    saldo_credito: float = 0.0


@dataclass
class ResultadoVenta:
    venta_id: int
    folio: str
    total: float
    cambio: float
    puntos_ganados: int
    ticket_data: dict = field(default_factory=dict)


class SalesEngine:
    """
    Motor transaccional de ventas.
    Todo (venta + detalles + inventario + caja + puntos) en UNA transacción.
    Compatible con el esquema real del proyecto SPJ.
    """

    PUNTOS_POR_PESO = 1   # 1 punto por peso MXN

    def __init__(self, conn: sqlite3.Connection, sucursal_id: int = 1):
        self.conn        = conn
        self.sucursal_id = sucursal_id

    def procesar_venta(
        self,
        items: list,           # list[ItemVenta]
        datos_pago: DatosPago,
        usuario: str,
        cliente_id: int = None,
        descuento_global: float = 0.0,
        iva_rate: float = 0.0,
        folio: str = None,
    ) -> ResultadoVenta:
        """
        Procesa venta de forma atómica.
        Lanza VentaError o StockInsuficienteError si algo falla.
        En error → ROLLBACK automático.
        """
        if not items:
            raise VentaError("La venta no tiene productos.")

        # ── Cálculos previos (sin tocar BD) ──────────────────────────────────
        bruto = sum(i.subtotal for i in items)
        desc_total = round(bruto * descuento_global, 2)
        subtotal = round(bruto - desc_total, 2)
        iva = round(subtotal * iva_rate, 2)
        total = round(subtotal + iva, 2)

        if total < 0:
            raise VentaError(f"Total negativo ({total}). Revise descuentos.")

        if datos_pago.forma_pago == "Efectivo":
            if datos_pago.efectivo_recibido < total - 0.01:
                raise VentaError(
                    f"Efectivo insuficiente: "
                    f"recibido=${datos_pago.efectivo_recibido:.2f}, total=${total:.2f}"
                )
            datos_pago.cambio = round(datos_pago.efectivo_recibido - total, 2)

        puntos = int(total * self.PUNTOS_POR_PESO) if cliente_id else 0
        if not folio:
            folio = self._generar_folio()

        # ── TRANSACCIÓN ATÓMICA ───────────────────────────────────────────────
        try:
            # Usamos SAVEPOINT para ser compatibles con conexiones que ya tienen
            # una transacción abierta (isolation_level != None en Python sqlite3)
            sp = f"venta_{folio.replace('-','_')}"
            try:
                self.conn.execute(f"SAVEPOINT {sp}")
            except Exception:
                self.conn.execute("BEGIN IMMEDIATE")
                sp = None

            # 1. Insertar venta
            cur = self.conn.execute("""
                INSERT INTO ventas
                    (folio, usuario, cliente_id, subtotal, descuento, iva, total,
                     forma_pago, efectivo_recibido, cambio, puntos_ganados, estado)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,'completada')
            """, (
                folio, usuario, cliente_id, subtotal, desc_total, iva, total,
                datos_pago.forma_pago, datos_pago.efectivo_recibido,
                datos_pago.cambio, puntos
            ))
            venta_id = cur.lastrowid

            # 2. Detalles + inventario (lanza StockInsuficienteError si falla)
            inv = InventoryEngine(self.conn, usuario)
            for item in items:
                self.conn.execute("""
                    INSERT INTO detalles_venta
                        (venta_id, producto_id, cantidad, precio_unitario,
                         descuento, subtotal, unidad, comentarios)
                    VALUES (?,?,?,?,?,?,?,?)
                """, (venta_id, item.producto_id, item.cantidad,
                      item.precio_unitario, item.descuento, item.subtotal,
                      item.unidad, item.comentarios))
                inv.registrar_salida_venta(item.producto_id, item.cantidad, venta_id)

            # 3. Movimiento de caja
            self.conn.execute("""
                INSERT INTO movimientos_caja
                    (tipo, monto, descripcion, usuario, venta_id, forma_pago)
                VALUES ('INGRESO',?,?,?,?,?)
            """, (total, f"Venta #{folio}", usuario, venta_id, datos_pago.forma_pago))

            # 4. Crédito si aplica
            if datos_pago.forma_pago == "Crédito" and cliente_id and datos_pago.saldo_credito > 0:
                self.conn.execute(
                    "UPDATE clientes SET saldo=saldo+? WHERE id=?",
                    (datos_pago.saldo_credito, cliente_id)
                )

            # 5. Puntos de fidelidad
            if cliente_id and puntos > 0:
                self.conn.execute("""
                    UPDATE clientes SET
                        puntos=puntos+?,
                        fecha_ultima_compra=datetime('now')
                    WHERE id=?
                """, (puntos, cliente_id))
                saldo_row = self.conn.execute(
                    "SELECT puntos FROM clientes WHERE id=?", (cliente_id,)
                ).fetchone()
                saldo_actual = saldo_row[0] if saldo_row else puntos
                self.conn.execute("""
                    INSERT INTO historico_puntos
                        (cliente_id, tipo, puntos, descripcion, saldo_actual, usuario, venta_id)
                    VALUES (?,'COMPRA',?,?,?,?,?)
                """, (cliente_id, puntos, f"Venta {folio}", saldo_actual, usuario, venta_id))

            if sp is not None:
                self.conn.execute(f"RELEASE {sp}")
            else:
                self.conn.execute("COMMIT")
            logger.info("Venta %s (#%s) OK. Total=$%.2f Puntos=%s",
                        folio, venta_id, total, puntos)

            # Registrar evento offline-first para sync multi-sucursal
            try:
                from sync.event_logger import EventLogger
                EventLogger(self.conn).registrar(
                    tipo="venta",
                    entidad="ventas",
                    entidad_id=venta_id,
                    payload={
                        "folio":      folio,
                        "total":      total,
                        "items":      len(items),
                        "forma_pago": datos_pago.forma_pago,
                        "puntos":     puntos,
                    },
                    sucursal_id=self.sucursal_id,
                    usuario=usuario,
                )
            except Exception as _ev_exc:
                logger.warning("EventLogger no pudo registrar venta %s: %s", folio, _ev_exc)

            return ResultadoVenta(
                venta_id=venta_id, folio=folio, total=total,
                cambio=datos_pago.cambio, puntos_ganados=puntos,
                ticket_data=self._ticket_data(
                    venta_id, folio, items, datos_pago,
                    subtotal, iva, total, usuario, cliente_id
                )
            )

        except Exception as exc:
            try:
                if sp is not None:
                    self.conn.execute(f"ROLLBACK TO {sp}")
                    self.conn.execute(f"RELEASE {sp}")
                else:
                    self.conn.execute("ROLLBACK")
            except Exception:
                pass
            logger.error("ROLLBACK venta %s. Error: %s", folio, exc, exc_info=True)
            if isinstance(exc, (VentaError, StockInsuficienteError)):
                raise
            raise VentaError(f"Error inesperado al guardar venta: {exc}") from exc

    def _generar_folio(self) -> str:
        row = self.conn.execute("SELECT COUNT(*) FROM ventas").fetchone()
        n = (row[0] or 0) + 1
        return f"V{datetime.now().strftime('%Y%m%d')}-{n:04d}"

    def _ticket_data(self, venta_id, folio, items, datos_pago,
                     subtotal, iva, total, usuario, cliente_id) -> dict:
        cliente_nombre = "Público General"
        if cliente_id:
            row = self.conn.execute(
                "SELECT nombre, COALESCE(apellido_paterno,'') FROM clientes WHERE id=?",
                (cliente_id,)
            ).fetchone()
            if row:
                cliente_nombre = f"{row[0]} {row[1]}".strip()

        return {
            "venta_id": venta_id,
            "folio": folio,
            "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "cajero": usuario,
            "cliente": cliente_nombre,
            "items": [{"nombre": i.nombre, "cantidad": i.cantidad,
                       "precio_unitario": i.precio_unitario,
                       "total": i.subtotal, "unidad": i.unidad}
                      for i in items],
            "totales": {"subtotal": subtotal, "impuestos": iva,
                        "descuento": 0, "total_final": total},
            "pago": {"forma_pago": datos_pago.forma_pago,
                     "efectivo_recibido": datos_pago.efectivo_recibido,
                     "cambio": datos_pago.cambio},
        }


# ── Silenciar logs de ROLLBACK en producción (son stderr de debug) ────────────
logging.getLogger("spj.services").setLevel(logging.WARNING)

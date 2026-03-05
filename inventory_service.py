# core/services/inventory_service.py
# InventoryService — Motor de inventario enterprise
# REGLA ABSOLUTA: ningún módulo puede hacer UPDATE productos SET existencia directamente.
# Todo movimiento pasa por este servicio → auditoría completa + consistencia.
from __future__ import annotations
import sqlite3
import logging
from dataclasses import dataclass
from datetime import date
from typing import List, Optional, Tuple

from core.db.connection import transaction

logger = logging.getLogger("spj.inventory")


# ── Excepciones de dominio ────────────────────────────────────────────────────

class InventarioError(Exception):
    pass


class StockInsuficienteError(InventarioError):
    def __init__(self, producto_id: int, nombre: str, disponible: float, requerido: float):
        self.producto_id = producto_id
        self.nombre      = nombre
        self.disponible  = disponible
        self.requerido   = requerido
        super().__init__(
            f"Stock insuficiente '{nombre}' — disponible: {disponible:.3f}, requerido: {requerido:.3f}"
        )


class ProductoNoEncontradoError(InventarioError):
    pass


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class PiezaTransformacion:
    """Una pieza resultante de la transformación (ej. pollo entero → pechuga, pierna, etc.)"""
    producto_id:  int
    kg:           float
    descripcion:  str = ""


@dataclass
class StockInfo:
    producto_id: int
    nombre:      str
    existencia:  float
    stock_min:   float
    bajo_minimo: bool


# ── Servicio principal ────────────────────────────────────────────────────────

class InventoryService:
    """
    Motor de inventario con auditoría completa.
    Todos los cambios de stock generan un registro en movimientos_inventario.

    Uso básico:
        svc = InventoryService(conn, usuario="admin")
        svc.registrar_entrada(producto_id=3, cantidad=50.0, descripcion="Compra proveedor")
        svc.registrar_salida_venta(producto_id=3, cantidad=1.5, venta_id=100)
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        usuario: str = "Sistema",
        sucursal_id: int = 1,
    ):
        self.conn        = conn
        self.usuario     = usuario or "Sistema"
        self.sucursal_id = sucursal_id

    # ── Lectura ──────────────────────────────────────────────────────────────

    def get_stock(self, producto_id: int) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(existencia, 0) FROM productos WHERE id=? AND _deleted=0",
            (producto_id,)
        ).fetchone()
        return float(row[0]) if row else 0.0

    def get_info(self, producto_id: int) -> StockInfo:
        row = self.conn.execute(
            "SELECT id, nombre, COALESCE(existencia,0), COALESCE(stock_minimo,0) "
            "FROM productos WHERE id=? AND _deleted=0",
            (producto_id,)
        ).fetchone()
        if not row:
            raise ProductoNoEncontradoError(f"Producto id={producto_id} no encontrado")
        e = float(row[2])
        m = float(row[3])
        return StockInfo(
            producto_id=row[0],
            nombre=row[1],
            existencia=e,
            stock_min=m,
            bajo_minimo=(e <= m),
        )

    def get_stock_lote(self, producto_ids: List[int]) -> dict[int, float]:
        """Consulta eficiente de varios productos en una sola query."""
        if not producto_ids:
            return {}
        placeholders = ",".join("?" * len(producto_ids))
        rows = self.conn.execute(
            f"SELECT id, COALESCE(existencia,0) FROM productos WHERE id IN ({placeholders})",
            producto_ids
        ).fetchall()
        return {r[0]: float(r[1]) for r in rows}

    def productos_bajo_minimo(self) -> List[StockInfo]:
        """Lista todos los productos cuyo stock <= stock_minimo."""
        rows = self.conn.execute(
            "SELECT id, nombre, COALESCE(existencia,0), COALESCE(stock_minimo,0) "
            "FROM productos WHERE activo=1 AND _deleted=0 AND existencia <= stock_minimo"
        ).fetchall()
        return [
            StockInfo(r[0], r[1], float(r[2]), float(r[3]), True)
            for r in rows
        ]

    # ── Escritura (API pública) ───────────────────────────────────────────────

    def registrar_entrada(
        self,
        producto_id: int,
        cantidad:    float,
        descripcion: str   = "Entrada de inventario",
        referencia:  str   = None,
        costo_unitario: float = 0.0,
        fecha_caducidad: Optional[date] = None,
    ) -> int:
        """
        Registra entrada de stock (compra, devolución, ajuste positivo).
        Retorna el ID del movimiento creado.
        """
        if cantidad <= 0:
            raise InventarioError(f"Cantidad debe ser > 0, recibido: {cantidad}")
        return self._aplicar(
            producto_id=producto_id,
            delta=+cantidad,
            tipo="entrada",
            descripcion=descripcion,
            referencia=referencia,
            costo_unitario=costo_unitario,
            fecha_caducidad=fecha_caducidad,
        )

    def registrar_salida_venta(
        self,
        producto_id: int,
        cantidad:    float,
        venta_id:    int,
    ) -> int:
        """
        Descuenta stock por venta. Valida suficiencia ANTES de modificar.
        Lanza StockInsuficienteError si no hay stock suficiente.
        """
        if cantidad <= 0:
            raise InventarioError(f"Cantidad debe ser > 0, recibido: {cantidad}")

        info = self.get_info(producto_id)
        if info.existencia < cantidad:
            raise StockInsuficienteError(
                producto_id, info.nombre, info.existencia, cantidad
            )
        return self._aplicar(
            producto_id=producto_id,
            delta=-cantidad,
            tipo="salida",
            descripcion=f"Venta #{venta_id}",
            referencia=str(venta_id),
            venta_id=venta_id,
        )

    def registrar_salida_manual(
        self,
        producto_id: int,
        cantidad:    float,
        motivo:      str = "Salida manual",
        referencia:  str = None,
    ) -> int:
        """Salida manual: merma, consumo interno, etc."""
        if cantidad <= 0:
            raise InventarioError(f"Cantidad debe ser > 0, recibido: {cantidad}")
        info = self.get_info(producto_id)
        if info.existencia < cantidad:
            raise StockInsuficienteError(
                producto_id, info.nombre, info.existencia, cantidad
            )
        return self._aplicar(
            producto_id=producto_id,
            delta=-cantidad,
            tipo="salida_manual",
            descripcion=motivo,
            referencia=referencia,
        )

    def ajustar_stock(
        self,
        producto_id:     int,
        cantidad_nueva:  float,
        motivo:          str = "Ajuste de inventario físico",
    ) -> int:
        """
        Ajusta stock al valor exacto (inventario físico).
        Calcula automáticamente el delta y lo registra como 'ajuste'.
        """
        if cantidad_nueva < 0:
            raise InventarioError("La existencia no puede ser negativa.")
        stock_actual = self.get_stock(producto_id)
        diff = round(cantidad_nueva - stock_actual, 4)
        if abs(diff) < 0.0001:
            return -1  # Sin cambio real
        tipo = "ajuste_entrada" if diff > 0 else "ajuste_salida"
        return self._aplicar(
            producto_id=producto_id,
            delta=diff,
            tipo=tipo,
            descripcion=motivo,
        )

    def transformar_pollo(
        self,
        producto_base_id: int,
        kg_descontar:     float,
        piezas:           List[PiezaTransformacion],
        merma_kg:         float = 0.0,
    ) -> List[int]:
        """
        Transforma pollo entero → cortes.
        - Descuenta kg_descontar de producto_base
        - Acredita cada pieza en piezas[]
        - merma_kg: peso que se pierde (hueso, vísceras, etc.)
        - Valida que piezas + merma <= kg_descontar * 1.05 (tolerancia 5%)
        Retorna lista de IDs de movimientos creados.
        """
        if kg_descontar <= 0:
            raise InventarioError("kg_descontar debe ser > 0")

        info = self.get_info(producto_base_id)
        if info.existencia < kg_descontar:
            raise StockInsuficienteError(
                producto_base_id, info.nombre, info.existencia, kg_descontar
            )

        total_salida = sum(p.kg for p in piezas) + merma_kg
        if total_salida > kg_descontar * 1.05:
            raise InventarioError(
                f"Piezas+merma ({total_salida:.3f}kg) exceden insumo ({kg_descontar:.3f}kg) con tolerancia 5%"
            )

        movimiento_ids = []
        # Salida producto base
        mid = self._aplicar(
            producto_id=producto_base_id,
            delta=-kg_descontar,
            tipo="transformacion_salida",
            descripcion=f"Transformación → {len(piezas)} cortes",
        )
        movimiento_ids.append(mid)

        # Entrada cada pieza
        for p in piezas:
            if p.kg <= 0:
                continue
            mid = self._aplicar(
                producto_id=p.producto_id,
                delta=+p.kg,
                tipo="transformacion_entrada",
                descripcion=p.descripcion or "Corte de pollo",
            )
            movimiento_ids.append(mid)

        logger.info(
            "Transformación: %.3fkg producto#%d → %d cortes | merma=%.3fkg",
            kg_descontar, producto_base_id, len(piezas), merma_kg
        )
        return movimiento_ids

    def transferir_entre_sucursales(
        self,
        producto_id:       int,
        cantidad:          float,
        sucursal_destino:  int,
        usuario_destino:   str,
        observaciones:     str = "",
    ) -> int:
        """
        Descuenta de la sucursal actual y registra transferencia pendiente.
        La entrada en destino se aplica al confirmar recepción.
        """
        info = self.get_info(producto_id)
        if info.existencia < cantidad:
            raise StockInsuficienteError(
                producto_id, info.nombre, info.existencia, cantidad
            )

        mid = self._aplicar(
            producto_id=producto_id,
            delta=-cantidad,
            tipo="transferencia_salida",
            descripcion=f"Transferencia → sucursal {sucursal_destino}: {observaciones}",
        )

        # Registrar en tabla de transferencias
        self.conn.execute("""
            INSERT INTO transferencias_inventario
                (producto_id, cantidad, sucursal_origen, sucursal_destino,
                 usuario_origen, usuario_destino, estado, observaciones)
            VALUES (?,?,?,?,?,?,'pendiente',?)
        """, (
            producto_id, cantidad,
            self.sucursal_id, sucursal_destino,
            self.usuario, usuario_destino, observaciones
        ))
        return mid

    # ── Historial ────────────────────────────────────────────────────────────

    def historial(
        self,
        producto_id:    int,
        limite:         int = 50,
        tipo_filtro:    str = None,
    ) -> list:
        sql = """
            SELECT id, tipo, cantidad, existencia_anterior, existencia_nueva,
                   descripcion, usuario, fecha
            FROM movimientos_inventario
            WHERE producto_id = ?
        """
        params = [producto_id]
        if tipo_filtro:
            sql += " AND tipo = ?"
            params.append(tipo_filtro)
        sql += " ORDER BY fecha DESC LIMIT ?"
        params.append(limite)
        return self.conn.execute(sql, params).fetchall()

    # ── Motor interno ─────────────────────────────────────────────────────────

    def _aplicar(
        self,
        producto_id:     int,
        delta:           float,
        tipo:            str,
        descripcion:     str   = "",
        referencia:      str   = None,
        costo_unitario:  float = 0.0,
        fecha_caducidad: Optional[date] = None,
        venta_id:        int   = None,
    ) -> int:
        """
        Aplica el delta al stock y registra el movimiento.
        Este método NUNCA debe llamarse directamente desde UI.
        Retorna el ID del movimiento creado.
        """
        stock_antes = self.get_stock(producto_id)
        stock_nuevo = round(stock_antes + delta, 4)

        if stock_nuevo < -0.001:
            info = self.get_info(producto_id)
            raise StockInsuficienteError(
                producto_id, info.nombre, stock_antes, abs(delta)
            )

        stock_final = max(stock_nuevo, 0.0)

        # Actualizar existencia en producto
        self.conn.execute(
            "UPDATE productos SET existencia=?, fecha_actualizacion=datetime('now') WHERE id=?",
            (stock_final, producto_id)
        )

        # Registrar movimiento de auditoría
        costo_total = round(abs(delta) * costo_unitario, 4)
        cur = self.conn.execute("""
            INSERT INTO movimientos_inventario
                (producto_id, tipo, tipo_movimiento, cantidad,
                 existencia_anterior, existencia_nueva,
                 costo_unitario, costo_total,
                 descripcion, referencia, venta_id,
                 usuario, sucursal_id, fecha, fecha_caducidad)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),?)
        """, (
            producto_id,
            tipo, tipo, abs(delta),
            stock_antes, stock_final,
            costo_unitario, costo_total,
            descripcion, referencia, venta_id,
            self.usuario, self.sucursal_id,
            str(fecha_caducidad) if fecha_caducidad else None,
        ))

        logger.debug(
            "Inv p#%d %s %.3f | %.3f → %.3f [%s]",
            producto_id, tipo, delta, stock_antes, stock_final, self.usuario
        )
        return cur.lastrowid

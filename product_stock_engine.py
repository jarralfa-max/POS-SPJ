# core/services/product_stock_engine.py
# ── PRODUCT STOCK ENGINE — SPJ Enterprise v9 ─────────────────────────────────
# Engine base unificado para consulta y gestión de stock de CUALQUIER tipo
# de producto: pollo (BIB/FIFO), abarrotes (existencia directa) y desechables.
#
# PROBLEMA QUE RESUELVE (#15):
#   - InventoryEngine solo maneja pollo (branch_inventory_batches)
#   - Abarrotes usan productos.existencia directamente
#   - No había una API unificada para "¿cuánto stock tengo de X producto?"
#
# REGLA:
#   Si el producto tiene chicken_batches activos → tipo POLLO (FIFO por BIB)
#   Si no, → tipo SIMPLE (productos.existencia)
#   La decisión la toma este engine automáticamente.
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger("spj.product_stock_engine")


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class StockInfo:
    producto_id:    int
    nombre:         str
    tipo:           str        # "pollo" | "simple"
    existencia:     float
    existencia_bib: float      # desde branch_inventory_batches (0 si tipo simple)
    existencia_tab: float      # desde productos.existencia
    stock_minimo:   float
    bajo_minimo:    bool
    unidad:         str
    sucursal_id:    int

    @property
    def diferencia_bib_tabla(self) -> float:
        """Diferencia entre BIB y tabla productos — útil para conciliación."""
        return round(self.existencia_bib - self.existencia_tab, 6)


# ── Engine ────────────────────────────────────────────────────────────────────

class ProductStockEngine:
    """
    API unificada de stock para cualquier tipo de producto.

    Instanciar:
        eng = ProductStockEngine(conn, sucursal_id=1)
        info = eng.get_stock(producto_id)
        eng.descontar(producto_id, cantidad, motivo="venta #42")
        eng.ingresar(producto_id, cantidad, motivo="ajuste")
    """

    def __init__(
        self,
        conn:        sqlite3.Connection,
        sucursal_id: int = 1,
        usuario:     str = "Sistema",
    ) -> None:
        self.conn        = conn
        self.sucursal_id = sucursal_id
        self.usuario     = usuario

    # ── Consultas ─────────────────────────────────────────────────────────────

    def get_stock(self, producto_id: int) -> StockInfo:
        """
        Retorna el stock unificado de un producto.
        Fuente autoritativa según tipo:
          - Pollo: SUM(branch_inventory_batches.cantidad_disponible)
          - Simple: productos.existencia
        """
        row = self.conn.execute(
            "SELECT nombre, COALESCE(existencia,0), COALESCE(stock_minimo,0), "
            "       COALESCE(unidad,'pza') FROM productos WHERE id=?",
            (producto_id,),
        ).fetchone()
        if not row:
            return StockInfo(
                producto_id=producto_id, nombre="?", tipo="simple",
                existencia=0, existencia_bib=0, existencia_tab=0,
                stock_minimo=0, bajo_minimo=False, unidad="pza",
                sucursal_id=self.sucursal_id,
            )
        nombre, exist_tab, minimo, unidad = row[0], float(row[1]), float(row[2]), row[3]

        # Verificar si tiene BIBs activos (es producto tipo pollo)
        bib_row = self.conn.execute(
            """
            SELECT COALESCE(SUM(cantidad_disponible), 0)
            FROM branch_inventory_batches
            WHERE branch_id = ? AND producto_id = ?
            """,
            (self.sucursal_id, producto_id),
        ).fetchone()
        exist_bib = float(bib_row[0]) if bib_row else 0.0

        # Tipo: pollo si tiene BIBs con stock > 0, simple si no
        tipo = "pollo" if exist_bib > 0 else "simple"
        existencia = exist_bib if tipo == "pollo" else exist_tab

        return StockInfo(
            producto_id=producto_id,
            nombre=nombre,
            tipo=tipo,
            existencia=existencia,
            existencia_bib=exist_bib,
            existencia_tab=exist_tab,
            stock_minimo=minimo,
            bajo_minimo=(minimo > 0 and existencia <= minimo),
            unidad=unidad,
            sucursal_id=self.sucursal_id,
        )

    def get_stock_multi(self, producto_ids: List[int]) -> Dict[int, StockInfo]:
        """Retorna StockInfo para múltiples productos en una sola query."""
        if not producto_ids:
            return {}

        # Tabla base
        ph = ",".join("?" * len(producto_ids))
        rows = self.conn.execute(
            f"SELECT id, nombre, COALESCE(existencia,0), "
            f"COALESCE(stock_minimo,0), COALESCE(unidad,'pza') "
            f"FROM productos WHERE id IN ({ph})",
            producto_ids,
        ).fetchall()

        # BIBs en una query
        bib_rows = self.conn.execute(
            f"""
            SELECT producto_id, COALESCE(SUM(cantidad_disponible), 0)
            FROM branch_inventory_batches
            WHERE branch_id = ? AND producto_id IN ({ph})
            GROUP BY producto_id
            """,
            [self.sucursal_id] + producto_ids,
        ).fetchall()
        bib_map: Dict[int, float] = {r[0]: float(r[1]) for r in bib_rows}

        result: Dict[int, StockInfo] = {}
        for row in rows:
            pid = row[0]
            exist_tab = float(row[2])
            minimo    = float(row[3])
            exist_bib = bib_map.get(pid, 0.0)
            tipo      = "pollo" if exist_bib > 0 else "simple"
            existencia = exist_bib if tipo == "pollo" else exist_tab
            result[pid] = StockInfo(
                producto_id=pid, nombre=row[1], tipo=tipo,
                existencia=existencia, existencia_bib=exist_bib,
                existencia_tab=exist_tab, stock_minimo=minimo,
                bajo_minimo=(minimo > 0 and existencia <= minimo),
                unidad=row[4], sucursal_id=self.sucursal_id,
            )
        return result

    def stock_bajo_minimo(self) -> List[StockInfo]:
        """Retorna todos los productos con existencia ≤ stock_minimo."""
        rows = self.conn.execute(
            "SELECT id FROM productos WHERE activo=1 AND stock_minimo > 0"
        ).fetchall()
        ids = [r[0] for r in rows]
        if not ids:
            return []
        todos = self.get_stock_multi(ids)
        return [s for s in todos.values() if s.bajo_minimo]

    # ── Operaciones (para productos SIMPLE — pollo usa InventoryEngine) ───────

    def descontar(
        self,
        producto_id: int,
        cantidad:    float,
        motivo:      str  = "",
        referencia_id:   Optional[int] = None,
        referencia_tipo: Optional[str] = None,
    ) -> float:
        """
        Descuenta stock de un producto SIMPLE (abarrotes/desechables).
        Para productos tipo pollo, usar InventoryEngine.descontar_fifo().

        Retorna la existencia nueva.
        Lanza ValueError si la existencia quedaría negativa.
        """
        info = self.get_stock(producto_id)
        if info.tipo == "pollo":
            raise ValueError(
                f"Producto '{info.nombre}' (ID {producto_id}) es tipo pollo — "
                "usar InventoryEngine.descontar_fifo() para descuentos FIFO."
            )
        if info.existencia_tab < cantidad - 1e-6:
            raise ValueError(
                f"Stock insuficiente '{info.nombre}': "
                f"disponible={info.existencia_tab:.3f} requerido={cantidad:.3f}"
            )
        nueva = round(info.existencia_tab - cantidad, 6)
        with self.conn:
            self.conn.execute(
                "UPDATE productos SET existencia=? WHERE id=?",
                (nueva, producto_id),
            )
            import uuid
            self.conn.execute(
                """
                INSERT INTO movimientos_inventario
                    (producto_id, tipo, tipo_movimiento, cantidad,
                     existencia_anterior, existencia_nueva,
                     descripcion, usuario, sucursal_id,
                     referencia_id, referencia_tipo, uuid, fecha)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                """,
                (
                    producto_id, "SALIDA", "descuento_simple", cantidad,
                    info.existencia_tab, nueva,
                    motivo, self.usuario, self.sucursal_id,
                    referencia_id, referencia_tipo, str(uuid.uuid4()),
                ),
            )
        logger.debug(
            "stock.descontar prod=%d %.3f → %.3f (%s)",
            producto_id, info.existencia_tab, nueva, motivo,
        )
        return nueva

    def ingresar(
        self,
        producto_id: int,
        cantidad:    float,
        motivo:      str  = "",
        referencia_id:   Optional[int] = None,
        referencia_tipo: Optional[str] = None,
    ) -> float:
        """Ingresa stock a un producto SIMPLE. Retorna existencia nueva."""
        if cantidad <= 0:
            raise ValueError("cantidad debe ser > 0")
        info    = self.get_stock(producto_id)
        nueva   = round(info.existencia_tab + cantidad, 6)
        with self.conn:
            self.conn.execute(
                "UPDATE productos SET existencia=? WHERE id=?", (nueva, producto_id)
            )
            import uuid
            self.conn.execute(
                """
                INSERT INTO movimientos_inventario
                    (producto_id, tipo, tipo_movimiento, cantidad,
                     existencia_anterior, existencia_nueva,
                     descripcion, usuario, sucursal_id,
                     referencia_id, referencia_tipo, uuid, fecha)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                """,
                (
                    producto_id, "ENTRADA", "ingreso_simple", cantidad,
                    info.existencia_tab, nueva,
                    motivo, self.usuario, self.sucursal_id,
                    referencia_id, referencia_tipo, str(uuid.uuid4()),
                ),
            )
        logger.debug(
            "stock.ingresar prod=%d %.3f → %.3f (%s)",
            producto_id, info.existencia_tab, nueva, motivo,
        )
        return nueva

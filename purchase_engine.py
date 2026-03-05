# core/services/purchase_engine.py
# ── PURCHASE ENGINE — SPJ Enterprise v9 ───────────────────────────────────────
# Motor de compras: registra adquisiciones y genera inventario global.
#
# PIPELINE:
#   Registro compra → gasto contable → inventario global → PUBLISH COMPRA_REGISTRADA
#
# SOPORTA DOS TIPOS DE COMPRA:
#   A) Pollo/proteína: llama InventoryEngine.recepcionar_lote()
#                      para crear chicken_batch + branch_inventory_batch
#   B) Abarrotes/desechables: llama ComprasInventariablesEngine.registrar_compra()
#                             para crear compra_inventariable + movimiento directo
#
# REGLA: Este engine NO modifica stock directamente. Delega a InventoryEngine.
from __future__ import annotations

import logging
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import List, Optional

logger = logging.getLogger("spj.purchase_engine")


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class CompraPolloResult:
    batch_id:       int
    gasto_id:       int
    producto_id:    int
    peso_kg:        float
    costo_kg:       float
    costo_total:    float
    proveedor:      str
    sucursal_id:    int
    op_uuid:        str


@dataclass
class CompraAbarrotesResult:
    compra_id:      int
    gasto_id:       int
    producto_id:    int
    volumen:        float
    unidad:         str
    costo_total:    float
    proveedor:      str
    estado:         str  # pagado | credito | parcial


@dataclass
class CompraResumen:
    """Resumen de una compra registrada — tipo genérico para listados."""
    id:              int
    tipo:            str   # pollo | abarrotes
    producto_nombre: str
    proveedor:       str
    volumen:         float
    unidad:          str
    costo_total:     float
    estado:          str
    sucursal_id:     int
    usuario:         str
    fecha:           str


# ── Engine ────────────────────────────────────────────────────────────────────

class PurchaseEngine:
    """
    Motor central de registro de compras.

    Instanciar con la conexión sqlite3 raw y datos del contexto:
        engine = PurchaseEngine(conn, sucursal_id=2, usuario="admin")

    Los eventos se publican al EventBus al finalizar cada compra.
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

    # ── Compra de pollo / proteína ────────────────────────────────────────────

    def registrar_compra_pollo(
        self,
        producto_id:       int,
        numero_pollos:     int,
        peso_kg:           float,
        costo_kg:          float,
        proveedor:         str         = "",
        forma_pago:        str         = "EFECTIVO",
        notas:             str         = "",
        sucursal_destino:  int         = None,
        categoria_gasto:   str         = "COMPRA_POLLO",
    ) -> CompraPolloResult:
        """
        Registra compra de pollo:
          1. INSERT gastos (registro contable)
          2. InventoryEngine.recepcionar_lote() → chicken_batch + BIB
          3. PUBLISH COMPRA_REGISTRADA

        Args:
            producto_id:      ID del producto pollo en la tabla productos.
            numero_pollos:    Cantidad de pollos enteros.
            peso_kg:          Peso total en kg.
            costo_kg:         Costo por kg.
            proveedor:        Nombre del proveedor (libre).
            forma_pago:       EFECTIVO | CREDITO | TRANSFERENCIA.
            notas:            Observaciones libres.
            sucursal_destino: Si != None, el lote se recibe en esa sucursal.
                              Si None, se usa self.sucursal_id.
            categoria_gasto:  Categoría contable (default COMPRA_POLLO).

        Returns:
            CompraPolloResult con IDs y montos.
        """
        if numero_pollos <= 0:
            raise ValueError("numero_pollos debe ser > 0")
        if peso_kg <= 0:
            raise ValueError("peso_kg debe ser > 0")
        if costo_kg < 0:
            raise ValueError("costo_kg no puede ser negativo")

        costo_total  = round(peso_kg * costo_kg, 4)
        op_uuid      = str(uuid.uuid4())
        branch_dest  = sucursal_destino or self.sucursal_id

        # Verificar que el producto existe
        row_prod = self.conn.execute(
            "SELECT nombre FROM productos WHERE id=?", (producto_id,)
        ).fetchone()
        if not row_prod:
            raise ValueError(f"Producto id={producto_id} no encontrado")
        nombre_prod = row_prod[0]

        with self.conn:
            # 1. INSERT gasto contable
            self.conn.execute(
                """
                INSERT INTO gastos
                    (concepto, monto, categoria, forma_pago, usuario,
                     notas, estado, fecha, sucursal_id)
                VALUES (?,?,?,?,?,?,'PAGADO',datetime('now'),?)
                """,
                (
                    f"Compra pollo — {nombre_prod} {numero_pollos}pz {peso_kg:.2f}kg",
                    costo_total,
                    categoria_gasto,
                    forma_pago,
                    self.usuario,
                    notas or f"Proveedor: {proveedor or 'Sin proveedor'}",
                    self.sucursal_id,
                ),
            )
            gasto_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            # 2. Recepcionar lote vía InventoryEngine (usa Connection wrapper)
            from core.database import Connection, get_db
            from core.services.inventory_engine import InventoryEngine

            db_conn = Connection(self.conn)
            inv_eng = InventoryEngine(db_conn, usuario=self.usuario, branch_id=branch_dest)
            batch_id = inv_eng.recepcionar_lote(
                producto_id=producto_id,
                numero_pollos=numero_pollos,
                peso_kg=peso_kg,
                costo_kg=costo_kg,
                proveedor=proveedor,
                compra_global_id=gasto_id,
                notas=notas,
            )

        logger.info(
            "Compra pollo: prod=%d %dpollos %.2fkg @$%.2f/kg batch#%d gasto#%d",
            producto_id, numero_pollos, peso_kg, costo_kg, batch_id, gasto_id,
        )

        result = CompraPolloResult(
            batch_id=batch_id,
            gasto_id=gasto_id,
            producto_id=producto_id,
            peso_kg=peso_kg,
            costo_kg=costo_kg,
            costo_total=costo_total,
            proveedor=proveedor,
            sucursal_id=branch_dest,
            op_uuid=op_uuid,
        )

        # 3. Publicar evento COMPRA_REGISTRADA al EventBus
        self._publicar_compra_registrada(result)
        return result

    # ── Compra de abarrotes / desechables ─────────────────────────────────────

    def registrar_compra_abarrotes(
        self,
        producto_id:       int,
        volumen:           float,
        unidad:            str,
        costo_unitario:    float,
        proveedor:         str           = "",
        forma_pago:        str           = "EFECTIVO",
        es_credito:        bool          = False,
        fecha_vencimiento: Optional[str] = None,
        notas:             str           = "",
    ) -> CompraAbarrotesResult:
        """
        Registra compra de abarrotes/desechables (inventario no-pollo):
          1. Delega a ComprasInventariablesEngine.registrar_compra()
          2. PUBLISH COMPRA_REGISTRADA

        El inventario se actualiza directamente en productos.existencia
        más un movimiento en movimientos_inventario.
        """
        if volumen <= 0:
            raise ValueError("volumen debe ser > 0")
        if costo_unitario < 0:
            raise ValueError("costo_unitario no puede ser negativo")

        from core.services.compras_inventariables_engine import ComprasInventariablesEngine

        eng = ComprasInventariablesEngine(
            conn=self.conn,
            sucursal_id=self.sucursal_id,
            usuario=self.usuario,
        )
        res = eng.registrar_compra(
            producto_id=producto_id,
            volumen=volumen,
            unidad=unidad,
            costo_unitario=costo_unitario,
            proveedor=proveedor,
            forma_pago=forma_pago,
            es_credito=es_credito,
            fecha_vencimiento=fecha_vencimiento,
            notas=notas,
        )

        logger.info(
            "Compra abarrotes: prod=%d %.3f%s @$%.2f compra#%d gasto#%d",
            producto_id, volumen, unidad, costo_unitario, res.compra_id, res.gasto_id,
        )

        result = CompraAbarrotesResult(
            compra_id=res.compra_id,
            gasto_id=res.gasto_id,
            producto_id=producto_id,
            volumen=volumen,
            unidad=unidad,
            costo_total=res.costo_total,
            proveedor=proveedor,
            estado=res.estado,
        )

        self._publicar_compra_registrada_abarrotes(result)
        return result

    # ── Listados ──────────────────────────────────────────────────────────────

    def listar_compras_pollo(
        self,
        desde:   Optional[str] = None,
        hasta:   Optional[str] = None,
        limit:   int            = 200,
    ) -> List[dict]:
        """Retorna compras de pollo (chicken_batches con compra_global_id)."""
        q = """
            SELECT cb.id, p.nombre, COALESCE(cb.proveedor,''),
                   cb.numero_pollos, cb.peso_kg_original, cb.costo_kg,
                   cb.costo_total, cb.fecha_recepcion,
                   cb.usuario_recepcion, cb.branch_id
            FROM chicken_batches cb
            JOIN productos p ON p.id = cb.producto_id
            WHERE cb.root_batch_id = cb.id
        """
        params = []
        if desde:
            q += " AND DATE(cb.fecha_recepcion) >= ?"; params.append(desde)
        if hasta:
            q += " AND DATE(cb.fecha_recepcion) <= ?"; params.append(hasta)
        q += " ORDER BY cb.fecha_recepcion DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(q, params).fetchall()
        return [
            {
                "id": r[0], "producto": r[1], "proveedor": r[2],
                "num_pollos": r[3], "peso_kg": r[4], "costo_kg": r[5],
                "costo_total": r[6], "fecha": str(r[7]),
                "usuario": r[8], "sucursal_id": r[9],
            }
            for r in rows
        ]

    def listar_compras_abarrotes(
        self,
        desde:       Optional[str] = None,
        hasta:       Optional[str] = None,
        producto_id: Optional[int] = None,
        estado:      Optional[str] = None,
        limit:       int            = 200,
    ) -> List[dict]:
        """Retorna compras_inventariables con detalle de producto."""
        from core.services.compras_inventariables_engine import ComprasInventariablesEngine

        eng = ComprasInventariablesEngine(
            conn=self.conn,
            sucursal_id=self.sucursal_id,
            usuario=self.usuario,
        )
        rows = eng.listar_compras(
            desde=desde, hasta=hasta,
            producto_id=producto_id, estado=estado, limit=limit,
        )
        return [
            {
                "id": r.id, "producto": r.producto_nombre, "proveedor": r.proveedor,
                "volumen": r.volumen, "unidad": r.unidad,
                "costo_unitario": r.costo_unitario, "costo_total": r.costo_total,
                "forma_pago": r.forma_pago, "estado": r.estado,
                "fecha": r.fecha, "usuario": r.usuario,
            }
            for r in rows
        ]

    def cuentas_por_pagar(self) -> List[dict]:
        """Retorna CXP pendientes de abarrotes."""
        from core.services.compras_inventariables_engine import ComprasInventariablesEngine

        eng = ComprasInventariablesEngine(
            conn=self.conn, sucursal_id=self.sucursal_id, usuario=self.usuario,
        )
        cxp = eng.cuentas_por_pagar()
        return [
            {
                "id": c.id, "proveedor": c.proveedor, "producto": c.producto_nombre,
                "monto_total": c.monto_total, "monto_pagado": c.monto_pagado,
                "saldo_pendiente": c.saldo_pendiente, "vencimiento": c.fecha_vencimiento,
                "estado": c.estado, "fecha": c.fecha,
            }
            for c in cxp
        ]

    # ── Eventos ───────────────────────────────────────────────────────────────

    def _publicar_compra_registrada(self, result: CompraPolloResult) -> None:
        try:
            from core.events.event_bus import get_bus, COMPRA_REGISTRADA
            get_bus().publish(COMPRA_REGISTRADA, {
                "tipo":         "pollo",
                "batch_id":     result.batch_id,
                "gasto_id":     result.gasto_id,
                "producto_id":  result.producto_id,
                "volumen":      result.peso_kg,
                "unidad":       "kg",
                "costo_total":  result.costo_total,
                "proveedor":    result.proveedor,
                "sucursal_id":  result.sucursal_id,
                "usuario":      self.usuario,
                "op_uuid":      result.op_uuid,
            })
        except Exception as exc:
            logger.warning("COMPRA_REGISTRADA event falló (no crítico): %s", exc)

    def _publicar_compra_registrada_abarrotes(self, result: CompraAbarrotesResult) -> None:
        try:
            from core.events.event_bus import get_bus, COMPRA_REGISTRADA
            get_bus().publish(COMPRA_REGISTRADA, {
                "tipo":         "abarrotes",
                "compra_id":    result.compra_id,
                "gasto_id":     result.gasto_id,
                "producto_id":  result.producto_id,
                "volumen":      result.volumen,
                "unidad":       result.unidad,
                "costo_total":  result.costo_total,
                "proveedor":    result.proveedor,
                "sucursal_id":  self.sucursal_id,
                "usuario":      self.usuario,
            })
        except Exception as exc:
            logger.warning("COMPRA_REGISTRADA (abarrotes) event falló: %s", exc)

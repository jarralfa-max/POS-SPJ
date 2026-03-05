# core/services/inventory_enterprise_engine.py
# ── INVENTORY ENTERPRISE ENGINE — SPJ Pollería v6 ─────────────────────────────
# Motor central desacoplado para inventario global/sucursal.
# Responsabilidades:
#   - registrar_compra_global()   → afecta inventario_global
#   - registrar_recepcion()       → sube inventario_sucursal, baja global
#   - descontar_por_venta()       → consume proporcional según receta
#   - registrar_traspaso()        → mueve entre sucursales
#   - guardar_receta_consumo()    → CRUD de recetas
#   - validar_existencia()        → check stock antes de operar
#
# REGLA: Ningún módulo UI toca inventario_global / inventario_sucursal
#        directamente. Todo pasa por este engine.
from __future__ import annotations

import logging
import uuid as _uuid
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple
import sqlite3

logger = logging.getLogger("spj.inventory_enterprise")


# ── Excepciones ───────────────────────────────────────────────────────────────

class InventarioEnterpriseError(Exception):
    pass

class StockInsuficienteError(InventarioEnterpriseError):
    def __init__(self, producto: str, disponible: float, requerido: float):
        self.producto   = producto
        self.disponible = disponible
        self.requerido  = requerido
        super().__init__(
            f"Stock insuficiente de '{producto}': "
            f"disponible={disponible:.3f}kg, requerido={requerido:.3f}kg"
        )

class RecetaInvalidaError(InventarioEnterpriseError):
    pass

class TraspasoError(InventarioEnterpriseError):
    pass


# ── DTOs ──────────────────────────────────────────────────────────────────────

@dataclass
class DetalleReceta:
    materia_prima_id: int
    nombre_mp:        str
    porcentaje:       float   # 0 < x <= 100; suma de todos debe ser 100


@dataclass
class RecetaConsumo:
    id:             Optional[int]
    producto_id:    int
    nombre:         str
    activo:         bool
    items:          List[DetalleReceta] = field(default_factory=list)

    @property
    def total_pct(self) -> float:
        return sum(i.porcentaje for i in self.items)

    @property
    def valida(self) -> bool:
        return bool(self.items) and abs(self.total_pct - 100.0) < 0.1


@dataclass
class ResultadoVentaConsumo:
    """Detalle de materias primas consumidas en una venta."""
    producto_id:    int
    nombre:         str
    peso_vendido:   float
    consumos:       List[Dict]  # [{mp_id, nombre, porcentaje, kg_consumido}]
    tiene_receta:   bool


@dataclass
class StockInfo:
    producto_id:   int
    nombre:        str
    kg_global:     float
    kg_sucursal:   float   # stock de la sucursal activa


# ══════════════════════════════════════════════════════════════════════════════

class InventoryEnterpriseEngine:
    """
    Motor de inventario enterprise para cadena pollería.

    Uso típico:
        eng = InventoryEnterpriseEngine(conn, sucursal_id=2, usuario="cajero1")

        # Admin registra compra
        eng.registrar_compra_global(producto_id=1, peso_kg=100.0, costo_total=4500.0)

        # Vendedor recibe pollo
        eng.registrar_recepcion(producto_id=1, peso_kg=30.0)

        # Al vender surtido de 2kg:
        resultado = eng.descontar_por_venta(producto_id=5, peso_kg=2.0, venta_id=101)

        # Traspaso entre sucursales
        eng.registrar_traspaso(producto_id=1, peso_kg=10.0, sucursal_destino_id=3)
    """

    def __init__(
        self,
        conn:        sqlite3.Connection,
        sucursal_id: int = 1,
        usuario:     str = "Sistema",
    ):
        self.conn        = conn
        self.sucursal_id = sucursal_id
        self.usuario     = usuario or "Sistema"

    # ── INVENTARIO GLOBAL ─────────────────────────────────────────────────────

    def registrar_compra_global(
        self,
        producto_id:  int,
        peso_kg:      float,
        costo_total:  float = 0.0,
        notas:        str   = "",
        compra_ref_id: int  = None,
    ) -> int:
        """
        Registra una compra global (admin).
        Aumenta inventario_global del producto.
        Retorna el id del registro en inventario_global.
        """
        if peso_kg <= 0:
            raise InventarioEnterpriseError("peso_kg debe ser > 0")

        costo_por_kg = round(costo_total / peso_kg, 4) if peso_kg > 0 else 0.0

        with self.conn:
            cur = self.conn.execute(
                """
                INSERT INTO inventario_global
                    (producto_id, peso_kg, costo_total, costo_por_kg,
                     compra_ref_id, usuario, notas)
                VALUES (?,?,?,?,?,?,?)
                """,
                (producto_id, peso_kg, costo_total, costo_por_kg,
                 compra_ref_id, self.usuario, notas),
            )
            reg_id = cur.lastrowid

        nombre = self._nombre_producto(producto_id)
        logger.info(
            "CompraGlobal: producto='%s' +%.3fkg costo=%.2f usuario=%s",
            nombre, peso_kg, costo_total, self.usuario,
        )
        return reg_id

    def stock_global(self, producto_id: int) -> float:
        """Suma de todas las entradas menos salidas en inventario_global."""
        # Entradas: suma de inventario_global
        entradas = self._scalar(
            "SELECT COALESCE(SUM(peso_kg),0) FROM inventario_global WHERE producto_id=?",
            (producto_id,),
        )
        # Salidas: total recepcionado en todas las sucursales para este producto
        salidas = self._scalar(
            """
            SELECT COALESCE(SUM(peso_kg),0)
            FROM recepciones_pollo
            WHERE producto_id=? AND estado='confirmada'
            """,
            (producto_id,),
        )
        return max(0.0, float(entradas) - float(salidas))

    # ── INVENTARIO SUCURSAL ───────────────────────────────────────────────────

    def stock_sucursal(self, producto_id: int, sucursal_id: int = None) -> float:
        """Stock disponible en la sucursal activa (o la indicada)."""
        suc = sucursal_id or self.sucursal_id
        val = self._scalar(
            """
            SELECT COALESCE(peso_kg, 0)
            FROM inventario_sucursal
            WHERE sucursal_id=? AND producto_id=?
            """,
            (suc, producto_id),
        )
        return max(0.0, float(val))

    def validar_existencia(
        self,
        producto_id: int,
        peso_requerido: float,
        sucursal_id: int = None,
    ) -> None:
        """
        Lanza StockInsuficienteError si no hay stock suficiente.
        Usar antes de procesar una venta o traspaso.
        """
        disponible = self.stock_sucursal(producto_id, sucursal_id)
        if disponible < peso_requerido - 1e-6:
            nombre = self._nombre_producto(producto_id)
            raise StockInsuficienteError(nombre, disponible, peso_requerido)

    def _actualizar_inventario_sucursal(
        self,
        sucursal_id: int,
        producto_id: int,
        delta_kg:    float,
    ) -> float:
        """
        Suma delta_kg al inventario_sucursal (puede ser negativo para restar).
        Usa INSERT OR IGNORE + UPDATE para manejar primera vez.
        Retorna el nuevo stock.
        Lanza InventarioEnterpriseError si el resultado sería negativo.
        """
        self.conn.execute(
            """
            INSERT OR IGNORE INTO inventario_sucursal
                (sucursal_id, producto_id, peso_kg)
            VALUES (?,?,0)
            """,
            (sucursal_id, producto_id),
        )
        nuevo = self._scalar(
            "SELECT peso_kg FROM inventario_sucursal WHERE sucursal_id=? AND producto_id=?",
            (sucursal_id, producto_id),
        )
        nuevo = round(float(nuevo) + delta_kg, 6)
        if nuevo < -1e-9:
            nombre = self._nombre_producto(producto_id)
            raise StockInsuficienteError(nombre, float(nuevo) - delta_kg, abs(delta_kg))
        nuevo = max(0.0, nuevo)
        self.conn.execute(
            """
            UPDATE inventario_sucursal
            SET peso_kg=?, fecha_actualizacion=datetime('now')
            WHERE sucursal_id=? AND producto_id=?
            """,
            (nuevo, sucursal_id, producto_id),
        )
        return nuevo

    # ── RECEPCIÓN ─────────────────────────────────────────────────────────────

    def registrar_recepcion(
        self,
        producto_id:      int,
        peso_kg:          float,
        costo_kg:         float    = 0.0,
        proveedor:        str      = "",
        lote_ref:         str      = "",
        compra_global_id: int      = None,
        notas:            str      = "",
    ) -> int:
        """
        Vendedor registra pollo recibido.
        Aumenta inventario_sucursal.
        Disminuye inventario_global (para conciliación).
        Retorna id de la recepción.
        """
        if peso_kg <= 0:
            raise InventarioEnterpriseError("peso_kg debe ser > 0")

        with self.conn:
            # Aumentar sucursal
            self._actualizar_inventario_sucursal(self.sucursal_id, producto_id, +peso_kg)

            cur = self.conn.execute(
                """
                INSERT INTO recepciones_pollo
                    (sucursal_id, producto_id, peso_kg, costo_kg,
                     proveedor, lote_ref, compra_global_id,
                     usuario_id, estado, notas)
                VALUES (?,?,?,?,?,?,?,?,'confirmada',?)
                """,
                (self.sucursal_id, producto_id, peso_kg, costo_kg,
                 proveedor, lote_ref, compra_global_id,
                 self.usuario, notas),
            )
            rec_id = cur.lastrowid

        nombre = self._nombre_producto(producto_id)
        logger.info(
            "Recepción #%d: suc=%d producto='%s' +%.3fkg",
            rec_id, self.sucursal_id, nombre, peso_kg,
        )
        return rec_id

    def anular_recepcion(self, recepcion_id: int) -> None:
        """Anula una recepción y revierte el stock de sucursal."""
        row = self.conn.execute(
            "SELECT sucursal_id, producto_id, peso_kg, estado FROM recepciones_pollo WHERE id=?",
            (recepcion_id,),
        ).fetchone()
        if not row:
            raise InventarioEnterpriseError(f"Recepción #{recepcion_id} no encontrada")
        if row[3] == "anulada":
            raise InventarioEnterpriseError("Recepción ya anulada")

        with self.conn:
            self._actualizar_inventario_sucursal(row[0], row[1], -row[2])
            self.conn.execute(
                "UPDATE recepciones_pollo SET estado='anulada' WHERE id=?",
                (recepcion_id,),
            )

    # ── CONSUMO POR VENTA ─────────────────────────────────────────────────────

    def descontar_por_venta(
        self,
        producto_id: int,
        peso_kg:     float,
        venta_id:    int   = None,
    ) -> ResultadoVentaConsumo:
        """
        Descuenta inventario de sucursal según la receta de consumo del producto.
        Si no hay receta, descuenta el producto directamente.
        Retorna desglose del consumo para auditoría.
        Lanza StockInsuficienteError si alguna materia prima no tiene stock.
        """
        nombre = self._nombre_producto(producto_id)
        receta = self._get_receta_activa(producto_id)

        consumos = []

        with self.conn:
            if receta and receta.valida:
                # Venta con receta: consumo proporcional por materias primas
                for item in receta.items:
                    kg_mp = round(peso_kg * (item.porcentaje / 100.0), 6)
                    if kg_mp <= 1e-9:
                        continue
                    self.validar_existencia(item.materia_prima_id, kg_mp)
                    nuevo = self._actualizar_inventario_sucursal(
                        self.sucursal_id, item.materia_prima_id, -kg_mp
                    )
                    consumos.append({
                        "mp_id":        item.materia_prima_id,
                        "nombre":       item.nombre_mp,
                        "porcentaje":   item.porcentaje,
                        "kg_consumido": kg_mp,
                        "stock_nuevo":  nuevo,
                    })
                tiene_receta = True
            else:
                # Sin receta: descuento directo del producto
                self.validar_existencia(producto_id, peso_kg)
                nuevo = self._actualizar_inventario_sucursal(
                    self.sucursal_id, producto_id, -peso_kg
                )
                consumos.append({
                    "mp_id":        producto_id,
                    "nombre":       nombre,
                    "porcentaje":   100.0,
                    "kg_consumido": peso_kg,
                    "stock_nuevo":  nuevo,
                })
                tiene_receta = False

        if consumos:
            logger.info(
                "Consumo venta #%s: producto='%s' %.3fkg | %d materias primas | receta=%s",
                venta_id, nombre, peso_kg, len(consumos), tiene_receta,
            )

        return ResultadoVentaConsumo(
            producto_id=producto_id,
            nombre=nombre,
            peso_vendido=peso_kg,
            consumos=consumos,
            tiene_receta=tiene_receta,
        )

    # ── TRASPASOS ─────────────────────────────────────────────────────────────

    def registrar_traspaso(
        self,
        producto_id:           int,
        peso_kg:               float,
        sucursal_destino_id:   int,
        observaciones:         str = "",
    ) -> int:
        """
        Registra un traspaso entre sucursales.
        Valida stock en origen, descuenta origen, suma destino.
        Retorna id del traspaso.
        """
        if sucursal_destino_id == self.sucursal_id:
            raise TraspasoError("Origen y destino no pueden ser la misma sucursal")
        if peso_kg <= 0:
            raise TraspasoError("peso_kg debe ser > 0")

        self.validar_existencia(producto_id, peso_kg)

        with self.conn:
            # Descontar origen
            self._actualizar_inventario_sucursal(self.sucursal_id, producto_id, -peso_kg)
            # Sumar destino
            self._actualizar_inventario_sucursal(sucursal_destino_id, producto_id, +peso_kg)

            cur = self.conn.execute(
                """
                INSERT INTO traspasos_pollo
                    (sucursal_origen_id, sucursal_destino_id, producto_id,
                     peso_kg, estado, usuario_origen, observaciones,
                     fecha_confirmacion)
                VALUES (?,?,?,?,'confirmado',?,?,datetime('now'))
                """,
                (self.sucursal_id, sucursal_destino_id, producto_id,
                 peso_kg, self.usuario, observaciones),
            )
            traspaso_id = cur.lastrowid

        nombre = self._nombre_producto(producto_id)
        logger.info(
            "Traspaso #%d: '%s' %.3fkg suc%d→suc%d",
            traspaso_id, nombre, peso_kg, self.sucursal_id, sucursal_destino_id,
        )
        return traspaso_id

    # ── RECETAS DE CONSUMO ────────────────────────────────────────────────────

    def _get_receta_activa(self, producto_id: int) -> Optional[RecetaConsumo]:
        """Carga la receta activa de un producto, o None si no existe."""
        row = self.conn.execute(
            "SELECT id, nombre FROM recetas_consumo WHERE producto_venta_id=? AND activo=1",
            (producto_id,),
        ).fetchone()
        if not row:
            return None

        receta_id, nombre = int(row[0]), row[1]
        detalles = self.conn.execute(
            """
            SELECT materia_prima_id, nombre_mp, porcentaje
            FROM recetas_consumo_detalle
            WHERE receta_id=?
            ORDER BY orden ASC, id ASC
            """,
            (receta_id,),
        ).fetchall()

        receta = RecetaConsumo(
            id=receta_id,
            producto_id=producto_id,
            nombre=str(nombre or ""),
            activo=True,
        )
        for d in detalles:
            receta.items.append(DetalleReceta(
                materia_prima_id=int(d[0]),
                nombre_mp=str(d[1] or ""),
                porcentaje=float(d[2]),
            ))
        return receta

    def get_receta(self, producto_id: int) -> Optional[RecetaConsumo]:
        """API pública para obtener receta activa de un producto."""
        return self._get_receta_activa(producto_id)

    def guardar_receta_consumo(
        self,
        producto_id: int,
        items:       List[Dict],
        nombre:      str = "",
    ) -> int:
        """
        Crea o reemplaza la receta de consumo de un producto.
        items = [{"materia_prima_id": int, "porcentaje": float}, ...]
        La suma de porcentajes debe ser ~100%.
        Retorna receta_id.
        """
        if not items:
            raise RecetaInvalidaError("La receta debe tener al menos 1 componente")

        total = sum(float(i.get("porcentaje", 0)) for i in items)
        if abs(total - 100.0) > 0.1:
            raise RecetaInvalidaError(
                f"La suma de porcentajes debe ser 100% (actual: {total:.2f}%)"
            )
        for i in items:
            if float(i.get("porcentaje", 0)) <= 0:
                raise RecetaInvalidaError("Todos los porcentajes deben ser > 0")

        with self.conn:
            # Desactivar receta anterior si existe
            self.conn.execute(
                "UPDATE recetas_consumo SET activo=0, actualizado_en=datetime('now') "
                "WHERE producto_venta_id=? AND activo=1",
                (producto_id,),
            )
            # Insertar nueva receta
            nombre_final = nombre or self._nombre_producto(producto_id)
            cur = self.conn.execute(
                """
                INSERT INTO recetas_consumo
                    (producto_venta_id, nombre, activo, creado_por)
                VALUES (?,?,1,?)
                """,
                (producto_id, nombre_final, self.usuario),
            )
            receta_id = cur.lastrowid

            # Insertar detalles
            for orden, item in enumerate(items):
                mp_id  = int(item["materia_prima_id"])
                pct    = float(item["porcentaje"])
                nombre_mp = str(item.get("nombre_mp", "")) or self._nombre_producto(mp_id)
                self.conn.execute(
                    """
                    INSERT INTO recetas_consumo_detalle
                        (receta_id, materia_prima_id, porcentaje, nombre_mp, orden)
                    VALUES (?,?,?,?,?)
                    """,
                    (receta_id, mp_id, pct, nombre_mp, orden),
                )

        logger.info(
            "Receta consumo guardada: producto=%d '%s' | %d items | total=%.1f%%",
            producto_id, nombre_final, len(items), total,
        )
        return receta_id

    def eliminar_receta_consumo(self, producto_id: int) -> None:
        """Soft-delete de la receta activa."""
        with self.conn:
            self.conn.execute(
                "UPDATE recetas_consumo SET activo=0, actualizado_en=datetime('now') "
                "WHERE producto_venta_id=? AND activo=1",
                (producto_id,),
            )

    def listar_recetas(self) -> List[Dict]:
        """Lista todos los productos con receta activa."""
        rows = self.conn.execute(
            """
            SELECT rc.id, rc.nombre, rc.producto_venta_id, p.nombre AS prod_nombre,
                   COUNT(rcd.id) AS n_items
            FROM recetas_consumo rc
            JOIN productos p ON p.id = rc.producto_venta_id
            LEFT JOIN recetas_consumo_detalle rcd ON rcd.receta_id = rc.id
            WHERE rc.activo = 1
            GROUP BY rc.id
            ORDER BY p.nombre
            """
        ).fetchall()
        return [
            {
                "id":           int(r[0]),
                "nombre":       r[1],
                "producto_id":  int(r[2]),
                "producto":     r[3],
                "n_items":      int(r[4]),
            }
            for r in (rows or [])
        ]

    # ── REPORTES / CONSULTAS ──────────────────────────────────────────────────

    def stock_info_producto(self, producto_id: int) -> StockInfo:
        nombre = self._nombre_producto(producto_id)
        return StockInfo(
            producto_id=producto_id,
            nombre=nombre,
            kg_global=self.stock_global(producto_id),
            kg_sucursal=self.stock_sucursal(producto_id),
        )

    def resumen_inventario_sucursal(self) -> List[Dict]:
        """Todos los productos con stock en la sucursal activa."""
        rows = self.conn.execute(
            """
            SELECT is_.producto_id, p.nombre, is_.peso_kg,
                   is_.fecha_actualizacion
            FROM inventario_sucursal is_
            JOIN productos p ON p.id = is_.producto_id
            WHERE is_.sucursal_id = ?
            ORDER BY p.nombre
            """,
            (self.sucursal_id,),
        ).fetchall()
        return [
            {
                "producto_id": int(r[0]),
                "nombre":      r[1],
                "kg":          float(r[2]),
                "actualizado": r[3],
            }
            for r in (rows or [])
        ]

    def resumen_inventario_global(self) -> List[Dict]:
        """Stock global neto por producto."""
        rows = self.conn.execute(
            """
            SELECT ig.producto_id, p.nombre,
                   COALESCE(SUM(ig.peso_kg), 0) AS total_comprado,
                   COALESCE((
                       SELECT SUM(r.peso_kg)
                       FROM recepciones_pollo r
                       WHERE r.producto_id = ig.producto_id
                         AND r.estado = 'confirmada'
                   ), 0) AS total_distribuido
            FROM inventario_global ig
            JOIN productos p ON p.id = ig.producto_id
            GROUP BY ig.producto_id
            ORDER BY p.nombre
            """
        ).fetchall()
        return [
            {
                "producto_id":       int(r[0]),
                "nombre":            r[1],
                "kg_comprado":       float(r[2]),
                "kg_distribuido":    float(r[3]),
                "kg_disponible":     max(0.0, float(r[2]) - float(r[3])),
            }
            for r in (rows or [])
        ]

    def historial_recepciones(self, limit: int = 100) -> List[Dict]:
        rows = self.conn.execute(
            """
            SELECT r.id, r.sucursal_id, p.nombre, r.peso_kg,
                   r.costo_kg, r.proveedor, r.lote_ref,
                   r.usuario_id, r.fecha, r.estado
            FROM recepciones_pollo r
            JOIN productos p ON p.id = r.producto_id
            WHERE r.sucursal_id = ?
            ORDER BY r.fecha DESC
            LIMIT ?
            """,
            (self.sucursal_id, limit),
        ).fetchall()
        return [
            {
                "id":         int(r[0]),
                "suc_id":     int(r[1]),
                "producto":   r[2],
                "kg":         float(r[3]),
                "costo_kg":   float(r[4] or 0),
                "proveedor":  r[5] or "",
                "lote_ref":   r[6] or "",
                "usuario":    r[7],
                "fecha":      r[8],
                "estado":     r[9],
            }
            for r in (rows or [])
        ]

    def historial_traspasos(self, limit: int = 100) -> List[Dict]:
        rows = self.conn.execute(
            """
            SELECT t.id, t.sucursal_origen_id, t.sucursal_destino_id,
                   p.nombre, t.peso_kg, t.estado,
                   t.usuario_origen, t.fecha_solicitud
            FROM traspasos_pollo t
            JOIN productos p ON p.id = t.producto_id
            WHERE t.sucursal_origen_id=? OR t.sucursal_destino_id=?
            ORDER BY t.fecha_solicitud DESC
            LIMIT ?
            """,
            (self.sucursal_id, self.sucursal_id, limit),
        ).fetchall()
        return [
            {
                "id":      int(r[0]),
                "origen":  int(r[1]),
                "destino": int(r[2]),
                "producto": r[3],
                "kg":      float(r[4]),
                "estado":  r[5],
                "usuario": r[6],
                "fecha":   r[7],
            }
            for r in (rows or [])
        ]

    # ── Helpers internos ──────────────────────────────────────────────────────

    def _scalar(self, sql: str, params: tuple = ()) -> object:
        row = self.conn.execute(sql, params).fetchone()
        return row[0] if row else 0

    def _nombre_producto(self, producto_id: int) -> str:
        val = self._scalar(
            "SELECT COALESCE(nombre,'?') FROM productos WHERE id=?",
            (producto_id,),
        )
        return str(val) if val else f"Producto#{producto_id}"

    def _listar_sucursales(self) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT id, nombre FROM sucursales WHERE activa=1 ORDER BY nombre"
        ).fetchall()
        if not rows:
            rows = self.conn.execute(
                "SELECT id, nombre FROM sucursales ORDER BY nombre"
            ).fetchall()
        return [{"id": int(r[0]), "nombre": r[1]} for r in (rows or [])]

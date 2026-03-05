# core/services/inventario_operativo_engine.py
# ── INVENTARIO OPERATIVO ENGINE — SPJ Enterprise v6 ───────────────────────────
# Motor central para inventario enterprise de pollería.
# Operaciones sobre: inventario_global, inventario_sucursal,
#                    recepciones_pollo, traspasos_pollo, recetas_consumo.
#
# REGLA: NINGÚN módulo UI toca estas tablas directamente.
#        Todo pasa por este engine.
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import sqlite3

logger = logging.getLogger("spj.inventario_operativo")


# ── Excepciones ───────────────────────────────────────────────────────────────

class InventarioOperativoError(Exception):
    pass

class StockInsuficienteLocalError(InventarioOperativoError):
    def __init__(self, producto: str, disponible: float, requerido: float):
        self.producto   = producto
        self.disponible = disponible
        self.requerido  = requerido
        super().__init__(
            f"Stock insuficiente '{producto}' "
            f"(local={disponible:.3f}kg req={requerido:.3f}kg)"
        )

class RecetaInvalidaError(InventarioOperativoError):
    pass

class TraspasoError(InventarioOperativoError):
    pass


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class StockGlobal:
    producto_id:   int
    nombre:        str
    kg_total:      float
    costo_total:   float
    costo_por_kg:  float


@dataclass
class StockSucursal:
    sucursal_id:   int
    sucursal_nombre: str
    producto_id:   int
    nombre:        str
    kg_disponible: float


@dataclass
class ResultadoRecepcion:
    recepcion_id:  int
    sucursal_id:   int
    producto_id:   int
    peso_kg:       float
    stock_nuevo:   float


@dataclass
class ConsumoReceta:
    """Detalle de consumo por receta tras una venta."""
    producto_venta_id:  int
    nombre_venta:       str
    kg_vendidos:        float
    breakdown:          List[Dict]  # [{materia_prima_id, nombre, porcentaje, kg_consumidos}]


@dataclass
class ResultadoVentaOperativa:
    venta_id:   int
    sucursal_id: int
    consumos:   List[ConsumoReceta] = field(default_factory=list)
    sin_receta: List[Dict]          = field(default_factory=list)


@dataclass
class ResultadoTraspaso:
    traspaso_id:   int
    uuid:          str
    origen_nombre: str
    destino_nombre: str
    producto:      str
    peso_kg:       float
    stock_origen:  float
    stock_destino: float


# ══════════════════════════════════════════════════════════════════════════════

class InventarioOperativoEngine:
    """
    Motor de inventario operativo para pollería enterprise.

    Uso:
        eng = InventarioOperativoEngine(conexion_raw, sucursal_id=2, usuario="vendedor1")

        # Registrar compra (admin)
        eng.registrar_compra_global(producto_id=1, peso_kg=200.0, costo_total=8400.0)

        # Registrar recepción en sucursal
        eng.registrar_recepcion(producto_id=1, peso_kg=50.0)

        # Procesar venta (hook post-venta)
        eng.procesar_venta_operativa(venta_id=123, items=[...])

        # Traspaso inter-sucursal
        eng.registrar_traspaso(destino_id=2, producto_id=1, peso_kg=20.0)
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

    # ── Lectura ───────────────────────────────────────────────────────────────

    def get_stock_global(self, producto_id: int = None) -> List[StockGlobal]:
        """
        Stock global por producto (suma de todas las compras registradas).
        Si producto_id es None retorna todos.
        """
        if producto_id:
            rows = self.conn.execute(
                """
                SELECT p.id, p.nombre,
                       COALESCE(SUM(ig.peso_kg),0),
                       COALESCE(SUM(ig.costo_total),0)
                FROM productos p
                LEFT JOIN inventario_global ig ON ig.producto_id = p.id
                WHERE p.id = ? AND p.activo = 1
                GROUP BY p.id, p.nombre
                """,
                (producto_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT p.id, p.nombre,
                       COALESCE(SUM(ig.peso_kg),0),
                       COALESCE(SUM(ig.costo_total),0)
                FROM productos p
                LEFT JOIN inventario_global ig ON ig.producto_id = p.id
                WHERE p.activo = 1
                GROUP BY p.id, p.nombre
                ORDER BY p.nombre
                """,
            ).fetchall()

        result = []
        for row in rows:
            pid, nombre, kg, costo = row
            costo_por_kg = round(costo / kg, 4) if kg > 0 else 0.0
            result.append(StockGlobal(
                producto_id=pid,
                nombre=nombre,
                kg_total=round(float(kg), 4),
                costo_total=round(float(costo), 2),
                costo_por_kg=costo_por_kg,
            ))
        return result

    def get_stock_sucursal(
        self,
        sucursal_id: int = None,
        producto_id: int = None,
    ) -> List[StockSucursal]:
        """Stock local por sucursal y/o producto."""
        suc_id = sucursal_id or self.sucursal_id
        params = [suc_id]
        extra  = ""
        if producto_id:
            extra = "AND inv.producto_id = ?"
            params.append(producto_id)

        rows = self.conn.execute(
            f"""
            SELECT s.id, s.nombre,
                   inv.producto_id, p.nombre,
                   COALESCE(inv.peso_kg, 0)
            FROM sucursales s
            LEFT JOIN inventario_sucursal inv ON inv.sucursal_id = s.id
            LEFT JOIN productos p ON p.id = inv.producto_id
            WHERE s.id = ? {extra}
            ORDER BY p.nombre
            """,
            params,
        ).fetchall()

        return [
            StockSucursal(
                sucursal_id=int(r[0]),
                sucursal_nombre=r[1] or "?",
                producto_id=int(r[2] or 0),
                nombre=r[3] or "?",
                kg_disponible=round(float(r[4]), 4),
            )
            for r in rows
            if r[2]  # excluir sucursales sin inventario
        ]

    def get_todas_sucursales(self) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT id, nombre FROM sucursales WHERE activa=1 ORDER BY id"
        ).fetchall()
        return [{"id": int(r[0]), "nombre": r[1]} for r in rows]

    def get_productos_activos(self) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT id, nombre, unidad FROM productos WHERE activo=1 ORDER BY nombre"
        ).fetchall()
        return [{"id": int(r[0]), "nombre": r[1], "unidad": r[2] or "kg"} for r in rows]

    # ── Compra global (admin) ─────────────────────────────────────────────────

    def registrar_compra_global(
        self,
        producto_id:  int,
        peso_kg:      float,
        costo_total:  float = 0.0,
        proveedor:    str   = "",
        notas:        str   = "",
        compra_ref_id: int  = None,
    ) -> int:
        """
        Registra una compra global de materia prima.
        Aumenta inventario_global (tabla-log, no upsert).
        Retorna el id insertado.
        """
        if peso_kg <= 0:
            raise InventarioOperativoError("peso_kg debe ser > 0")

        costo_por_kg = round(costo_total / peso_kg, 4) if peso_kg > 0 else 0.0

        cur = self.conn.execute(
            """
            INSERT INTO inventario_global
                (producto_id, peso_kg, costo_total, costo_por_kg,
                 compra_ref_id, fecha, usuario, notas)
            VALUES (?,?,?,?,?,datetime('now'),?,?)
            """,
            (producto_id, peso_kg, costo_total, costo_por_kg,
             compra_ref_id, self.usuario, notas or ""),
        )
        ig_id = cur.lastrowid
        self.conn.commit()

        logger.info(
            "Compra global #%d: prod=%d kg=%.3f costo=%.2f usuario=%s",
            ig_id, producto_id, peso_kg, costo_total, self.usuario,
        )
        return ig_id

    # ── Recepción operativa ───────────────────────────────────────────────────

    def registrar_recepcion(
        self,
        producto_id:      int,
        peso_kg:          float,
        costo_kg:         float   = 0.0,
        proveedor:        str     = "",
        lote_ref:         str     = "",
        notas:            str     = "",
        compra_global_id: int     = None,
    ) -> ResultadoRecepcion:
        """
        Registra recepción de mercancía en la sucursal actual.
        Aumenta inventario_sucursal. NO decrementa global
        (la global es el registro de compras, la reconciliación es manual).
        """
        if peso_kg <= 0:
            raise InventarioOperativoError("peso_kg debe ser > 0")

        try:
            # INSERT recepción
            cur = self.conn.execute(
                """
                INSERT INTO recepciones_pollo
                    (sucursal_id, producto_id, peso_kg, costo_kg,
                     proveedor, lote_ref, compra_global_id,
                     usuario_id, fecha, estado, notas)
                VALUES (?,?,?,?,?,?,?,?,datetime('now'),'confirmada',?)
                """,
                (self.sucursal_id, producto_id, peso_kg, costo_kg,
                 proveedor or "", lote_ref or "", compra_global_id,
                 self.usuario, notas or ""),
            )
            rec_id = cur.lastrowid

            # UPSERT inventario_sucursal
            self.conn.execute(
                """
                INSERT INTO inventario_sucursal (sucursal_id, producto_id, peso_kg, fecha_actualizacion)
                VALUES (?,?,?,datetime('now'))
                ON CONFLICT(sucursal_id, producto_id)
                DO UPDATE SET
                    peso_kg = peso_kg + excluded.peso_kg,
                    fecha_actualizacion = datetime('now')
                """,
                (self.sucursal_id, producto_id, peso_kg),
            )

            stock_nuevo = self.conn.execute(
                "SELECT peso_kg FROM inventario_sucursal WHERE sucursal_id=? AND producto_id=?",
                (self.sucursal_id, producto_id),
            ).fetchone()
            stock_nuevo = float(stock_nuevo[0]) if stock_nuevo else peso_kg

            self.conn.commit()

            logger.info(
                "Recepción #%d suc=%d prod=%d kg=%.3f → stock=%.3f",
                rec_id, self.sucursal_id, producto_id, peso_kg, stock_nuevo,
            )
            return ResultadoRecepcion(
                recepcion_id=rec_id,
                sucursal_id=self.sucursal_id,
                producto_id=producto_id,
                peso_kg=peso_kg,
                stock_nuevo=stock_nuevo,
            )

        except sqlite3.Error as exc:
            self.conn.rollback()
            logger.error("Recepción falló: %s", exc)
            raise InventarioOperativoError(f"Error al registrar recepción: {exc}") from exc

    # ── Traspaso inter-sucursal ───────────────────────────────────────────────

    def registrar_traspaso(
        self,
        destino_id:  int,
        producto_id: int,
        peso_kg:     float,
        observaciones: str = "",
    ) -> ResultadoTraspaso:
        """
        Traspasa peso_kg del producto de la sucursal actual hacia destino.
        Valida stock origen, decrementa origen, incrementa destino.
        Estado final: 'confirmado' (directo sin aprobación pendiente).
        """
        if peso_kg <= 0:
            raise TraspasoError("peso_kg debe ser > 0")
        if destino_id == self.sucursal_id:
            raise TraspasoError("Origen y destino no pueden ser la misma sucursal")

        # Validar stock origen
        row_stock = self.conn.execute(
            "SELECT COALESCE(peso_kg,0) FROM inventario_sucursal WHERE sucursal_id=? AND producto_id=?",
            (self.sucursal_id, producto_id),
        ).fetchone()
        stock_origen = float(row_stock[0]) if row_stock else 0.0

        if stock_origen < peso_kg - 1e-6:
            nombre = self._nombre_producto(producto_id)
            raise StockInsuficienteLocalError(nombre, stock_origen, peso_kg)

        # Nombres de sucursales para el resultado
        origen_nombre  = self._nombre_sucursal(self.sucursal_id)
        destino_nombre = self._nombre_sucursal(destino_id)
        nombre_prod    = self._nombre_producto(producto_id)

        try:
            # INSERT traspaso
            cur = self.conn.execute(
                """
                INSERT INTO traspasos_pollo
                    (sucursal_origen_id, sucursal_destino_id, producto_id,
                     peso_kg, estado, usuario_origen, usuario_destino,
                     observaciones, fecha_solicitud, fecha_confirmacion)
                VALUES (?,?,?,?,'confirmado',?,?,?,datetime('now'),datetime('now'))
                """,
                (self.sucursal_id, destino_id, producto_id, peso_kg,
                 self.usuario, self.usuario, observaciones or ""),
            )
            traspaso_id = cur.lastrowid

            row_uuid = self.conn.execute(
                "SELECT uuid FROM traspasos_pollo WHERE id=?", (traspaso_id,)
            ).fetchone()
            traspaso_uuid = row_uuid[0] if row_uuid else ""

            # Decrementar origen
            self.conn.execute(
                """
                UPDATE inventario_sucursal
                SET peso_kg = MAX(0, peso_kg - ?), fecha_actualizacion=datetime('now')
                WHERE sucursal_id=? AND producto_id=?
                """,
                (peso_kg, self.sucursal_id, producto_id),
            )

            # Incrementar destino (upsert)
            self.conn.execute(
                """
                INSERT INTO inventario_sucursal (sucursal_id, producto_id, peso_kg, fecha_actualizacion)
                VALUES (?,?,?,datetime('now'))
                ON CONFLICT(sucursal_id, producto_id)
                DO UPDATE SET
                    peso_kg = peso_kg + excluded.peso_kg,
                    fecha_actualizacion = datetime('now')
                """,
                (destino_id, producto_id, peso_kg),
            )

            # Leer stocks post-traspaso
            def _stock(suc, prod):
                r = self.conn.execute(
                    "SELECT COALESCE(peso_kg,0) FROM inventario_sucursal WHERE sucursal_id=? AND producto_id=?",
                    (suc, prod),
                ).fetchone()
                return float(r[0]) if r else 0.0

            stock_origen_post  = _stock(self.sucursal_id, producto_id)
            stock_destino_post = _stock(destino_id, producto_id)

            self.conn.commit()

            logger.info(
                "Traspaso #%d %s→%s prod=%d kg=%.3f | orig=%.3f dest=%.3f",
                traspaso_id, origen_nombre, destino_nombre,
                producto_id, peso_kg, stock_origen_post, stock_destino_post,
            )
            return ResultadoTraspaso(
                traspaso_id=traspaso_id,
                uuid=traspaso_uuid,
                origen_nombre=origen_nombre,
                destino_nombre=destino_nombre,
                producto=nombre_prod,
                peso_kg=peso_kg,
                stock_origen=stock_origen_post,
                stock_destino=stock_destino_post,
            )

        except (InventarioOperativoError, TraspasoError):
            self.conn.rollback()
            raise
        except sqlite3.Error as exc:
            self.conn.rollback()
            logger.error("Traspaso falló: %s", exc)
            raise TraspasoError(f"Error al registrar traspaso: {exc}") from exc

    # ── Procesamiento de venta ────────────────────────────────────────────────

    def procesar_venta_operativa(
        self,
        venta_id:   int,
        items:      List[Dict],  # [{producto_id, cantidad, nombre}]
    ) -> ResultadoVentaOperativa:
        """
        Hook post-venta: descuenta inventario_sucursal según recetas_consumo.
        Para productos sin receta, descuenta directamente.
        NO lanza si el stock es insuficiente (best-effort, la venta ya ocurrió).
        Loguea advertencias en lugar de bloqueantes.
        """
        resultado = ResultadoVentaOperativa(
            venta_id=venta_id,
            sucursal_id=self.sucursal_id,
        )

        for item in items:
            pid     = item.get("producto_id")
            kg      = float(item.get("cantidad", 0))
            nombre  = item.get("nombre", f"Prod#{pid}")

            if not pid or kg <= 0:
                continue

            # Buscar receta de consumo (recetas_consumo_detalle)
            receta = self._get_receta_consumo(pid)

            if receta:
                consumo = ConsumoReceta(
                    producto_venta_id=pid,
                    nombre_venta=nombre,
                    kg_vendidos=kg,
                    breakdown=[],
                )
                for detalle in receta:
                    mp_id   = detalle["materia_prima_id"]
                    pct     = detalle["porcentaje"] / 100.0
                    mp_nom  = detalle["nombre_mp"]
                    kg_cons = round(kg * pct, 6)

                    if kg_cons <= 0:
                        continue

                    self._descontar_sucursal(mp_id, kg_cons, nombre)
                    consumo.breakdown.append({
                        "materia_prima_id": mp_id,
                        "nombre":           mp_nom,
                        "porcentaje":       detalle["porcentaje"],
                        "kg_consumidos":    kg_cons,
                    })
                resultado.consumos.append(consumo)

            else:
                # Sin receta: descuento directo
                self._descontar_sucursal(pid, kg, nombre)
                resultado.sin_receta.append({"producto_id": pid, "nombre": nombre, "kg": kg})

        try:
            self.conn.commit()
        except Exception as exc:
            logger.error("Commit venta operativa falló: %s", exc)

        return resultado

    # ── Recetas de consumo ────────────────────────────────────────────────────

    def get_receta(self, producto_venta_id: int) -> Optional[Dict]:
        """
        Retorna la receta activa del producto con su detalle.
        None si no existe receta activa.
        """
        row = self.conn.execute(
            """
            SELECT id, nombre, notas FROM recetas_consumo
            WHERE producto_venta_id=? AND activo=1
            """,
            (producto_venta_id,),
        ).fetchone()
        if not row:
            return None

        receta_id, nombre, notas = row[0], row[1], row[2]
        detalle = self.conn.execute(
            """
            SELECT rcd.materia_prima_id, p.nombre, rcd.porcentaje, rcd.orden
            FROM recetas_consumo_detalle rcd
            JOIN productos p ON p.id = rcd.materia_prima_id
            WHERE rcd.receta_id = ?
            ORDER BY rcd.orden, rcd.id
            """,
            (receta_id,),
        ).fetchall()

        return {
            "receta_id":         receta_id,
            "nombre":            nombre,
            "notas":             notas,
            "producto_venta_id": producto_venta_id,
            "detalle": [
                {
                    "materia_prima_id": int(r[0]),
                    "nombre_mp":        r[1],
                    "porcentaje":       float(r[2]),
                    "orden":            int(r[3] or 0),
                }
                for r in (detalle or [])
            ],
        }

    def get_todas_recetas(self) -> List[Dict]:
        rows = self.conn.execute(
            """
            SELECT rc.id, rc.producto_venta_id, p.nombre, rc.activo,
                   COUNT(rcd.id) as num_items,
                   COALESCE(SUM(rcd.porcentaje),0) as total_pct
            FROM recetas_consumo rc
            JOIN productos p ON p.id = rc.producto_venta_id
            LEFT JOIN recetas_consumo_detalle rcd ON rcd.receta_id = rc.id
            GROUP BY rc.id
            ORDER BY p.nombre
            """,
        ).fetchall()
        return [
            {
                "receta_id":         int(r[0]),
                "producto_venta_id": int(r[1]),
                "nombre_producto":   r[2],
                "activo":            bool(r[3]),
                "num_items":         int(r[4]),
                "total_pct":         round(float(r[5]), 2),
                "valida":            abs(float(r[5]) - 100.0) < 0.1,
            }
            for r in rows
        ]

    def guardar_receta(
        self,
        producto_venta_id: int,
        detalle: List[Dict],  # [{materia_prima_id, porcentaje}]
        notas: str = "",
    ) -> int:
        """
        Guarda (upsert) la receta de consumo para un producto.
        Valida que la suma de porcentajes sea 100% ±0.1%.
        Retorna el receta_id.
        """
        if not detalle:
            raise RecetaInvalidaError("La receta debe tener al menos un item")

        total_pct = sum(float(d.get("porcentaje", 0)) for d in detalle)
        if abs(total_pct - 100.0) > 0.1:
            raise RecetaInvalidaError(
                f"La suma de porcentajes debe ser 100% (actual: {total_pct:.2f}%)"
            )

        for d in detalle:
            pct = float(d.get("porcentaje", 0))
            if pct <= 0:
                raise RecetaInvalidaError("Cada porcentaje debe ser > 0")

        try:
            # Upsert receta header
            row = self.conn.execute(
                "SELECT id FROM recetas_consumo WHERE producto_venta_id=?",
                (producto_venta_id,),
            ).fetchone()

            nombre_prod = self._nombre_producto(producto_venta_id)

            if row:
                receta_id = int(row[0])
                self.conn.execute(
                    "UPDATE recetas_consumo SET activo=1, notas=?, actualizado_en=datetime('now') WHERE id=?",
                    (notas, receta_id),
                )
            else:
                cur = self.conn.execute(
                    """
                    INSERT INTO recetas_consumo
                        (producto_venta_id, nombre, activo, creado_por, notas)
                    VALUES (?,?,1,?,?)
                    """,
                    (producto_venta_id, nombre_prod, self.usuario, notas),
                )
                receta_id = cur.lastrowid

            # Borrar detalle anterior
            self.conn.execute(
                "DELETE FROM recetas_consumo_detalle WHERE receta_id=?",
                (receta_id,),
            )

            # Insertar nuevo detalle
            for orden, d in enumerate(detalle):
                mp_id = int(d["materia_prima_id"])
                pct   = float(d["porcentaje"])
                nombre_mp = self._nombre_producto(mp_id)
                self.conn.execute(
                    """
                    INSERT INTO recetas_consumo_detalle
                        (receta_id, materia_prima_id, porcentaje, nombre_mp, orden)
                    VALUES (?,?,?,?,?)
                    """,
                    (receta_id, mp_id, pct, nombre_mp, orden),
                )

            self.conn.commit()
            logger.info(
                "Receta guardada: prod=%d receta_id=%d items=%d",
                producto_venta_id, receta_id, len(detalle),
            )
            return receta_id

        except (RecetaInvalidaError,):
            self.conn.rollback()
            raise
        except sqlite3.Error as exc:
            self.conn.rollback()
            raise RecetaInvalidaError(f"Error al guardar receta: {exc}") from exc

    def eliminar_receta(self, producto_venta_id: int) -> None:
        self.conn.execute(
            "UPDATE recetas_consumo SET activo=0 WHERE producto_venta_id=?",
            (producto_venta_id,),
        )
        self.conn.commit()

    # ── Históricos ────────────────────────────────────────────────────────────

    def historial_recepciones(self, limit: int = 200) -> List[Dict]:
        rows = self.conn.execute(
            """
            SELECT rp.id, rp.fecha, rp.sucursal_id, s.nombre,
                   rp.producto_id, p.nombre, rp.peso_kg,
                   rp.costo_kg, rp.usuario_id, rp.estado
            FROM recepciones_pollo rp
            LEFT JOIN sucursales s ON s.id = rp.sucursal_id
            LEFT JOIN productos p  ON p.id = rp.producto_id
            WHERE rp.sucursal_id = ?
            ORDER BY rp.fecha DESC
            LIMIT ?
            """,
            (self.sucursal_id, limit),
        ).fetchall()
        return [
            {
                "id":           int(r[0]),
                "fecha":        r[1],
                "sucursal_id":  int(r[2]),
                "sucursal":     r[3] or "?",
                "producto_id":  int(r[4]),
                "producto":     r[5] or "?",
                "peso_kg":      round(float(r[6]), 4),
                "costo_kg":     round(float(r[7] or 0), 4),
                "usuario":      r[8],
                "estado":       r[9],
            }
            for r in rows
        ]

    def historial_traspasos(self, limit: int = 200) -> List[Dict]:
        rows = self.conn.execute(
            """
            SELECT tp.id, tp.fecha_solicitud, tp.uuid,
                   tp.sucursal_origen_id, so.nombre,
                   tp.sucursal_destino_id, sd.nombre,
                   tp.producto_id, p.nombre,
                   tp.peso_kg, tp.estado, tp.usuario_origen
            FROM traspasos_pollo tp
            LEFT JOIN sucursales so ON so.id = tp.sucursal_origen_id
            LEFT JOIN sucursales sd ON sd.id = tp.sucursal_destino_id
            LEFT JOIN productos p   ON p.id  = tp.producto_id
            WHERE tp.sucursal_origen_id=? OR tp.sucursal_destino_id=?
            ORDER BY tp.fecha_solicitud DESC
            LIMIT ?
            """,
            (self.sucursal_id, self.sucursal_id, limit),
        ).fetchall()
        return [
            {
                "id":            int(r[0]),
                "fecha":         r[1],
                "uuid":          (r[2] or "")[:8],
                "origen_id":     int(r[3]),
                "origen":        r[4] or "?",
                "destino_id":    int(r[5]),
                "destino":       r[6] or "?",
                "producto_id":   int(r[7]),
                "producto":      r[8] or "?",
                "peso_kg":       round(float(r[9]), 4),
                "estado":        r[10],
                "usuario":       r[11],
            }
            for r in rows
        ]

    # ── Privados ──────────────────────────────────────────────────────────────

    def _get_receta_consumo(self, producto_id: int) -> Optional[List[Dict]]:
        row = self.conn.execute(
            "SELECT id FROM recetas_consumo WHERE producto_venta_id=? AND activo=1",
            (producto_id,),
        ).fetchone()
        if not row:
            return None

        detalle = self.conn.execute(
            """
            SELECT rcd.materia_prima_id, rcd.nombre_mp, rcd.porcentaje
            FROM recetas_consumo_detalle rcd
            WHERE rcd.receta_id=?
            ORDER BY rcd.orden, rcd.id
            """,
            (int(row[0]),),
        ).fetchall()
        return [
            {"materia_prima_id": int(r[0]), "nombre_mp": r[1], "porcentaje": float(r[2])}
            for r in detalle
        ]

    def _descontar_sucursal(self, producto_id: int, kg: float, descripcion: str = "") -> None:
        """
        Descuenta kg de inventario_sucursal.
        Nunca deja negativo (floor en 0). Loguea advertencia si stock insuficiente.
        """
        row = self.conn.execute(
            "SELECT COALESCE(peso_kg,0) FROM inventario_sucursal WHERE sucursal_id=? AND producto_id=?",
            (self.sucursal_id, producto_id),
        ).fetchone()
        stock_actual = float(row[0]) if row else 0.0

        if stock_actual < kg - 1e-6:
            nombre = self._nombre_producto(producto_id)
            logger.warning(
                "Stock insuficiente (operativo) '%s': tiene=%.4f req=%.4f → %s",
                nombre, stock_actual, kg, descripcion,
            )

        nuevo = max(0.0, round(stock_actual - kg, 6))
        self.conn.execute(
            """
            INSERT INTO inventario_sucursal (sucursal_id, producto_id, peso_kg, fecha_actualizacion)
            VALUES (?,?,?,datetime('now'))
            ON CONFLICT(sucursal_id, producto_id)
            DO UPDATE SET
                peso_kg = MAX(0, ?) ,
                fecha_actualizacion = datetime('now')
            """,
            (self.sucursal_id, producto_id, nuevo, nuevo),
        )

    def _nombre_producto(self, producto_id: int) -> str:
        row = self.conn.execute(
            "SELECT nombre FROM productos WHERE id=?", (producto_id,)
        ).fetchone()
        return row[0] if row else f"Producto#{producto_id}"

    def _nombre_sucursal(self, sucursal_id: int) -> str:
        row = self.conn.execute(
            "SELECT nombre FROM sucursales WHERE id=?", (sucursal_id,)
        ).fetchone()
        return row[0] if row else f"Sucursal#{sucursal_id}"

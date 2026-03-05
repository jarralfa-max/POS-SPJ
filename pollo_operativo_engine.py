# core/services/pollo_operativo_engine.py
# ── POLLO OPERATIVO ENGINE — SPJ Enterprise v6 ────────────────────────────────
# Motor desacoplado para operaciones de inventario avícola.
# Maneja:
#   - Registro de compras globales (admin)
#   - Recepciones operativas (vendedor → sucursal)
#   - Consumo por venta con recetas de rendimiento
#   - Traspasos inter-sucursal
#   - Validación de existencias
# Principio: ningún módulo UI toca inventario directamente.
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import sqlite3

logger = logging.getLogger("spj.pollo_engine")


# ── Excepciones de dominio ────────────────────────────────────────────────────

class PolloError(Exception):
    pass

class StockInsuficienteError(PolloError):
    def __init__(self, producto: str, disponible: float, requerido: float):
        self.producto   = producto
        self.disponible = disponible
        self.requerido  = requerido
        super().__init__(
            f"Stock insuficiente '{producto}' (disp={disponible:.3f}kg, req={requerido:.3f}kg)"
        )

class RecetaNoValidaError(PolloError):
    pass

class TraspasoError(PolloError):
    pass


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class RecetaLine:
    materia_prima_id: int
    nombre_mp:        str
    porcentaje:       float
    kg_consumido:     float = 0.0


@dataclass
class VentaConsumoResult:
    producto_venta_id:   int
    nombre_venta:        str
    kg_vendidos:         float
    lineas:              List[RecetaLine]
    tiene_receta:        bool

    @property
    def total_kg_consumidos(self) -> float:
        return sum(l.kg_consumido for l in self.lineas)


@dataclass
class TraspasoResult:
    traspaso_id:        int
    origen:             str
    destino:            str
    producto:           str
    peso_kg:            float
    estado:             str


@dataclass
class StockProducto:
    producto_id:    int
    nombre:         str
    peso_global:    float
    peso_sucursal:  float


# ══════════════════════════════════════════════════════════════════════════════
# ENGINE PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════

class PolloOperativoEngine:
    """
    Motor operativo central de inventario avícola.

    Uso:
        eng = PolloOperativoEngine(conn, usuario="cajero1", sucursal_id=2)
        eng.registrar_compra_global(producto_id=1, peso_kg=100.0, costo_total=4800.0)
        eng.registrar_recepcion(producto_id=1, peso_kg=30.0)
        result = eng.procesar_venta(items=[{producto_id:3, cantidad:5.0}])
    """

    def __init__(
        self,
        conn:        sqlite3.Connection,
        usuario:     str = "Sistema",
        sucursal_id: int = 1,
    ):
        self.conn        = conn
        self.usuario     = usuario or "Sistema"
        self.sucursal_id = sucursal_id

    # ── 1. Compra global (admin) ──────────────────────────────────────────────

    def registrar_compra_global(
        self,
        producto_id:  int,
        peso_kg:      float,
        costo_total:  float,
        notas:        str = "",
    ) -> int:
        """
        Registra una compra y aumenta inventario_global.
        Retorna el id del registro en inventario_global.
        """
        if peso_kg <= 0:
            raise PolloError("peso_kg debe ser > 0")
        if costo_total < 0:
            raise PolloError("costo_total no puede ser negativo")

        costo_kg = costo_total / peso_kg if peso_kg > 0 else 0.0

        with self._tx():
            # Actualizar o crear inventario_global
            existing = self.conn.execute(
                "SELECT id, peso_kg, costo_total FROM inventario_global WHERE producto_id=? ORDER BY id DESC LIMIT 1",
                (producto_id,)
            ).fetchone()

            if existing:
                ig_id      = existing[0]
                new_kg     = float(existing[1]) + peso_kg
                new_costo  = float(existing[2]) + costo_total
                new_ckg    = new_costo / new_kg if new_kg > 0 else 0.0
                self.conn.execute(
                    """
                    UPDATE inventario_global
                    SET peso_kg=?, costo_total=?, costo_por_kg=?, notas=?, fecha=datetime('now')
                    WHERE id=?
                    """,
                    (new_kg, new_costo, new_ckg, notas, ig_id),
                )
            else:
                cur = self.conn.execute(
                    """
                    INSERT INTO inventario_global
                        (producto_id, peso_kg, costo_total, costo_por_kg, notas, usuario)
                    VALUES (?,?,?,?,?,?)
                    """,
                    (producto_id, peso_kg, costo_total, costo_kg, notas, self.usuario),
                )
                ig_id = cur.lastrowid

            self._log_movimiento(
                tipo="compra_global",
                producto_id=producto_id,
                sucursal_id=None,
                kg_delta=peso_kg,
                descripcion=f"Compra global — {peso_kg:.3f}kg @ ${costo_kg:.2f}/kg. {notas}",
            )

        logger.info("Compra global prod=%d kg=%.3f costo=%.2f", producto_id, peso_kg, costo_total)
        return ig_id

    # ── 2. Recepción operativa (vendedor) ─────────────────────────────────────

    def registrar_recepcion(
        self,
        producto_id: int,
        peso_kg:     float,
        costo_kg:    float = 0.0,
        proveedor:   str   = "",
        lote_ref:    str   = "",
        notas:       str   = "",
    ) -> int:
        """
        Registra recepción en sucursal:
          - Aumenta inventario_sucursal
          - Descuenta inventario_global
          - Inserta en recepciones_pollo (con proveedor y lote_ref)
        Retorna id de la recepción.
        """
        if peso_kg <= 0:
            raise PolloError("peso_kg debe ser > 0")

        # Verificar que haya stock global suficiente
        stock_global = self._get_stock_global(producto_id)
        if stock_global < peso_kg - 1e-6:
            nombre = self._nombre_producto(producto_id)
            raise StockInsuficienteError(f"{nombre} (global)", stock_global, peso_kg)

        with self._tx():
            # Insertar recepción
            cur = self.conn.execute(
                """
                INSERT INTO recepciones_pollo
                    (sucursal_id, producto_id, peso_kg, costo_kg,
                     proveedor, lote_ref, usuario_id, notas)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (self.sucursal_id, producto_id, peso_kg, costo_kg,
                 proveedor, lote_ref, self.usuario, notas),
            )
            rec_id = cur.lastrowid

            # Descontar inventario_global (actualiza la fila más reciente)
            ig_row = self.conn.execute(
                "SELECT id, peso_kg FROM inventario_global WHERE producto_id=? ORDER BY id DESC LIMIT 1",
                (producto_id,)
            ).fetchone()
            if ig_row:
                self.conn.execute(
                    "UPDATE inventario_global SET peso_kg=MAX(0,?-?), fecha=datetime('now') WHERE id=?",
                    (float(ig_row[1]), peso_kg, ig_row[0]),
                )

            # Aumentar inventario_sucursal (upsert)
            self._upsert_inv_sucursal(producto_id, +peso_kg)

            self._log_movimiento(
                tipo="recepcion",
                producto_id=producto_id,
                sucursal_id=self.sucursal_id,
                kg_delta=peso_kg,
                descripcion=f"Recepción sucursal — {peso_kg:.3f}kg",
            )

        nombre = self._nombre_producto(producto_id)
        logger.info("Recepción #%d: suc=%d prod=%s kg=%.3f",
                    rec_id, self.sucursal_id, nombre, peso_kg)
        return rec_id

    # ── 3. Consumo por venta ──────────────────────────────────────────────────

    def procesar_venta(
        self,
        items: List[Dict],  # [{producto_id, cantidad(kg), nombre?}]
    ) -> List[VentaConsumoResult]:
        """
        Descuenta inventario por cada item vendido.
        Si el producto tiene receta de consumo → descuenta materias primas.
        Si no tiene receta → descuenta el producto directamente.
        Retorna lista de VentaConsumoResult.
        """
        resultados: List[VentaConsumoResult] = []

        with self._tx():
            for item in items:
                pid      = item["producto_id"]
                cantidad = float(item.get("cantidad", 0))
                nombre   = item.get("nombre") or self._nombre_producto(pid)

                if cantidad <= 0:
                    continue

                receta = self._get_receta(pid)

                if receta:
                    lineas = self._consumir_por_receta(pid, cantidad, receta)
                    resultados.append(VentaConsumoResult(
                        producto_venta_id=pid,
                        nombre_venta=nombre,
                        kg_vendidos=cantidad,
                        lineas=lineas,
                        tiene_receta=True,
                    ))
                else:
                    # Descuento directo
                    self._descontar_sucursal(pid, cantidad, f"Venta {nombre}")
                    resultados.append(VentaConsumoResult(
                        producto_venta_id=pid,
                        nombre_venta=nombre,
                        kg_vendidos=cantidad,
                        lineas=[],
                        tiene_receta=False,
                    ))

        return resultados

    def _consumir_por_receta(
        self,
        producto_venta_id: int,
        kg_vendidos:       float,
        receta:            List[dict],
    ) -> List[RecetaLine]:
        """Consume materias primas proporcionalmente según la receta."""
        lineas: List[RecetaLine] = []
        for row in receta:
            mp_id   = row["materia_prima_id"]
            nombre  = row["nombre_mp"] or self._nombre_producto(mp_id)
            pct     = float(row["porcentaje"])
            kg_mp   = round(kg_vendidos * (pct / 100.0), 6)
            if kg_mp <= 0:
                continue
            self._descontar_sucursal(mp_id, kg_mp,
                f"Venta receta {self._nombre_producto(producto_venta_id)}")
            lineas.append(RecetaLine(
                materia_prima_id=mp_id,
                nombre_mp=nombre,
                porcentaje=pct,
                kg_consumido=kg_mp,
            ))
        return lineas

    def _descontar_sucursal(
        self,
        producto_id: int,
        kg:          float,
        descripcion: str = "",
    ) -> None:
        """Descuenta stock de inventario_sucursal. Lanza StockInsuficienteError si no alcanza."""
        stock = self._get_stock_sucursal(producto_id)
        if stock < kg - 1e-6:
            nombre = self._nombre_producto(producto_id)
            raise StockInsuficienteError(nombre, stock, kg)

        self.conn.execute(
            """
            UPDATE inventario_sucursal
            SET peso_kg = MAX(0, peso_kg - ?),
                fecha_actualizacion = datetime('now')
            WHERE sucursal_id=? AND producto_id=?
            """,
            (kg, self.sucursal_id, producto_id),
        )
        self._log_movimiento(
            tipo="salida_venta",
            producto_id=producto_id,
            sucursal_id=self.sucursal_id,
            kg_delta=-kg,
            descripcion=descripcion,
        )

    # ── 4. Traspaso inter-sucursal ────────────────────────────────────────────

    def registrar_traspaso(
        self,
        sucursal_destino_id: int,
        producto_id:         int,
        peso_kg:             float,
        observaciones:       str = "",
    ) -> TraspasoResult:
        """
        Crea un traspaso confirmado:
          - Descuenta inventario_sucursal en origen
          - Aumenta inventario_sucursal en destino
          - Registra en traspasos_pollo
        """
        if peso_kg <= 0:
            raise TraspasoError("peso_kg debe ser > 0")
        if sucursal_destino_id == self.sucursal_id:
            raise TraspasoError("Origen y destino no pueden ser iguales")

        stock_origen = self._get_stock_sucursal(producto_id)
        if stock_origen < peso_kg - 1e-6:
            nombre = self._nombre_producto(producto_id)
            raise StockInsuficienteError(nombre, stock_origen, peso_kg)

        with self._tx():
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

            # Descontar origen
            self._upsert_inv_sucursal(producto_id, -peso_kg)

            # Aumentar destino
            self._upsert_inv_sucursal(producto_id, +peso_kg,
                                      sucursal_id=sucursal_destino_id)

            self._log_movimiento(
                tipo="traspaso_salida",
                producto_id=producto_id,
                sucursal_id=self.sucursal_id,
                kg_delta=-peso_kg,
                descripcion=f"Traspaso #{traspaso_id} → suc#{sucursal_destino_id}",
            )
            self._log_movimiento(
                tipo="traspaso_entrada",
                producto_id=producto_id,
                sucursal_id=sucursal_destino_id,
                kg_delta=+peso_kg,
                descripcion=f"Traspaso #{traspaso_id} desde suc#{self.sucursal_id}",
            )

        # Obtener nombres para el resultado
        orig_nombre = self._nombre_sucursal(self.sucursal_id)
        dest_nombre = self._nombre_sucursal(sucursal_destino_id)
        prod_nombre = self._nombre_producto(producto_id)

        logger.info(
            "Traspaso #%d: suc%d→suc%d prod=%s kg=%.3f",
            traspaso_id, self.sucursal_id, sucursal_destino_id, prod_nombre, peso_kg,
        )
        return TraspasoResult(
            traspaso_id=traspaso_id,
            origen=orig_nombre,
            destino=dest_nombre,
            producto=prod_nombre,
            peso_kg=peso_kg,
            estado="confirmado",
        )

    # ── 5. Gestión de recetas de consumo ─────────────────────────────────────

    def guardar_receta(
        self,
        producto_venta_id: int,
        nombre:            str,
        lineas:            List[Dict],  # [{materia_prima_id, porcentaje, nombre_mp?}]
    ) -> int:
        """
        Guarda o actualiza la receta de consumo de un producto.
        lineas debe sumar 100% ± 0.1%.
        Retorna el receta_id.
        """
        total_pct = sum(float(l["porcentaje"]) for l in lineas)
        if abs(total_pct - 100.0) > 0.5:
            raise RecetaNoValidaError(
                f"Los porcentajes suman {total_pct:.2f}% — deben sumar 100%"
            )
        if not lineas:
            raise RecetaNoValidaError("La receta debe tener al menos un ingrediente")

        with self._tx():
            # Upsert receta cabecera
            self.conn.execute(
                """
                INSERT INTO recetas_consumo (producto_venta_id, nombre, activo, creado_por)
                VALUES (?,?,1,?)
                ON CONFLICT(producto_venta_id) DO UPDATE SET
                    nombre=excluded.nombre,
                    activo=1,
                    actualizado_en=datetime('now')
                """,
                (producto_venta_id, nombre, self.usuario),
            )
            row = self.conn.execute(
                "SELECT id FROM recetas_consumo WHERE producto_venta_id=?",
                (producto_venta_id,)
            ).fetchone()
            receta_id = row[0]

            # Reemplazar detalle
            self.conn.execute(
                "DELETE FROM recetas_consumo_detalle WHERE receta_id=?",
                (receta_id,)
            )
            for l in lineas:
                mp_nombre = l.get("nombre_mp") or self._nombre_producto(
                    int(l["materia_prima_id"])
                )
                self.conn.execute(
                    """
                    INSERT INTO recetas_consumo_detalle
                        (receta_id, materia_prima_id, porcentaje, nombre_mp)
                    VALUES (?,?,?,?)
                    """,
                    (receta_id, l["materia_prima_id"],
                     float(l["porcentaje"]), mp_nombre),
                )

        logger.info(
            "Receta guardada: id=%d prod=%d '%s' lineas=%d",
            receta_id, producto_venta_id, nombre, len(lineas),
        )
        return receta_id

    def eliminar_receta(self, producto_venta_id: int) -> None:
        """Desactiva la receta (soft delete)."""
        self.conn.execute(
            "UPDATE recetas_consumo SET activo=0 WHERE producto_venta_id=?",
            (producto_venta_id,)
        )
        self.conn.commit()

    # ── 6. Validaciones ───────────────────────────────────────────────────────

    def validar_existencia_sucursal(self, producto_id: int, kg: float) -> bool:
        return self._get_stock_sucursal(producto_id) >= kg - 1e-6

    def validar_existencia_global(self, producto_id: int, kg: float) -> bool:
        return self._get_stock_global(producto_id) >= kg - 1e-6

    # ── 7. Consultas ──────────────────────────────────────────────────────────

    def stock_global(self) -> List[StockProducto]:
        rows = self.conn.execute(
            """
            SELECT ig.producto_id, p.nombre,
                   COALESCE(SUM(ig.peso_kg), 0) AS peso_global,
                   COALESCE(is2.peso_kg, 0) AS peso_local
            FROM inventario_global ig
            JOIN productos p ON p.id = ig.producto_id
            LEFT JOIN inventario_sucursal is2
                ON is2.producto_id = ig.producto_id
               AND is2.sucursal_id = ?
            GROUP BY ig.producto_id, p.nombre, is2.peso_kg
            ORDER BY p.nombre
            """,
            (self.sucursal_id,)
        ).fetchall()
        return [
            StockProducto(
                producto_id=r[0], nombre=r[1],
                peso_global=float(r[2]), peso_sucursal=float(r[3])
            )
            for r in rows
        ]

    def stock_sucursal(self, sucursal_id: int = None) -> List[StockProducto]:
        sid = sucursal_id or self.sucursal_id
        rows = self.conn.execute(
            """
            SELECT is2.producto_id, p.nombre,
                   COALESCE(ig.peso_kg, 0),
                   COALESCE(is2.peso_kg, 0)
            FROM inventario_sucursal is2
            JOIN productos p ON p.id = is2.producto_id
            LEFT JOIN inventario_global ig ON ig.producto_id = is2.producto_id
            WHERE is2.sucursal_id = ?
            ORDER BY p.nombre
            """,
            (sid,)
        ).fetchall()
        return [
            StockProducto(
                producto_id=r[0], nombre=r[1],
                peso_global=float(r[2]), peso_sucursal=float(r[3])
            )
            for r in rows
        ]

    def recepciones_recientes(self, limite: int = 50) -> List[dict]:
        rows = self.conn.execute(
            """
            SELECT rp.id, rp.fecha, p.nombre, rp.peso_kg, rp.costo_kg,
                   rp.usuario_id, rp.notas, rp.estado,
                   COALESCE(s.nombre, CAST(rp.sucursal_id AS TEXT)),
                   COALESCE(rp.proveedor, ''),
                   COALESCE(rp.lote_ref, '')
            FROM recepciones_pollo rp
            JOIN productos p ON p.id = rp.producto_id
            LEFT JOIN sucursales s ON s.id = rp.sucursal_id
            WHERE rp.sucursal_id = ?
            ORDER BY rp.fecha DESC
            LIMIT ?
            """,
            (self.sucursal_id, limite)
        ).fetchall()
        return [
            {
                "id":        r[0], "fecha":    r[1], "producto":  r[2],
                "peso_kg":   float(r[3] or 0),
                "costo_kg":  float(r[4] or 0),
                "usuario":   r[5], "notas":     r[6] or "",
                "estado":    r[7], "sucursal":  r[8],
                "proveedor": r[9] or "", "lote_ref": r[10] or "",
            }
            for r in (rows or [])
        ]

    def traspasos_recientes(self, limite: int = 50) -> List[dict]:
        rows = self.conn.execute(
            """
            SELECT tp.id, tp.fecha_solicitud, p.nombre, tp.peso_kg,
                   so.nombre, sd.nombre, tp.estado, tp.usuario_origen
            FROM traspasos_pollo tp
            JOIN productos p ON p.id = tp.producto_id
            LEFT JOIN sucursales so ON so.id = tp.sucursal_origen_id
            LEFT JOIN sucursales sd ON sd.id = tp.sucursal_destino_id
            WHERE tp.sucursal_origen_id=? OR tp.sucursal_destino_id=?
            ORDER BY tp.fecha_solicitud DESC
            LIMIT ?
            """,
            (self.sucursal_id, self.sucursal_id, limite)
        ).fetchall()
        return [
            {
                "id":       r[0], "fecha":   r[1], "producto": r[2],
                "peso_kg":  r[3], "origen":  r[4], "destino":  r[5],
                "estado":   r[6], "usuario": r[7],
            }
            for r in rows
        ]

    def recetas_activas(self) -> List[dict]:
        rows = self.conn.execute(
            """
            SELECT rc.id, p.nombre, rc.nombre, rc.actualizado_en,
                   COUNT(rcd.id) as n_lineas
            FROM recetas_consumo rc
            JOIN productos p ON p.id = rc.producto_venta_id
            LEFT JOIN recetas_consumo_detalle rcd ON rcd.receta_id = rc.id
            WHERE rc.activo=1
            GROUP BY rc.id
            ORDER BY p.nombre
            """
        ).fetchall()
        return [
            {
                "receta_id":  r[0], "producto": r[1], "nombre": r[2],
                "actualizado": r[3], "n_lineas": r[4],
            }
            for r in rows
        ]

    def detalle_receta(self, producto_venta_id: int) -> Optional[dict]:
        """Retorna la receta activa con su detalle, o None."""
        row = self.conn.execute(
            """
            SELECT rc.id, rc.nombre, rc.activo
            FROM recetas_consumo rc
            WHERE rc.producto_venta_id=? AND rc.activo=1
            """,
            (producto_venta_id,)
        ).fetchone()
        if not row:
            return None
        receta_id = row[0]
        lineas = self.conn.execute(
            """
            SELECT rcd.materia_prima_id, rcd.nombre_mp, rcd.porcentaje, p.nombre
            FROM recetas_consumo_detalle rcd
            JOIN productos p ON p.id = rcd.materia_prima_id
            WHERE rcd.receta_id=?
            ORDER BY rcd.orden, rcd.id
            """,
            (receta_id,)
        ).fetchall()
        return {
            "receta_id":   receta_id,
            "nombre":      row[1],
            "activo":      bool(row[2]),
            "lineas": [
                {
                    "materia_prima_id": l[0],
                    "nombre_mp":        l[1] or l[3],
                    "porcentaje":       l[2],
                }
                for l in lineas
            ],
        }

    def productos_activos(self) -> List[dict]:
        rows = self.conn.execute(
            "SELECT id, nombre, unidad_medida FROM productos WHERE activo=1 ORDER BY nombre"
        ).fetchall()
        return [{"id": r[0], "nombre": r[1], "unidad": r[2] or "kg"} for r in rows]

    def sucursales_activas(self) -> List[dict]:
        try:
            rows = self.conn.execute(
                "SELECT id, nombre FROM sucursales WHERE activa=1 ORDER BY id"
            ).fetchall()
            return [{"id": r[0], "nombre": r[1]} for r in rows]
        except Exception:
            return [{"id": 1, "nombre": "Principal"}]

    # ── Helpers internos ──────────────────────────────────────────────────────

    def _get_stock_global(self, producto_id: int) -> float:
        """Suma del peso disponible en inventario_global (puede haber múltiples filas)."""
        row = self.conn.execute(
            "SELECT COALESCE(SUM(peso_kg),0) FROM inventario_global WHERE producto_id=?",
            (producto_id,)
        ).fetchone()
        return float(row[0]) if row else 0.0

    def _get_stock_sucursal(self, producto_id: int, sucursal_id: int = None) -> float:
        sid = sucursal_id or self.sucursal_id
        row = self.conn.execute(
            """
            SELECT COALESCE(peso_kg,0) FROM inventario_sucursal
            WHERE sucursal_id=? AND producto_id=?
            """,
            (sid, producto_id)
        ).fetchone()
        return float(row[0]) if row else 0.0

    def _upsert_inv_sucursal(
        self,
        producto_id: int,
        delta_kg:    float,
        sucursal_id: int = None,
    ) -> None:
        sid = sucursal_id or self.sucursal_id
        existing = self.conn.execute(
            "SELECT id, peso_kg FROM inventario_sucursal WHERE sucursal_id=? AND producto_id=?",
            (sid, producto_id)
        ).fetchone()
        if existing:
            new_kg = max(0.0, float(existing[1]) + delta_kg)
            self.conn.execute(
                "UPDATE inventario_sucursal SET peso_kg=?, fecha_actualizacion=datetime('now') WHERE id=?",
                (new_kg, existing[0])
            )
        else:
            new_kg = max(0.0, delta_kg)
            self.conn.execute(
                "INSERT INTO inventario_sucursal (sucursal_id, producto_id, peso_kg) VALUES (?,?,?)",
                (sid, producto_id, new_kg)
            )

    def _get_receta(self, producto_id: int) -> Optional[List[dict]]:
        """Retorna las líneas de receta activa o None."""
        row = self.conn.execute(
            "SELECT id FROM recetas_consumo WHERE producto_venta_id=? AND activo=1",
            (producto_id,)
        ).fetchone()
        if not row:
            return None
        lineas = self.conn.execute(
            """
            SELECT materia_prima_id, nombre_mp, porcentaje
            FROM recetas_consumo_detalle
            WHERE receta_id=?
            ORDER BY orden, id
            """,
            (row[0],)
        ).fetchall()
        return [
            {"materia_prima_id": l[0], "nombre_mp": l[1], "porcentaje": float(l[2])}
            for l in lineas
        ]

    def _nombre_producto(self, producto_id: int) -> str:
        row = self.conn.execute(
            "SELECT COALESCE(nombre,'?') FROM productos WHERE id=?",
            (producto_id,)
        ).fetchone()
        return row[0] if row else f"Producto#{producto_id}"

    def _nombre_sucursal(self, sucursal_id: int) -> str:
        try:
            row = self.conn.execute(
                "SELECT COALESCE(nombre,'?') FROM sucursales WHERE id=?",
                (sucursal_id,)
            ).fetchone()
            return row[0] if row else f"Sucursal#{sucursal_id}"
        except Exception:
            return f"Sucursal#{sucursal_id}"

    def _log_movimiento(
        self,
        tipo:        str,
        producto_id: int,
        sucursal_id: Optional[int],
        kg_delta:    float,
        descripcion: str = "",
    ) -> None:
        """Registra en movimientos_inventario para trazabilidad."""
        try:
            self.conn.execute(
                """
                INSERT INTO movimientos_inventario
                    (producto_id, tipo, tipo_movimiento, cantidad,
                     descripcion, usuario, sucursal_id, fecha)
                VALUES (?,?,?,?,?,?,?,datetime('now'))
                """,
                (
                    producto_id,
                    tipo, tipo,
                    abs(kg_delta),
                    descripcion,
                    self.usuario,
                    sucursal_id or 0,
                ),
            )
        except Exception as exc:
            logger.warning("_log_movimiento falló (no crítico): %s", exc)

    def _tx(self):
        """Context manager transaccional compatible con sqlite3 raw."""
        from contextlib import contextmanager

        @contextmanager
        def _ctx():
            try:
                yield self.conn
                self.conn.commit()
            except Exception:
                try:
                    self.conn.rollback()
                except Exception:
                    pass
                raise

        return _ctx()

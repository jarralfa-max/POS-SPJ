# core/services/distribution_engine.py
# ── DISTRIBUTION ENGINE — SPJ Enterprise v9 ───────────────────────────────────
# Motor de distribución de inventario: recepciones, traspasos, ajustes,
# conciliación global vs local.
#
# FLUJO:
#   Inventario Global → [DistributionEngine] → Inventario Local por Sucursal
#
# OPERACIONES:
#   1. registrar_traspaso()      → solicitud de traspaso entre sucursales
#   2. confirmar_traspaso()      → recepción en sucursal destino
#   3. registrar_ajuste()        → ajuste manual de inventario (merma, corrección)
#   4. confirmar_recepcion_directa() → recepción sin traspaso previo (desde compra)
#   5. conciliar()               → calcula diferencias global vs suma local
#
# REGLA: Toda modificación de stock pasa por InventoryEngine.
from __future__ import annotations

import logging
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict

logger = logging.getLogger("spj.distribution_engine")


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class TraspasoResult:
    traspaso_id:       int
    op_uuid:           str
    producto_id:       int
    cantidad:          float
    sucursal_origen:   int
    sucursal_destino:  int
    estado:            str   # pendiente | recibido | cancelado


@dataclass
class RecepcionResult:
    traspaso_id:       int
    producto_id:       int
    cantidad:          float
    sucursal_destino:  int
    batch_ids:         List[int]   # BIBs afectados


@dataclass
class AjusteResult:
    movimiento_id:    int
    producto_id:      int
    cantidad:         float   # positivo=ENTRADA, negativo=SALIDA
    existencia_antes: float
    existencia_ahora: float
    motivo:           str


@dataclass
class ConciliacionResult:
    sucursal_id:    int
    fecha:          str
    global_stock:   float
    local_stock:    float
    diferencia:     float       # global - local
    productos_diff: List[dict]  # productos con diferencia individual
    alerta:         bool        # True si |diferencia| > umbral


# ── Engine ────────────────────────────────────────────────────────────────────

class DistributionEngine:
    """
    Motor de distribución de inventario.

    Instanciar con la conexión sqlite3 raw y contexto:
        eng = DistributionEngine(conn, sucursal_id=1, usuario="admin")

    Para operaciones de traspaso que requieren FIFO, crea internamente
    una instancia de InventoryEngine con el wrapper de Connection.
    """

    # Umbral de diferencia (kg) que dispara alerta de conciliación.
    # Configurable desde loyalty_config tabla en el futuro.
    UMBRAL_CONCILIACION_KG: float = 1.0

    def __init__(
        self,
        conn:        sqlite3.Connection,
        sucursal_id: int = 1,
        usuario:     str = "Sistema",
    ) -> None:
        self.conn        = conn
        self.sucursal_id = sucursal_id
        self.usuario     = usuario

    # ── Traspasos ─────────────────────────────────────────────────────────────

    def registrar_traspaso(
        self,
        producto_id:       int,
        cantidad:          float,
        sucursal_destino:  int,
        observaciones:     str = "",
        usuario_destino:   str = "",
    ) -> TraspasoResult:
        """
        Inicia un traspaso de inventario desde self.sucursal_id hacia sucursal_destino.

        El stock se descuenta inmediatamente del origen (FIFO).
        El destino lo recibe cuando confirma con confirmar_traspaso().

        Raises:
            StockInsuficienteError: si el origen no tiene suficiente stock.
            ValueError: si producto o sucursal no existen.
        """
        if cantidad <= 0:
            raise ValueError("cantidad debe ser > 0")
        if sucursal_destino == self.sucursal_id:
            raise ValueError("sucursal_destino no puede ser igual a origen")

        # Verificar existencia del producto
        row = self.conn.execute(
            "SELECT nombre FROM productos WHERE id=?", (producto_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"Producto id={producto_id} no encontrado")

        # InventoryEngine maneja FIFO y crea el traspaso
        from core.database import Connection
        from core.services.inventory_engine import InventoryEngine

        inv = InventoryEngine(
            Connection(self.conn),
            usuario=self.usuario,
            branch_id=self.sucursal_id,
        )
        traspaso_id = inv.transferir_entre_sucursales(
            producto_id=producto_id,
            cantidad=cantidad,
            sucursal_destino=sucursal_destino,
            usuario_destino=usuario_destino,
            observaciones=observaciones,
        )

        logger.info(
            "Traspaso #%d iniciado: prod=%d %.3f unidades %d→%d",
            traspaso_id, producto_id, cantidad,
            self.sucursal_id, sucursal_destino,
        )

        result = TraspasoResult(
            traspaso_id=traspaso_id,
            op_uuid=str(uuid.uuid4()),
            producto_id=producto_id,
            cantidad=cantidad,
            sucursal_origen=self.sucursal_id,
            sucursal_destino=sucursal_destino,
            estado="pendiente",
        )
        self._publicar_traspaso_iniciado(result)
        return result

    def confirmar_traspaso(
        self,
        traspaso_id:    int,
        observaciones:  str = "",
    ) -> RecepcionResult:
        """
        Confirma recepción de un traspaso en la sucursal destino.

        1. Verifica que el traspaso existe y está pendiente.
        2. Suma el inventario al destino (nuevo BIB con costo del origen).
        3. Actualiza traspaso a estado='recibido'.
        4. Registra movimiento de entrada.
        5. Publica RECEPCION_CONFIRMADA.

        La instancia debe ser de la sucursal DESTINO:
            eng = DistributionEngine(conn, sucursal_id=destino_id)
            eng.confirmar_traspaso(traspaso_id)
        """
        row = self.conn.execute(
            """
            SELECT t.id, t.sucursal_destino_id, t.producto_id, t.cantidad,
                   t.estado, t.sucursal_origen_id
            FROM traspasos_inventario t
            WHERE t.id = ?
            """,
            (traspaso_id,),
        ).fetchone()

        if not row:
            raise ValueError(f"Traspaso id={traspaso_id} no encontrado")
        if row[4] != "pendiente":
            raise ValueError(
                f"Traspaso #{traspaso_id} no está pendiente (estado={row[4]})"
            )

        _tid, sucursal_destino, producto_id, cantidad, _estado, sucursal_origen = (
            int(row[0]), int(row[1]), int(row[2]),
            float(row[3]), str(row[4]), int(row[5]),
        )

        # Obtener costo unitario del lote original para mantener trazabilidad
        costo_unitario = self._obtener_costo_origen(producto_id, sucursal_origen)

        op_uuid = str(uuid.uuid4())

        with self.conn:
            # Buscar/crear BIB de destino para este producto
            bib_row = self.conn.execute(
                """
                SELECT id, cantidad_disponible
                FROM branch_inventory_batches
                WHERE branch_id = ? AND producto_id = ?
                  AND es_derivado = 0
                ORDER BY fecha_entrada ASC
                LIMIT 1
                """,
                (sucursal_destino, producto_id),
            ).fetchone()

            if bib_row:
                # Sumar al BIB más antiguo del destino
                bib_id = bib_row[0]
                antes  = float(bib_row[1])
                despues = round(antes + cantidad, 6)
                self.conn.execute(
                    "UPDATE branch_inventory_batches "
                    "SET cantidad_disponible=?, fecha_actualizacion=datetime('now') "
                    "WHERE id=?",
                    (despues, bib_id),
                )
                batch_id = self.conn.execute(
                    "SELECT batch_id FROM branch_inventory_batches WHERE id=?", (bib_id,)
                ).fetchone()[0]
            else:
                # Crear BIB nuevo para este producto en destino
                # Primero necesitamos un chicken_batch de referencia o creamos uno
                batch_id = self._crear_batch_traspaso(
                    producto_id=producto_id,
                    sucursal_id=sucursal_destino,
                    cantidad=cantidad,
                    costo_unitario=costo_unitario,
                    traspaso_id=traspaso_id,
                )
                self.conn.execute(
                    """
                    INSERT INTO branch_inventory_batches
                        (batch_id, branch_id, producto_id,
                         cantidad_original, cantidad_disponible,
                         costo_unitario, es_derivado)
                    VALUES (?,?,?,?,?,?,1)
                    """,
                    (batch_id, sucursal_destino, producto_id,
                     cantidad, cantidad, costo_unitario),
                )
                bib_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                antes  = 0.0
                despues = cantidad

            # Registrar movimiento de entrada
            self.conn.execute(
                """
                INSERT INTO movimientos_inventario
                    (producto_id, tipo, tipo_movimiento, cantidad,
                     existencia_anterior, existencia_nueva,
                     costo_unitario, descripcion, usuario, sucursal_id,
                     referencia_id, referencia_tipo, uuid, fecha)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                """,
                (
                    producto_id, "ENTRADA", "recepcion_traspaso", cantidad,
                    antes, despues,
                    costo_unitario,
                    f"Recepción traspaso #{traspaso_id} desde sucursal {sucursal_origen}",
                    self.usuario, sucursal_destino,
                    traspaso_id, "traspaso", op_uuid,
                ),
            )

            # Actualizar existencia en tabla productos (retrocompat UI)
            self._sync_existencia(producto_id, sucursal_destino)

            # Actualizar estado traspaso
            self.conn.execute(
                """
                UPDATE traspasos_inventario
                SET estado='recibido',
                    fecha_recepcion=datetime('now'),
                    usuario_destino=?,
                    observaciones=COALESCE(observaciones||' | ','') || ?
                WHERE id=?
                """,
                (self.usuario, observaciones or "Recepción confirmada", traspaso_id),
            )

        logger.info(
            "Traspaso #%d confirmado: prod=%d %.3f unidades → sucursal %d",
            traspaso_id, producto_id, cantidad, sucursal_destino,
        )

        result = RecepcionResult(
            traspaso_id=traspaso_id,
            producto_id=producto_id,
            cantidad=cantidad,
            sucursal_destino=sucursal_destino,
            batch_ids=[bib_id],
        )
        self._publicar_recepcion_confirmada(result)
        return result

    def cancelar_traspaso(
        self,
        traspaso_id: int,
        motivo:      str = "",
    ) -> None:
        """
        Cancela un traspaso pendiente y devuelve el stock al origen.
        """
        row = self.conn.execute(
            "SELECT sucursal_origen_id, producto_id, cantidad, estado "
            "FROM traspasos_inventario WHERE id=?",
            (traspaso_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"Traspaso #{traspaso_id} no encontrado")
        if row[3] != "pendiente":
            raise ValueError(f"Traspaso #{traspaso_id} no está pendiente (estado={row[3]})")

        sucursal_origen, producto_id, cantidad = int(row[0]), int(row[1]), float(row[2])

        with self.conn:
            # Devolver stock al origen
            self._sumar_existencia_directo(producto_id, sucursal_origen, cantidad)
            self.conn.execute(
                "INSERT INTO movimientos_inventario "
                "(producto_id, tipo, tipo_movimiento, cantidad, descripcion, "
                " usuario, sucursal_id, referencia_id, referencia_tipo, uuid, fecha) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now'))",
                (
                    producto_id, "ENTRADA", "cancelacion_traspaso", cantidad,
                    f"Cancelación traspaso #{traspaso_id}: {motivo}",
                    self.usuario, sucursal_origen, traspaso_id, "traspaso",
                    str(uuid.uuid4()),
                ),
            )
            self.conn.execute(
                "UPDATE traspasos_inventario "
                "SET estado='cancelado', observaciones=? WHERE id=?",
                (f"CANCELADO: {motivo}", traspaso_id),
            )

        logger.info("Traspaso #%d cancelado. Stock devuelto a sucursal %d", traspaso_id, sucursal_origen)

    # ── Ajustes de inventario ─────────────────────────────────────────────────

    def registrar_ajuste(
        self,
        producto_id: int,
        cantidad:    float,
        motivo:      str,
        sucursal_id: Optional[int] = None,
        tipo:        str            = "AJUSTE",  # AJUSTE | MERMA | DEVOLUCION
    ) -> AjusteResult:
        """
        Registra un ajuste manual de inventario.
        cantidad positivo = ENTRADA (devolución, corrección+)
        cantidad negativo = SALIDA  (merma, robo, corrección-)

        Raises:
            ValueError: si no hay stock suficiente para ajuste negativo.
        """
        if cantidad == 0:
            raise ValueError("cantidad de ajuste no puede ser 0")

        branch = sucursal_id or self.sucursal_id

        # Stock actual
        existencia_antes = self._get_existencia(producto_id, branch)
        existencia_nueva = round(existencia_antes + cantidad, 6)

        if existencia_nueva < 0:
            raise ValueError(
                f"Ajuste dejaría stock negativo: actual={existencia_antes:.3f} "
                f"ajuste={cantidad:.3f}"
            )

        tipo_mov = "entrada_ajuste" if cantidad > 0 else "salida_ajuste"
        tipo_main = "ENTRADA" if cantidad > 0 else "SALIDA"

        op_uuid = str(uuid.uuid4())

        with self.conn:
            # Actualizar existencia directamente en productos (simplificado para ajustes)
            self.conn.execute(
                "UPDATE productos SET existencia = existencia + ? WHERE id=?",
                (cantidad, producto_id),
            )
            # Movimiento de auditoría
            self.conn.execute(
                """
                INSERT INTO movimientos_inventario
                    (producto_id, tipo, tipo_movimiento, cantidad,
                     existencia_anterior, existencia_nueva,
                     descripcion, usuario, sucursal_id, uuid, fecha)
                VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now'))
                """,
                (
                    producto_id, tipo_main, tipo_mov, abs(cantidad),
                    existencia_antes, existencia_nueva,
                    f"{tipo}: {motivo}", self.usuario, branch, op_uuid,
                ),
            )
            mov_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        logger.info(
            "Ajuste inventario: prod=%d %.3f → %.3f (delta=%.3f) motivo=%s",
            producto_id, existencia_antes, existencia_nueva, cantidad, motivo,
        )

        self._publicar_ajuste(producto_id, cantidad)
        return AjusteResult(
            movimiento_id=mov_id,
            producto_id=producto_id,
            cantidad=cantidad,
            existencia_antes=existencia_antes,
            existencia_ahora=existencia_nueva,
            motivo=motivo,
        )

    # ── Conciliación ─────────────────────────────────────────────────────────

    def conciliar(
        self,
        sucursal_id:    Optional[int]   = None,
        umbral_kg:      Optional[float] = None,
        solo_alertas:   bool            = False,
    ) -> ConciliacionResult:
        """
        Compara el stock global (chicken_batches) contra el stock local
        de la sucursal y detecta diferencias.

        Si la diferencia supera el umbral, publica CONCILIACION_DIFERENCIA.

        Args:
            sucursal_id:  Sucursal a conciliar (default self.sucursal_id).
            umbral_kg:    Umbral de alerta en unidades (default UMBRAL_CONCILIACION_KG).
            solo_alertas: Si True, solo retorna resultado si hay diferencia.
        """
        branch = sucursal_id or self.sucursal_id
        umbral = umbral_kg if umbral_kg is not None else self.UMBRAL_CONCILIACION_KG

        # Stock global: suma de todos los BIB disponibles de todos los productos
        # para esta sucursal (lo que debería tener)
        global_rows = self.conn.execute(
            """
            SELECT bib.producto_id, p.nombre,
                   SUM(bib.cantidad_disponible) as stock_bib,
                   COALESCE(p.existencia, 0) as stock_prod
            FROM branch_inventory_batches bib
            JOIN productos p ON p.id = bib.producto_id
            WHERE bib.branch_id = ?
            GROUP BY bib.producto_id
            """,
            (branch,),
        ).fetchall()

        productos_diff = []
        total_global   = 0.0
        total_local    = 0.0

        for row in global_rows:
            prod_id    = row[0]
            prod_nombre= row[1]
            stock_bib  = float(row[2] or 0)
            stock_prod = float(row[3] or 0)
            diff       = round(stock_bib - stock_prod, 6)

            total_global += stock_bib
            total_local  += stock_prod

            if abs(diff) > 0.001:   # diferencia mínima para reportar
                productos_diff.append({
                    "producto_id":    prod_id,
                    "nombre":         prod_nombre,
                    "stock_bib":      stock_bib,
                    "stock_producto": stock_prod,
                    "diferencia":     diff,
                    "alerta":         abs(diff) >= umbral,
                })

        total_diff = round(total_global - total_local, 6)
        hay_alerta = abs(total_diff) >= umbral or any(
            p["alerta"] for p in productos_diff
        )

        result = ConciliacionResult(
            sucursal_id=branch,
            fecha=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            global_stock=total_global,
            local_stock=total_local,
            diferencia=total_diff,
            productos_diff=productos_diff,
            alerta=hay_alerta,
        )

        # Guardar resultado en DB
        import json
        try:
            self.conn.execute(
                """
                INSERT INTO conciliation_runs
                    (branch_id, usuario, tolerancia_kg,
                     diferencia_kg, detalle_json, estado)
                VALUES (?,?,?,?,?,'completado')
                """,
                (
                    branch, self.usuario, umbral,
                    abs(total_diff),
                    json.dumps(productos_diff, ensure_ascii=False, default=str),
                ),
            )
            self.conn.commit()
        except Exception as db_exc:
            logger.warning("No se pudo guardar conciliation_run: %s", db_exc)

        if hay_alerta:
            logger.warning(
                "CONCILIACIÓN ALERTA sucursal=%d: global=%.3f local=%.3f diff=%.3f",
                branch, total_global, total_local, total_diff,
            )
            self._publicar_conciliacion_diferencia(result)
        else:
            logger.info(
                "Conciliación OK sucursal=%d: diff=%.3f", branch, total_diff,
            )

        return result

    # ── Helpers internos ──────────────────────────────────────────────────────

    def _get_existencia(self, producto_id: int, sucursal_id: int) -> float:
        row = self.conn.execute(
            """
            SELECT COALESCE(SUM(bib.cantidad_disponible), 0)
            FROM branch_inventory_batches bib
            WHERE bib.branch_id = ? AND bib.producto_id = ?
            """,
            (sucursal_id, producto_id),
        ).fetchone()
        return float(row[0]) if row else 0.0

    def _sync_existencia(self, producto_id: int, sucursal_id: int) -> None:
        """Sincroniza productos.existencia desde la suma de BIBs (retrocompat)."""
        stock = self._get_existencia(producto_id, sucursal_id)
        try:
            self.conn.execute(
                "UPDATE productos SET existencia=? WHERE id=?",
                (stock, producto_id),
            )
        except Exception:
            pass  # tabla puede no tener esta columna en algunas configs

    def _sumar_existencia_directo(
        self, producto_id: int, sucursal_id: int, cantidad: float
    ) -> None:
        """Suma directamente al producto (para cancelaciones y ajustes simples)."""
        self.conn.execute(
            "UPDATE productos SET existencia = existencia + ? WHERE id=?",
            (cantidad, producto_id),
        )

    def _obtener_costo_origen(self, producto_id: int, sucursal_origen: int) -> float:
        """Obtiene el costo unitario promedio del BIB más reciente en origen."""
        row = self.conn.execute(
            """
            SELECT AVG(bib.costo_unitario)
            FROM branch_inventory_batches bib
            WHERE bib.branch_id = ? AND bib.producto_id = ?
              AND bib.cantidad_disponible > 0
            """,
            (sucursal_origen, producto_id),
        ).fetchone()
        return float(row[0]) if row and row[0] else 0.0

    def _crear_batch_traspaso(
        self,
        producto_id:   int,
        sucursal_id:   int,
        cantidad:      float,
        costo_unitario: float,
        traspaso_id:   int,
    ) -> int:
        """Crea un chicken_batch derivado para el traspaso."""
        batch_uuid = str(uuid.uuid4())
        self.conn.execute(
            """
            INSERT INTO chicken_batches
                (uuid, branch_id, producto_id, numero_pollos,
                 peso_kg_original, peso_kg_disponible,
                 costo_kg, costo_total, proveedor,
                 estado, fecha_recepcion, usuario_recepcion, notas,
                 root_batch_id)
            VALUES (?,?,?,1,?,?,?,?,'Traspaso','disponible',date('now'),?,?,0)
            """,
            (
                batch_uuid, sucursal_id, producto_id,
                cantidad, cantidad,
                costo_unitario, round(cantidad * costo_unitario, 4),
                self.usuario,
                f"Derivado de traspaso #{traspaso_id}",
            ),
        )
        batch_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        # root_batch_id apunta al batch padre si pudiéramos rastrearlo; usamos self
        self.conn.execute(
            "UPDATE chicken_batches SET root_batch_id=? WHERE id=?",
            (batch_id, batch_id),
        )
        return batch_id

    # ── Publicación de eventos ─────────────────────────────────────────────────

    def _publicar_traspaso_iniciado(self, result: TraspasoResult) -> None:
        try:
            from core.events.event_bus import get_bus, TRASPASO_INICIADO
            get_bus().publish(TRASPASO_INICIADO, {
                "traspaso_id":      result.traspaso_id,
                "producto_id":      result.producto_id,
                "cantidad":         result.cantidad,
                "sucursal_origen":  result.sucursal_origen,
                "sucursal_destino": result.sucursal_destino,
                "usuario":          self.usuario,
            })
        except Exception as exc:
            logger.warning("TRASPASO_INICIADO event falló: %s", exc)

    def _publicar_recepcion_confirmada(self, result: RecepcionResult) -> None:
        try:
            from core.events.event_bus import get_bus, RECEPCION_CONFIRMADA
            get_bus().publish(RECEPCION_CONFIRMADA, {
                "traspaso_id":      result.traspaso_id,
                "producto_id":      result.producto_id,
                "cantidad":         result.cantidad,
                "sucursal_destino": result.sucursal_destino,
                "usuario":          self.usuario,
            })
        except Exception as exc:
            logger.warning("RECEPCION_CONFIRMADA event falló: %s", exc)

    def _publicar_ajuste(self, producto_id: int, cantidad: float) -> None:
        try:
            from core.events.event_bus import get_bus, AJUSTE_INVENTARIO
            get_bus().publish(AJUSTE_INVENTARIO, {
                "producto_id":  producto_id,
                "cantidad":     cantidad,
                "sucursal_id":  self.sucursal_id,
                "usuario":      self.usuario,
            })
        except Exception as exc:
            logger.warning("AJUSTE_INVENTARIO event falló: %s", exc)

    def _publicar_conciliacion_diferencia(self, result: ConciliacionResult) -> None:
        try:
            from core.events.event_bus import get_bus, CONCILIACION_DIFERENCIA
            get_bus().publish(CONCILIACION_DIFERENCIA, {
                "sucursal_id":  result.sucursal_id,
                "diferencia":   result.diferencia,
                "umbral":       self.UMBRAL_CONCILIACION_KG,
                "global_stock": result.global_stock,
                "local_stock":  result.local_stock,
            })
        except Exception as exc:
            logger.warning("CONCILIACION_DIFERENCIA event falló: %s", exc)

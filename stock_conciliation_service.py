# core/services/stock_conciliation_service.py — SPJ Enterprise v9.1
# Fix #9: Conciliación automática + validación sobre-recepción
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

logger = logging.getLogger("spj.conciliation")


@dataclass
class DivergenciaItem:
    producto_id:   int
    nombre:        str
    stock_bib:     float   # suma branch_inventory_batches
    stock_prod:    float   # productos.existencia (retrocompat)
    diferencia:    float   # bib - prod
    es_alerta:     bool


@dataclass
class ConciliacionReport:
    sucursal_id:  int
    fecha:        str
    total_bib:    float
    total_prod:   float
    diferencia:   float
    divergencias: List[DivergenciaItem]
    alerta:       bool
    acciones:     List[str] = field(default_factory=list)


class StockConciliationService:
    """
    Fix #9 — Conciliación automática + validación sobre-recepción.

    Operaciones:
        conciliar()          → detecta divergencias BIB vs existencia
        auto_corregir()      → aplica correcciones automáticas (umbral pequeño)
        validar_recepcion()  → bloquea recepciones que superarían stock global autorizado
        run_scheduled()      → punto de entrada para el scheduler
    """

    UMBRAL_AUTO_KG = 0.05   # diferencias ≤ 50g se corrigen automáticamente
    UMBRAL_ALERTA_KG = 1.0  # diferencias > 1kg generan alerta

    def __init__(self, conn: sqlite3.Connection, sucursal_id: int = 1,
                 usuario: str = "Sistema") -> None:
        self.conn        = conn
        self.sucursal_id = sucursal_id
        self.usuario     = usuario

    # ── Conciliación ─────────────────────────────────────────────────────────

    def conciliar(self, auto_corregir: bool = False) -> ConciliacionReport:
        """
        Compara branch_inventory_batches vs productos.existencia.
        Si auto_corregir=True, aplica correcciones ≤ UMBRAL_AUTO_KG.
        """
        rows = self.conn.execute(
            """
            SELECT bib.producto_id, p.nombre,
                   COALESCE(SUM(bib.cantidad_disponible), 0) AS stock_bib,
                   COALESCE(p.existencia, 0)                 AS stock_prod
            FROM branch_inventory_batches bib
            JOIN productos p ON p.id = bib.producto_id
            WHERE bib.branch_id = ?
            GROUP BY bib.producto_id
            """,
            (self.sucursal_id,),
        ).fetchall()

        divergencias: List[DivergenciaItem] = []
        total_bib = total_prod = 0.0

        for r in rows:
            pid, nombre = int(r[0]), str(r[1])
            s_bib  = round(float(r[2]), 6)
            s_prod = round(float(r[3]), 6)
            diff   = round(s_bib - s_prod, 6)
            total_bib  += s_bib
            total_prod += s_prod

            if abs(diff) > 0.001:
                divergencias.append(DivergenciaItem(
                    producto_id=pid, nombre=nombre,
                    stock_bib=s_bib, stock_prod=s_prod,
                    diferencia=diff,
                    es_alerta=abs(diff) >= self.UMBRAL_ALERTA_KG,
                ))

        hay_alerta = any(d.es_alerta for d in divergencias)
        acciones: List[str] = []

        if auto_corregir and divergencias:
            corregidas = self._auto_corregir(divergencias)
            if corregidas:
                acciones.append(f"Auto-corregidos {len(corregidas)} productos (diff ≤ {self.UMBRAL_AUTO_KG}kg)")

        report = ConciliacionReport(
            sucursal_id=self.sucursal_id,
            fecha=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            total_bib=round(total_bib, 3),
            total_prod=round(total_prod, 3),
            diferencia=round(total_bib - total_prod, 3),
            divergencias=divergencias,
            alerta=hay_alerta,
            acciones=acciones,
        )

        self._persistir(report)

        if hay_alerta:
            logger.warning(
                "conciliacion ALERTA suc=%d: %d divergencias, diff=%.3f",
                self.sucursal_id, len(divergencias), report.diferencia,
            )
            self._publicar_evento(report)
        else:
            logger.info(
                "conciliacion OK suc=%d: diff=%.3f kg",
                self.sucursal_id, report.diferencia,
            )

        return report

    def _auto_corregir(self, divergencias: List[DivergenciaItem]) -> List[int]:
        """Corrige divergencias pequeñas sincronizando productos.existencia ← BIB."""
        corregidos = []
        for d in divergencias:
            if abs(d.diferencia) <= self.UMBRAL_AUTO_KG:
                try:
                    self.conn.execute(
                        "UPDATE productos SET existencia = ? WHERE id = ?",
                        (d.stock_bib, d.producto_id),
                    )
                    self.conn.execute(
                        """
                        INSERT INTO movimientos_inventario
                            (producto_id, tipo, tipo_movimiento, cantidad,
                             existencia_anterior, existencia_nueva,
                             descripcion, usuario, sucursal_id, fecha)
                        VALUES (?,?,?,?,?,?,?,?,?,datetime('now'))
                        """,
                        (
                            d.producto_id, "AJUSTE", "conciliacion_auto",
                            abs(d.diferencia), d.stock_prod, d.stock_bib,
                            f"Auto-conciliación: diff={d.diferencia:+.4f}kg",
                            self.usuario, self.sucursal_id,
                        ),
                    )
                    corregidos.append(d.producto_id)
                except Exception as exc:
                    logger.warning("auto_corregir prod=%d: %s", d.producto_id, exc)
        if corregidos:
            try:
                self.conn.commit()
            except Exception:
                pass
        return corregidos

    # ── Validación sobre-recepción ────────────────────────────────────────────

    def validar_recepcion(
        self,
        producto_id:   int,
        cantidad_kg:   float,
        max_permitido: Optional[float] = None,
    ) -> bool:
        """
        Bloquea recepciones que superarían el stock global autorizado.
        Retorna True si la recepción es válida.
        """
        stock_actual = self._stock_global(producto_id)

        if max_permitido is None:
            # Por defecto: no permitir más de 10x el promedio diario de ventas (7d)
            row = self.conn.execute(
                """
                SELECT COALESCE(AVG(dv.cantidad), 0) * 10
                FROM detalles_venta dv
                JOIN ventas v ON v.id = dv.venta_id
                WHERE dv.producto_id = ?
                  AND v.fecha >= date('now', '-7 days')
                  AND v.estado = 'completada'
                """,
                (producto_id,),
            ).fetchone()
            max_permitido = float(row[0]) if row and row[0] else 9999.0

        if stock_actual + cantidad_kg > max_permitido:
            logger.warning(
                "SOBRE-RECEPCION bloqueada: prod=%d actual=%.3f + recep=%.3f > max=%.3f",
                producto_id, stock_actual, cantidad_kg, max_permitido,
            )
            return False

        return True

    def _stock_global(self, producto_id: int) -> float:
        row = self.conn.execute(
            """
            SELECT COALESCE(SUM(cantidad_disponible), 0)
            FROM branch_inventory_batches
            WHERE producto_id = ?
            """,
            (producto_id,),
        ).fetchone()
        return float(row[0]) if row else 0.0

    def _persistir(self, report: ConciliacionReport) -> None:
        import json
        try:
            self.conn.execute(
                """
                INSERT INTO conciliation_runs
                    (branch_id, usuario, tolerancia_kg, diferencia_kg,
                     detalle_json, estado)
                VALUES (?,?,?,?,?,'completado')
                """,
                (
                    self.sucursal_id, self.usuario,
                    self.UMBRAL_ALERTA_KG, abs(report.diferencia),
                    json.dumps(
                        [{"prod": d.producto_id, "nombre": d.nombre,
                          "diff": d.diferencia, "alerta": d.es_alerta}
                         for d in report.divergencias],
                        ensure_ascii=False,
                    ),
                ),
            )
            self.conn.commit()
        except Exception as exc:
            logger.debug("_persistir conciliation_run: %s", exc)

    def _publicar_evento(self, report: ConciliacionReport) -> None:
        try:
            from core.events.event_bus import get_bus, CONCILIACION_DIFERENCIA
            get_bus().publish(CONCILIACION_DIFERENCIA, {
                "sucursal_id":   report.sucursal_id,
                "diferencia_kg": report.diferencia,
                "n_divergencias": len(report.divergencias),
                "alerta":        report.alerta,
            })
        except Exception as exc:
            logger.debug("publicar_evento conciliacion: %s", exc)

    def run_scheduled(self) -> ConciliacionReport:
        """Punto de entrada para el scheduler. Auto-corrige diferencias pequeñas."""
        return self.conciliar(auto_corregir=True)

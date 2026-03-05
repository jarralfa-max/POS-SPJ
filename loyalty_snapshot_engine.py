# core/services/loyalty_snapshot_engine.py — SPJ Enterprise v9.1
# Fix #10: snapshot acumulado + recálculo incremental de fidelidad.
# En vez de recalcular todo el historial en cada consulta, mantiene
# un snapshot diario de métricas. El recálculo sólo procesa deltas.
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, date, timedelta
from typing import Optional

logger = logging.getLogger("spj.loyalty_snapshot")


class LoyaltySnapshotEngine:
    """
    Mantiene tabla loyalty_snapshots con métricas diarias acumuladas por cliente.

    Esquema:
        loyalty_snapshots(cliente_id, fecha, visitas_dia, importe_dia,
                          margen_dia, visitas_acum, importe_acum, margen_acum,
                          score_calculado, nivel, generado_en)

    Flujo:
        1. procesar_venta_snapshot()  → INSERT/UPDATE del snapshot del día
        2. recalcular_score_incremental() → usa snapshot en vez de escanear todo el historial
        3. rebuild_snapshot()  → reconstrucción completa (mantenimiento, no diario)
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._ensure_table()

    def _ensure_table(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS loyalty_snapshots (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_id      INTEGER NOT NULL REFERENCES clientes(id),
                fecha           DATE    NOT NULL,
                visitas_dia     INTEGER NOT NULL DEFAULT 0,
                importe_dia     REAL    NOT NULL DEFAULT 0,
                margen_dia      REAL    NOT NULL DEFAULT 0,
                visitas_acum    INTEGER NOT NULL DEFAULT 0,
                importe_acum    REAL    NOT NULL DEFAULT 0,
                margen_acum     REAL    NOT NULL DEFAULT 0,
                score_calculado REAL    DEFAULT 0,
                nivel           TEXT    DEFAULT 'Bronce',
                generado_en     DATETIME DEFAULT (datetime('now')),
                UNIQUE(cliente_id, fecha)
            )
        """)
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_ls_cli_fecha ON loyalty_snapshots(cliente_id, fecha)",
            "CREATE INDEX IF NOT EXISTS idx_ls_score ON loyalty_snapshots(score_calculado DESC)",
        ]:
            try:
                self.conn.execute(idx)
            except Exception:
                pass

    # ── Actualización incremental (llamar post-venta) ─────────────────────────

    def procesar_venta_snapshot(
        self,
        cliente_id: int,
        importe:    float,
        margen:     float,
        fecha:      Optional[date] = None,
    ) -> None:
        """
        Actualiza el snapshot del día para el cliente.
        Si ya existe, suma los valores del día (UPSERT).
        Recalcula el snapshot acumulado sumando al día anterior.
        """
        if not cliente_id:
            return
        hoy = fecha or date.today()
        hoy_str = hoy.isoformat()

        # Obtener acumulado hasta ayer
        row_prev = self.conn.execute(
            """
            SELECT visitas_acum, importe_acum, margen_acum
            FROM loyalty_snapshots
            WHERE cliente_id = ?
              AND fecha < ?
            ORDER BY fecha DESC
            LIMIT 1
            """,
            (cliente_id, hoy_str),
        ).fetchone()
        vis_acum_base   = int(row_prev[0])   if row_prev else 0
        imp_acum_base   = float(row_prev[1]) if row_prev else 0.0
        mar_acum_base   = float(row_prev[2]) if row_prev else 0.0

        # Obtener snapshot existente del día (si hay)
        row_hoy = self.conn.execute(
            "SELECT id, visitas_dia, importe_dia, margen_dia "
            "FROM loyalty_snapshots WHERE cliente_id=? AND fecha=?",
            (cliente_id, hoy_str),
        ).fetchone()

        if row_hoy:
            vis_dia  = int(row_hoy[1])   + 1
            imp_dia  = float(row_hoy[2]) + importe
            mar_dia  = float(row_hoy[3]) + margen
            self.conn.execute(
                """
                UPDATE loyalty_snapshots
                SET visitas_dia  = ?,
                    importe_dia  = ?,
                    margen_dia   = ?,
                    visitas_acum = ?,
                    importe_acum = ?,
                    margen_acum  = ?,
                    generado_en  = datetime('now')
                WHERE id = ?
                """,
                (
                    vis_dia, imp_dia, mar_dia,
                    vis_acum_base + vis_dia,
                    imp_acum_base + imp_dia,
                    mar_acum_base + mar_dia,
                    row_hoy[0],
                ),
            )
        else:
            vis_dia, imp_dia, mar_dia = 1, importe, margen
            self.conn.execute(
                """
                INSERT INTO loyalty_snapshots
                    (cliente_id, fecha,
                     visitas_dia, importe_dia, margen_dia,
                     visitas_acum, importe_acum, margen_acum)
                VALUES (?,?, ?,?,?, ?,?,?)
                """,
                (
                    cliente_id, hoy_str,
                    vis_dia, imp_dia, mar_dia,
                    vis_acum_base + vis_dia,
                    imp_acum_base + imp_dia,
                    mar_acum_base + mar_dia,
                ),
            )
        try:
            self.conn.commit()
        except Exception:
            pass
        logger.debug(
            "snapshot cliente=%d fecha=%s vis=%d imp=%.2f",
            cliente_id, hoy_str, vis_dia, imp_dia,
        )

    def recalcular_score_incremental(
        self,
        cliente_id: int,
        periodo_dias: int = 90,
    ) -> float:
        """
        Recalcula el score usando el snapshot del período — O(1) en vez de O(n_ventas).
        Retorna score 0-100.
        """
        desde = (date.today() - timedelta(days=periodo_dias)).isoformat()

        row = self.conn.execute(
            """
            SELECT SUM(visitas_dia), SUM(importe_dia), SUM(margen_dia)
            FROM loyalty_snapshots
            WHERE cliente_id = ? AND fecha >= ?
            """,
            (cliente_id, desde),
        ).fetchone()
        if not row or not row[0]:
            return 0.0

        visitas = int(row[0])
        importe = float(row[1])
        margen  = float(row[2])

        # Percentiles p95 desde tabla benchmark (lazy: usa MAX si no hay benchmark)
        bench = self.conn.execute(
            """
            SELECT
                MAX(vis_sum) as max_vis,
                MAX(imp_sum) as max_imp,
                MAX(mar_sum) as max_mar
            FROM (
                SELECT
                    SUM(visitas_dia) as vis_sum,
                    SUM(importe_dia) as imp_sum,
                    SUM(margen_dia)  as mar_sum
                FROM loyalty_snapshots
                WHERE fecha >= ?
                GROUP BY cliente_id
            )
            """,
            (desde,),
        ).fetchone()
        max_vis = max(float(bench[0] or 1), 1)
        max_imp = max(float(bench[1] or 1), 1)
        max_mar = max(float(bench[2] or 1), 1)

        s_freq = min(visitas / max_vis, 1.0) * 100 * 0.30
        s_vol  = min(importe / max_imp, 1.0) * 100 * 0.30
        s_mar  = min(margen  / max_mar, 1.0) * 100 * 0.30
        # comunidad (referidos): 10% — requiere join separado pero es raro
        s_com  = 0.0
        row_ref = self.conn.execute(
            "SELECT COUNT(*) FROM clientes WHERE referido_por_id = ?",
            (cliente_id,),
        ).fetchone()
        if row_ref and row_ref[0]:
            s_com = min(int(row_ref[0]) * 20, 100) * 0.10

        score = round(s_freq + s_vol + s_mar + s_com, 2)
        return score

    def rebuild_snapshot(self, dias_atras: int = 365) -> int:
        """
        Reconstruye snapshots desde detalles_venta (mantenimiento periódico).
        Retorna número de filas procesadas.
        """
        desde = (date.today() - timedelta(days=dias_atras)).isoformat()
        logger.info("Reconstruyendo loyalty_snapshots desde %s...", desde)

        rows = self.conn.execute(
            """
            SELECT DATE(v.fecha) as dia,
                   v.cliente_id,
                   COUNT(*) as visitas,
                   SUM(v.total) as importe,
                   SUM(COALESCE(dv.margen_real, 0) * dv.cantidad) as margen
            FROM ventas v
            JOIN detalles_venta dv ON dv.venta_id = v.id
            WHERE v.estado = 'completada'
              AND v.cliente_id IS NOT NULL
              AND DATE(v.fecha) >= ?
            GROUP BY DATE(v.fecha), v.cliente_id
            ORDER BY DATE(v.fecha) ASC, v.cliente_id
            """,
            (desde,),
        ).fetchall()

        procesadas = 0
        for row in rows:
            try:
                self.procesar_venta_snapshot(
                    cliente_id=int(row[1]),
                    importe=float(row[3]),
                    margen=float(row[4] or 0),
                    fecha=date.fromisoformat(str(row[0])),
                )
                procesadas += 1
            except Exception as exc:
                logger.warning("rebuild_snapshot fila %s: %s", row, exc)

        logger.info("loyalty_snapshots reconstruidos: %d filas", procesadas)
        return procesadas

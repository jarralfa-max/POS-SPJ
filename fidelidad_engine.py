# core/services/fidelidad_engine.py
# ── Motor de Fidelidad Multivariable SPJ v9 ────────────────────────────────
# Calcula score 0-100 por cliente usando 4 dimensiones:
#   Frecuencia  = n_visitas / periodo
#   Volumen     = importe total comprado
#   Margen      = margen_real generado (desde detalles_venta)
#   Comunidad   = referidos + engagement
# Pesos configurables vía loyalty_config.
# Motor es idempotente — UPSERT en loyalty_scores tras cada cálculo.
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger("spj.fidelidad")


# ── DTOs ──────────────────────────────────────────────────────────────────────

@dataclass
class LoyaltyScore:
    cliente_id:       int
    cliente_nombre:   str
    score_frecuencia: float
    score_volumen:    float
    score_margen:     float
    score_comunidad:  float
    score_total:      float
    nivel:            str
    visitas_periodo:  int
    importe_total:    float
    margen_generado:  float
    referidos:        int
    periodo_inicio:   date
    periodo_fin:      date


@dataclass
class LoyaltyConfig:
    peso_frecuencia: float = 30.0
    peso_volumen:    float = 30.0
    peso_margen:     float = 30.0
    peso_comunidad:  float = 10.0
    periodo_dias:    int   = 90
    umbral_plata:    float = 40.0
    umbral_oro:      float = 65.0
    umbral_platino:  float = 85.0
    puntos_por_peso: float = 1.0
    bonus_referido:  int   = 50


@dataclass
class PuntosResult:
    cliente_id:   int
    puntos_antes: int
    puntos_ganados: int
    puntos_totales: int
    nivel_antes:  str
    nivel_despues: str
    subio_nivel:  bool


# ── FidelidadEngine ───────────────────────────────────────────────────────────

class FidelidadEngine:
    """
    Motor de fidelidad multivariable.

    Uso típico (post-venta):
        eng = FidelidadEngine(conn)
        result = eng.procesar_post_venta(cliente_id, venta_id, total)
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._cfg: Optional[LoyaltyConfig] = None

    # ── Config ────────────────────────────────────────────────────────────────

    def _load_config(self) -> LoyaltyConfig:
        if self._cfg is not None:
            return self._cfg
        rows = self.conn.execute(
            "SELECT clave, valor FROM loyalty_config"
        ).fetchall()
        cfg_dict: Dict[str, str] = {r[0]: r[1] for r in rows}

        def fv(k: str, d: float) -> float:
            return float(cfg_dict.get(k, d))

        def iv(k: str, d: int) -> int:
            return int(cfg_dict.get(k, d))

        self._cfg = LoyaltyConfig(
            peso_frecuencia = fv("peso_frecuencia", 30.0),
            peso_volumen    = fv("peso_volumen",    30.0),
            peso_margen     = fv("peso_margen",     30.0),
            peso_comunidad  = fv("peso_comunidad",  10.0),
            periodo_dias    = iv("periodo_dias",    90),
            umbral_plata    = fv("umbral_plata",    40.0),
            umbral_oro      = fv("umbral_oro",      65.0),
            umbral_platino  = fv("umbral_platino",  85.0),
            puntos_por_peso = fv("puntos_por_peso", 1.0),
            bonus_referido  = iv("bonus_referido",  50),
        )
        return self._cfg

    def reload_config(self) -> LoyaltyConfig:
        """Fuerza recarga de config (útil tras guardar cambios)."""
        self._cfg = None
        return self._load_config()

    # ── Nivel ─────────────────────────────────────────────────────────────────

    def _nivel(self, score: float) -> str:
        cfg = self._load_config()
        if score >= cfg.umbral_platino:
            return "Platino"
        if score >= cfg.umbral_oro:
            return "Oro"
        if score >= cfg.umbral_plata:
            return "Plata"
        return "Bronce"

    # ── Cálculo de score ──────────────────────────────────────────────────────

    def calcular_score(self, cliente_id: int) -> LoyaltyScore:
        """
        Calcula score 0-100 para el cliente dado.
        Consulta ventas del período configurado.
        """
        cfg = self._load_config()
        hoy = date.today()
        inicio = hoy - timedelta(days=cfg.periodo_dias)

        # Datos de ventas del período
        row = self.conn.execute(
            """
            SELECT
                COUNT(DISTINCT DATE(v.fecha))         AS dias_con_visita,
                COUNT(v.id)                           AS n_ventas,
                COALESCE(SUM(v.total), 0)             AS importe_total,
                COALESCE(SUM(dv.margen_real), 0)      AS margen_total
            FROM ventas v
            LEFT JOIN detalles_venta dv ON dv.venta_id = v.id
            WHERE v.cliente_id = ?
              AND v.estado = 'completada'
              AND DATE(v.fecha) BETWEEN ? AND ?
            """,
            (cliente_id, inicio.isoformat(), hoy.isoformat())
        ).fetchone()

        dias_visita   = int(row[0]) if row else 0
        importe_total = float(row[2]) if row else 0.0
        margen_total  = float(row[3]) if row else 0.0

        # Referidos
        referidos = self.conn.execute(
            "SELECT COUNT(*) FROM clientes WHERE referido_por = ?",
            (cliente_id,)
        ).fetchone()[0] or 0

        # Nombre cliente
        nombre_row = self.conn.execute(
            "SELECT nombre FROM clientes WHERE id = ?", (cliente_id,)
        ).fetchone()
        nombre = nombre_row[0] if nombre_row else "Desconocido"

        # Benchmarks del período (todos los clientes activos)
        bench = self.conn.execute(
            """
            SELECT
                MAX(cnt_dias),
                MAX(total_importe),
                MAX(total_margen)
            FROM (
                SELECT
                    v.cliente_id,
                    COUNT(DISTINCT DATE(v.fecha)) AS cnt_dias,
                    SUM(v.total)                  AS total_importe,
                    COALESCE(SUM(dv.margen_real),0) AS total_margen
                FROM ventas v
                LEFT JOIN detalles_venta dv ON dv.venta_id = v.id
                WHERE v.estado = 'completada'
                  AND DATE(v.fecha) BETWEEN ? AND ?
                  AND v.cliente_id IS NOT NULL
                GROUP BY v.cliente_id
            )
            """,
            (inicio.isoformat(), hoy.isoformat())
        ).fetchone()

        max_dias    = float(bench[0]) if bench and bench[0] else 1.0
        max_import  = float(bench[1]) if bench and bench[1] else 1.0
        max_margen  = float(bench[2]) if bench and bench[2] else 1.0
        max_referid = max(float(referidos), 1.0)

        # Max referidos en sistema
        max_ref_row = self.conn.execute(
            "SELECT MAX(cnt) FROM ("
            "  SELECT COUNT(*) AS cnt FROM clientes "
            "  WHERE referido_por IS NOT NULL GROUP BY referido_por)"
        ).fetchone()
        max_referidos_global = float(max_ref_row[0]) if max_ref_row and max_ref_row[0] else 1.0

        # Scores normalizados 0-100
        s_frecuencia = min(100.0, (dias_visita / max_dias) * 100.0)
        s_volumen    = min(100.0, (importe_total / max_import) * 100.0)
        s_margen     = min(100.0, (margen_total / max(abs(max_margen), 1.0)) * 100.0)
        s_comunidad  = min(100.0, (referidos / max_referidos_global) * 100.0)

        # Score ponderado
        total_peso = cfg.peso_frecuencia + cfg.peso_volumen + cfg.peso_margen + cfg.peso_comunidad
        if total_peso <= 0:
            total_peso = 100.0

        score_total = (
            s_frecuencia * cfg.peso_frecuencia +
            s_volumen    * cfg.peso_volumen    +
            s_margen     * cfg.peso_margen     +
            s_comunidad  * cfg.peso_comunidad
        ) / total_peso

        score_total = round(min(100.0, max(0.0, score_total)), 2)

        return LoyaltyScore(
            cliente_id       = cliente_id,
            cliente_nombre   = nombre,
            score_frecuencia = round(s_frecuencia, 2),
            score_volumen    = round(s_volumen, 2),
            score_margen     = round(s_margen, 2),
            score_comunidad  = round(s_comunidad, 2),
            score_total      = score_total,
            nivel            = self._nivel(score_total),
            visitas_periodo  = dias_visita,
            importe_total    = round(importe_total, 2),
            margen_generado  = round(margen_total, 2),
            referidos        = referidos,
            periodo_inicio   = inicio,
            periodo_fin      = hoy,
        )

    def guardar_score(self, score: LoyaltyScore) -> None:
        """UPSERT loyalty_scores para el cliente."""
        self.conn.execute(
            """
            INSERT INTO loyalty_scores
                (cliente_id, score_frecuencia, score_volumen, score_margen,
                 score_comunidad, score_total, nivel,
                 visitas_periodo, importe_total, margen_generado, referidos,
                 fecha_calculo, periodo_inicio, periodo_fin)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'),?,?)
            ON CONFLICT(cliente_id) DO UPDATE SET
                score_frecuencia = excluded.score_frecuencia,
                score_volumen    = excluded.score_volumen,
                score_margen     = excluded.score_margen,
                score_comunidad  = excluded.score_comunidad,
                score_total      = excluded.score_total,
                nivel            = excluded.nivel,
                visitas_periodo  = excluded.visitas_periodo,
                importe_total    = excluded.importe_total,
                margen_generado  = excluded.margen_generado,
                referidos        = excluded.referidos,
                fecha_calculo    = datetime('now'),
                periodo_inicio   = excluded.periodo_inicio,
                periodo_fin      = excluded.periodo_fin
            """,
            (
                score.cliente_id,
                score.score_frecuencia, score.score_volumen,
                score.score_margen,     score.score_comunidad,
                score.score_total,      score.nivel,
                score.visitas_periodo,  score.importe_total,
                score.margen_generado,  score.referidos,
                score.periodo_inicio.isoformat(), score.periodo_fin.isoformat(),
            )
        )
        # Actualizar nivel en tarjeta asignada
        self.conn.execute(
            "UPDATE tarjetas_fidelidad SET nivel = ? WHERE id_cliente = ? AND estado = 'asignada'",
            (score.nivel, score.cliente_id)
        )
        self.conn.commit()

    # ── Post-venta: puntos + score ────────────────────────────────────────────

    def procesar_post_venta(
        self,
        cliente_id: int,
        venta_id: int,
        total_venta: float,
    ) -> PuntosResult:
        """
        Llamar tras confirmar venta cuando hay cliente asignado.
        1. Sumar puntos (total * puntos_por_peso)
        2. Recalcular score multivariable
        3. Actualizar nivel en tarjeta
        Retorna PuntosResult.
        """
        cfg = self._load_config()

        # Puntos actuales
        row = self.conn.execute(
            "SELECT puntos FROM clientes WHERE id = ?", (cliente_id,)
        ).fetchone()
        puntos_antes = int(row[0]) if row else 0

        # Nivel antes
        score_prev = self.conn.execute(
            "SELECT nivel FROM loyalty_scores WHERE cliente_id = ?",
            (cliente_id,)
        ).fetchone()
        nivel_antes = score_prev[0] if score_prev else "Bronce"

        # Calcular puntos ganados
        puntos_ganados = max(1, int(total_venta * cfg.puntos_por_peso))
        puntos_nuevos  = puntos_antes + puntos_ganados

        # Actualizar clientes
        self.conn.execute(
            "UPDATE clientes SET puntos = ? WHERE id = ?",
            (puntos_nuevos, cliente_id)
        )

        # Recalcular score multivariable
        score = self.calcular_score(cliente_id)
        self.guardar_score(score)

        subio = score.nivel != nivel_antes and score.nivel in ("Plata", "Oro", "Platino")

        logger.info(
            "post_venta cliente=%d venta=%d puntos+%d total=%d nivel=%s→%s",
            cliente_id, venta_id, puntos_ganados, puntos_nuevos,
            nivel_antes, score.nivel
        )

        return PuntosResult(
            cliente_id    = cliente_id,
            puntos_antes  = puntos_antes,
            puntos_ganados= puntos_ganados,
            puntos_totales= puntos_nuevos,
            nivel_antes   = nivel_antes,
            nivel_despues = score.nivel,
            subio_nivel   = subio,
        )

    # ── Ranking ───────────────────────────────────────────────────────────────

    def ranking_clientes(self, limit: int = 20) -> List[Dict]:
        rows = self.conn.execute(
            """
            SELECT
                c.id, c.nombre, c.telefono,
                ls.score_total, ls.nivel,
                ls.visitas_periodo, ls.importe_total,
                ls.margen_generado, ls.referidos,
                ls.fecha_calculo
            FROM loyalty_scores ls
            JOIN clientes c ON c.id = ls.cliente_id
            ORDER BY ls.score_total DESC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()
        return [
            {
                "cliente_id":   r[0], "nombre": r[1], "telefono": r[2],
                "score":        r[3], "nivel":  r[4],
                "visitas":      r[5], "importe": r[6],
                "margen":       r[7], "referidos": r[8],
                "calculado_en": r[9],
            }
            for r in rows
        ]

    def distribucion_niveles(self) -> Dict[str, int]:
        rows = self.conn.execute(
            "SELECT nivel, COUNT(*) FROM loyalty_scores GROUP BY nivel"
        ).fetchall()
        return {r[0]: r[1] for r in rows}

    # ── Bono referido ─────────────────────────────────────────────────────────

    def registrar_referido(self, cliente_id: int, referido_por_id: int) -> int:
        """
        Marca referido_por en cliente y aplica bono de puntos al referidor.
        Retorna puntos_bonus otorgados.
        """
        cfg = self._load_config()
        try:
            self.conn.execute(
                "UPDATE clientes SET referido_por = ? WHERE id = ? AND referido_por IS NULL",
                (referido_por_id, cliente_id)
            )
            self.conn.execute(
                "UPDATE clientes SET puntos = puntos + ? WHERE id = ?",
                (cfg.bonus_referido, referido_por_id)
            )
            self.conn.commit()
            # Recalcular score del referidor
            score = self.calcular_score(referido_por_id)
            self.guardar_score(score)
            return cfg.bonus_referido
        except Exception as exc:
            logger.error("registrar_referido: %s", exc)
            return 0

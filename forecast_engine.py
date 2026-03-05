# core/services/forecast_engine.py
# ── Motor de Pronóstico Diario SPJ v9 ─────────────────────────────────────
# Métodos: media_movil, tendencia (OLS), promedio_simple
# Genera predicciones por producto × sucursal × horizonte
# Calcula MAPE sobre ventana de validación
# Genera compras_sugeridas considerando stock actual y lead_time
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("spj.forecast")


# ── DTOs ──────────────────────────────────────────────────────────────────────

@dataclass
class SerieItem:
    fecha: date
    cantidad: float


@dataclass
class ForecastResult:
    producto_id:       int
    producto_nombre:   str
    sucursal_id:       int
    fecha_prediccion:  date
    cantidad_predicha: float
    intervalo_bajo:    float
    intervalo_alto:    float
    metodo:            str
    mape:              float
    stock_actual:      float
    compra_sugerida:   float


@dataclass
class CompraSugerida:
    producto_id:     int
    producto_nombre: str
    stock_actual:    float
    demanda_diaria:  float
    dias_restantes:  float
    lead_time_dias:  int
    unidad:          str
    cantidad_sugerida: float
    urgente:         bool


# ── ForecastEngine ────────────────────────────────────────────────────────────

class ForecastEngine:
    """
    Motor de pronóstico diario de ventas por producto.

    forecast_engine = ForecastEngine(conn, sucursal_id=1)
    resultados = forecast_engine.generar_forecast(horizonte_dias=14)
    sugeridas  = forecast_engine.compras_sugeridas()
    """

    VENTANA_MOVIL       = 7    # días para media móvil
    VENTANA_TENDENCIA   = 30   # días para regresión lineal
    VALIDACION_DIAS     = 7    # días de validación para MAPE
    LEAD_TIME_DEFAULT   = 2    # días de lead time si no configurado
    STOCK_SEGURIDAD_PCT = 0.20  # 20% extra sobre demanda proyectada

    def __init__(self, conn: sqlite3.Connection, sucursal_id: int = 1) -> None:
        self.conn        = conn
        self.sucursal_id = sucursal_id

    # ── Serie histórica ───────────────────────────────────────────────────────

    def _serie_diaria(
        self,
        producto_id: int,
        desde: date,
        hasta: date,
    ) -> List[SerieItem]:
        """Ventas diarias de un producto en el rango dado."""
        rows = self.conn.execute(
            """
            SELECT DATE(v.fecha) AS dia,
                   SUM(dv.cantidad) AS total_qty
            FROM detalles_venta dv
            JOIN ventas v ON v.id = dv.venta_id
            WHERE dv.producto_id = ?
              AND v.estado = 'completada'
              AND (? = 0 OR v.sucursal_id = ?)
              AND DATE(v.fecha) BETWEEN ? AND ?
            GROUP BY dia
            ORDER BY dia
            """,
            (producto_id, self.sucursal_id, self.sucursal_id,
             desde.isoformat(), hasta.isoformat())
        ).fetchall()

        # Rellenar días vacíos con 0
        serie: Dict[date, float] = {}
        d = desde
        while d <= hasta:
            serie[d] = 0.0
            d += timedelta(days=1)
        for row in rows:
            try:
                dia = date.fromisoformat(row[0])
                serie[dia] = float(row[1])
            except Exception:
                pass

        return [SerieItem(fecha=k, cantidad=v) for k, v in sorted(serie.items())]

    # ── Media móvil ───────────────────────────────────────────────────────────

    def _media_movil(self, serie: List[SerieItem], ventana: int) -> List[float]:
        if len(serie) < ventana:
            avg = sum(s.cantidad for s in serie) / max(len(serie), 1)
            return [avg] * len(serie)
        preds = [0.0] * len(serie)
        for i in range(len(serie)):
            inicio = max(0, i - ventana + 1)
            vals   = [serie[j].cantidad for j in range(inicio, i + 1)]
            preds[i] = sum(vals) / len(vals)
        return preds

    # ── Tendencia lineal (OLS simple) ─────────────────────────────────────────

    def _tendencia_ols(
        self, serie: List[SerieItem]
    ) -> Tuple[float, float]:
        """Retorna (pendiente, intercepto)."""
        n = len(serie)
        if n < 2:
            avg = serie[0].cantidad if serie else 0.0
            return 0.0, avg
        xs = list(range(n))
        ys = [s.cantidad for s in serie]
        x_mean = sum(xs) / n
        y_mean = sum(ys) / n
        num   = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
        denom = sum((x - x_mean) ** 2 for x in xs)
        if denom == 0:
            return 0.0, y_mean
        m = num / denom
        b = y_mean - m * x_mean
        return m, b

    # ── MAPE ──────────────────────────────────────────────────────────────────

    def _mape(self, actual: List[float], pred: List[float]) -> float:
        errors = []
        for a, p in zip(actual, pred):
            if a > 0:
                errors.append(abs(a - p) / a * 100)
        return round(sum(errors) / len(errors), 2) if errors else 0.0

    # ── Forecast un producto ──────────────────────────────────────────────────

    def _forecast_producto(
        self,
        producto_id: int,
        horizonte:   int,
        metodo:      str,
    ) -> List[ForecastResult]:
        hoy   = date.today()
        hasta_hist = hoy - timedelta(days=1)
        desde_hist = hasta_hist - timedelta(days=self.VENTANA_TENDENCIA + self.VALIDACION_DIAS)

        serie = self._serie_diaria(producto_id, desde_hist, hasta_hist)
        if not serie:
            return []

        # Nombre producto
        prow = self.conn.execute(
            "SELECT nombre FROM productos WHERE id = ?", (producto_id,)
        ).fetchone()
        nombre = prow[0] if prow else str(producto_id)

        # Stock actual
        stock_row = self.conn.execute(
            "SELECT COALESCE(SUM(existencia), 0) FROM productos WHERE id = ?",
            (producto_id,)
        ).fetchone()
        stock = float(stock_row[0]) if stock_row else 0.0

        # Entrenamiento + validación
        n_val  = min(self.VALIDACION_DIAS, len(serie) - 1)
        n_train = len(serie) - n_val

        if n_train < 2:
            n_train = len(serie)
            n_val   = 0

        train = serie[:n_train]
        val   = serie[n_train:] if n_val > 0 else []

        # Calcular predicciones de validación para MAPE
        mape_val = 0.0
        if val:
            if metodo == "media_movil":
                preds_train = self._media_movil(train, self.VENTANA_MOVIL)
                pred_val    = [preds_train[-1]] * len(val)
            elif metodo == "tendencia":
                m, b = self._tendencia_ols(train)
                pred_val = [max(0.0, m * (n_train + i) + b) for i in range(len(val))]
            else:
                avg_train = sum(s.cantidad for s in train) / len(train)
                pred_val  = [avg_train] * len(val)
            actual_val = [s.cantidad for s in val]
            mape_val   = self._mape(actual_val, pred_val)

        # Predicción futura
        if metodo == "media_movil":
            preds_full  = self._media_movil(serie, self.VENTANA_MOVIL)
            base_pred   = preds_full[-1]
            std_err     = 0.0
            if len(preds_full) >= self.VENTANA_MOVIL:
                ventana_vals = [serie[i].cantidad for i in range(-self.VENTANA_MOVIL, 0)]
                mean_v = sum(ventana_vals) / len(ventana_vals)
                std_err = (sum((v - mean_v) ** 2 for v in ventana_vals) / len(ventana_vals)) ** 0.5
            results = []
            for d in range(horizonte):
                pred_dia = max(0.0, base_pred)
                results.append((hoy + timedelta(days=d), pred_dia, std_err))

        elif metodo == "tendencia":
            m, b = self._tendencia_ols(serie)
            n = len(serie)
            residuos = [serie[i].cantidad - (m * i + b) for i in range(n)]
            std_err  = (sum(r ** 2 for r in residuos) / max(n - 2, 1)) ** 0.5
            results = []
            for d in range(horizonte):
                x       = n + d
                pred_dia = max(0.0, m * x + b)
                results.append((hoy + timedelta(days=d), pred_dia, std_err))

        else:  # promedio_simple
            avg     = sum(s.cantidad for s in serie) / max(len(serie), 1)
            std_err = (sum((s.cantidad - avg) ** 2 for s in serie) / max(len(serie), 1)) ** 0.5
            results = [(hoy + timedelta(days=d), max(0.0, avg), std_err) for d in range(horizonte)]

        return [
            ForecastResult(
                producto_id       = producto_id,
                producto_nombre   = nombre,
                sucursal_id       = self.sucursal_id,
                fecha_prediccion  = dia,
                cantidad_predicha = round(pred, 4),
                intervalo_bajo    = round(max(0.0, pred - 1.96 * std_err), 4),
                intervalo_alto    = round(pred + 1.96 * std_err, 4),
                metodo            = metodo,
                mape              = mape_val,
                stock_actual      = stock,
                compra_sugerida   = 0.0,  # se calcula en compras_sugeridas
            )
            for dia, pred, std_err in results
        ]

    # ── Generar forecast todos los productos ──────────────────────────────────

    def generar_forecast(
        self,
        horizonte_dias: int = 14,
        metodo:         str = "media_movil",
        guardar_cache:  bool = True,
    ) -> List[ForecastResult]:
        """
        Genera forecast para todos los productos con ventas históricas.
        Retorna lista de ForecastResult (horizonte × n_productos).
        """
        productos = self.conn.execute(
            """
            SELECT DISTINCT dv.producto_id
            FROM detalles_venta dv
            JOIN ventas v ON v.id = dv.venta_id
            WHERE v.estado = 'completada'
              AND DATE(v.fecha) >= DATE('now', '-90 days')
            """
        ).fetchall()

        all_results: List[ForecastResult] = []
        for (prod_id,) in productos:
            try:
                res = self._forecast_producto(prod_id, horizonte_dias, metodo)
                all_results.extend(res)
            except Exception as exc:
                logger.warning("forecast prod=%d: %s", prod_id, exc)

        if guardar_cache and all_results:
            self._guardar_cache(all_results)

        logger.info(
            "forecast sucursal=%d horizonte=%d metodo=%s productos=%d registros=%d",
            self.sucursal_id, horizonte_dias, metodo,
            len(productos), len(all_results)
        )
        return all_results

    def _guardar_cache(self, results: List[ForecastResult]) -> None:
        for r in results:
            self.conn.execute(
                """
                INSERT INTO forecast_cache
                    (producto_id, sucursal_id, fecha_prediccion,
                     cantidad_predicha, intervalo_bajo, intervalo_alto,
                     metodo, mape)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(producto_id, sucursal_id, fecha_prediccion, metodo)
                DO UPDATE SET
                    cantidad_predicha = excluded.cantidad_predicha,
                    intervalo_bajo    = excluded.intervalo_bajo,
                    intervalo_alto    = excluded.intervalo_alto,
                    mape              = excluded.mape,
                    generado_en       = datetime('now')
                """,
                (
                    r.producto_id, r.sucursal_id, r.fecha_prediccion.isoformat(),
                    r.cantidad_predicha, r.intervalo_bajo, r.intervalo_alto,
                    r.metodo, r.mape,
                )
            )
        self.conn.commit()

    # ── Compras sugeridas ─────────────────────────────────────────────────────

    def compras_sugeridas(
        self,
        lead_time_dias: int = None,
        horizonte_dias: int = 7,
    ) -> List[CompraSugerida]:
        """
        Calcula cantidad sugerida de compra para cada producto.
        Considera: demanda proyectada + stock seguridad - stock actual.
        """
        lt = lead_time_dias if lead_time_dias is not None else self.LEAD_TIME_DEFAULT
        horizonte = horizonte_dias + lt  # ventana total de cobertura

        resultados = self.generar_forecast(horizonte_dias=horizonte, guardar_cache=False)
        if not resultados:
            return []

        # Agrupar por producto
        por_producto: Dict[int, List[ForecastResult]] = {}
        for r in resultados:
            por_producto.setdefault(r.producto_id, []).append(r)

        sugeridas: List[CompraSugerida] = []
        for prod_id, preds in por_producto.items():
            demanda_total = sum(p.cantidad_predicha for p in preds)
            demanda_diaria = demanda_total / max(len(preds), 1)
            stock_actual   = preds[0].stock_actual
            nombre         = preds[0].producto_nombre

            # Unidad del producto
            u_row = self.conn.execute(
                "SELECT unidad FROM productos WHERE id = ?", (prod_id,)
            ).fetchone()
            unidad = u_row[0] if u_row else "pza"

            # Días de cobertura con stock actual
            dias_restantes = stock_actual / max(demanda_diaria, 0.001)

            # Cantidad a pedir: cubrir horizonte + seguridad - stock
            demanda_horizonte = demanda_diaria * horizonte_dias
            stock_seguridad   = demanda_horizonte * self.STOCK_SEGURIDAD_PCT
            cantidad_neta     = demanda_horizonte + stock_seguridad - stock_actual
            cantidad_sugerida = max(0.0, round(cantidad_neta, 2))

            urgente = dias_restantes < lt  # stock no cubre lead time

            if cantidad_sugerida > 0 or urgente:
                sugeridas.append(CompraSugerida(
                    producto_id      = prod_id,
                    producto_nombre  = nombre,
                    stock_actual     = round(stock_actual, 3),
                    demanda_diaria   = round(demanda_diaria, 3),
                    dias_restantes   = round(dias_restantes, 1),
                    lead_time_dias   = lt,
                    unidad           = unidad,
                    cantidad_sugerida= cantidad_sugerida,
                    urgente          = urgente,
                ))

        sugeridas.sort(key=lambda x: (not x.urgente, -x.cantidad_sugerida))
        return sugeridas

    # ── Serie para gráfica ────────────────────────────────────────────────────

    def serie_historica_producto(
        self,
        producto_id:  int,
        dias_atras:   int = 30,
    ) -> List[SerieItem]:
        hasta = date.today() - timedelta(days=1)
        desde = hasta - timedelta(days=dias_atras)
        return self._serie_diaria(producto_id, desde, hasta)

    def forecast_desde_cache(
        self,
        producto_id:  int,
        dias:         int = 7,
        metodo:       str = "media_movil",
    ) -> List[ForecastResult]:
        hoy = date.today()
        rows = self.conn.execute(
            """
            SELECT fecha_prediccion, cantidad_predicha, intervalo_bajo,
                   intervalo_alto, mape
            FROM forecast_cache
            WHERE producto_id = ?
              AND sucursal_id = ?
              AND metodo = ?
              AND fecha_prediccion >= ?
            ORDER BY fecha_prediccion
            LIMIT ?
            """,
            (producto_id, self.sucursal_id, metodo, hoy.isoformat(), dias)
        ).fetchall()

        if not rows:
            return self._forecast_producto(producto_id, dias, metodo)

        prow = self.conn.execute(
            "SELECT nombre FROM productos WHERE id = ?", (producto_id,)
        ).fetchone()
        nombre = prow[0] if prow else str(producto_id)

        return [
            ForecastResult(
                producto_id       = producto_id,
                producto_nombre   = nombre,
                sucursal_id       = self.sucursal_id,
                fecha_prediccion  = date.fromisoformat(r[0]),
                cantidad_predicha = r[1],
                intervalo_bajo    = r[2],
                intervalo_alto    = r[3],
                metodo            = metodo,
                mape              = r[4],
                stock_actual      = 0.0,
                compra_sugerida   = 0.0,
            )
            for r in rows
        ]

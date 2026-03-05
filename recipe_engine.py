# core/services/recipe_engine.py
# ── RECIPE ENGINE — SPJ Enterprise v9 ─────────────────────────────────────────
# Motor de recetas: gestiona recetas de consumo automático y recetas de
# transformación de pollo (rendimientos por corte).
#
# NOTA IMPORTANTE:
#   El consumo de inventario en venta ya lo hace SalesEngine.procesar_venta()
#   de forma inline usando InventoryEngine.consume_product_mix() y
#   ProductRecipeRepository. RecipeEngine provee:
#
#   1. Handler VENTA_COMPLETADA (hook post-venta para registro adicional)
#   2. Simulación de rendimiento para producción
#   3. Gestión de recetas de transformación (recetas_pollo)
#   4. Validación de stock antes de venta
#   5. Consumo de recetas de ABARROTES (no-pollo con ratio/merma)
#
# INTEGRACIÓN SIN MODIFICAR ventas.py:
#   RecipeEngine se suscribe a VENTA_COMPLETADA en el EventBus.
#   El handler ejecuta lógica adicional post-venta (logs, alertas, forecast data).
from __future__ import annotations

import logging
import sqlite3
import uuid
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple

logger = logging.getLogger("spj.recipe_engine")


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class IngredienteReceta:
    ingrediente_id:    int
    nombre:            str
    ratio:             float   # unidades_ingrediente por unidad de producto_compuesto
    merma:             float   # porcentaje merma (0–100)
    unidad:            str
    stock_disponible:  float
    requerido:         float   # ratio × cantidad_a_producir × (1 + merma/100)
    stock_suficiente:  bool


@dataclass
class SimulacionResult:
    producto_id:     int
    producto_nombre: str
    cantidad:        float
    factible:        bool        # True si hay stock para todos los ingredientes
    ingredientes:    List[IngredienteReceta]
    advertencias:    List[str]


@dataclass
class ConsumoRecetaResult:
    producto_id:     int
    cantidad:        float
    ingredientes_consumidos: List[Dict]   # {ingrediente_id, nombre, cantidad}
    movimiento_ids:  List[int]


@dataclass
class RecetaTransformacion:
    """Receta de transformación: cómo se corta un pollo en piezas vendibles."""
    id:             int
    nombre_receta:  str
    producto_base:  str
    activa:         bool
    detalles:       List[Dict]  # {producto_resultado, rendimiento_pct, merma_pct}


# ── Engine ────────────────────────────────────────────────────────────────────

class RecipeEngine:
    """
    Motor de gestión de recetas.

    Uso como handler de eventos (suscribir al EventBus):
        engine = RecipeEngine(conn, sucursal_id=1)
        EventBus.subscribe("VENTA_COMPLETADA", engine.handle_venta_completada)

    Uso directo (validación antes de venta):
        ok = engine.validar_stock_receta(producto_id, cantidad)
        sim = engine.simular_rendimiento(producto_id, cantidad)
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

    # ── Handler EventBus ──────────────────────────────────────────────────────

    def handle_venta_completada(self, payload: dict) -> None:
        """
        Handler suscrito a VENTA_COMPLETADA.

        SalesEngine ya descuenta inventario inline. Este handler hace:
          - Verifica que los consumos sean coherentes (auditoría)
          - Detecta si algún producto compuesto bajó de stock mínimo
          - Publica STOCK_BAJO_MINIMO si corresponde
          - Alimenta el dataset de forecast con consumo real del día

        El payload esperado es el de SalesEngine.ResultadoVenta serializado.
        """
        venta_id    = payload.get("venta_id")
        sucursal_id = payload.get("sucursal_id", self.sucursal_id)
        items       = payload.get("items", [])

        if not venta_id or not items:
            return

        for item in items:
            producto_id = item.get("producto_id")
            if not producto_id:
                continue
            try:
                self._verificar_stock_minimo(producto_id, sucursal_id)
            except Exception as exc:
                logger.error("Error verificando stock mínimo prod=%d: %s", producto_id, exc)

    # ── Validación pre-venta ───────────────────────────────────────────────────

    def validar_stock_receta(self, producto_id: int, cantidad: float) -> bool:
        """
        Verifica si hay stock suficiente para vender `cantidad` unidades
        del producto compuesto (validando todos sus ingredientes).

        Retorna True si hay stock suficiente, False si hay deficiencia.
        Útil para mostrar advertencia temprana en el carrito de ventas.
        """
        receta = self._get_receta_abarrotes(producto_id)
        if not receta:
            # Producto simple o pollo (manejado por InventoryEngine) → asumir OK
            return True

        for ingrediente_id, _nombre, ratio, merma, _unidad in receta:
            requerido = cantidad * ratio * (1 + merma / 100)
            stock = self._get_stock_ingrediente(ingrediente_id)
            if stock < requerido:
                return False
        return True

    # ── Simulación de rendimiento ─────────────────────────────────────────────

    def simular_rendimiento(
        self,
        producto_id: int,
        cantidad:    float,
    ) -> SimulacionResult:
        """
        Simula el consumo de ingredientes para producir `cantidad` unidades
        de producto_id SIN modificar stock.

        Retorna SimulacionResult con lista de ingredientes, cantidades requeridas
        y si hay stock suficiente para cada uno.

        Útil para:
          - Preview en carrito de ventas antes de confirmar
          - Planificación de producción en ModuloInventarioEnterprise
        """
        row_prod = self.conn.execute(
            "SELECT nombre FROM productos WHERE id=?", (producto_id,)
        ).fetchone()
        nombre_prod = row_prod[0] if row_prod else f"Producto#{producto_id}"

        receta = self._get_receta_abarrotes(producto_id)
        if not receta:
            # Producto sin receta abarrotes: pollo maneja internamente
            return SimulacionResult(
                producto_id=producto_id,
                producto_nombre=nombre_prod,
                cantidad=cantidad,
                factible=True,
                ingredientes=[],
                advertencias=["Producto simple o pollo — sin receta de ingredientes."],
            )

        ingredientes: List[IngredienteReceta] = []
        advertencias: List[str] = []
        factible = True

        for ingrediente_id, nombre_ing, ratio, merma, unidad in receta:
            requerido = round(cantidad * ratio * (1 + merma / 100), 6)
            stock     = self._get_stock_ingrediente(ingrediente_id)
            suficiente = stock >= requerido

            if not suficiente:
                factible = False
                deficit  = round(requerido - stock, 4)
                advertencias.append(
                    f"⚠ Stock insuficiente '{nombre_ing}': "
                    f"necesario={requerido:.3f}, disponible={stock:.3f} "
                    f"(déficit={deficit:.3f} {unidad})"
                )

            ingredientes.append(IngredienteReceta(
                ingrediente_id=ingrediente_id,
                nombre=nombre_ing,
                ratio=ratio,
                merma=merma,
                unidad=unidad,
                stock_disponible=stock,
                requerido=requerido,
                stock_suficiente=suficiente,
            ))

        return SimulacionResult(
            producto_id=producto_id,
            producto_nombre=nombre_prod,
            cantidad=cantidad,
            factible=factible,
            ingredientes=ingredientes,
            advertencias=advertencias,
        )

    # ── Consumo de recetas de abarrotes (no-pollo) ────────────────────────────

    def consumir_receta_abarrotes(
        self,
        producto_id: int,
        cantidad:    float,
        venta_id:    Optional[int] = None,
        descripcion: str           = "",
    ) -> ConsumoRecetaResult:
        """
        Descuenta ingredientes de abarrotes para fabricar `cantidad` unidades
        del producto_id compuesto.

        Este método es llamado por SalesEngine si el producto tiene receta
        de tipo 'ingredientes' (no pollo).

        Fórmula: consumo_i = cantidad × ratio_i × (1 + merma_i/100)
        """
        receta = self._get_receta_abarrotes(producto_id)
        if not receta:
            raise ValueError(
                f"Producto id={producto_id} no tiene receta de ingredientes activa."
            )

        ingredientes_consumidos = []
        movimiento_ids          = []

        with self.conn:
            for ingrediente_id, nombre_ing, ratio, merma, unidad in receta:
                consumo = round(cantidad * ratio * (1 + merma / 100), 6)

                # Verificar stock
                stock = self._get_stock_ingrediente(ingrediente_id)
                if stock < consumo - 1e-6:
                    from core.services.inventory_engine import StockInsuficienteError
                    raise StockInsuficienteError(
                        ingrediente_id, nombre_ing, stock, consumo,
                    )

                # Descontar de productos.existencia directamente (abarrotes no usan BIB)
                existencia_antes = stock
                existencia_nueva = round(existencia_antes - consumo, 6)

                self.conn.execute(
                    "UPDATE productos SET existencia=? WHERE id=?",
                    (existencia_nueva, ingrediente_id),
                )

                # Movimiento de auditoría
                op_uuid = str(uuid.uuid4())
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
                        ingrediente_id, "SALIDA", "consumo_receta", consumo,
                        existencia_antes, existencia_nueva,
                        descripcion or f"Consumo receta prod#{producto_id} — {nombre_ing}",
                        self.usuario, self.sucursal_id,
                        venta_id, "venta", op_uuid,
                    ),
                )
                mov_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                movimiento_ids.append(mov_id)

                ingredientes_consumidos.append({
                    "ingrediente_id": ingrediente_id,
                    "nombre":         nombre_ing,
                    "cantidad":       consumo,
                    "unidad":         unidad,
                })

        logger.info(
            "Consumo receta abarrotes: prod=%d qty=%.3f venta=%s → %d ingredientes",
            producto_id, cantidad, venta_id, len(ingredientes_consumidos),
        )

        return ConsumoRecetaResult(
            producto_id=producto_id,
            cantidad=cantidad,
            ingredientes_consumidos=ingredientes_consumidos,
            movimiento_ids=movimiento_ids,
        )

    # ── Recetas de transformación de pollo ────────────────────────────────────

    def listar_recetas_pollo(self, solo_activas: bool = True) -> List[RecetaTransformacion]:
        """Retorna recetas de transformación (cómo se corta el pollo en piezas)."""
        q = """
            SELECT rp.id, rp.nombre_receta, COALESCE(p.nombre,'?') as prod_base,
                   rp.activa
            FROM recetas_pollo rp
            LEFT JOIN productos p ON p.id = rp.producto_base_id
        """
        params = []
        if solo_activas:
            q += " WHERE rp.activa = 1"
        q += " ORDER BY rp.nombre_receta"

        rows = self.conn.execute(q, params).fetchall()
        result = []
        for row in rows:
            detalles = self.conn.execute(
                """
                SELECT rpd.producto_resultado_id, pr.nombre,
                       rpd.porcentaje_rendimiento, rpd.porcentaje_merma
                FROM recetas_pollo_detalle rpd
                JOIN productos pr ON pr.id = rpd.producto_resultado_id
                WHERE rpd.receta_id = ?
                ORDER BY rpd.orden
                """,
                (row[0],),
            ).fetchall()
            result.append(RecetaTransformacion(
                id=row[0],
                nombre_receta=row[1],
                producto_base=row[2],
                activa=bool(row[3]),
                detalles=[
                    {
                        "producto_resultado_id": d[0],
                        "nombre": d[1],
                        "rendimiento_pct": float(d[2]),
                        "merma_pct": float(d[3]),
                    }
                    for d in detalles
                ],
            ))
        return result

    def obtener_receta_completa(self, producto_id: int) -> Optional[Dict]:
        """
        Retorna receta de ingredientes + disponibilidad de stock.
        Útil para mostrar en la UI de Productos → Tab Recetas.
        """
        receta = self._get_receta_abarrotes(producto_id)
        if not receta:
            return None

        row_prod = self.conn.execute(
            "SELECT nombre, existencia FROM productos WHERE id=?", (producto_id,)
        ).fetchone()

        ingredientes_detalle = []
        for ing_id, nombre, ratio, merma, unidad in receta:
            stock = self._get_stock_ingrediente(ing_id)
            # Cuántas unidades del producto compuesto se pueden producir con este stock
            if ratio > 0:
                max_con_este_ing = stock / (ratio * (1 + merma / 100))
            else:
                max_con_este_ing = 0
            ingredientes_detalle.append({
                "ingrediente_id": ing_id,
                "nombre":         nombre,
                "ratio":          ratio,
                "merma_pct":      merma,
                "unidad":         unidad,
                "stock_actual":   stock,
                "max_produccion": round(max_con_este_ing, 3),
                "stock_ok":       stock > 0,
            })

        max_global = min(
            (ing["max_produccion"] for ing in ingredientes_detalle),
            default=0,
        )

        return {
            "producto_id":     producto_id,
            "producto_nombre": row_prod[0] if row_prod else "?",
            "ingredientes":    ingredientes_detalle,
            "max_produccion":  round(max_global, 3),
        }

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _get_receta_abarrotes(
        self, producto_id: int
    ) -> List[Tuple[int, str, float, float, str]]:
        """
        Retorna la receta de ingredientes activa del producto.
        Tabla: product_recipes_abarrotes (o product_recipes con columnas ratio/merma).

        Retorna lista de (ingrediente_id, nombre, ratio, merma, unidad)
        o [] si el producto no tiene receta de ingredientes.
        """
        # Primero intentar tabla product_recipes_abarrotes si existe
        try:
            rows = self.conn.execute(
                """
                SELECT pra.ingrediente_id, p.nombre, pra.ratio, pra.merma, pra.unidad
                FROM product_recipes_abarrotes pra
                JOIN productos p ON p.id = pra.ingrediente_id
                WHERE pra.producto_id = ? AND pra.activo = 1
                ORDER BY pra.id
                """,
                (producto_id,),
            ).fetchall()
            if rows:
                return [(r[0], r[1], float(r[2]), float(r[3]), r[4]) for r in rows]
        except sqlite3.OperationalError:
            pass  # tabla no existe aún

        # Fallback: ninguna receta de abarrotes para este producto
        return []

    def _get_stock_ingrediente(self, ingrediente_id: int) -> float:
        """Obtiene stock de un ingrediente (abarrote) desde productos.existencia."""
        row = self.conn.execute(
            "SELECT COALESCE(existencia, 0) FROM productos WHERE id=?",
            (ingrediente_id,),
        ).fetchone()
        return float(row[0]) if row else 0.0

    def _verificar_stock_minimo(
        self,
        producto_id: int,
        sucursal_id: int,
    ) -> None:
        """Verifica si el stock del producto bajó del mínimo y publica alerta."""
        row = self.conn.execute(
            "SELECT nombre, existencia, stock_minimo FROM productos WHERE id=?",
            (producto_id,),
        ).fetchone()
        if not row:
            return

        nombre, existencia, minimo = row[1], float(row[1] or 0), float(row[2] or 0)
        if minimo > 0 and existencia <= minimo:
            try:
                from core.events.event_bus import get_bus, STOCK_BAJO_MINIMO
                # Calcular demanda diaria promedio (últimos 7 días)
                row_dem = self.conn.execute(
                    """
                    SELECT COALESCE(AVG(dv.cantidad), 0)
                    FROM detalles_venta dv
                    JOIN ventas v ON v.id = dv.venta_id
                    WHERE dv.producto_id = ?
                      AND v.fecha >= date('now', '-7 days')
                      AND v.estado = 'completada'
                    """,
                    (producto_id,),
                ).fetchone()
                demanda_diaria = float(row_dem[0]) if row_dem else 0
                dias_restantes = round(existencia / demanda_diaria, 1) if demanda_diaria > 0 else None

                get_bus().publish(STOCK_BAJO_MINIMO, {
                    "producto_id":      producto_id,
                    "producto_nombre":  nombre,
                    "existencia_actual": existencia,
                    "stock_minimo":     minimo,
                    "sucursal_id":      sucursal_id,
                    "dias_restantes":   dias_restantes,
                    "demanda_diaria":   demanda_diaria,
                })
                logger.warning(
                    "STOCK_BAJO_MINIMO prod=%d '%s': %.3f ≤ %.3f",
                    producto_id, nombre, existencia, minimo,
                )
            except Exception as exc:
                logger.warning("STOCK_BAJO_MINIMO event falló: %s", exc)

    def suscribir_al_bus(self) -> None:
        """
        Registra el handler handle_venta_completada en el EventBus global.
        Llamar UNA VEZ al inicializar el sistema en main.py.

        Ejemplo:
            recipe_engine = RecipeEngine(get_db().raw, sucursal_id, usuario)
            recipe_engine.suscribir_al_bus()
        """
        try:
            from core.events.event_bus import get_bus, VENTA_COMPLETADA
            get_bus().subscribe(
                VENTA_COMPLETADA,
                self.handle_venta_completada,
                priority=80,  # Ejecuta antes de forecast (50) y loyalty (90)
                label="RecipeEngine.handle_venta_completada",
            )
            logger.info("RecipeEngine suscrito a VENTA_COMPLETADA")
        except Exception as exc:
            logger.warning("RecipeEngine.suscribir_al_bus falló: %s", exc)

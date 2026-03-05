# core/services/product_recipe_repository.py
# ── ProductRecipeRepository — SPJ Enterprise ──────────────────────────────────
# Acceso a datos de recetas de consumo de producto.
# REGLA: Solo este repositorio toca product_recipes.
#        InventoryEngine consume get_recipe() para hacer FIFO proporcional.
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional

from core.database import Connection

logger = logging.getLogger("spj.product_recipe")


# ── DTOs ──────────────────────────────────────────────────────────────────────

@dataclass
class RecipeItem:
    """Un componente dentro de una receta de consumo."""
    id:               int
    product_id:       int       # producto padre (surtido)
    piece_product_id: int       # pieza interna (pierna, pechuga…)
    piece_name:       str
    percentage:       float     # 0 < x <= 100
    orden:            int
    active:           bool


@dataclass
class ProductRecipe:
    """Receta completa de consumo de un producto."""
    product_id:   int
    product_name: str
    items:        List[RecipeItem] = field(default_factory=list)

    @property
    def total_percentage(self) -> float:
        return sum(i.percentage for i in self.items)

    @property
    def is_valid(self) -> bool:
        """Válida si tiene items activos y la suma es ~100%."""
        active = [i for i in self.items if i.active]
        return bool(active) and abs(sum(i.percentage for i in active) - 100.0) < 0.01


# ── Repository ────────────────────────────────────────────────────────────────

class ProductRecipeRepository:
    """
    CRUD de product_recipes.

    Uso:
        repo = ProductRecipeRepository(db)
        recipe = repo.get_recipe(product_id)
        if recipe and recipe.is_valid:
            ...
    """

    def __init__(self, db: Connection):
        self.db = db

    # ── Lectura ───────────────────────────────────────────────────────────────

    def has_recipe(self, product_id: int) -> bool:
        """Retorna True si el producto tiene al menos una receta activa válida."""
        n = self.db.fetchscalar(
            "SELECT COUNT(*) FROM product_recipes WHERE product_id=? AND active=1",
            (product_id,),
            default=0,
        )
        return int(n) > 0

    def get_recipe(self, product_id: int) -> Optional[ProductRecipe]:
        """
        Retorna la receta activa del producto, o None si no existe.
        Solo devuelve la receta si la suma de porcentajes es ~100%.
        """
        nombre = self.db.fetchscalar(
            "SELECT nombre FROM productos WHERE id=?",
            (product_id,),
            default=None,
        )
        if nombre is None:
            return None

        rows = self.db.fetchall(
            """
            SELECT pr.id,
                   pr.product_id,
                   pr.piece_product_id,
                   COALESCE(pr.piece_name, p.nombre, '') AS piece_name,
                   pr.percentage,
                   pr.orden,
                   pr.active
            FROM product_recipes pr
            LEFT JOIN productos p ON p.id = pr.piece_product_id
            WHERE pr.product_id = ? AND pr.active = 1
            ORDER BY pr.orden ASC, pr.id ASC
            """,
            (product_id,),
        )

        if not rows:
            return None

        recipe = ProductRecipe(
            product_id=product_id,
            product_name=str(nombre),
        )
        for r in rows:
            recipe.items.append(RecipeItem(
                id=int(r[0]),
                product_id=int(r[1]),
                piece_product_id=int(r[2]),
                piece_name=str(r[3]),
                percentage=float(r[4]),
                orden=int(r[5]),
                active=bool(r[6]),
            ))

        return recipe if recipe.is_valid else None

    def list_products_with_recipes(self) -> List[dict]:
        """Lista todos los productos que tienen receta activa."""
        rows = self.db.fetchall(
            """
            SELECT DISTINCT p.id, p.nombre
            FROM product_recipes pr
            JOIN productos p ON p.id = pr.product_id
            WHERE pr.active = 1 AND p.activo = 1
            ORDER BY p.nombre
            """
        )
        return [{"id": int(r[0]), "nombre": str(r[1])} for r in (rows or [])]

    def get_recipe_raw(self, product_id: int) -> List[dict]:
        """
        Retorna todas las filas (activas e inactivas) para el editor UI.
        Incluye nombre de pieza desde productos.
        """
        rows = self.db.fetchall(
            """
            SELECT pr.id,
                   pr.piece_product_id,
                   COALESCE(pr.piece_name, p.nombre, 'Sin nombre') AS piece_name,
                   pr.percentage,
                   pr.orden,
                   pr.active
            FROM product_recipes pr
            LEFT JOIN productos p ON p.id = pr.piece_product_id
            WHERE pr.product_id = ?
            ORDER BY pr.orden ASC, pr.id ASC
            """,
            (product_id,),
        )
        return [
            {
                "id":               int(r[0]),
                "piece_product_id": int(r[1]),
                "piece_name":       str(r[2]),
                "percentage":       float(r[3]),
                "orden":            int(r[4]),
                "active":           bool(r[5]),
            }
            for r in (rows or [])
        ]

    # ── Escritura ─────────────────────────────────────────────────────────────

    def save_recipe(self, product_id: int, items: List[dict]) -> None:
        """
        Reemplaza la receta de consumo de un producto de forma atómica.

        items = [
            {"piece_product_id": 2, "piece_name": "Pierna", "percentage": 40.0, "orden": 0},
            {"piece_product_id": 3, "piece_name": "Pechuga", "percentage": 35.0, "orden": 1},
            ...
        ]

        Validaciones:
        - Al menos 1 item.
        - Cada percentage > 0.
        - Suma total ~100% (tolerancia ±0.1).
        - piece_product_id distinto del product_id.
        Lanza ValueError en caso de violación.
        """
        if not items:
            raise ValueError("La receta debe tener al menos un componente.")

        for idx, item in enumerate(items):
            pct = float(item.get("percentage", 0))
            if pct <= 0:
                raise ValueError(
                    f"El porcentaje del ítem {idx+1} debe ser mayor a 0 "
                    f"(recibido: {pct})."
                )
            if int(item["piece_product_id"]) == int(product_id):
                raise ValueError(
                    "Un producto no puede ser componente de sí mismo."
                )

        total = sum(float(i.get("percentage", 0)) for i in items)
        if abs(total - 100.0) > 0.1:
            raise ValueError(
                f"La suma de porcentajes debe ser 100% "
                f"(actual: {total:.2f}%). Diferencia: {total - 100:.2f}%."
            )

        with self.db.transaction():
            # Soft-delete todos los items activos anteriores
            self.db.execute(
                "UPDATE product_recipes SET active=0, updated_at=datetime('now') "
                "WHERE product_id=? AND active=1",
                (product_id,),
            )
            # Insertar nuevos items
            for orden, item in enumerate(items):
                pid     = int(item["piece_product_id"])
                pct     = float(item["percentage"])
                nombre  = str(item.get("piece_name", "")).strip()
                if not nombre:
                    # Obtener nombre desde BD
                    nombre = str(self.db.fetchscalar(
                        "SELECT nombre FROM productos WHERE id=?",
                        (pid,), default="",
                    ))
                self.db.execute(
                    """
                    INSERT INTO product_recipes
                        (product_id, piece_product_id, percentage, piece_name, orden,
                         active, created_at, updated_at)
                    VALUES (?,?,?,?,?,1,datetime('now'),datetime('now'))
                    """,
                    (product_id, pid, pct, nombre, orden),
                )

        logger.info(
            "Receta consumo guardada: product_id=%d | items=%d | total=%.2f%%",
            product_id, len(items), total,
        )

    def delete_recipe(self, product_id: int) -> None:
        """Soft-delete de toda la receta activa."""
        with self.db.transaction():
            self.db.execute(
                "UPDATE product_recipes SET active=0, updated_at=datetime('now') "
                "WHERE product_id=? AND active=1",
                (product_id,),
            )
        logger.info("Receta consumo eliminada: product_id=%d", product_id)

    # ── Validación ────────────────────────────────────────────────────────────

    def validate_recipe(self, product_id: int) -> dict:
        """
        Valida la receta activa.
        Retorna:
            {"valid": bool, "total": float, "errors": [str], "warnings": [str]}
        """
        rows = self.db.fetchall(
            """
            SELECT pr.piece_product_id,
                   COALESCE(pr.piece_name, p.nombre, '?') AS piece_name,
                   pr.percentage,
                   p.id AS pid_real
            FROM product_recipes pr
            LEFT JOIN productos p ON p.id = pr.piece_product_id
            WHERE pr.product_id = ? AND pr.active = 1
            """,
            (product_id,),
        )

        errors:   List[str] = []
        warnings: List[str] = []

        if not rows:
            errors.append("El producto no tiene receta de consumo activa.")
            return {"valid": False, "total": 0.0, "errors": errors, "warnings": warnings}

        total = 0.0
        for r in rows:
            pid_real, piece_name, pct = r[3], r[1], float(r[2])
            total += pct
            if pid_real is None:
                errors.append(
                    f"La pieza '{piece_name}' (ID {r[0]}) no existe en productos."
                )
            if pct <= 0:
                errors.append(f"Porcentaje de '{piece_name}' debe ser > 0.")

        if abs(total - 100.0) > 0.1:
            errors.append(
                f"La suma de porcentajes es {total:.2f}% (debe ser 100%)."
            )

        if total < 95:
            warnings.append(
                f"La suma es {total:.1f}%. El {100-total:.1f}% restante "
                "no se descontará de ninguna pieza."
            )

        return {
            "valid":    len(errors) == 0,
            "total":    total,
            "errors":   errors,
            "warnings": warnings,
        }

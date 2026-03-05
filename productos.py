# repositories/productos.py
# ── ProductRepository — Enterprise Repository Layer ──────────────────────────
# All DB access for productos goes through this class.
# No SQL in UI modules. Enforces soft-delete, unique names, deletion guards.
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Dict, List, Optional

from core.events.event_bus import EventBus

logger = logging.getLogger("spj.repositories.productos")

PRODUCTO_CREADO     = "PRODUCTO_CREADO"
PRODUCTO_ACTUALIZADO = "PRODUCTO_ACTUALIZADO"
PRODUCTO_ELIMINADO  = "PRODUCTO_ELIMINADO"


class ProductoDeletionError(Exception):
    pass


class ProductoNombreDuplicadoError(Exception):
    pass


class ProductoRepository:

    def __init__(self, db):
        self.db = db

    def _now(self) -> str:
        return datetime.utcnow().isoformat()

    # ── Read ─────────────────────────────────────────────────────────────────

    def get_all(self, *, include_inactive: bool = False, categoria: str = "",
                search: str = "") -> List[Dict]:
        conditions = []
        params: List = []
        if not include_inactive:
            conditions.append("p.is_active = 1")
        if categoria:
            conditions.append("p.categoria = ?")
            params.append(categoria)
        if search:
            conditions.append("(p.nombre LIKE ? OR p.categoria LIKE ?)")
            params.extend([f"%{search}%", f"%{search}%"])
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = self.db.fetchall(f"""
            SELECT p.id, p.nombre, p.precio, p.existencia, p.stock_minimo,
                   p.unidad, p.categoria, p.oculto, p.es_compuesto,
                   p.es_subproducto, p.is_active, p.deleted_at,
                   p.imagen_path, p.tipo_producto,
                   COALESCE(p.stock_minimo,0) AS stock_min_val,
                   p.producto_padre_id
            FROM productos p
            {where}
            ORDER BY p.nombre
        """, params)
        return [dict(r) for r in rows]

    def get_by_id(self, producto_id: int) -> Optional[Dict]:
        row = self.db.fetchone("SELECT * FROM productos WHERE id = ?", (producto_id,))
        return dict(row) if row else None

    def get_categories(self) -> List[str]:
        rows = self.db.fetchall("""
            SELECT DISTINCT categoria FROM productos
            WHERE categoria IS NOT NULL AND categoria != '' AND is_active = 1
            ORDER BY categoria
        """)
        return [r["categoria"] for r in rows]

    def get_for_sale(self, search: str = "") -> List[Dict]:
        params: List = []
        where_extra = ""
        if search:
            where_extra = "AND (p.nombre LIKE ? OR p.categoria LIKE ?)"
            params.extend([f"%{search}%", f"%{search}%"])
        rows = self.db.fetchall(f"""
            SELECT p.id, p.nombre, p.precio, p.existencia,
                   p.unidad, p.categoria, p.imagen_path,
                   p.es_compuesto, p.es_subproducto
            FROM productos p
            WHERE p.is_active = 1 AND p.oculto = 0
            {where_extra}
            ORDER BY p.nombre
        """, params)
        return [dict(r) for r in rows]

    def check_name_available(self, nombre: str,
                              exclude_id: Optional[int] = None) -> bool:
        normalised = nombre.strip().lower()
        if exclude_id:
            row = self.db.fetchone("""
                SELECT id FROM productos
                WHERE nombre_normalizado = ? AND id != ? AND is_active = 1
            """, (normalised, exclude_id))
        else:
            row = self.db.fetchone("""
                SELECT id FROM productos
                WHERE nombre_normalizado = ? AND is_active = 1
            """, (normalised,))
        return row is None

    def has_sales(self, producto_id: int) -> bool:
        row = self.db.fetchone("""
            SELECT COUNT(*) AS c FROM detalles_venta WHERE producto_id = ?
        """, (producto_id,))
        return (row["c"] if row else 0) > 0

    def has_movements(self, producto_id: int) -> bool:
        row = self.db.fetchone("""
            SELECT COUNT(*) AS c FROM batch_movements WHERE product_id = ?
        """, (producto_id,))
        return (row["c"] if row else 0) > 0

    def has_recipes(self, producto_id: int) -> bool:
        row = self.db.fetchone("""
            SELECT COUNT(*) AS c FROM product_recipe_components
            WHERE component_product_id = ?
        """, (producto_id,))
        if row and row["c"] > 0:
            return True
        row2 = self.db.fetchone("""
            SELECT COUNT(*) AS c FROM product_recipes WHERE base_product_id = ?
        """, (producto_id,))
        return (row2["c"] if row2 else 0) > 0

    # ── Write ────────────────────────────────────────────────────────────────

    def create(self, data: Dict, usuario: str) -> int:
        nombre = data.get("nombre", "").strip()
        if not nombre:
            raise ValueError("NOMBRE_OBLIGATORIO")

        if not self.check_name_available(nombre):
            raise ProductoNombreDuplicadoError(f"NOMBRE_DUPLICADO: {nombre}")

        normalised = nombre.lower()

        with self.db.transaction("PRODUCTO_CREATE"):
            # Prevent seeding: skip if exact row exists even inactive
            existing = self.db.fetchone(
                "SELECT id FROM productos WHERE nombre_normalizado = ?",
                (normalised,)
            )
            if existing:
                raise ProductoNombreDuplicadoError(f"NOMBRE_DUPLICADO: {nombre}")

            self.db.execute("""
                INSERT INTO productos (
                    nombre, nombre_normalizado, precio, existencia,
                    stock_minimo, unidad, categoria, oculto,
                    es_compuesto, es_subproducto, producto_padre_id,
                    imagen_path, is_active, tipo_producto
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,1,?)
            """, (
                nombre, normalised,
                data.get("precio", 0),
                data.get("existencia", 0),
                data.get("stock_minimo", 0),
                data.get("unidad", "kg"),
                data.get("categoria", ""),
                1 if data.get("oculto") else 0,
                1 if data.get("es_compuesto") else 0,
                1 if data.get("es_subproducto") else 0,
                data.get("producto_padre_id"),
                data.get("imagen_path"),
                data.get("tipo_producto", "simple"),
            ))
            row = self.db.fetchone(
                "SELECT id FROM productos WHERE nombre_normalizado = ? ORDER BY id DESC LIMIT 1",
                (normalised,)
            )
            producto_id = row["id"]

        self._write_audit("CREATE", str(producto_id), data, usuario)
        EventBus.publish(PRODUCTO_CREADO, {"producto_id": producto_id, "nombre": nombre})
        return producto_id

    def update(self, producto_id: int, data: Dict, usuario: str) -> None:
        nombre = data.get("nombre", "").strip()
        if not nombre:
            raise ValueError("NOMBRE_OBLIGATORIO")

        if not self.check_name_available(nombre, exclude_id=producto_id):
            raise ProductoNombreDuplicadoError(f"NOMBRE_DUPLICADO: {nombre}")

        normalised = nombre.lower()

        with self.db.transaction("PRODUCTO_UPDATE"):
            self.db.execute("""
                UPDATE productos SET
                    nombre = ?,
                    nombre_normalizado = ?,
                    precio = ?,
                    existencia = ?,
                    stock_minimo = ?,
                    unidad = ?,
                    categoria = ?,
                    oculto = ?,
                    es_compuesto = ?,
                    es_subproducto = ?,
                    producto_padre_id = ?,
                    imagen_path = ?,
                    tipo_producto = ?,
                    fecha_actualizacion = ?
                WHERE id = ?
            """, (
                nombre, normalised,
                data.get("precio", 0),
                data.get("existencia", 0),
                data.get("stock_minimo", 0),
                data.get("unidad", "kg"),
                data.get("categoria", ""),
                1 if data.get("oculto") else 0,
                1 if data.get("es_compuesto") else 0,
                1 if data.get("es_subproducto") else 0,
                data.get("producto_padre_id"),
                data.get("imagen_path"),
                data.get("tipo_producto", "simple"),
                self._now(),
                producto_id,
            ))

        self._write_audit("UPDATE", str(producto_id), data, usuario)
        EventBus.publish(PRODUCTO_ACTUALIZADO, {
            "producto_id": producto_id, "nombre": nombre
        })

    def set_visibility(self, producto_id: int, oculto: bool,
                       usuario: str) -> None:
        with self.db.transaction("PRODUCTO_VISIBILITY"):
            self.db.execute(
                "UPDATE productos SET oculto = ? WHERE id = ?",
                (1 if oculto else 0, producto_id)
            )
        EventBus.publish(PRODUCTO_ACTUALIZADO, {
            "producto_id": producto_id,
            "action": "visibility",
            "oculto": oculto,
        })

    def soft_delete(self, producto_id: int, usuario: str) -> None:
        if self.has_sales(producto_id):
            raise ProductoDeletionError("TIENE_VENTAS")
        if self.has_movements(producto_id):
            raise ProductoDeletionError("TIENE_MOVIMIENTOS")
        if self.has_recipes(producto_id):
            raise ProductoDeletionError("TIENE_RECETAS")

        with self.db.transaction("PRODUCTO_DELETE"):
            self.db.execute("""
                UPDATE productos SET
                    is_active = 0,
                    deleted_at = ?,
                    deleted_by = ?,
                    oculto = 1
                WHERE id = ?
            """, (self._now(), usuario, producto_id))

        self._write_audit("DELETE", str(producto_id), {}, usuario)
        EventBus.publish(PRODUCTO_ELIMINADO, {
            "producto_id": producto_id, "usuario": usuario
        })

    def update_stock(self, producto_id: int, new_qty: float,
                     conn=None) -> None:
        db = conn if conn is not None else self.db
        db.execute(
            "UPDATE productos SET existencia = ? WHERE id = ?",
            (new_qty, producto_id)
        )

    # ── Internals ─────────────────────────────────────────────────────────────

    def _write_audit(self, action: str, entity_id: str,
                     data: Dict, usuario: str) -> None:
        import json
        try:
            self.db.execute("""
                INSERT INTO json_audit_log (event_type, entity_type, entity_id, payload, usuario)
                VALUES (?,?,?,?,?)
            """, (
                f"PRODUCTO_{action}",
                "productos",
                entity_id,
                json.dumps({k: v for k, v in data.items()
                            if not isinstance(v, bytes)},
                           default=str),
                usuario,
            ))
        except Exception as exc:
            logger.warning("audit_log write failed: %s", exc)

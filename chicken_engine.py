# core/services/chicken_engine.py
# ChickenEngine — Motor enterprise para operaciones de pollo.
# Compra global, recepción sucursal, transformación por receta, conciliación.
# Usa InventoryEngine (FIFO) cuando está disponible; fallback a InventoryService.
from __future__ import annotations
import sqlite3
import uuid
import logging
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger("spj.chicken")

# ── Importar motor FIFO (preferido) o legacy ─────────────────────────────────
try:
    from core.database import get_db, Connection
    from core.services.inventory_engine import (
        InventoryEngine, InventarioError, StockInsuficienteError
    )
    _FIFO_AVAILABLE = True
except ImportError:
    _FIFO_AVAILABLE = False
    from core.services.inventory_service import (
        InventoryService as InventoryEngine,
        InventarioError, StockInsuficienteError,
    )

try:
    from core.db.connection import transaction
except ImportError:
    from contextlib import contextmanager
    @contextmanager
    def transaction(conn=None):
        yield conn


# ── Excepciones ───────────────────────────────────────────────────────────────

class ChickenError(Exception):
    pass

class RecetaNoEncontradaError(ChickenError):
    pass


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class ResultadoTransformacion:
    movimiento_ids: List[int]
    kg_procesados:  float
    kg_merma:       float
    cortes:         List[dict] = field(default_factory=list)


@dataclass
class ResultadoConciliacion:
    global_kg:          float
    global_pollos:      int
    total_sucursales_kg: float
    diferencia_kg:      float
    sucursales:         List[dict] = field(default_factory=list)


# ══════════════════════════════════════════════════════════════════════════════

class ChickenEngine:
    """
    Orquesta todas las operaciones de pollo.
    Usa InventoryEngine FIFO cuando está disponible.
    """

    def __init__(
        self,
        conn,              # sqlite3.Connection o core.database.Connection
        usuario:    str = "Sistema",
        sucursal_id: int = 1,
    ):
        self.conn        = conn
        self.usuario     = usuario or "Sistema"
        self.sucursal_id = sucursal_id

        # Construir motor de inventario apropiado
        if _FIFO_AVAILABLE and hasattr(conn, "fetchone"):
            # conn es core.database.Connection
            self._inv = InventoryEngine(conn, usuario, sucursal_id)
            self._raw = conn.raw
        elif _FIFO_AVAILABLE:
            # conn es sqlite3.Connection — envolverlo
            from core.database import Connection as _Conn
            _db = _Conn(conn)
            self._inv = InventoryEngine(_db, usuario, sucursal_id)
            self._raw = conn
        else:
            self._inv = InventoryEngine(conn, usuario, sucursal_id)
            self._raw = conn

    # ── Compra global ─────────────────────────────────────────────────────────

    def registrar_compra_global(
        self,
        numero_pollos: int,
        peso_total_kg: float,
        costo_total: float,
        proveedor: str = "",
        producto_base_id: int = None,
        sucursal_destino_id: int = None,
        notas: str = "",
    ) -> int:
        if numero_pollos < 0:
            raise ChickenError("número de pollos no puede ser negativo")
        if peso_total_kg <= 0:
            raise ChickenError("peso total debe ser > 0 kg")
        if costo_total <= 0:
            raise ChickenError("costo total debe ser > 0")

        costo_por_kg = round(costo_total / peso_total_kg, 4)
        lote_id = str(uuid.uuid4())[:16].upper()

        cur = self._raw.execute(
            """
            INSERT INTO compras_pollo_global
                (fecha, proveedor, numero_pollos, peso_total_kg,
                 costo_total, costo_por_kg, lote_id, estado,
                 usuario_registro, sucursal_destino_id, notas)
            VALUES (date('now'),?,?,?,?,?,?,'activo',?,?,?)
            """,
            (proveedor, numero_pollos, peso_total_kg,
             costo_total, costo_por_kg, lote_id,
             self.usuario, sucursal_destino_id, notas),
        )
        compra_id = cur.lastrowid

        if producto_base_id:
            if _FIFO_AVAILABLE and isinstance(self._inv, InventoryEngine):
                self._inv.recepcionar_lote(
                    producto_id=producto_base_id,
                    numero_pollos=numero_pollos,
                    peso_kg=peso_total_kg,
                    costo_kg=costo_por_kg,
                    proveedor=proveedor,
                    compra_global_id=compra_id,
                    notas=notas,
                )
            else:
                self._inv.registrar_entrada(
                    producto_id=producto_base_id,
                    cantidad=peso_total_kg,
                    descripcion=f"Compra global #{compra_id} — {numero_pollos} pollos",
                    referencia=f"CG:{compra_id}",
                    costo_unitario=costo_por_kg,
                )

        self._raw.commit()
        logger.info("Compra global #%d: %d pollos %.3fkg $%.2f", compra_id, numero_pollos, peso_total_kg, costo_total)
        return compra_id

    # ── Recepción en sucursal ─────────────────────────────────────────────────

    def recepcionar_en_sucursal(
        self,
        numero_pollos:    int,
        peso_kg:          float,
        producto_base_id: int,
        compra_global_id: int   = None,
        costo_kg:         float = 0.0,
    ) -> int:
        if numero_pollos < 0:
            raise ChickenError("número de pollos no puede ser negativo")
        if peso_kg <= 0:
            raise ChickenError("peso debe ser > 0 kg")

        if _FIFO_AVAILABLE and isinstance(self._inv, InventoryEngine):
            batch_id = self._inv.recepcionar_lote(
                producto_id=producto_base_id,
                numero_pollos=numero_pollos,
                peso_kg=peso_kg,
                costo_kg=costo_kg,
                compra_global_id=compra_global_id,
            )
            rec_id = batch_id
        else:
            cur = self._raw.execute(
                """
                INSERT INTO inventario_pollo_sucursal
                    (sucursal_id, compra_global_id, numero_pollos,
                     peso_kg_disponible, peso_kg_original,
                     fecha_recepcion, costo_kg, estado, usuario_recepcion)
                VALUES (?,?,?,?,?,date('now'),?,'disponible',?)
                """,
                (self.sucursal_id, compra_global_id, numero_pollos,
                 peso_kg, peso_kg, costo_kg, self.usuario),
            )
            rec_id = cur.lastrowid
            self._inv.registrar_entrada(
                producto_id=producto_base_id, cantidad=peso_kg,
                descripcion=f"Recepción #{rec_id} — {numero_pollos} pollos",
                costo_unitario=costo_kg,
            )

        self._raw.commit()
        return rec_id

    # ── Transformación por receta ─────────────────────────────────────────────

    def transformar_por_receta(
        self,
        receta_id:       int,
        kg_procesar:     float,
        producto_base_id: int,
        batch_id:        int = None,
    ) -> ResultadoTransformacion:
        if kg_procesar <= 0:
            raise ChickenError("kg_procesar debe ser > 0")

        # Usar InventoryEngine FIFO si disponible y batch_id dado
        if _FIFO_AVAILABLE and isinstance(self._inv, InventoryEngine) and batch_id:
            result = self._inv.transformar_parcial(
                batch_id=batch_id,
                kg_procesar=kg_procesar,
                receta_id=receta_id,
            )
            self._raw.commit()
            cortes = [
                {"producto_id": s["producto_id"], "nombre": s["nombre"],
                 "kg": s["kg"], "merma_kg": s["merma_kg"]}
                for s in result.sub_batches
            ]
            return ResultadoTransformacion(
                movimiento_ids=result.movimiento_ids,
                kg_procesados=result.kg_procesados,
                kg_merma=result.kg_merma,
                cortes=cortes,
            )

        # Fallback legacy
        detalle = self._raw.execute(
            """
            SELECT d.producto_resultado_id, p.nombre,
                   d.porcentaje_rendimiento, d.porcentaje_merma
            FROM recetas_pollo_detalle d
            JOIN productos p ON p.id = d.producto_resultado_id
            WHERE d.receta_id = ?
            ORDER BY d.orden, d.id
            """,
            (receta_id,),
        ).fetchall()

        if not detalle:
            raise RecetaNoEncontradaError(f"Receta #{receta_id} sin detalle")

        total_pct = sum(float(r[2]) + float(r[3]) for r in detalle)
        if total_pct > 105.0:
            raise ChickenError(f"Receta #{receta_id}: rendimiento+merma ({total_pct:.1f}%) > 105%")

        from core.services.inventory_service import PiezaTransformacion
        piezas = []
        kg_merma_total = 0.0
        cortes_resultado = []

        for row in detalle:
            pid, nombre = int(row[0]), row[1]
            pct_rend, pct_merma = float(row[2]), float(row[3])
            kg = round(kg_procesar * (pct_rend / 100.0), 4)
            km = round(kg_procesar * (pct_merma / 100.0), 4)
            kg_merma_total += km
            if kg > 0:
                piezas.append(PiezaTransformacion(producto_id=pid, kg=kg,
                    descripcion=f"Transformación receta#{receta_id} — {nombre}"))
            cortes_resultado.append({"producto_id": pid, "nombre": nombre, "kg": kg, "merma_kg": km})

        ids = self._inv.transformar_pollo(producto_base_id, kg_procesar, piezas, round(kg_merma_total, 4))
        self._raw.commit()

        return ResultadoTransformacion(
            movimiento_ids=ids if isinstance(ids, list) else [],
            kg_procesados=kg_procesar,
            kg_merma=round(kg_merma_total, 4),
            cortes=cortes_resultado,
        )

    # ── Recetas ───────────────────────────────────────────────────────────────

    def listar_recetas(self) -> list:
        return self._raw.execute(
            """
            SELECT r.id, r.nombre_receta, p.nombre AS producto_base, r.activa
            FROM recetas_pollo r
            JOIN productos p ON p.id = r.producto_base_id
            ORDER BY r.nombre_receta
            """
        ).fetchall()

    def obtener_detalle_receta(self, receta_id: int) -> list:
        return self._raw.execute(
            """
            SELECT d.id, p.nombre, d.porcentaje_rendimiento, d.porcentaje_merma, d.orden
            FROM recetas_pollo_detalle d
            JOIN productos p ON p.id = d.producto_resultado_id
            WHERE d.receta_id = ?
            ORDER BY d.orden, d.id
            """,
            (receta_id,),
        ).fetchall()

    def guardar_receta(
        self, nombre_receta: str, producto_base_id: int, cortes: List[dict], receta_id: int = None
    ) -> int:
        if receta_id:
            self._raw.execute(
                "UPDATE recetas_pollo SET nombre_receta=?, producto_base_id=? WHERE id=?",
                (nombre_receta, producto_base_id, receta_id),
            )
            self._raw.execute("DELETE FROM recetas_pollo_detalle WHERE receta_id=?", (receta_id,))
        else:
            cur = self._raw.execute(
                "INSERT INTO recetas_pollo (nombre_receta, producto_base_id, creado_por) VALUES (?,?,?)",
                (nombre_receta, producto_base_id, self.usuario),
            )
            receta_id = cur.lastrowid

        for i, corte in enumerate(cortes):
            self._raw.execute(
                """
                INSERT INTO recetas_pollo_detalle
                    (receta_id, producto_resultado_id, porcentaje_rendimiento,
                     porcentaje_merma, orden)
                VALUES (?,?,?,?,?)
                """,
                (receta_id, corte["producto_resultado_id"],
                 float(corte.get("porcentaje_rendimiento", 0)),
                 float(corte.get("porcentaje_merma", 0)),
                 corte.get("orden", i)),
            )
        return receta_id

    # ── Conciliación ──────────────────────────────────────────────────────────

    def conciliar(self) -> ResultadoConciliacion:
        row_g = self._raw.execute(
            """
            SELECT COALESCE(SUM(peso_total_kg),0), COALESCE(SUM(numero_pollos),0)
            FROM compras_pollo_global WHERE estado != 'cancelado'
            """
        ).fetchone()
        kg_global     = float(row_g[0]) if row_g else 0.0
        pollos_global = int(row_g[1])   if row_g else 0

        rows_l = self._raw.execute(
            """
            SELECT sucursal_id, COALESCE(SUM(numero_pollos),0),
                   COALESCE(SUM(peso_kg_disponible),0)
            FROM inventario_pollo_sucursal
            WHERE estado != 'cancelado'
            GROUP BY sucursal_id
            ORDER BY sucursal_id
            """
        ).fetchall()

        total_local = sum(float(r[2]) for r in rows_l)

        return ResultadoConciliacion(
            global_kg=kg_global,
            global_pollos=pollos_global,
            total_sucursales_kg=round(total_local, 3),
            diferencia_kg=round(kg_global - total_local, 3),
            sucursales=[{"sucursal_id": r[0], "pollos": int(r[1]), "kg": float(r[2])} for r in rows_l],
        )

    def stock_pollo_sucursal(self) -> dict:
        row = self._raw.execute(
            """
            SELECT COALESCE(SUM(numero_pollos),0), COALESCE(SUM(peso_kg_disponible),0)
            FROM inventario_pollo_sucursal
            WHERE sucursal_id=? AND estado='disponible'
            """,
            (self.sucursal_id,),
        ).fetchone()
        return {"pollos": int(row[0]) if row else 0, "kg": float(row[1]) if row else 0.0}

# Compra global, recepción sucursal, transformación por receta, conciliación.
# REGLA: todos los cambios de stock pasan por InventoryService.
from __future__ import annotations
import sqlite3
import uuid
import logging
from dataclasses import dataclass, field
from typing import List, Optional

from core.db.connection import transaction
from core.services.inventory_service import (
    InventoryService,
    PiezaTransformacion,
    InventarioError,
    StockInsuficienteError,
)

logger = logging.getLogger("spj.chicken")


# ── Excepciones propias ───────────────────────────────────────────────────────

class ChickenError(Exception):
    pass


class RecetaNoEncontradaError(ChickenError):
    pass


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class CorteReceta:
    producto_resultado_id: int
    nombre: str
    porcentaje_rendimiento: float   # 0.0 – 100.0
    porcentaje_merma: float = 0.0


@dataclass
class ResultadoTransformacion:
    movimiento_ids: List[int]
    kg_procesados: float
    kg_merma: float
    cortes: List[dict] = field(default_factory=list)


@dataclass
class ResultadoConciliacion:
    global_kg: float
    global_pollos: int
    total_sucursales_kg: float
    diferencia_kg: float
    sucursales: List[dict] = field(default_factory=list)


# ── Motor principal ───────────────────────────────────────────────────────────

class ChickenEngine:
    """
    Orquesta todas las operaciones de pollo:
    - Compra global (nivel admin)
    - Recepción en sucursal
    - Transformación por receta configurada
    - Conciliación global vs sucursales

    Depende de InventoryService para todos los movimientos de stock.
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        usuario: str = "Sistema",
        sucursal_id: int = 1,
    ):
        self.conn        = conn
        self.usuario     = usuario or "Sistema"
        self.sucursal_id = sucursal_id
        self._inv        = InventoryService(conn, usuario, sucursal_id)

    # ── Compra global ─────────────────────────────────────────────────────────

    def registrar_compra_global(
        self,
        numero_pollos: int,
        peso_total_kg: float,
        costo_total: float,
        proveedor: str = "",
        producto_base_id: int = None,
        sucursal_destino_id: int = None,
        notas: str = "",
    ) -> int:
        """
        Registra compra centralizada de pollo para toda la cadena.
        Si se pasa producto_base_id, también acredita el stock en inventario.
        Retorna el ID de la compra global creada.
        """
        if numero_pollos <= 0:
            raise ChickenError("número de pollos debe ser > 0")
        if peso_total_kg <= 0:
            raise ChickenError("peso total debe ser > 0 kg")
        if costo_total <= 0:
            raise ChickenError("costo total debe ser > 0")

        costo_por_kg = round(costo_total / peso_total_kg, 4)
        lote_id = str(uuid.uuid4())[:16].upper()

        with transaction(self.conn) as c:
            cur = c.execute(
                """
                INSERT INTO compras_pollo_global
                    (fecha, proveedor, numero_pollos, peso_total_kg,
                     costo_total, costo_por_kg, lote_id, estado,
                     usuario_registro, sucursal_destino_id, notas)
                VALUES (date('now'),?,?,?,?,?,?,'activo',?,?,?)
                """,
                (
                    proveedor, numero_pollos, peso_total_kg,
                    costo_total, costo_por_kg, lote_id,
                    self.usuario, sucursal_destino_id, notas,
                ),
            )
            compra_id = cur.lastrowid

            if producto_base_id:
                self._inv.registrar_entrada(
                    producto_id=producto_base_id,
                    cantidad=peso_total_kg,
                    descripcion=f"Compra global #{compra_id} — {numero_pollos} pollos | {proveedor}",
                    referencia=f"CG:{compra_id}",
                    costo_unitario=costo_por_kg,
                )

        logger.info(
            "Compra global #%d: %d pollos %.3f kg $%.2f costo_kg=$%.4f",
            compra_id, numero_pollos, peso_total_kg, costo_total, costo_por_kg,
        )
        return compra_id

    # ── Recepción en sucursal ─────────────────────────────────────────────────

    def recepcionar_en_sucursal(
        self,
        numero_pollos: int,
        peso_kg: float,
        producto_base_id: int,
        compra_global_id: int = None,
        costo_kg: float = 0.0,
    ) -> int:
        """
        Registra recepción de pollos en la sucursal actual.
        Acredita el stock en inventario.
        Retorna el ID del registro de recepción.
        """
        if numero_pollos <= 0:
            raise ChickenError("número de pollos debe ser > 0")
        if peso_kg <= 0:
            raise ChickenError("peso debe ser > 0 kg")

        with transaction(self.conn) as c:
            # Registrar en inventario_pollo_sucursal
            cur = c.execute(
                """
                INSERT INTO inventario_pollo_sucursal
                    (sucursal_id, compra_global_id, numero_pollos,
                     peso_kg_disponible, peso_kg_original,
                     fecha_recepcion, costo_kg, estado, usuario_recepcion)
                VALUES (?,?,?,?,?,date('now'),?,'disponible',?)
                """,
                (
                    self.sucursal_id, compra_global_id, numero_pollos,
                    peso_kg, peso_kg, costo_kg, self.usuario,
                ),
            )
            rec_id = cur.lastrowid

            # Acreditar stock
            self._inv.registrar_entrada(
                producto_id=producto_base_id,
                cantidad=peso_kg,
                descripcion=f"Recepción sucursal #{rec_id} — {numero_pollos} pollos",
                referencia=f"REC:{rec_id}",
                costo_unitario=costo_kg,
            )

        logger.info(
            "Recepción sucursal %d: %d pollos %.3f kg (rec_id=%d)",
            self.sucursal_id, numero_pollos, peso_kg, rec_id,
        )
        return rec_id

    # ── Transformación por receta ─────────────────────────────────────────────

    def transformar_por_receta(
        self,
        receta_id: int,
        kg_procesar: float,
        producto_base_id: int,
    ) -> ResultadoTransformacion:
        """
        Aplica una receta de transformación:
        - Lee los porcentajes de rendimiento y merma de recetas_pollo_detalle
        - Calcula kg por corte
        - Descuenta kg_procesar del producto base
        - Acredita cada corte resultante
        Retorna ResultadoTransformacion con detalles completos.
        """
        if kg_procesar <= 0:
            raise ChickenError("kg_procesar debe ser > 0")

        detalle = self.conn.execute(
            """
            SELECT d.producto_resultado_id, p.nombre,
                   d.porcentaje_rendimiento, d.porcentaje_merma
            FROM recetas_pollo_detalle d
            JOIN productos p ON p.id = d.producto_resultado_id
            WHERE d.receta_id = ?
            ORDER BY d.orden, d.id
            """,
            (receta_id,),
        ).fetchall()

        if not detalle:
            raise RecetaNoEncontradaError(
                f"Receta {receta_id} no tiene detalle configurado"
            )

        # Verificar que rendimiento total no supere 105%
        total_rend = sum(float(r[2]) for r in detalle)
        total_merma = sum(float(r[3]) for r in detalle)
        if total_rend + total_merma > 105.0:
            raise ChickenError(
                f"Rendimiento+merma total ({total_rend+total_merma:.1f}%) "
                f"supera 105% de tolerancia"
            )

        piezas: List[PiezaTransformacion] = []
        kg_merma_total = 0.0
        cortes_resultado = []

        for row in detalle:
            pid, nombre, pct_rend, pct_merma = int(row[0]), row[1], float(row[2]), float(row[3])
            kg = round(kg_procesar * (pct_rend / 100.0), 4)
            km = round(kg_procesar * (pct_merma / 100.0), 4)
            kg_merma_total += km

            if kg > 0:
                piezas.append(
                    PiezaTransformacion(
                        producto_id=pid,
                        kg=kg,
                        descripcion=f"Transformación receta#{receta_id} — {nombre}",
                    )
                )
            cortes_resultado.append(
                {"producto_id": pid, "nombre": nombre, "kg": kg, "merma_kg": km}
            )

        with transaction(self.conn):
            movimiento_ids = self._inv.transformar_pollo(
                producto_base_id=producto_base_id,
                kg_descontar=kg_procesar,
                piezas=piezas,
                merma_kg=round(kg_merma_total, 4),
            )

        logger.info(
            "Transformación receta#%d: %.3f kg → %d cortes | merma=%.3f kg",
            receta_id, kg_procesar, len(piezas), kg_merma_total,
        )
        return ResultadoTransformacion(
            movimiento_ids=movimiento_ids,
            kg_procesados=kg_procesar,
            kg_merma=round(kg_merma_total, 4),
            cortes=cortes_resultado,
        )

    # ── Recetas ───────────────────────────────────────────────────────────────

    def listar_recetas(self) -> list:
        return self.conn.execute(
            """
            SELECT r.id, r.nombre_receta, p.nombre AS producto_base, r.activa
            FROM recetas_pollo r
            JOIN productos p ON p.id = r.producto_base_id
            ORDER BY r.nombre_receta
            """
        ).fetchall()

    def obtener_detalle_receta(self, receta_id: int) -> list:
        return self.conn.execute(
            """
            SELECT d.id, p.nombre, d.porcentaje_rendimiento, d.porcentaje_merma, d.orden
            FROM recetas_pollo_detalle d
            JOIN productos p ON p.id = d.producto_resultado_id
            WHERE d.receta_id = ?
            ORDER BY d.orden, d.id
            """,
            (receta_id,),
        ).fetchall()

    def guardar_receta(
        self,
        nombre_receta: str,
        producto_base_id: int,
        cortes: List[dict],
        receta_id: int = None,
    ) -> int:
        """
        Crea o actualiza una receta con su detalle.
        cortes: [{'producto_resultado_id': int, 'porcentaje_rendimiento': float,
                  'porcentaje_merma': float, 'orden': int}, ...]
        """
        with transaction(self.conn) as c:
            if receta_id:
                c.execute(
                    "UPDATE recetas_pollo SET nombre_receta=?, producto_base_id=? WHERE id=?",
                    (nombre_receta, producto_base_id, receta_id),
                )
                c.execute("DELETE FROM recetas_pollo_detalle WHERE receta_id=?", (receta_id,))
            else:
                cur = c.execute(
                    "INSERT INTO recetas_pollo (nombre_receta, producto_base_id, creado_por) VALUES (?,?,?)",
                    (nombre_receta, producto_base_id, self.usuario),
                )
                receta_id = cur.lastrowid

            for i, corte in enumerate(cortes):
                c.execute(
                    """
                    INSERT INTO recetas_pollo_detalle
                        (receta_id, producto_resultado_id, porcentaje_rendimiento,
                         porcentaje_merma, orden)
                    VALUES (?,?,?,?,?)
                    """,
                    (
                        receta_id,
                        corte["producto_resultado_id"],
                        float(corte.get("porcentaje_rendimiento", 0)),
                        float(corte.get("porcentaje_merma", 0)),
                        corte.get("orden", i),
                    ),
                )
        return receta_id

    # ── Conciliación global ───────────────────────────────────────────────────

    def conciliar(self) -> ResultadoConciliacion:
        """
        Compara inventario global (compras_pollo_global) contra la suma
        de inventarios locales de todas las sucursales.
        Retorna ResultadoConciliacion con diferencias.
        """
        row_global = self.conn.execute(
            """
            SELECT COALESCE(SUM(peso_total_kg), 0),
                   COALESCE(SUM(numero_pollos), 0)
            FROM compras_pollo_global
            WHERE estado != 'cancelado'
            """
        ).fetchone()
        kg_global     = float(row_global[0]) if row_global else 0.0
        pollos_global = int(row_global[1])   if row_global else 0

        rows_locales = self.conn.execute(
            """
            SELECT sucursal_id,
                   COALESCE(SUM(numero_pollos), 0),
                   COALESCE(SUM(peso_kg_disponible), 0)
            FROM inventario_pollo_sucursal
            WHERE estado != 'cancelado'
            GROUP BY sucursal_id
            ORDER BY sucursal_id
            """
        ).fetchall()

        total_local_kg = sum(float(r[2]) for r in rows_locales)
        diferencia_kg  = round(kg_global - total_local_kg, 3)

        return ResultadoConciliacion(
            global_kg=kg_global,
            global_pollos=pollos_global,
            total_sucursales_kg=round(total_local_kg, 3),
            diferencia_kg=diferencia_kg,
            sucursales=[
                {
                    "sucursal_id": r[0],
                    "pollos":      int(r[1]),
                    "kg":          float(r[2]),
                }
                for r in rows_locales
            ],
        )

    # ── Stock actual pollo en sucursal ────────────────────────────────────────

    def stock_pollo_sucursal(self) -> dict:
        """Retorna stock de pollo disponible en la sucursal actual."""
        row = self.conn.execute(
            """
            SELECT COALESCE(SUM(numero_pollos), 0),
                   COALESCE(SUM(peso_kg_disponible), 0)
            FROM inventario_pollo_sucursal
            WHERE sucursal_id = ? AND estado = 'disponible'
            """,
            (self.sucursal_id,),
        ).fetchone()
        return {
            "pollos": int(row[0]) if row else 0,
            "kg":     float(row[1]) if row else 0.0,
        }

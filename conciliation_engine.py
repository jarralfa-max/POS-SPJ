# core/services/conciliation_engine.py
# ── CONCILIATION ENGINE — SPJ Enterprise v6 ───────────────────────────────────
# Conciliación por árbol de transformación (base → derivados).
# Registra cada ejecución en conciliation_runs.
# Bloquea ventas, transferencias y transformaciones durante la operación.
from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from core.database import Connection, get_db
from core.services.inventory_engine import InventoryEngine, LockActivoError

logger = logging.getLogger("spj.conciliation")


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class BatchNode:
    """Nodo en el árbol de transformación de un lote."""
    batch_id:          int
    uuid:              str
    producto_id:       int
    producto_nombre:   str
    branch_id:         int
    kg_original:       float
    kg_disponible:     float     # BIB actual
    kg_consumido:      float     # suma movimientos de salida
    kg_esperado:       float     # original - consumido
    diferencia:        float     # disponible - esperado
    dentro_tolerancia: bool
    estado:            str
    es_derivado:       bool
    parent_batch_id:   Optional[int]
    transformation_id: Optional[str]
    hijos:             List["BatchNode"] = field(default_factory=list)


@dataclass
class ConciliationReport:
    run_id:             Optional[int]
    branch_id:          int
    fecha:              str
    tolerancia_kg:      float
    total_batches:      int
    batches_ok:         int
    batches_diff:       int
    diferencia_total_kg: float
    arboles:            List[BatchNode]   = field(default_factory=list)
    diffs:              List[BatchNode]   = field(default_factory=list)  # nodos con diff
    resumen_global:     dict              = field(default_factory=dict)
    ajustes_aplicados:  List[dict]        = field(default_factory=list)
    duracion_ms:        int               = 0


# ══════════════════════════════════════════════════════════════════════════════

class ConciliationEngine:
    """
    Motor de conciliación industrial por árbol de transformación.

    Flujo:
        1. adquirir_locks()         → bloquea ventas, transferencias, transformacion
        2. conciliar(branch_id)     → construye árbol base→derivados, calcula diffs
        3. aplicar_ajustes(report)  → opcional, corrige BIBs fuera de tolerancia
        4. liberar_locks()          → desbloquea operaciones
        5. guardar_run(report)      → persiste en conciliation_runs

    Context manager:
        with ConciliationEngine(db, usuario) as engine:
            report = engine.conciliar(branch_id=2, tolerancia_kg=0.05)
            engine.aplicar_ajustes(report)
            engine.guardar_run(report)
    """

    LOCK_VENTAS         = "ventas"
    LOCK_TRANSFERENCIAS = "transferencias"
    LOCK_TRANSFORMACION = "transformacion"
    LOCK_CONCILIACION   = "conciliacion"
    LOCK_TTL_SEG        = 600

    def __init__(
        self,
        db:            Connection,
        usuario:       str   = "Sistema",
        tolerancia_kg: float = 0.05,
    ):
        self.db            = db
        self.usuario       = usuario or "Sistema"
        self.tolerancia_kg = tolerancia_kg
        self._locks_held   = False

    # ── Context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "ConciliationEngine":
        self.adquirir_locks()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.liberar_locks()
        return False

    # ── Locks ─────────────────────────────────────────────────────────────────

    def adquirir_locks(self) -> None:
        """Bloquea ventas, transferencias y transformaciones globalmente."""
        motivo = (
            f"Conciliación iniciada por {self.usuario} "
            f"— {datetime.now():%Y-%m-%d %H:%M}"
        )
        for key in [
            self.LOCK_VENTAS,
            self.LOCK_TRANSFERENCIAS,
            self.LOCK_TRANSFORMACION,
            self.LOCK_CONCILIACION,
        ]:
            try:
                self.db.execute(
                    """
                    INSERT OR REPLACE INTO system_locks
                        (lock_key, branch_id, adquirido_por, motivo,
                         adquirido_en, expira_en, activo)
                    VALUES (?, NULL, ?, ?, datetime('now'),
                            datetime('now', ? || ' seconds'), 1)
                    """,
                    (key, self.usuario, motivo, self.LOCK_TTL_SEG),
                )
            except Exception as exc:
                logger.error("No se pudo adquirir lock '%s': %s", key, exc)
                raise LockActivoError(f"No se pudo adquirir lock '{key}': {exc}") from exc

        self.db.raw.commit()
        self._locks_held = True
        logger.info("Conciliación: 4 locks adquiridos por %s", self.usuario)

    def liberar_locks(self) -> None:
        if not self._locks_held:
            return
        try:
            self.db.execute(
                "UPDATE system_locks SET activo=0 WHERE lock_key IN (?,?,?,?)",
                (self.LOCK_VENTAS, self.LOCK_TRANSFERENCIAS,
                 self.LOCK_TRANSFORMACION, self.LOCK_CONCILIACION),
            )
            self.db.raw.commit()
            self._locks_held = False
            logger.info("Conciliación: locks liberados por %s", self.usuario)
        except Exception as exc:
            logger.error("Error liberando locks: %s", exc)

    # ── Conciliación principal ────────────────────────────────────────────────

    def conciliar(
        self,
        branch_id:     int,
        tolerancia_kg: float = None,
    ) -> ConciliationReport:
        """
        Construye el árbol de transformación de todos los lotes de la sucursal
        y calcula diferencias respecto al stock esperado.

        Lógica por árbol:
            - Lotes raíz (parent_batch_id IS NULL): stock esperado = original - consumido
            - Lotes derivados: forman parte del árbol de su padre
            - Un lote agotado por transformación es CORRECTO si sus hijos tienen el material
        """
        t0 = time.monotonic()
        tol = tolerancia_kg if tolerancia_kg is not None else self.tolerancia_kg
        fecha_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ── 1. Cargar todos los batches de la sucursal ────────────────────────
        rows = self.db.fetchall(
            """
            SELECT
                cb.id, cb.uuid, cb.producto_id, p.nombre,
                cb.peso_kg_original,
                COALESCE(bib.cantidad_disponible, 0) AS kg_disp,
                cb.estado,
                COALESCE(bib.es_derivado, 0) AS es_derivado,
                cb.parent_batch_id,
                cb.transformation_id,
                COALESCE(cb.root_batch_id, cb.id) AS root_batch_id
            FROM chicken_batches cb
            JOIN productos p ON p.id = cb.producto_id
            LEFT JOIN branch_inventory_batches bib
                ON bib.batch_id = cb.id
               AND bib.branch_id = ?
            WHERE cb.branch_id = ?
              AND cb.estado NOT IN ('cancelado')
            ORDER BY cb.fecha_recepcion ASC, cb.id ASC
            """,
            (branch_id, branch_id),
        )

        # ── 2. Construir nodos ────────────────────────────────────────────────
        from core.services.inventory_engine import InventoryEngine
        _inv = InventoryEngine(self.db, branch_id=branch_id)

        # Pre-calcular equivalencias por root (una sola vez por árbol raíz)
        root_equivalencias: Dict[int, dict] = {}

        nodes: Dict[int, BatchNode] = {}
        for row in rows:
            batch_id    = int(row[0])
            uuid_str    = row[1]
            prod_id     = int(row[2])
            prod_nombre = row[3]
            kg_orig     = float(row[4])
            kg_disp     = float(row[5])
            estado      = row[6]
            es_deriv    = bool(row[7])
            parent_id   = row[8]   # puede ser None
            tx_id       = row[9]
            root_id     = int(row[10])

            kg_consumido = self._kg_consumido_batch(batch_id)

            # Para lotes raíz: usar reconstruct_batch_equivalence (matemáticamente exacto)
            if parent_id is None or parent_id == batch_id:
                if root_id not in root_equivalencias:
                    try:
                        root_equivalencias[root_id] = _inv.reconstruct_batch_equivalence(root_id)
                    except Exception:
                        root_equivalencias[root_id] = {}
                eq = root_equivalencias.get(root_id, {})
                diferencia = round(eq.get("diferencia_kg", 0.0), 6)
                dentro     = abs(diferencia) <= tol
                kg_esperado = round(kg_orig - kg_consumido, 6)
            else:
                kg_esperado = round(kg_orig - kg_consumido, 6)
                diferencia  = round(kg_disp - kg_esperado, 6)
                dentro      = abs(diferencia) <= tol

            nodes[batch_id] = BatchNode(
                batch_id=batch_id,
                uuid=uuid_str,
                producto_id=prod_id,
                producto_nombre=prod_nombre,
                branch_id=branch_id,
                kg_original=kg_orig,
                kg_disponible=kg_disp,
                kg_consumido=kg_consumido,
                kg_esperado=kg_esperado,
                diferencia=diferencia,
                dentro_tolerancia=dentro,
                estado=estado,
                es_derivado=es_deriv,
                parent_batch_id=int(parent_id) if parent_id else None,
                transformation_id=tx_id,
            )

        # ── 3. Construir árbol: enlazar hijos a padres ────────────────────────
        arboles: List[BatchNode] = []
        for node in nodes.values():
            if node.parent_batch_id and node.parent_batch_id in nodes:
                nodes[node.parent_batch_id].hijos.append(node)
            else:
                arboles.append(node)  # nodo raíz

        # ── 4. Calcular métricas ──────────────────────────────────────────────
        all_nodes   = list(nodes.values())
        diffs       = [n for n in all_nodes if not n.dentro_tolerancia]
        batches_ok  = sum(1 for n in all_nodes if n.dentro_tolerancia)
        total_diff  = sum(abs(n.diferencia) for n in diffs)

        # ── 5. Resumen global ─────────────────────────────────────────────────
        row_g = self.db.fetchone(
            """
            SELECT COALESCE(SUM(peso_total_kg),0),
                   COALESCE(SUM(numero_pollos),0),
                   COALESCE(SUM(costo_total),0)
            FROM compras_pollo_global
            WHERE estado != 'cancelado'
            """
        )
        row_s = self.db.fetchone(
            """
            SELECT COALESCE(SUM(bib.cantidad_disponible),0)
            FROM branch_inventory_batches bib
            JOIN chicken_batches cb ON cb.id=bib.batch_id
            WHERE cb.estado NOT IN ('cancelado')
            """
        )
        resumen_global = {
            "kg_total_comprado":  float(row_g[0]) if row_g else 0.0,
            "pollos_total":       int(row_g[1])   if row_g else 0,
            "costo_total":        float(row_g[2]) if row_g else 0.0,
            "kg_en_sucursales":   float(row_s[0]) if row_s else 0.0,
        }

        duracion_ms = int((time.monotonic() - t0) * 1000)

        report = ConciliationReport(
            run_id=None,
            branch_id=branch_id,
            fecha=fecha_str,
            tolerancia_kg=tol,
            total_batches=len(all_nodes),
            batches_ok=batches_ok,
            batches_diff=len(diffs),
            diferencia_total_kg=round(total_diff, 4),
            arboles=arboles,
            diffs=diffs,
            resumen_global=resumen_global,
            duracion_ms=duracion_ms,
        )

        logger.info(
            "Conciliación branch=%d: %d batches | %d OK | %d diff | "
            "total_diff=%.4fkg | %dms",
            branch_id, len(all_nodes), batches_ok, len(diffs),
            total_diff, duracion_ms,
        )
        return report

    # ── Aplicar ajustes ───────────────────────────────────────────────────────

    def aplicar_ajustes(
        self,
        report: ConciliationReport,
        solo_fuera_tolerancia: bool = True,
    ) -> List[dict]:
        """
        Aplica ajustes de inventario para cada nodo con diferencia.
        Registra movimiento 'conciliacion_ajuste' por cada cambio.
        No ajusta lotes agotados por transformación (estado='agotado' con hijos).
        """
        ajustes = []
        inv = InventoryEngine(self.db, usuario=self.usuario, branch_id=report.branch_id)

        with self.db.transaction():
            for node in report.diffs:
                if solo_fuera_tolerancia and node.dentro_tolerancia:
                    continue
                if abs(node.diferencia) < 1e-6:
                    continue

                # No ajustar si el lote está agotado por transformación correcta
                # (sus hijos llevan el material — es correcto que esté en 0)
                if node.estado == "agotado" and node.hijos:
                    logger.debug(
                        "Batch #%d agotado por transformación con %d hijos — skip ajuste",
                        node.batch_id, len(node.hijos),
                    )
                    continue

                bib = self.db.fetchone(
                    "SELECT id, cantidad_disponible FROM branch_inventory_batches "
                    "WHERE batch_id=? AND branch_id=?",
                    (node.batch_id, report.branch_id),
                )
                if not bib:
                    continue

                bib_id  = int(bib[0])
                kg_real = float(bib[1])
                delta   = -node.diferencia  # corregir hacia esperado
                nuevo   = max(0.0, round(kg_real + delta, 6))

                self.db.execute(
                    "UPDATE branch_inventory_batches SET cantidad_disponible=?, "
                    "fecha_actualizacion=datetime('now') WHERE id=?",
                    (nuevo, bib_id),
                )

                inv._registrar_movimiento_batch(
                    batch_id=node.batch_id,
                    bib_id=bib_id,
                    tipo="conciliacion_ajuste",
                    cantidad=abs(delta),
                    cantidad_antes=kg_real,
                    cantidad_despues=nuevo,
                    descripcion=(
                        f"Conciliación {report.fecha} — "
                        f"diff={node.diferencia:+.4f}kg tol={report.tolerancia_kg}kg"
                    ),
                )
                inv._sync_existencia_producto(node.producto_id)

                ajustes.append({
                    "batch_id":    node.batch_id,
                    "producto_id": node.producto_id,
                    "delta_kg":    round(delta, 6),
                    "kg_antes":    kg_real,
                    "kg_despues":  nuevo,
                })

        report.ajustes_aplicados = ajustes
        logger.info("Conciliación: %d ajustes en branch=%d", len(ajustes), report.branch_id)
        return ajustes

    # ── Persistencia del run ──────────────────────────────────────────────────

    def guardar_run(self, report: ConciliationReport) -> int:
        """
        Persiste el resultado de la conciliación en conciliation_runs.
        Retorna el run_id generado.
        """
        # Serializar diffs a JSON (sin objetos cíclicos)
        def _node_to_dict(n: BatchNode) -> dict:
            return {
                "batch_id":    n.batch_id,
                "uuid":        n.uuid,
                "producto":    n.producto_nombre,
                "kg_orig":     n.kg_original,
                "kg_disp":     n.kg_disponible,
                "kg_esp":      n.kg_esperado,
                "diff":        n.diferencia,
                "ok":          n.dentro_tolerancia,
                "estado":      n.estado,
                "hijos":       len(n.hijos),
            }

        detalle_json = json.dumps(
            [_node_to_dict(n) for n in report.diffs],
            ensure_ascii=False,
        )

        try:
            _, run_id = self.db.execute_returning(
                """
                INSERT INTO conciliation_runs
                    (branch_id, usuario, tolerancia_kg,
                     total_batches, batches_ok, batches_diff,
                     diferencia_kg, ajustes_count,
                     estado, detalle_json, duracion_ms)
                VALUES (?,?,?,?,?,?,?,?,'completado',?,?)
                """,
                (
                    report.branch_id,
                    self.usuario,
                    report.tolerancia_kg,
                    report.total_batches,
                    report.batches_ok,
                    report.batches_diff,
                    report.diferencia_total_kg,
                    len(report.ajustes_aplicados),
                    detalle_json,
                    report.duracion_ms,
                ),
            )
            self.db.raw.commit()
            report.run_id = run_id
            logger.info("conciliation_run #%d guardado (branch=%d)", run_id, report.branch_id)
            return run_id
        except Exception as exc:
            logger.error("guardar_run falló: %s", exc)
            return -1

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _kg_consumido_batch(self, batch_id: int) -> float:
        """Suma de salidas legítimas de un batch (excluye entradas y ajustes)."""
        TIPOS_SALIDA = (
            "salida_venta", "salida_manual",
            "transformacion_salida",
            "transferencia_salida",
            "merma", "ajuste_salida",
        )
        placeholders = ",".join("?" * len(TIPOS_SALIDA))
        val = self.db.fetchscalar(
            f"""
            SELECT COALESCE(SUM(cantidad), 0)
            FROM batch_movements
            WHERE batch_id = ? AND tipo IN ({placeholders})
            """,
            (batch_id, *TIPOS_SALIDA),
            default=0.0,
        )
        return float(val)

    def estado_locks(self) -> List[dict]:
        rows = self.db.fetchall(
            """
            SELECT lock_key, adquirido_por, motivo, adquirido_en, expira_en
            FROM system_locks
            WHERE activo=1 AND expira_en > datetime('now')
            ORDER BY adquirido_en
            """
        )
        return [
            {
                "lock_key":      r[0],
                "adquirido_por": r[1],
                "motivo":        r[2],
                "adquirido_en":  r[3],
                "expira_en":     r[4],
            }
            for r in rows
        ]

    def limpiar_locks_expirados(self) -> int:
        cur = self.db.execute(
            "UPDATE system_locks SET activo=0 WHERE expira_en <= datetime('now') AND activo=1"
        )
        n = cur.rowcount
        if n > 0:
            logger.info("ConciliationEngine: %d locks expirados limpiados", n)
        return n

    def historial_runs(self, branch_id: int, limit: int = 20) -> List[dict]:
        """Últimas N conciliaciones de la sucursal."""
        rows = self.db.fetchall(
            """
            SELECT id, uuid, usuario, tolerancia_kg,
                   total_batches, batches_ok, batches_diff,
                   diferencia_kg, ajustes_count, estado,
                   ejecutado_en, duracion_ms
            FROM conciliation_runs
            WHERE branch_id = ?
            ORDER BY ejecutado_en DESC
            LIMIT ?
            """,
            (branch_id, limit),
        )
        return [
            {
                "id":             int(r[0]),
                "uuid":           r[1],
                "usuario":        r[2],
                "tolerancia_kg":  float(r[3]),
                "total_batches":  int(r[4]),
                "batches_ok":     int(r[5]),
                "batches_diff":   int(r[6]),
                "diferencia_kg":  float(r[7]),
                "ajustes_count":  int(r[8]),
                "estado":         r[9],
                "ejecutado_en":   r[10],
                "duracion_ms":    int(r[11] or 0),
            }
            for r in (rows or [])
        ]

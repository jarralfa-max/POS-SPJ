# migrations/v16_batch_tree_enhanced.py
# ── MIGRACIÓN v16 — Árbol de lotes para trazabilidad de transformaciones ─────
# Mejora el soporte para árboles de lotes permitiendo trazabilidad completa
# de transformaciones, auditoría de pesos y reconciliación de inventario.
#
# Ejemplo: Lote de tortas (parent) se transforma en:
#   - lote_hijo_pan: peso=1.0kg
#   - lote_hijo_jamon: peso=0.15kg
#   - lote_hijo_queso: peso=0.08kg
#
# Idempotente — seguro correr múltiples veces.
from __future__ import annotations
import sqlite3
import logging
import uuid

logger = logging.getLogger("spj.migrations.v16_batch_tree")


def _add_col(conn: sqlite3.Connection, table: str, col: str, defn: str) -> None:
    """Agrega columna si no existe (idempotente)"""
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
        logger.debug(f"Columna {col} agregada a {table}")
    except sqlite3.OperationalError:
        logger.debug(f"Columna {col} ya existe en {table}")


def up(conn: sqlite3.Connection) -> None:
    """
    Aplica migración v16:
    1. Agrega columnas de árbol a batches
    2. Crea tabla de auditoría batch_tree_audits
    3. Agrega columnas de sincronización a batches
    4. Crea índices para consultas jerárquicas
    """
    
    # ── 1. Agregar columnas de árbol a batches ───────────────────────────────
    logger.info("Agregando columnas de árbol a batches")
    
    _add_col(conn, "batches", "parent_batch_id",   "INTEGER")
    _add_col(conn, "batches", "root_batch_id",     "INTEGER")
    _add_col(conn, "batches", "transformation_group_id", "TEXT")
    _add_col(conn, "batches", "tree_level",        "INTEGER DEFAULT 0")
    _add_col(conn, "batches", "leaf_node",         "INTEGER DEFAULT 0")
    _add_col(conn, "batches", "transformation_uuid", "TEXT")
    
    # ── 2. Índices para búsqueda jerárquica ──────────────────────────────────
    logger.info("Creando índices de árbol")
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_batches_parent
        ON batches(parent_batch_id)
        WHERE parent_batch_id IS NOT NULL
    """)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_batches_root
        ON batches(root_batch_id)
        WHERE root_batch_id IS NOT NULL
    """)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_batches_group
        ON batches(transformation_group_id)
    """)
    
    # ── 3. Tabla de auditoría batch_tree_audits ───────────────────────────────
    logger.info("Creando tabla batch_tree_audits")
    
    conn.execute("""
        CREATE TABLE IF NOT EXISTS batch_tree_audits (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            root_batch_id       INTEGER NOT NULL,
            original_weight     REAL NOT NULL,
            reconstructed_weight REAL NOT NULL,
            difference          REAL NOT NULL,
            audit_uuid          TEXT UNIQUE NOT NULL,
            transformation_group_id TEXT,
            discrepancy_percent REAL GENERATED ALWAYS AS 
                (ABS(difference) * 100.0 / NULLIF(original_weight, 0)) STORED,
            resolved            INTEGER DEFAULT 0,
            resolved_at         DATETIME,
            resolved_by         TEXT,
            notes               TEXT,
            created_at          DATETIME DEFAULT (datetime('now'))
        )
    """)
    
    # ── 4. Índices de auditoría ──────────────────────────────────────────────
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_audits_root
        ON batch_tree_audits(root_batch_id, created_at)
    """)
    
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_audits_unresolved
        ON batch_tree_audits(discrepancy_percent, created_at)
        WHERE resolved = 0 AND discrepancy_percent > 1.0
    """)
    
    # ── 5. Agregar columnas de sincronización a batches ───────────────────────
    logger.info("Agregando columnas de sincronización")
    
    _add_col(conn, "batches", "sync_version",      "INTEGER DEFAULT 1")
    _add_col(conn, "batches", "sync_status",       "TEXT DEFAULT 'pending'")
    _add_col(conn, "batches", "sync_error",        "TEXT")
    _add_col(conn, "batches", "last_sync_attempt", "DATETIME")
    
    # ── 6. Trigger para mantener root_batch_id automático ─────────────────────
    logger.info("Creando triggers de mantenimiento")
    
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_batches_set_root
        AFTER INSERT ON batches
        WHEN NEW.parent_batch_id IS NOT NULL
        BEGIN
            UPDATE batches 
            SET root_batch_id = (
                SELECT COALESCE(root_batch_id, id) 
                FROM batches 
                WHERE id = NEW.parent_batch_id
            ),
            tree_level = (
                SELECT COALESCE(tree_level, -1) + 1
                FROM batches
                WHERE id = NEW.parent_batch_id
            )
            WHERE id = NEW.id;
        END;
    """)
    
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_batches_update_root
        AFTER UPDATE OF parent_batch_id ON batches
        WHEN NEW.parent_batch_id IS NOT NULL 
         AND (OLD.parent_batch_id IS NULL OR OLD.parent_batch_id != NEW.parent_batch_id)
        BEGIN
            UPDATE batches 
            SET root_batch_id = (
                SELECT COALESCE(root_batch_id, id) 
                FROM batches 
                WHERE id = NEW.parent_batch_id
            ),
            tree_level = (
                SELECT COALESCE(tree_level, -1) + 1
                FROM batches
                WHERE id = NEW.parent_batch_id
            )
            WHERE id = NEW.id;
        END;
    """)
    
    # ── 7. Trigger para marcar hojas ──────────────────────────────────────────
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_batches_mark_leaf
        AFTER INSERT ON batches
        BEGIN
            UPDATE batches 
            SET leaf_node = NOT EXISTS (
                SELECT 1 FROM batches b2 
                WHERE b2.parent_batch_id = NEW.id
            )
            WHERE id = NEW.id;
            
            -- Actualizar padre si existía
            UPDATE batches 
            SET leaf_node = NOT EXISTS (
                SELECT 1 FROM batches b2 
                WHERE b2.parent_batch_id = batches.id
            )
            WHERE id = NEW.parent_batch_id;
        END;
    """)
    
    # ── 8. Vista para análisis de árbol ───────────────────────────────────────
    logger.info("Creando vista batch_tree_analysis")
    
    conn.execute("""
        CREATE VIEW IF NOT EXISTS v_batch_tree_analysis AS
        WITH RECURSIVE batch_tree AS (
            -- Raíces
            SELECT 
                id,
                parent_batch_id,
                id as root_id,
                0 as level,
                weight,
                producto_id,
                CAST(id AS TEXT) as path
            FROM batches
            WHERE parent_batch_id IS NULL
            
            UNION ALL
            
            -- Hijos
            SELECT 
                b.id,
                b.parent_batch_id,
                bt.root_id,
                bt.level + 1,
                b.weight,
                b.producto_id,
                bt.path || '/' || b.id
            FROM batches b
            JOIN batch_tree bt ON b.parent_batch_id = bt.id
        )
        SELECT 
            bt.*,
            p.nombre as producto_nombre,
            (SELECT SUM(weight) 
             FROM batch_tree bt2 
             WHERE bt2.root_id = bt.root_id 
               AND bt2.level > 0) as total_hijos_weight,
            bt.weight - (SELECT SUM(weight) 
                        FROM batch_tree bt2 
                        WHERE bt2.root_id = bt.root_id 
                          AND bt2.level > 0) as weight_discrepancy
        FROM batch_tree bt
        JOIN productos p ON p.id = bt.producto_id
    """)
    
    conn.commit()
    logger.info("Migración v16 completada exitosamente")


def down(conn: sqlite3.Connection) -> None:
    """
    Revierte migración v16 (solo para desarrollo)
    """
    logger.warning("Revirtiendo migración v16 (ELIMINANDO DATOS)")
    
    # Eliminar triggers
    conn.execute("DROP TRIGGER IF EXISTS trg_batches_set_root")
    conn.execute("DROP TRIGGER IF EXISTS trg_batches_update_root")
    conn.execute("DROP TRIGGER IF EXISTS trg_batches_mark_leaf")
    
    # Eliminar vista
    conn.execute("DROP VIEW IF EXISTS v_batch_tree_analysis")
    
    # Eliminar índices
    conn.execute("DROP INDEX IF EXISTS idx_batches_parent")
    conn.execute("DROP INDEX IF EXISTS idx_batches_root")
    conn.execute("DROP INDEX IF EXISTS idx_batches_group")
    conn.execute("DROP INDEX IF EXISTS idx_audits_root")
    conn.execute("DROP INDEX IF EXISTS idx_audits_unresolved")
    
    # Eliminar tabla de auditoría
    conn.execute("DROP TABLE IF EXISTS batch_tree_audits")
    
    # No eliminamos columnas (SQLite no soporta DROP COLUMN fácilmente)
    # En su lugar, marcamos como no usadas con comentario
    logger.info("Nota: Las columnas agregadas permanecen en batches (SQLite limitation)")
    
    conn.commit()


# ── Helper class para operaciones comunes ────────────────────────────────────

class BatchTreeHelper:
    """
    Helper para operaciones comunes con árboles de lotes.
    Útil desde TransformationEngine y sincronización.
    """
    
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
    
    def register_transformation(
        self,
        parent_batch_id: int,
        child_batch_ids: list[int],
        group_id: str = None,
        transformation_uuid: str = None
    ) -> dict:
        """
        Registra una transformación completa (padre → hijos)
        
        Args:
            parent_batch_id: Lote original que se transforma
            child_batch_ids: Lista de lotes resultantes
            group_id: Identificador del grupo (opcional)
            transformation_uuid: UUID de la transformación
            
        Returns:
            dict: Resultado con métricas de auditoría
        """
        import uuid
        
        trans_uuid = transformation_uuid or str(uuid.uuid4())
        
        # Actualizar hijos
        placeholders = ",".join("?" * len(child_batch_ids))
        self.conn.execute(f"""
            UPDATE batches 
            SET parent_batch_id = ?,
                transformation_group_id = COALESCE(?, transformation_group_id),
                transformation_uuid = ?
            WHERE id IN ({placeholders})
        """, [parent_batch_id, group_id, trans_uuid] + child_batch_ids)
        
        # Obtener root del padre
        root = self.conn.execute(
            "SELECT COALESCE(root_batch_id, id) FROM batches WHERE id = ?",
            (parent_batch_id,)
        ).fetchone()[0]
        
        # Actualizar root en hijos (trigger lo haría, pero forzamos)
        self.conn.execute(f"""
            UPDATE batches 
            SET root_batch_id = ?
            WHERE id IN ({placeholders})
        """, [root] + child_batch_ids)
        
        # Auditar reconstrucción de peso
        audit = self.audit_weight_reconstruction(root)
        
        self.conn.commit()
        return audit
    
    def audit_weight_reconstruction(self, root_batch_id: int) -> dict:
        """
        Audita si el peso de los hijos suma el peso del padre
        """
        # Peso del root
        root = self.conn.execute(
            "SELECT weight, producto_id FROM batches WHERE id = ?",
            (root_batch_id,)
        ).fetchone()
        
        if not root:
            return {"error": "Root batch not found"}
        
        original_weight = root[0] or 0.0
        
        # Suma de pesos hoja (nodos sin hijos)
        leaf_weight = self.conn.execute("""
            WITH RECURSIVE batch_tree AS (
                SELECT id, weight, 0 as is_leaf
                FROM batches WHERE id = ?
                UNION ALL
                SELECT b.id, b.weight, 
                       NOT EXISTS (SELECT 1 FROM batches b2 WHERE b2.parent_batch_id = b.id)
                FROM batches b
                JOIN batch_tree bt ON b.parent_batch_id = bt.id
            )
            SELECT COALESCE(SUM(weight), 0)
            FROM batch_tree
            WHERE is_leaf = 1 AND id != ?
        """, (root_batch_id, root_batch_id)).fetchone()[0]
        
        diff = leaf_weight - original_weight
        diff_pct = (diff * 100.0 / original_weight) if original_weight else 0
        
        # Registrar auditoría
        audit_uuid = str(uuid.uuid4())
        self.conn.execute("""
            INSERT INTO batch_tree_audits
                (root_batch_id, original_weight, reconstructed_weight,
                 difference, audit_uuid)
            VALUES (?, ?, ?, ?, ?)
        """, (root_batch_id, original_weight, leaf_weight, diff, audit_uuid))
        
        return {
            "root_batch_id": root_batch_id,
            "original_weight": original_weight,
            "reconstructed_weight": leaf_weight,
            "difference": diff,
            "difference_percent": diff_pct,
            "audit_uuid": audit_uuid,
            "balanced": abs(diff) < 0.01  # Tolerancia 10g
        }
    
    def get_transformation_chain(self, batch_id: int) -> list[dict]:
        """
        Obtiene cadena completa de transformaciones para un lote
        """
        rows = self.conn.execute("""
            WITH RECURSIVE batch_chain AS (
                -- Ancestros
                SELECT id, parent_batch_id, 0 as depth, 'ancestor' as direction
                FROM batches WHERE id = ?
                UNION ALL
                SELECT b.id, b.parent_batch_id, bc.depth - 1, 'ancestor'
                FROM batches b
                JOIN batch_chain bc ON b.id = bc.parent_batch_id
                WHERE bc.parent_batch_id IS NOT NULL
                
                UNION ALL
                
                -- Descendientes
                SELECT b.id, b.parent_batch_id, bc.depth + 1, 'descendant'
                FROM batches b
                JOIN batch_chain bc ON b.parent_batch_id = bc.id
                WHERE bc.direction = 'descendant' OR bc.id = ?
            )
            SELECT DISTINCT 
                bc.id,
                bc.parent_batch_id,
                bc.depth,
                bc.direction,
                b.weight,
                b.producto_id,
                p.nombre as producto,
                b.transformation_uuid,
                b.transformation_group_id
            FROM batch_chain bc
            JOIN batches b ON b.id = bc.id
            LEFT JOIN productos p ON p.id = b.producto_id
            ORDER BY bc.depth
        """, (batch_id, batch_id)).fetchall()
        
        return [
            {
                "id": r[0],
                "parent_id": r[1],
                "depth": r[2],
                "direction": r[3],
                "weight": r[4],
                "producto_id": r[5],
                "producto": r[6],
                "transformation_uuid": r[7],
                "group_id": r[8]
            }
            for r in rows
        ]


# ── Ejemplo de integración ────────────────────────────────────────────────────

"""
from migrations.v16_batch_tree_enhanced import BatchTreeHelper

class TransformationEngine:
    
    def __init__(self, conn):
        self.conn = conn
        self.tree_helper = BatchTreeHelper(conn)
    
    def execute_recipe(self, recipe_id, cantidad):
        # Crear lote padre (producto compuesto)
        parent_id = self.create_batch(recipe_id, cantidad)
        
        # Crear lotes hijos (ingredientes consumidos)
        child_ids = []
        for ing in self.get_ingredientes(recipe_id):
            child_id = self.create_batch(
                ing.producto_id, 
                cantidad * ing.ratio * (1 + ing.merma/100)
            )
            child_ids.append(child_id)
        
        # Registrar transformación y auditar
        audit = self.tree_helper.register_transformation(
            parent_batch_id=parent_id,
            child_batch_ids=child_ids,
            group_id=f"recipe_{recipe_id}"
        )
        
        if not audit["balanced"]:
            logger.warning(
                f"Discrepancia en transformación: {audit['difference']:.3f}kg "
                f"({audit['difference_percent']:.1f}%)"
            )
        
        return parent_id, child_ids
"""


if __name__ == "__main__":
    # Test rápido
    logging.basicConfig(level=logging.INFO)
    
    conn = sqlite3.connect(":memory:")
    
    # Crear tablas base
    conn.execute("""
        CREATE TABLE productos (
            id INTEGER PRIMARY KEY,
            nombre TEXT
        )
    """)
    
    conn.execute("""
        CREATE TABLE batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_id INTEGER,
            weight REAL
        )
    """)
    
    # Aplicar migración
    up(conn)
    print("✅ Migración v16 aplicada correctamente")
    
    conn.close()
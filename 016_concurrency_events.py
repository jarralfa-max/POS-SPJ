# db/migrations/versions/002_create_concurrency_events.py
"""
Migración: Crear tabla de eventos de concurrencia para manejo de operaciones distribuidas.
Soporta seguimiento de reintentos y estado final de operaciones en sistema multi-sucursal.
"""
from __future__ import annotations
import sqlite3
import logging
from typing import Optional

logger = logging.getLogger("spj.migrations.concurrency_events")

# -----------------------------------------------------------------------------
# Constantes de esquema
# -----------------------------------------------------------------------------

TABLE_NAME = "concurrency_events"
INDEX_NAME = "idx_concurrency_operation"

SCHEMA_SQL = f"""
BEGIN IMMEDIATE;

CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_id TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    retries INTEGER NOT NULL DEFAULT 0,
    final_status TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    device_id TEXT,
    sucursal_id INTEGER NOT NULL DEFAULT 1,
    metadata TEXT,
    -- Restricciones de dominio
    CHECK (retries >= 0),
    CHECK (final_status IN ('PENDING', 'SUCCESS', 'FAILED', 'CONFLICT', 'RETRYING'))
);

CREATE INDEX IF NOT EXISTS {INDEX_NAME}
ON {TABLE_NAME}(operation_id, final_status);

CREATE INDEX IF NOT EXISTS idx_concurrency_cleanup
ON {TABLE_NAME}(created_at, final_status)
WHERE final_status IN ('SUCCESS', 'FAILED');

PRAGMA foreign_keys=ON;

COMMIT;
"""

# -----------------------------------------------------------------------------
# Versión y dependencias
# -----------------------------------------------------------------------------

REVISION = "002"  # Número de migración
DOWN_REVISION = "001"  # Migración anterior (event_log)
DESCRIPTION = "Crea tabla concurrency_events para manejo de concurrencia distribuida"

# -----------------------------------------------------------------------------
# Función de upgrade (aplicar migración)
# -----------------------------------------------------------------------------

def upgrade(conn: sqlite3.Connection) -> bool:
    """
    Aplica la migración creando la tabla concurrency_events.
    
    Args:
        conn: Conexión a SQLite (sucursal local)
        
    Returns:
        bool: True si éxito, False si falla
    
    Raises:
        sqlite3.Error: Si hay error en la operación
    """
    cursor = None
    try:
        cursor = conn.cursor()
        
        # Verificar si ya existe la tabla
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=?
        """, (TABLE_NAME,))
        
        if cursor.fetchone():
            logger.info(f"Tabla {TABLE_NAME} ya existe, verificando esquema...")
            
            # Verificar columnas requeridas (migración parcial)
            cursor.execute(f"PRAGMA table_info({TABLE_NAME})")
            existing_columns = {col[1] for col in cursor.fetchall()}
            
            required_columns = {
                'id', 'operation_id', 'operation_type', 'retries',
                'final_status', 'created_at', 'updated_at', 
                'device_id', 'sucursal_id', 'metadata'
            }
            
            missing_columns = required_columns - existing_columns
            
            if missing_columns:
                logger.warning(f"Columnas faltantes en {TABLE_NAME}: {missing_columns}")
                # Aquí se podrían agregar columnas faltantes con ALTER TABLE
                # Pero por simplicidad, recreamos la tabla (solo en desarrollo)
                if input("¿Recrear tabla? (solo desarrollo): ").lower() == 's':
                    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
                else:
                    raise Exception("Migración requiere recrear tabla manualmente")
            else:
                logger.info(f"Tabla {TABLE_NAME} ya tiene esquema completo")
                return True
        
        # Ejecutar creación de tabla
        logger.info(f"Creando tabla {TABLE_NAME}...")
        
        # Ejecutar cada sentencia por separado (sqlite3 no soporta múltiples)
        for statement in SCHEMA_SQL.split(';'):
            stmt = statement.strip()
            if stmt and not stmt.upper().startswith('BEGIN'):
                cursor.execute(stmt)
        
        conn.commit()
        logger.info(f"Migración {REVISION} aplicada exitosamente: {DESCRIPTION}")
        
        # Registrar migración en tabla de control (si existe)
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO schema_migrations (version, applied_at)
                VALUES (?, datetime('now'))
            """, (REVISION,))
            conn.commit()
        except sqlite3.OperationalError:
            # Tabla schema_migrations no existe, ignorar
            pass
        
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Error en migración {REVISION}: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()

# -----------------------------------------------------------------------------
# Función de downgrade (revertir migración)
# -----------------------------------------------------------------------------

def downgrade(conn: sqlite3.Connection) -> bool:
    """
    Revierte la migración eliminando la tabla concurrency_events.
    
    Args:
        conn: Conexión a SQLite
        
    Returns:
        bool: True si éxito
    
    Warning:
        Esta operación elimina datos de concurrencia permanentemente
    """
    cursor = None
    try:
        cursor = conn.cursor()
        
        logger.warning(f"Revirtiendo migración {REVISION}: eliminando tabla {TABLE_NAME}")
        
        # Verificar si hay datos importantes antes de eliminar
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        count = cursor.fetchone()[0]
        
        if count > 0:
            logger.warning(f"La tabla contiene {count} registros de concurrencia")
            
        # Eliminar tabla
        cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
        
        # Eliminar índices (automático con DROP TABLE)
        
        conn.commit()
        logger.info(f"Downgrade {REVISION} completado: tabla {TABLE_NAME} eliminada")
        
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Error en downgrade {REVISION}: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()

# -----------------------------------------------------------------------------
# Clase helper para operaciones de concurrencia
# -----------------------------------------------------------------------------

class ConcurrencyEventManager:
    """
    Gestor de eventos de concurrencia para operaciones distribuidas.
    Útil para tracking de reintentos y resolución de conflictos.
    """
    
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._ensure_table()
    
    def _ensure_table(self) -> None:
        """Asegura que la tabla existe (útil para tests)"""
        try:
            self.conn.execute("SELECT 1 FROM concurrency_events LIMIT 1")
        except sqlite3.OperationalError:
            upgrade(self.conn)
    
    def register_operation(
        self,
        operation_id: str,
        operation_type: str,
        sucursal_id: int = 1,
        device_id: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> int:
        """
        Registra inicio de operación concurrente.
        
        Args:
            operation_id: Identificador único de la operación
            operation_type: Tipo (SYNC, TRANSFORMATION, etc)
            sucursal_id: ID de sucursal
            device_id: ID del dispositivo (opcional)
            metadata: Metadatos adicionales (JSON)
        
        Returns:
            int: ID del registro
        """
        import json
        
        metadata_json = json.dumps(metadata) if metadata else None
        
        cursor = self.conn.execute("""
            INSERT INTO concurrency_events
                (operation_id, operation_type, retries, final_status,
                 device_id, sucursal_id, metadata)
            VALUES (?, ?, 0, 'PENDING', ?, ?, ?)
        """, (operation_id, operation_type, device_id, sucursal_id, metadata_json))
        
        self.conn.commit()
        return cursor.lastrowid
    
    def update_status(
        self,
        operation_id: str,
        status: str,
        retries: Optional[int] = None
    ) -> bool:
        """
        Actualiza estado de operación.
        
        Args:
            operation_id: ID de la operación
            status: Nuevo estado (SUCCESS, FAILED, CONFLICT, RETRYING)
            retries: Número de reintentos (opcional, auto-increment si no se especifica)
        
        Returns:
            bool: True si se actualizó
        """
        if retries is None:
            # Auto-incrementar reintentos
            self.conn.execute("""
                UPDATE concurrency_events
                SET final_status = ?,
                    retries = retries + 1,
                    updated_at = datetime('now')
                WHERE operation_id = ?
            """, (status, operation_id))
        else:
            self.conn.execute("""
                UPDATE concurrency_events
                SET final_status = ?,
                    retries = ?,
                    updated_at = datetime('now')
                WHERE operation_id = ?
            """, (status, retries, operation_id))
        
        self.conn.commit()
        return self.conn.total_changes > 0
    
    def get_operation(self, operation_id: str) -> Optional[dict]:
        """Obtiene información de una operación"""
        row = self.conn.execute("""
            SELECT id, operation_id, operation_type, retries,
                   final_status, created_at, updated_at,
                   device_id, sucursal_id, metadata
            FROM concurrency_events
            WHERE operation_id = ?
        """, (operation_id,)).fetchone()
        
        if not row:
            return None
        
        import json
        return {
            'id': row[0],
            'operation_id': row[1],
            'operation_type': row[2],
            'retries': row[3],
            'final_status': row[4],
            'created_at': row[5],
            'updated_at': row[6],
            'device_id': row[7],
            'sucursal_id': row[8],
            'metadata': json.loads(row[9]) if row[9] else None
        }
    
    def cleanup_old_operations(self, days: int = 7) -> int:
        """
        Limpia operaciones exitosas/fallidas antiguas.
        
        Args:
            days: Días a mantener
        
        Returns:
            int: Número de registros eliminados
        """
        cursor = self.conn.execute("""
            DELETE FROM concurrency_events
            WHERE final_status IN ('SUCCESS', 'FAILED')
              AND created_at < datetime('now', ?)
        """, (f'-{days} days',))
        
        self.conn.commit()
        return cursor.rowcount
    
    def get_pending_operations(self, limit: int = 100) -> list:
        """Obtiene operaciones pendientes o en reintento"""
        rows = self.conn.execute("""
            SELECT operation_id, operation_type, retries,
                   created_at, device_id, metadata
            FROM concurrency_events
            WHERE final_status IN ('PENDING', 'RETRYING')
            ORDER BY created_at ASC
            LIMIT ?
        """, (limit,)).fetchall()
        
        import json
        return [
            {
                'operation_id': r[0],
                'operation_type': r[1],
                'retries': r[2],
                'created_at': r[3],
                'device_id': r[4],
                'metadata': json.loads(r[5]) if r[5] else None
            }
            for r in rows
        ]

# -----------------------------------------------------------------------------
# Ejemplo de uso con el SyncWorker existente
# -----------------------------------------------------------------------------

"""
Ejemplo de integración con sync_worker.py:

from db.migrations.versions002_concurrency_events import ConcurrencyEventManager

class SyncWorkerConcurrency(SyncWorker):
    
    def _ciclo_sync(self) -> SyncResult:
        # Registrar inicio de operación de sincronización
        event_mgr = ConcurrencyEventManager(self._conn_factory())
        event_id = event_mgr.register_operation(
            operation_id=f"sync_{datetime.now().isoformat()}",
            operation_type="SYNC_BATCH",
            sucursal_id=self._config.sucursal_id,
            device_id=self._device_id,
            metadata={"batch_size": self._config.batch_size}
        )
        
        try:
            result = super()._ciclo_sync()
            
            # Actualizar estado según resultado
            if result.errores == 0:
                event_mgr.update_status(
                    operation_id=f"sync_{datetime.now().isoformat()}",
                    status="SUCCESS"
                )
            else:
                event_mgr.update_status(
                    operation_id=f"sync_{datetime.now().isoformat()}",
                    status="FAILED",
                    retries=result.errores
                )
            
            return result
            
        except Exception as e:
            event_mgr.update_status(
                operation_id=f"sync_{datetime.now().isoformat()}",
                status="FAILED"
            )
            raise
"""

# -----------------------------------------------------------------------------
# Punto de entrada para ejecución directa
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Probar migración
    conn = sqlite3.connect(":memory:")
    success = upgrade(conn)
    
    if success:
        print("✅ Migración aplicada correctamente")
        
        # Probar manager
        mgr = ConcurrencyEventManager(conn)
        op_id = mgr.register_operation(
            operation_id="test_001",
            operation_type="TEST",
            metadata={"test": True}
        )
        print(f"✅ Operación registrada: {op_id}")
        
        mgr.update_status("test_001", "SUCCESS")
        op = mgr.get_operation("test_001")
        print(f"✅ Estado actualizado: {op['final_status']}")
        
    conn.close()
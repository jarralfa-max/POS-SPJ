# database/conexion.py — SPJ Enterprise v9.1
# Hardened: busy_timeout, WAL, singleton thread-local, logging, auto-reconexión.
from __future__ import annotations

import logging
import os
import sqlite3
import sys
import threading
import time
from typing import Optional

logger = logging.getLogger("spj.db.conexion")

# ── Ruta absoluta robusta ─────────────────────────────────────────────────────
if getattr(sys, "frozen", False):
    _BASE = os.path.dirname(sys.executable)
else:
    _BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_PATH = os.path.join(_BASE, "data", "punto_venta.db")


def set_db_path(path: str) -> None:
    global DB_PATH
    DB_PATH = path
    _singleton.__dict__.clear()
    logger.info("DB_PATH cambiado a: %s", path)


_singleton   = threading.local()
_create_lock = threading.Lock()


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    for pragma, value in [
        ("busy_timeout",  "5000"),
        ("journal_mode",  "WAL"),
        ("foreign_keys",  "ON"),
        ("synchronous",   "NORMAL"),
        ("cache_size",    "-16000"),
        ("temp_store",    "MEMORY"),
        ("mmap_size",     "134217728"),
    ]:
        try:
            conn.execute(f"PRAGMA {pragma}={value}")
        except sqlite3.Error as exc:
            logger.warning("PRAGMA %s=%s falló: %s", pragma, value, exc)


def _open_new() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(
        DB_PATH,
        check_same_thread=False,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        isolation_level=None,
    )
    conn.row_factory = sqlite3.Row
    _apply_pragmas(conn)
    logger.debug("Conexión DB abierta (hilo=%s)", threading.current_thread().name)
    return conn


def _is_alive(conn: Optional[sqlite3.Connection]) -> bool:
    if conn is None:
        return False
    try:
        conn.execute("SELECT 1")
        return True
    except Exception:
        return False


def get_db_connection(max_retries: int = 3, retry_delay: float = 0.5) -> sqlite3.Connection:
    """
    Retorna conexión singleton del hilo actual con reconexión automática.
    Raises sqlite3.Error si falla tras max_retries intentos.
    """
    conn: Optional[sqlite3.Connection] = getattr(_singleton, "conn", None)
    if _is_alive(conn):
        return conn

    with _create_lock:
        conn = getattr(_singleton, "conn", None)
        if _is_alive(conn):
            return conn

        last_exc: Optional[Exception] = None
        for attempt in range(1, max_retries + 1):
            try:
                conn = _open_new()
                _singleton.conn = conn
                if attempt > 1:
                    logger.info("Reconexión DB exitosa (intento %d)", attempt)
                return conn
            except sqlite3.Error as exc:
                last_exc = exc
                logger.warning("Conexión DB falló (intento %d/%d): %s", attempt, max_retries, exc)
                if attempt < max_retries:
                    time.sleep(retry_delay * attempt)

        logger.error("No se pudo abrir BD tras %d intentos: %s", max_retries, last_exc)
        raise last_exc


def close_db_connection() -> None:
    conn = getattr(_singleton, "conn", None)
    if conn is not None:
        try:
            conn.close()
        except Exception as exc:
            logger.debug("Error al cerrar conexión: %s", exc)
        finally:
            _singleton.conn = None
            logger.debug("Conexión DB cerrada (hilo=%s)", threading.current_thread().name)


def aplicar_migraciones_estructurales(conn: sqlite3.Connection) -> None:
    _asegurar_tabla_configuracion(conn)
    _agregar_columnas_faltantes(conn)
    try:
        conn.commit()
    except Exception:
        pass


def _asegurar_tabla_configuracion(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS configuracion (
            clave               TEXT PRIMARY KEY,
            valor               TEXT,
            descripcion         TEXT,
            editable            INTEGER DEFAULT 1,
            categoria           TEXT    DEFAULT 'General',
            fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)


def _add_col_safe(conn: sqlite3.Connection, tabla: str, col: str, defn: str) -> None:
    try:
        existing = {r[1] for r in conn.execute(f"PRAGMA table_info({tabla})").fetchall()}
        if col not in existing:
            conn.execute(f"ALTER TABLE {tabla} ADD COLUMN {col} {defn}")
            logger.debug("Columna agregada: %s.%s", tabla, col)
    except sqlite3.OperationalError as exc:
        logger.debug("_add_col_safe ignorado (%s.%s): %s", tabla, col, exc)


def _agregar_columnas_faltantes(conn: sqlite3.Connection) -> None:
    parches = [
        ("configuracion",   "descripcion",        "TEXT"),
        ("configuracion",   "editable",           "INTEGER DEFAULT 1"),
        ("configuracion",   "categoria",          "TEXT DEFAULT 'General'"),
        ("usuarios",        "nombre",             "TEXT DEFAULT ''"),
        ("usuarios",        "email",              "TEXT"),
        ("usuarios",        "sucursal_id",        "INTEGER DEFAULT 1"),
        ("usuarios",        "ultimo_acceso",      "TIMESTAMP"),
        ("usuarios",        "modulos_permitidos", "TEXT"),
        ("ventas",          "folio",              "TEXT"),
        ("ventas",          "subtotal",           "DECIMAL(10,2) DEFAULT 0"),
        ("ventas",          "descuento",          "DECIMAL(10,2) DEFAULT 0"),
        ("ventas",          "iva",                "DECIMAL(10,2) DEFAULT 0"),
        ("ventas",          "estado",             "TEXT DEFAULT 'completada'"),
        ("ventas",          "efectivo_recibido",  "DECIMAL(10,2) DEFAULT 0"),
        ("ventas",          "cambio",             "DECIMAL(10,2) DEFAULT 0"),
        ("ventas",          "puntos_ganados",     "INTEGER DEFAULT 0"),
        ("detalles_venta",  "descuento",          "DECIMAL(10,2) DEFAULT 0"),
        ("detalles_venta",  "unidad",             "TEXT"),
        ("detalles_venta",  "comentarios",        "TEXT"),
        ("productos",       "descripcion",        "TEXT"),
        ("productos",       "stock_minimo",       "DECIMAL(10,3) DEFAULT 0"),
        ("productos",       "codigo_barras",      "TEXT"),
        ("productos",       "activo",             "BOOLEAN DEFAULT 1"),
        ("clientes",        "apellido",           "TEXT"),
        ("clientes",        "email",              "TEXT"),
        ("clientes",        "saldo",              "REAL DEFAULT 0"),
        ("clientes",        "puntos",             "INTEGER DEFAULT 0"),
        ("movimientos_inventario", "tipo_movimiento",     "TEXT"),
        ("movimientos_inventario", "existencia_anterior", "REAL DEFAULT 0"),
        ("movimientos_inventario", "existencia_nueva",    "REAL DEFAULT 0"),
        ("gastos",          "estado",             "TEXT DEFAULT 'PAGADO'"),
        ("gastos",          "usuario",            "TEXT"),
        ("tarjetas_fidelidad", "id_cliente",      "INTEGER"),
        ("tarjetas_fidelidad", "codigo_qr",       "TEXT"),
        ("tarjetas_fidelidad", "fecha_creacion",  "DATETIME DEFAULT CURRENT_TIMESTAMP"),
        ("tarjetas_fidelidad", "puntos_actuales", "INTEGER DEFAULT 0"),
    ]
    for tabla, col, defn in parches:
        _add_col_safe(conn, tabla, col, defn)

    try:
        conn.execute("DROP VIEW IF EXISTS detalle_venta")
        conn.execute("""
            CREATE VIEW IF NOT EXISTS detalle_venta AS
            SELECT id, venta_id, producto_id, cantidad,
                   precio_unitario AS precio, subtotal AS total,
                   precio_unitario, descuento, subtotal, unidad, comentarios
            FROM detalles_venta
        """)
    except Exception as exc:
        logger.debug("Vista detalle_venta: %s", exc)

    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS composicion_productos (
                id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                producto_compuesto_id  INTEGER NOT NULL,
                producto_componente_id INTEGER NOT NULL,
                porcentaje             DECIMAL(5,2) DEFAULT 0,
                cantidad               DECIMAL(10,3) DEFAULT 0,
                unidad                 TEXT DEFAULT 'pza',
                UNIQUE(producto_compuesto_id, producto_componente_id)
            )
        """)
    except Exception as exc:
        logger.debug("composicion_productos: %s", exc)


def verificar_password(pwd_ingresado: str, pwd_bd: str) -> bool:
    if not pwd_ingresado or not pwd_bd:
        return False
    if str(pwd_bd).startswith(("$2b$", "$2a$")):
        try:
            import bcrypt
            return bcrypt.checkpw(pwd_ingresado.encode("utf-8"), pwd_bd.encode("utf-8"))
        except Exception:
            return False
    return pwd_ingresado == pwd_bd


def migrar_password_a_bcrypt(conn: sqlite3.Connection, usuario: str, pwd_plano: str) -> None:
    try:
        import bcrypt
        row = conn.execute(
            "SELECT id, contrasena FROM usuarios WHERE usuario=?", (usuario,)
        ).fetchone()
        if row and not str(row["contrasena"]).startswith("$2"):
            h = bcrypt.hashpw(pwd_plano.encode("utf-8"), bcrypt.gensalt(12)).decode("utf-8")
            conn.execute("UPDATE usuarios SET contrasena=? WHERE id=?", (h, row["id"]))
            conn.commit()
            logger.info("Password migrado a bcrypt para '%s'", usuario)
    except ImportError:
        pass
    except Exception as exc:
        logger.warning("migrar_password_a_bcrypt '%s': %s", usuario, exc)

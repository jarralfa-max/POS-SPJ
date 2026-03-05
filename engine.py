# migrations/engine.py
# Motor de migraciones versionadas para SQLite
# Detecta versión actual, ejecuta deltas incrementales, nunca pierde datos.
from __future__ import annotations
import sqlite3
import logging
import hashlib
from dataclasses import dataclass
from typing import List, Callable

logger = logging.getLogger("spj.migrations")


@dataclass
class Migration:
    version: int
    description: str
    up: Callable[[sqlite3.Connection], None]
    checksum: str = ""  # autocomputed


# ── Tabla de control de versiones ────────────────────────────────────────────

def _bootstrap(conn: sqlite3.Connection) -> None:
    """Crea la tabla schema_migrations si no existe."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version     INTEGER PRIMARY KEY,
            description TEXT,
            applied_at  DATETIME DEFAULT (datetime('now')),
            checksum    TEXT
        )
    """)
    conn.commit()


def _current_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(version),0) FROM schema_migrations"
    ).fetchone()
    return row[0] if row else 0


def run_migrations(conn: sqlite3.Connection, migrations: List[Migration]) -> int:
    """
    Ejecuta todas las migraciones pendientes en orden.
    Retorna número de migraciones aplicadas.
    """
    _bootstrap(conn)
    current = _current_version(conn)
    applied = 0

    for m in sorted(migrations, key=lambda x: x.version):
        if m.version <= current:
            continue
        try:
            conn.execute("BEGIN IMMEDIATE")
            m.up(conn)
            conn.execute(
                "INSERT INTO schema_migrations (version, description, checksum) VALUES (?,?,?)",
                (m.version, m.description, m.checksum)
            )
            conn.execute("COMMIT")
            logger.info("Migration v%d aplicada: %s", m.version, m.description)
            applied += 1
        except Exception as e:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            logger.error("FALLO migración v%d: %s", m.version, e, exc_info=True)
            raise RuntimeError(f"Migración v{m.version} fallida: {e}") from e

    return applied


def add_column_if_missing(conn: sqlite3.Connection, table: str, column: str, definition: str) -> bool:
    """
    Helper idempotente: agrega columna solo si no existe.
    Silencioso si la tabla no existe aún (será creada después por base.py).
    Retorna True si se agregó.
    """
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        if not rows:
            return False  # tabla no existe todavía, skip
        existing = {r[1] for r in rows}
        if column not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
            logger.debug("Columna %s.%s agregada", table, column)
            return True
        return False
    except Exception as e:
        logger.debug("add_column_if_missing %s.%s: %s", table, column, e)
        return False


# ── Definición de migraciones del proyecto SPJ ───────────────────────────────

def _m001_initial_schema(conn: sqlite3.Connection) -> None:
    """Esquema base completo con UUID global y campos offline-first."""

    conn.executescript("""
        -- Usuarios con bcrypt obligatorio
        CREATE TABLE IF NOT EXISTS usuarios (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid                TEXT UNIQUE NOT NULL DEFAULT (lower(hex(randomblob(16)))),
            usuario             TEXT UNIQUE NOT NULL,
            contrasena          TEXT NOT NULL,
            nombre              TEXT NOT NULL DEFAULT '',
            rol                 TEXT NOT NULL DEFAULT 'Cajero',
            activo              INTEGER NOT NULL DEFAULT 1,
            email               TEXT,
            telefono            TEXT,
            ultimo_acceso       DATETIME,
            fecha_creacion      DATETIME DEFAULT (datetime('now')),
            modulos_permitidos  TEXT,
            sucursal_id         INTEGER DEFAULT 1,
            _sync_version       INTEGER DEFAULT 0,
            _deleted            INTEGER DEFAULT 0
        );

        -- Categorías (columnas completas compatibles con modulos/base.py)
        CREATE TABLE IF NOT EXISTS categorias (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre          TEXT UNIQUE NOT NULL,
            descripcion     TEXT,
            activo          INTEGER DEFAULT 1,
            fecha_creacion  DATETIME DEFAULT (datetime('now')),
            color           TEXT DEFAULT '#888888',
            icono           TEXT
        );

        -- Productos con UUID global (necesario para multi-sucursal)
        CREATE TABLE IF NOT EXISTS productos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid            TEXT UNIQUE NOT NULL DEFAULT (lower(hex(randomblob(16)))),
            codigo_barras   TEXT UNIQUE,
            codigo          TEXT,
            nombre          TEXT NOT NULL,
            descripcion     TEXT,
            precio          DECIMAL(10,2) NOT NULL DEFAULT 0,
            precio_compra   DECIMAL(10,2) DEFAULT 0,
            costo           DECIMAL(10,2) DEFAULT 0,
            existencia      DECIMAL(10,3) NOT NULL DEFAULT 0,
            stock_minimo    DECIMAL(10,3) DEFAULT 0,
            unidad          TEXT NOT NULL DEFAULT 'pza',
            unidad_medida   TEXT DEFAULT 'pza',
            categoria_id    INTEGER REFERENCES categorias(id),
            categoria       TEXT,
            imagen_path     TEXT,
            proveedor_id    INTEGER,
            ubicacion       TEXT,
            notas           TEXT,
            es_compuesto    INTEGER DEFAULT 0,
            es_subproducto  INTEGER DEFAULT 0,
            producto_padre_id INTEGER REFERENCES productos(id),
            activo          INTEGER DEFAULT 1,
            oculto          INTEGER DEFAULT 0,
            sucursal_id     INTEGER DEFAULT 1,
            _sync_version   INTEGER DEFAULT 0,
            _deleted        INTEGER DEFAULT 0,
            fecha_actualizacion DATETIME DEFAULT (datetime('now'))
        );

        -- Componentes de producto compuesto
        CREATE TABLE IF NOT EXISTS composicion_productos (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_compuesto_id  INTEGER NOT NULL REFERENCES productos(id) ON DELETE CASCADE,
            producto_componente_id INTEGER NOT NULL REFERENCES productos(id) ON DELETE CASCADE,
            cantidad               DECIMAL(10,3) DEFAULT 0,
            porcentaje             DECIMAL(5,2) DEFAULT 0,
            unidad                 TEXT DEFAULT 'pza',
            UNIQUE(producto_compuesto_id, producto_componente_id)
        );

        -- Clientes con UUID global
        CREATE TABLE IF NOT EXISTS clientes (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid                TEXT UNIQUE NOT NULL DEFAULT (lower(hex(randomblob(16)))),
            nombre              TEXT NOT NULL,
            apellido            TEXT,
            apellido_paterno    TEXT,
            apellido_materno    TEXT,
            telefono            TEXT,
            email               TEXT,
            rfc                 TEXT,
            fecha_nacimiento    DATE,
            tipo_cliente        TEXT DEFAULT 'NORMAL',
            nivel_fidelidad     TEXT DEFAULT 'BASICO',
            puntos              INTEGER DEFAULT 0,
            descuento           REAL DEFAULT 0,
            saldo               REAL DEFAULT 0,
            limite_credito      REAL DEFAULT 0,
            codigo_qr           TEXT,
            referencia          TEXT,
            observaciones       TEXT,
            fecha_registro      DATETIME DEFAULT (datetime('now')),
            fecha_ultima_compra DATETIME,
            activo              INTEGER DEFAULT 1,
            _sync_version       INTEGER DEFAULT 0,
            _deleted            INTEGER DEFAULT 0
        );

        -- Proveedores
        CREATE TABLE IF NOT EXISTS proveedores (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre      TEXT NOT NULL,
            contacto    TEXT,
            telefono    TEXT,
            email       TEXT,
            direccion   TEXT,
            rfc         TEXT,
            activo      INTEGER DEFAULT 1
        );

        -- Ventas con UUID global para multi-sucursal
        CREATE TABLE IF NOT EXISTS ventas (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid                TEXT UNIQUE NOT NULL DEFAULT (lower(hex(randomblob(16)))),
            folio               TEXT UNIQUE,
            sucursal_id         INTEGER DEFAULT 1,
            usuario             TEXT NOT NULL,
            cliente_id          INTEGER REFERENCES clientes(id),
            subtotal            DECIMAL(10,2) NOT NULL DEFAULT 0,
            descuento           DECIMAL(10,2) DEFAULT 0,
            iva                 DECIMAL(10,2) DEFAULT 0,
            total               DECIMAL(10,2) NOT NULL DEFAULT 0,
            forma_pago          TEXT NOT NULL DEFAULT 'Efectivo',
            efectivo_recibido   DECIMAL(10,2) DEFAULT 0,
            cambio              DECIMAL(10,2) DEFAULT 0,
            puntos_ganados      INTEGER DEFAULT 0,
            puntos_usados       INTEGER DEFAULT 0,
            descuento_puntos    DECIMAL(10,2) DEFAULT 0,
            estado              TEXT NOT NULL DEFAULT 'completada',
            impreso             INTEGER DEFAULT 0,
            fecha_impresion     DATETIME,
            fecha               DATETIME DEFAULT (datetime('now')),
            _sync_version       INTEGER DEFAULT 0,
            _synced             INTEGER DEFAULT 0
        );

        -- Detalles de venta
        CREATE TABLE IF NOT EXISTS detalles_venta (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            venta_id        INTEGER NOT NULL REFERENCES ventas(id) ON DELETE CASCADE,
            producto_id     INTEGER NOT NULL REFERENCES productos(id),
            cantidad        DECIMAL(10,3) NOT NULL,
            precio_unitario DECIMAL(10,2) NOT NULL,
            descuento       DECIMAL(10,2) DEFAULT 0,
            subtotal        DECIMAL(10,2) NOT NULL,
            unidad          TEXT,
            comentarios     TEXT
        );

        -- Vista compatibilidad: detalle_venta (sin 's')
        DROP VIEW IF EXISTS detalle_venta;
        CREATE VIEW detalle_venta AS
            SELECT id, venta_id, producto_id, cantidad,
                   precio_unitario AS precio,
                   subtotal        AS total,
                   precio_unitario, descuento, subtotal, unidad, comentarios
            FROM detalles_venta;

        -- Movimientos inventario — ÚNICA fuente de verdad del stock
        CREATE TABLE IF NOT EXISTS movimientos_inventario (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid                TEXT UNIQUE NOT NULL DEFAULT (lower(hex(randomblob(16)))),
            producto_id         INTEGER NOT NULL REFERENCES productos(id),
            tipo                TEXT NOT NULL,
            tipo_movimiento     TEXT NOT NULL,
            cantidad            DECIMAL(10,3) NOT NULL,
            existencia_anterior DECIMAL(10,3) DEFAULT 0,
            existencia_nueva    DECIMAL(10,3) DEFAULT 0,
            costo_unitario      REAL DEFAULT 0,
            costo_total         REAL DEFAULT 0,
            descripcion         TEXT,
            referencia          TEXT,
            venta_id            INTEGER REFERENCES ventas(id),
            compra_id           INTEGER,
            usuario             TEXT,
            sucursal_id         INTEGER DEFAULT 1,
            lote_id             INTEGER,
            fecha               DATETIME DEFAULT (datetime('now')),
            fecha_caducidad     DATE,
            ubicacion           TEXT,
            _synced             INTEGER DEFAULT 0
        );

        -- Movimientos de caja
        CREATE TABLE IF NOT EXISTS movimientos_caja (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo        TEXT NOT NULL,  -- INGRESO, EGRESO, APERTURA, CIERRE
            monto       DECIMAL(10,2) NOT NULL,
            descripcion TEXT,
            usuario     TEXT,
            venta_id    INTEGER REFERENCES ventas(id),
            forma_pago  TEXT,
            caja_id     INTEGER,
            referencia  TEXT,
            fecha       DATETIME DEFAULT (datetime('now'))
        );

        -- Cajas registradoras / turnos
        CREATE TABLE IF NOT EXISTS cajas (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre              TEXT NOT NULL,
            ubicacion           TEXT,
            fondo_inicial       DECIMAL(10,2) DEFAULT 0,
            saldo_actual        DECIMAL(10,2) DEFAULT 0,
            estado              TEXT DEFAULT 'CERRADA',
            fecha_apertura      TIMESTAMP,
            fecha_cierre        TIMESTAMP,
            usuario_apertura    TEXT,
            usuario_cierre      TEXT,
            usuario             TEXT,
            monto_inicial       DECIMAL(10,2) DEFAULT 0,
            monto_final         DECIMAL(10,2),
            observaciones       TEXT,
            sucursal_id         INTEGER DEFAULT 1
        );

        -- Historico de puntos de fidelidad
        CREATE TABLE IF NOT EXISTS historico_puntos (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id  INTEGER NOT NULL REFERENCES clientes(id) ON DELETE CASCADE,
            tipo        TEXT NOT NULL,  -- COMPRA, REDENCIÓN, AJUSTE
            puntos      INTEGER NOT NULL,
            descripcion TEXT,
            saldo_actual INTEGER DEFAULT 0,
            usuario     TEXT,
            venta_id    INTEGER REFERENCES ventas(id),
            fecha       DATETIME DEFAULT (datetime('now'))
        );

        -- Tarjetas de fidelidad
        CREATE TABLE IF NOT EXISTS tarjetas_fidelidad (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            numero          TEXT UNIQUE NOT NULL,
            id_cliente      INTEGER REFERENCES clientes(id),
            codigo_qr       TEXT,
            puntos_actuales INTEGER DEFAULT 0,
            puntos_iniciales INTEGER DEFAULT 0,
            activa          INTEGER DEFAULT 1,
            es_pregenerada  INTEGER DEFAULT 0,
            fecha_creacion  DATETIME DEFAULT (datetime('now')),
            fecha_asignacion DATETIME
        );

        -- Gastos operativos
        CREATE TABLE IF NOT EXISTS gastos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha           DATE,
            categoria       TEXT,
            concepto        TEXT DEFAULT '',
            descripcion     TEXT,
            monto           DECIMAL(10,2) NOT NULL DEFAULT 0,
            monto_pagado    DECIMAL(10,2) DEFAULT 0,
            metodo_pago     TEXT DEFAULT 'EFECTIVO',
            estado          TEXT DEFAULT 'PAGADO',
            referencia      TEXT,
            comprobante     TEXT,
            proveedor_id    INTEGER,
            usuario         TEXT,
            recurrente      INTEGER DEFAULT 0,
            frecuencia      TEXT,
            fecha_proximo   DATETIME,
            activo          INTEGER DEFAULT 1,
            fecha_registro  DATETIME DEFAULT (datetime('now'))
        );

        -- Compras de pollo (negocio específico)
        CREATE TABLE IF NOT EXISTS compras_pollo (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha           DATE,
            numero_pollos   INTEGER DEFAULT 0,
            kilos_totales   DECIMAL(10,3) DEFAULT 0,
            costo_total     DECIMAL(10,2) DEFAULT 0,
            costo_kilo      DECIMAL(10,2) DEFAULT 0,
            proveedor       TEXT,
            proveedor_id    INTEGER REFERENCES proveedores(id),
            estado          TEXT DEFAULT 'PAGADO',
            metodo_pago     TEXT DEFAULT 'EFECTIVO',
            descripcion     TEXT,
            usuario         TEXT,
            lote            TEXT,
            sucursal_id     INTEGER DEFAULT 1,
            fecha_registro  DATETIME DEFAULT (datetime('now')),
            observaciones   TEXT,
            peso_bruto      DECIMAL(10,3),
            peso_neto       DECIMAL(10,3),
            precio_kg       DECIMAL(10,2),
            total           DECIMAL(10,2)
        );

        -- Rendimiento de transformación pollo→piezas
        CREATE TABLE IF NOT EXISTS rendimiento_pollo (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            compra_id       INTEGER REFERENCES compras_pollo(id),
            peso_entrada    DECIMAL(10,3),
            peso_piezas     DECIMAL(10,3),
            merma           DECIMAL(10,3),
            porcentaje_merma DECIMAL(5,2),
            usuario         TEXT,
            fecha           DATETIME DEFAULT (datetime('now'))
        );

        -- Transferencias entre sucursales
        CREATE TABLE IF NOT EXISTS transferencias_inventario (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_id         INTEGER NOT NULL REFERENCES productos(id),
            cantidad            DECIMAL(10,3),
            sucursal_origen     INTEGER,
            sucursal_destino    INTEGER,
            usuario_origen      TEXT,
            usuario_destino     TEXT,
            estado              TEXT DEFAULT 'pendiente',
            observaciones       TEXT,
            fecha               DATETIME DEFAULT (datetime('now')),
            fecha_recepcion     DATETIME
        );

        -- Personal / empleados
        CREATE TABLE IF NOT EXISTS personal (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre                  TEXT NOT NULL,
            apellidos               TEXT,
            apellido                TEXT,
            puesto                  TEXT,
            turno                   TEXT,
            salario                 REAL DEFAULT 0,
            fecha_ingreso           DATE,
            activo                  INTEGER DEFAULT 1,
            telefono                TEXT,
            email                   TEXT,
            direccion               TEXT,
            fecha_nacimiento        DATE,
            curp                    TEXT,
            rfc                     TEXT,
            nss                     TEXT,
            contacto_emergencia     TEXT,
            telefono_emergencia     TEXT,
            observaciones           TEXT
        );

        -- Asistencias
        CREATE TABLE IF NOT EXISTS asistencias (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            personal_id INTEGER NOT NULL REFERENCES personal(id),
            fecha       DATE NOT NULL,
            hora_entrada DATETIME,
            hora_salida  DATETIME,
            horas_trabajadas DECIMAL(4,2),
            observaciones TEXT
        );

        -- Configuración del sistema
        CREATE TABLE IF NOT EXISTS configuracion (
            clave               TEXT PRIMARY KEY,
            valor               TEXT,
            descripcion         TEXT,
            editable            INTEGER DEFAULT 1,
            categoria           TEXT DEFAULT 'General',
            fecha_actualizacion DATETIME DEFAULT (datetime('now'))
        );

        -- Logs del sistema
        CREATE TABLE IF NOT EXISTS logs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            nivel       TEXT NOT NULL DEFAULT 'INFO',
            modulo      TEXT,
            usuario     TEXT,
            mensaje     TEXT NOT NULL,
            detalles    TEXT,
            fecha       DATETIME DEFAULT (datetime('now'))
        );

        -- Permisos de módulo por usuario (normaliza el JSON actual)
        CREATE TABLE IF NOT EXISTS usuario_modulos (
            usuario_id  INTEGER NOT NULL REFERENCES usuarios(id) ON DELETE CASCADE,
            modulo      TEXT NOT NULL,
            PRIMARY KEY (usuario_id, modulo)
        );

        -- Sucursales
        CREATE TABLE IF NOT EXISTS sucursales (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre      TEXT NOT NULL,
            direccion   TEXT,
            telefono    TEXT,
            activa      INTEGER DEFAULT 1,
            es_matriz   INTEGER DEFAULT 0
        );

        -- Insertar sucursal por defecto
        INSERT OR IGNORE INTO sucursales (id, nombre, es_matriz) VALUES (1, 'Principal', 1);
    """)


def _m002_sync_tables(conn: sqlite3.Connection) -> None:
    """Tablas para sincronización offline-first."""
    conn.executescript("""
        -- Registro de eventos para sincronización
        CREATE TABLE IF NOT EXISTS sync_eventos (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid        TEXT UNIQUE NOT NULL DEFAULT (lower(hex(randomblob(16)))),
            tabla       TEXT NOT NULL,
            operacion   TEXT NOT NULL,  -- INSERT, UPDATE, DELETE
            registro_id INTEGER,
            registro_uuid TEXT,
            payload     TEXT,           -- JSON del registro
            sucursal_id INTEGER DEFAULT 1,
            usuario     TEXT,
            creado_en   DATETIME DEFAULT (datetime('now')),
            enviado     INTEGER DEFAULT 0,
            enviado_en  DATETIME,
            error       TEXT,
            intentos    INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_sync_no_enviado
            ON sync_eventos(enviado, creado_en)
            WHERE enviado = 0;

        -- Cola de sincronización pendiente
        CREATE TABLE IF NOT EXISTS sync_cola (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            evento_uuid TEXT NOT NULL REFERENCES sync_eventos(uuid),
            prioridad   INTEGER DEFAULT 5,
            bloqueado   INTEGER DEFAULT 0,
            proximo_intento DATETIME DEFAULT (datetime('now'))
        );

        -- Conflictos de sincronización
        CREATE TABLE IF NOT EXISTS sync_conflictos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            tabla           TEXT NOT NULL,
            registro_uuid   TEXT NOT NULL,
            payload_local   TEXT,
            payload_remoto  TEXT,
            resuelto        INTEGER DEFAULT 0,
            resolucion      TEXT,  -- 'local', 'remoto', 'manual'
            creado_en       DATETIME DEFAULT (datetime('now')),
            resuelto_en     DATETIME
        );
    """)


def _m003_indexes(conn: sqlite3.Connection) -> None:
    """Índices críticos para performance en alto volumen."""
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_ventas_fecha       ON ventas(fecha);
        CREATE INDEX IF NOT EXISTS idx_ventas_usuario     ON ventas(usuario);
        CREATE INDEX IF NOT EXISTS idx_ventas_cliente     ON ventas(cliente_id);
        CREATE INDEX IF NOT EXISTS idx_ventas_estado      ON ventas(estado);
        CREATE INDEX IF NOT EXISTS idx_detalles_venta     ON detalles_venta(venta_id);
        CREATE INDEX IF NOT EXISTS idx_detalles_producto  ON detalles_venta(producto_id);
        CREATE INDEX IF NOT EXISTS idx_mov_inv_producto   ON movimientos_inventario(producto_id, fecha);
        CREATE INDEX IF NOT EXISTS idx_mov_inv_tipo       ON movimientos_inventario(tipo);
        CREATE INDEX IF NOT EXISTS idx_mov_caja_fecha     ON movimientos_caja(fecha);
        CREATE INDEX IF NOT EXISTS idx_productos_nombre   ON productos(nombre);
        CREATE INDEX IF NOT EXISTS idx_productos_codigo   ON productos(codigo_barras) WHERE codigo_barras IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_clientes_nombre    ON clientes(nombre);
        CREATE INDEX IF NOT EXISTS idx_clientes_tel       ON clientes(telefono) WHERE telefono IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_logs_fecha         ON logs(fecha);
        CREATE INDEX IF NOT EXISTS idx_hist_puntos_cliente ON historico_puntos(cliente_id, fecha);
    """)


def _m004_modulos_permitidos_normalization(conn: sqlite3.Connection) -> None:
    """
    Migra modulos_permitidos (JSON/CSV en TEXT) → tabla usuario_modulos normalizada.
    Conserva la columna original para compatibilidad con código legacy durante transición.
    """
    import json

    # Verificar que la tabla usuario_modulos existe
    try:
        conn.execute("SELECT 1 FROM usuario_modulos LIMIT 1")
    except Exception:
        return  # Tabla aún no existe, skip

    try:
        rows = conn.execute("SELECT id, modulos_permitidos FROM usuarios WHERE modulos_permitidos IS NOT NULL").fetchall()
    except Exception:
        return  # Tabla usuarios sin la columna, skip
    for row in rows:
        raw = row["modulos_permitidos"] or ""
        modulos: list = []
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                modulos = [str(m) for m in parsed]
        except (json.JSONDecodeError, ValueError):
            # Fallback CSV
            modulos = [m.strip() for m in raw.split(",") if m.strip()]

        for modulo in modulos:
            conn.execute(
                "INSERT OR IGNORE INTO usuario_modulos (usuario_id, modulo) VALUES (?,?)",
                (row["id"], modulo)
            )


def _m006_columnas_faltantes_completo(conn: sqlite3.Connection) -> None:
    """
    Reparación exhaustiva: garantiza que TODAS las columnas que base.py
    necesita existan, sin importar qué versión de la BD creó las tablas.
    Completamente idempotente — ADD COLUMN solo si falta.
    """
    reparaciones = [
        # ── categorias ────────────────────────────────────────────────────────
        ("categorias",              "descripcion",          "TEXT"),
        ("categorias",              "activo",               "INTEGER DEFAULT 1"),
        ("categorias",              "fecha_creacion",       "DATETIME DEFAULT (datetime('now'))"),
        ("categorias",              "icono",                "TEXT"),
        # ── cajas ─────────────────────────────────────────────────────────────
        ("cajas",                   "ubicacion",            "TEXT"),
        ("cajas",                   "fondo_inicial",        "DECIMAL(10,2) DEFAULT 0"),
        ("cajas",                   "saldo_actual",         "DECIMAL(10,2) DEFAULT 0"),
        ("cajas",                   "estado",               "TEXT DEFAULT 'CERRADA'"),
        ("cajas",                   "fecha_apertura",       "TIMESTAMP"),
        ("cajas",                   "fecha_cierre",         "TIMESTAMP"),
        ("cajas",                   "usuario_apertura",     "TEXT"),
        ("cajas",                   "usuario_cierre",       "TEXT"),
        ("cajas",                   "observaciones",        "TEXT"),
        ("cajas",                   "sucursal_id",          "INTEGER DEFAULT 1"),
        # ── movimientos_caja ──────────────────────────────────────────────────
        ("movimientos_caja",        "venta_id",             "INTEGER"),
        ("movimientos_caja",        "forma_pago",           "TEXT"),
        ("movimientos_caja",        "referencia",           "TEXT"),
        ("movimientos_caja",        "caja_id",              "INTEGER"),
        # ── movimientos_inventario ────────────────────────────────────────────
        ("movimientos_inventario",  "tipo_movimiento",      "TEXT"),
        ("movimientos_inventario",  "costo_unitario",       "DECIMAL(10,2)"),
        ("movimientos_inventario",  "costo_total",          "DECIMAL(10,2)"),
        ("movimientos_inventario",  "existencia_anterior",  "REAL"),
        ("movimientos_inventario",  "existencia_nueva",     "REAL"),
        ("movimientos_inventario",  "referencia",           "TEXT"),
        ("movimientos_inventario",  "descripcion",          "TEXT"),
        ("movimientos_inventario",  "sucursal_id",          "INTEGER DEFAULT 1"),
        ("movimientos_inventario",  "uuid",                 "TEXT"),
        ("movimientos_inventario",  "_synced",              "INTEGER DEFAULT 0"),
        # ── productos ─────────────────────────────────────────────────────────
        ("productos",               "descripcion",          "TEXT"),
        ("productos",               "costo",                "DECIMAL(10,2) DEFAULT 0"),
        ("productos",               "unidad",               "TEXT DEFAULT 'kg'"),
        ("productos",               "categoria",            "TEXT"),
        ("productos",               "existencia",           "REAL DEFAULT 0"),
        ("productos",               "existencia_minima",    "REAL DEFAULT 0"),
        ("productos",               "activo",               "INTEGER DEFAULT 1"),
        ("productos",               "imagen",               "TEXT"),
        ("productos",               "codigo_barras",        "TEXT"),
        ("productos",               "es_compuesto",         "INTEGER DEFAULT 0"),
        ("productos",               "tipo_producto",        "TEXT DEFAULT 'producto'"),
        ("productos",               "sucursal_id",          "INTEGER DEFAULT 1"),
        ("productos",               "_sync_version",        "INTEGER DEFAULT 0"),
        ("productos",               "_deleted",             "INTEGER DEFAULT 0"),
        # ── ventas ────────────────────────────────────────────────────────────
        ("ventas",                  "cliente_id",           "INTEGER"),
        ("ventas",                  "subtotal",             "DECIMAL(10,2) DEFAULT 0"),
        ("ventas",                  "descuento",            "DECIMAL(10,2) DEFAULT 0"),
        ("ventas",                  "total",                "DECIMAL(10,2) DEFAULT 0"),
        ("ventas",                  "forma_pago",           "TEXT DEFAULT 'EFECTIVO'"),
        ("ventas",                  "estado",               "TEXT DEFAULT 'COMPLETADA'"),
        ("ventas",                  "puntos_generados",     "INTEGER DEFAULT 0"),
        ("ventas",                  "puntos_canjeados",     "INTEGER DEFAULT 0"),
        ("ventas",                  "notas",                "TEXT"),
        ("ventas",                  "sucursal_id",          "INTEGER DEFAULT 1"),
        ("ventas",                  "uuid",                 "TEXT"),
        ("ventas",                  "_sync_version",        "INTEGER DEFAULT 0"),
        ("ventas",                  "_synced",              "INTEGER DEFAULT 0"),
        # ── detalles_venta ────────────────────────────────────────────────────
        ("detalles_venta",          "nombre_producto",      "TEXT"),
        ("detalles_venta",          "precio_unitario",      "DECIMAL(10,2) DEFAULT 0"),
        ("detalles_venta",          "subtotal",             "DECIMAL(10,2) DEFAULT 0"),
        ("detalles_venta",          "descuento",            "DECIMAL(10,2) DEFAULT 0"),
        # ── clientes ─────────────────────────────────────────────────────────
        ("clientes",                "apellido_paterno",     "TEXT"),
        ("clientes",                "apellido_materno",     "TEXT"),
        ("clientes",                "telefono",             "TEXT"),
        ("clientes",                "email",                "TEXT"),
        ("clientes",                "direccion",            "TEXT"),
        ("clientes",                "puntos",               "INTEGER DEFAULT 0"),
        ("clientes",                "activo",               "INTEGER DEFAULT 1"),
        ("clientes",                "fecha_registro",       "DATETIME DEFAULT (datetime('now'))"),
        ("clientes",                "uuid",                 "TEXT"),
        ("clientes",                "_sync_version",        "INTEGER DEFAULT 0"),
        ("clientes",                "_deleted",             "INTEGER DEFAULT 0"),
        # ── usuarios ──────────────────────────────────────────────────────────
        ("usuarios",                "nombre",               "TEXT"),
        ("usuarios",                "email",                "TEXT"),
        ("usuarios",                "telefono",             "TEXT"),
        ("usuarios",                "activo",               "INTEGER DEFAULT 1"),
        ("usuarios",                "fecha_creacion",       "DATETIME DEFAULT (datetime('now'))"),
        ("usuarios",                "ultimo_acceso",        "DATETIME"),
        ("usuarios",                "modulos_permitidos",   "TEXT"),
        ("usuarios",                "sucursal_id",          "INTEGER DEFAULT 1"),
        ("usuarios",                "uuid",                 "TEXT"),
        ("usuarios",                "_sync_version",        "INTEGER DEFAULT 0"),
        ("usuarios",                "_deleted",             "INTEGER DEFAULT 0"),
        # ── historico_puntos ──────────────────────────────────────────────────
        ("historico_puntos",        "saldo_actual",         "INTEGER DEFAULT 0"),
        ("historico_puntos",        "usuario",              "TEXT"),
        ("historico_puntos",        "venta_id",             "INTEGER"),
        # ── compras_pollo ─────────────────────────────────────────────────────
        ("compras_pollo",           "lote",                 "TEXT"),
        ("compras_pollo",           "fecha_registro",       "DATETIME DEFAULT (datetime('now'))"),
        # ── gastos ────────────────────────────────────────────────────────────
        ("gastos",                  "monto_pagado",         "DECIMAL(10,2) DEFAULT 0"),
        ("gastos",                  "metodo_pago",          "TEXT DEFAULT 'EFECTIVO'"),
        ("gastos",                  "proveedor_id",         "INTEGER"),
        ("gastos",                  "estado",               "TEXT DEFAULT 'PAGADO'"),
        ("gastos",                  "usuario",              "TEXT"),
        ("gastos",                  "referencia",           "TEXT"),
        ("gastos",                  "comprobante",          "TEXT"),
        # ── configuracion ────────────────────────────────────────────────────
        ("configuracion",           "editable",             "INTEGER DEFAULT 1"),
        ("configuracion",           "categoria",            "TEXT"),
        ("configuracion",           "fecha_actualizacion",  "DATETIME"),
        # ── logs ──────────────────────────────────────────────────────────────
        ("logs",                    "ip",                   "TEXT"),
        ("logs",                    "user_agent",           "TEXT"),
    ]
    for tabla, col, defn in reparaciones:
        add_column_if_missing(conn, tabla, col, defn)


# ── Registro público de todas las migraciones ────────────────────────────────


def _m007_lote_enterprise(conn: sqlite3.Connection) -> None:
    """
    M007: PolloEngine enterprise columns.
    - movimientos_inventario.lote_id  (FIFO per-lote tracking)
    - compras_pollo.sucursal_id       (multi-branch)
    - inventario_subproductos.lote    (folio ref)
    - rendimiento_derivados           (recipe vs actual)
    - transformaciones_pollo          (transformation log)
    """
    add_column_if_missing(conn, "movimientos_inventario", "lote_id",      "INTEGER")
    add_column_if_missing(conn, "compras_pollo",          "sucursal_id",  "INTEGER DEFAULT 1")
    add_column_if_missing(conn, "inventario_subproductos","lote",         "TEXT")
    add_column_if_missing(conn, "compras_pollo",          "folio_lote",   "TEXT")

    conn.executescript("""
        CREATE TABLE IF NOT EXISTS rendimiento_derivados (
            id                     INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_pollo_id      INTEGER REFERENCES productos(id),
            producto_derivado_id   INTEGER NOT NULL REFERENCES productos(id),
            porcentaje_rendimiento DECIMAL(5,2) NOT NULL DEFAULT 0,
            activo                 INTEGER DEFAULT 1,
            UNIQUE(producto_pollo_id, producto_derivado_id)
        );

        CREATE TABLE IF NOT EXISTS transformaciones_pollo (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            lote_id         INTEGER NOT NULL REFERENCES compras_pollo(id),
            kg_entrada      DECIMAL(10,3) NOT NULL,
            kg_piezas       DECIMAL(10,3) DEFAULT 0,
            kg_merma        DECIMAL(10,3) DEFAULT 0,
            pct_rendimiento DECIMAL(5,2)  DEFAULT 0,
            pct_merma       DECIMAL(5,2)  DEFAULT 0,
            usuario         TEXT,
            notas           TEXT,
            fecha           DATETIME DEFAULT (datetime('now'))
        );
    """)


def _m008_enterprise_pollo_full(conn: sqlite3.Connection) -> None:
    """
    M008: Tablas enterprise completas para cadena pollería multi-sucursal.
    Agrega: compras_pollo_global, inventario_pollo_sucursal,
            recetas_pollo, recetas_pollo_detalle, traspasos_inventario, event_log.
    """
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS compras_pollo_global (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha               DATE    NOT NULL,
            proveedor           TEXT    DEFAULT '',
            numero_pollos       INTEGER NOT NULL CHECK(numero_pollos > 0),
            peso_total_kg       REAL    NOT NULL CHECK(peso_total_kg > 0),
            costo_total         DECIMAL(10,2) NOT NULL,
            costo_por_kg        DECIMAL(10,4) NOT NULL,
            lote_id             TEXT    UNIQUE,
            estado              TEXT    NOT NULL DEFAULT 'activo',
            usuario_registro    TEXT    NOT NULL DEFAULT 'Sistema',
            sucursal_destino_id INTEGER,
            notas               TEXT,
            fecha_registro      DATETIME DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS inventario_pollo_sucursal (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            sucursal_id         INTEGER NOT NULL,
            compra_global_id    INTEGER REFERENCES compras_pollo_global(id),
            numero_pollos       INTEGER NOT NULL,
            peso_kg_disponible  REAL    NOT NULL DEFAULT 0,
            peso_kg_original    REAL    NOT NULL,
            fecha_recepcion     DATE    NOT NULL,
            lote_id             TEXT,
            costo_kg            DECIMAL(10,4) DEFAULT 0,
            estado              TEXT    DEFAULT 'disponible',
            usuario_recepcion   TEXT,
            fecha_actualizacion DATETIME DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_inv_pollo_suc
            ON inventario_pollo_sucursal(sucursal_id);

        CREATE TABLE IF NOT EXISTS recetas_pollo (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            nombre_receta   TEXT    NOT NULL UNIQUE,
            producto_base_id INTEGER NOT NULL,
            activa          INTEGER DEFAULT 1,
            notas           TEXT,
            creado_por      TEXT,
            fecha_creacion  DATETIME DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS recetas_pollo_detalle (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            receta_id               INTEGER NOT NULL
                REFERENCES recetas_pollo(id) ON DELETE CASCADE,
            producto_resultado_id   INTEGER NOT NULL,
            porcentaje_rendimiento  REAL    NOT NULL DEFAULT 0
                CHECK(porcentaje_rendimiento >= 0 AND porcentaje_rendimiento <= 100),
            porcentaje_merma        REAL    DEFAULT 0
                CHECK(porcentaje_merma >= 0 AND porcentaje_merma <= 100),
            descripcion             TEXT,
            orden                   INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_receta_detalle_receta
            ON recetas_pollo_detalle(receta_id);

        CREATE TABLE IF NOT EXISTS traspasos_inventario (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid                TEXT    NOT NULL UNIQUE,
            sucursal_origen_id  INTEGER NOT NULL,
            sucursal_destino_id INTEGER NOT NULL,
            producto_id         INTEGER NOT NULL,
            cantidad            REAL    NOT NULL CHECK(cantidad > 0),
            estado              TEXT    DEFAULT 'pendiente',
            fecha_solicitud     DATETIME DEFAULT (datetime('now')),
            fecha_envio         DATETIME,
            fecha_recepcion     DATETIME,
            usuario_origen      TEXT    NOT NULL DEFAULT 'Sistema',
            usuario_destino     TEXT,
            observaciones       TEXT,
            movimiento_salida_id  INTEGER,
            movimiento_entrada_id INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_traspasos_estado
            ON traspasos_inventario(estado);

        CREATE TABLE IF NOT EXISTS event_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid            TEXT    NOT NULL UNIQUE,
            tipo            TEXT    NOT NULL,
            entidad         TEXT    NOT NULL,
            entidad_id      INTEGER,
            payload         TEXT    NOT NULL,
            sucursal_id     INTEGER NOT NULL DEFAULT 1,
            usuario         TEXT    NOT NULL DEFAULT 'Sistema',
            synced          INTEGER DEFAULT 0,
            sync_intentos   INTEGER DEFAULT 0,
            sync_error      TEXT,
            fecha           DATETIME DEFAULT (datetime('now')),
            fecha_sync      DATETIME
        );
        CREATE INDEX IF NOT EXISTS idx_event_log_synced
            ON event_log(synced);
        CREATE INDEX IF NOT EXISTS idx_event_log_tipo
            ON event_log(tipo);
    """)




def _m011_structural_hardening(conn) -> None:
    """Delega en módulo externo."""
    from migrations.v11_structural_hardening import up as _up
    _up(conn)


def _m012_inventario_enterprise(conn) -> None:
    """Delega en módulo externo."""
    from migrations.v12_inventario_enterprise import up as _up
    _up(conn)


def _m013_v8_industrial(conn) -> None:
    """Delega en módulo externo."""
    from migrations.v13_v8_industrial import up as _up
    _up(conn)


def _m010_product_recipes(conn: sqlite3.Connection) -> None:
    """Delega en el módulo externo para mantener engine.py limpio."""
    from migrations.v10_product_recipes import up as _up
    _up(conn)


def _m009_industrial_batches(conn: sqlite3.Connection) -> None:
    """Delega en el módulo externo para mantener engine.py limpio."""
    from migrations.v9_industrial_batches import up as _up
    _up(conn)

MIGRATIONS: List[Migration] = [
    Migration(1, "Esquema base SPJ enterprise",          _m001_initial_schema),
    Migration(2, "Tablas sync offline-first",             _m002_sync_tables),
    Migration(3, "Índices de performance",                _m003_indexes),
    Migration(4, "Normalización modulos_permitidos JSON", _m004_modulos_permitidos_normalization),
    Migration(5, "Reparación columnas faltantes (categorias + sync)", _m006_columnas_faltantes_completo),
    Migration(6, "Reparación exhaustiva todas las tablas",            _m006_columnas_faltantes_completo),
    Migration(7, "PolloEngine enterprise — lote_id + transformaciones",   _m007_lote_enterprise),
    Migration(8, "Enterprise pollo multi-sucursal — tablas completas",    _m008_enterprise_pollo_full),
    Migration(9, "Industrial FIFO — chicken_batches + branch_inventory_batches + system_locks + índices enterprise", _m009_industrial_batches),
    Migration(10, "Recetas consumo producto — product_recipes (surtidos/retazos/combos)", _m010_product_recipes),
    Migration(11, "Endurecimiento estructural — parent_batch_id, conciliation_runs, índices enterprise", _m011_structural_hardening),
    Migration(12, "Inventario enterprise pollería — global/sucursal/recepciones/recetas/traspasos", _m012_inventario_enterprise),
    Migration(13, "v8 Industrial — root_batch_id, origin_device_id, costo_unitario_real, margen_real, índices", _m013_v8_industrial),
]


def _m014_v9_enterprise(conn):
    """Delega en el módulo externo para mantener engine.py limpio."""
    from migrations.v14_v9_enterprise import up as _up
    _up(conn)


# Registrar v14 post-facto
MIGRATIONS.append(Migration(14, "v9 Enterprise — card_batches, loyalty_scores, forecast_cache, hardware_config, ticket_design, compras_inventariables", _m014_v9_enterprise))


def _m015_recipe_abarrotes(conn: sqlite3.Connection) -> None:
    from migrations.v15_recipe_abarrotes import up
    up(conn)


MIGRATIONS.append(Migration(
    15,
    "v9.1 — product_recipes_abarrotes (recetas ingredientes), movimientos_inventario cols extras",
    _m015_recipe_abarrotes,
))

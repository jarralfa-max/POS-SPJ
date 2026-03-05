# migrations/v14_v9_enterprise.py
# ── MIGRACIÓN v14 — v9 ENTERPRISE MULTI-SUCURSAL ────────────────────────────
# A. card_batches          — Lotes de tarjetas de fidelidad (trazabilidad)
# B. tarjetas_fidelidad    — +estado, +batch_id, +fecha_impresion
# C. card_assignment_history — Historial asignaciones/cambios de tarjeta
# D. loyalty_scores        — Puntuación multivariable por cliente
# E. loyalty_config        — Pesos configurables por dimensión
# F. forecast_cache        — Cache pronósticos diarios por producto
# G. hardware_config       — Configuración impresora/cajón/scanner
# H. ticket_design_config  — Diseños de tickets y etiquetas
# I. compras_inventariables — Compras que generan lote global de inventario
# Idempotente. Seguro correr múltiples veces.
from __future__ import annotations
import sqlite3


def _add_col(conn: sqlite3.Connection, table: str, col: str, defn: str) -> None:
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
    except sqlite3.OperationalError:
        pass  # columna ya existe


def up(conn: sqlite3.Connection) -> None:
    # ── A. card_batches — lotes de tarjetas ──────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS card_batches (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid              TEXT UNIQUE NOT NULL,
            nombre            TEXT NOT NULL,
            codigo_inicio     TEXT NOT NULL,
            codigo_fin        TEXT NOT NULL,
            cantidad          INTEGER NOT NULL DEFAULT 0,
            cantidad_libres   INTEGER NOT NULL DEFAULT 0,
            cantidad_asignadas INTEGER NOT NULL DEFAULT 0,
            estado            TEXT NOT NULL DEFAULT 'activo'
                                  CHECK(estado IN ('activo','cerrado','anulado')),
            notas             TEXT,
            generado_por      TEXT,
            fecha_generacion  DATETIME DEFAULT (datetime('now')),
            fecha_cierre      DATETIME
        )
    """)

    # ── B. tarjetas_fidelidad — extender con estado y batch ──────────────────
    _add_col(conn, "tarjetas_fidelidad", "batch_id",
             "INTEGER REFERENCES card_batches(id)")
    _add_col(conn, "tarjetas_fidelidad", "estado",
             "TEXT NOT NULL DEFAULT 'libre' CHECK(estado IN "
             "('generada','impresa','libre','asignada','bloqueada'))")
    _add_col(conn, "tarjetas_fidelidad", "fecha_impresion",  "DATETIME")
    _add_col(conn, "tarjetas_fidelidad", "bloqueado_por",    "TEXT")
    _add_col(conn, "tarjetas_fidelidad", "motivo_bloqueo",   "TEXT")
    _add_col(conn, "tarjetas_fidelidad", "nivel",            "TEXT DEFAULT 'Bronce'")

    # Backfill estado: tarjetas con cliente = asignadas, resto = libres
    try:
        conn.execute("""
            UPDATE tarjetas_fidelidad
            SET estado = CASE
                WHEN id_cliente IS NOT NULL THEN 'asignada'
                WHEN activa = 0             THEN 'bloqueada'
                ELSE 'libre'
            END
            WHERE estado = 'libre'
              AND (id_cliente IS NOT NULL OR activa = 0)
        """)
    except Exception:
        pass

    # ── C. card_assignment_history ───────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS card_assignment_history (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            tarjeta_id       INTEGER NOT NULL REFERENCES tarjetas_fidelidad(id),
            cliente_id_prev  INTEGER REFERENCES clientes(id),
            cliente_id_nuevo INTEGER REFERENCES clientes(id),
            accion           TEXT NOT NULL
                                 CHECK(accion IN ('asignacion','reasignacion',
                                                  'bloqueo','desbloqueo','liberacion')),
            motivo           TEXT,
            usuario          TEXT,
            fecha            DATETIME DEFAULT (datetime('now'))
        )
    """)

    # ── D. loyalty_scores ────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_scores (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            cliente_id        INTEGER NOT NULL REFERENCES clientes(id),
            score_frecuencia  REAL DEFAULT 0,
            score_volumen     REAL DEFAULT 0,
            score_margen      REAL DEFAULT 0,
            score_comunidad   REAL DEFAULT 0,
            score_total       REAL DEFAULT 0,
            nivel             TEXT DEFAULT 'Bronce'
                                  CHECK(nivel IN ('Bronce','Plata','Oro','Platino')),
            visitas_periodo   INTEGER DEFAULT 0,
            importe_total     REAL DEFAULT 0,
            margen_generado   REAL DEFAULT 0,
            referidos         INTEGER DEFAULT 0,
            fecha_calculo     DATETIME DEFAULT (datetime('now')),
            periodo_inicio    DATE,
            periodo_fin       DATE,
            UNIQUE(cliente_id)
        )
    """)

    # ── E. loyalty_config ────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS loyalty_config (
            clave  TEXT PRIMARY KEY,
            valor  TEXT NOT NULL,
            descripcion TEXT
        )
    """)
    # Valores por defecto de pesos scoring
    defaults = [
        ("peso_frecuencia",   "30", "% peso dimensión frecuencia (0-100)"),
        ("peso_volumen",      "30", "% peso dimensión volumen de compra"),
        ("peso_margen",       "30", "% peso dimensión margen generado"),
        ("peso_comunidad",    "10", "% peso dimensión comunidad/referidos"),
        ("periodo_dias",      "90", "Días atrás para calcular métricas"),
        ("umbral_plata",      "40", "Score mínimo para nivel Plata"),
        ("umbral_oro",        "65", "Score mínimo para nivel Oro"),
        ("umbral_platino",    "85", "Score mínimo para nivel Platino"),
        ("puntos_por_peso",   "1",  "Puntos por peso gastado"),
        ("bonus_referido",    "50", "Puntos bono por cada referido registrado"),
    ]
    for clave, valor, desc in defaults:
        conn.execute(
            "INSERT OR IGNORE INTO loyalty_config (clave, valor, descripcion) VALUES (?,?,?)",
            (clave, valor, desc)
        )

    # ── F. forecast_cache ────────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS forecast_cache (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            producto_id       INTEGER NOT NULL REFERENCES productos(id),
            sucursal_id       INTEGER DEFAULT 1,
            fecha_prediccion  DATE NOT NULL,
            cantidad_predicha REAL NOT NULL DEFAULT 0,
            intervalo_bajo    REAL DEFAULT 0,
            intervalo_alto    REAL DEFAULT 0,
            metodo            TEXT DEFAULT 'media_movil'
                                  CHECK(metodo IN ('media_movil','tendencia','promedio_simple')),
            mape              REAL DEFAULT 0,
            generado_en       DATETIME DEFAULT (datetime('now')),
            UNIQUE(producto_id, sucursal_id, fecha_prediccion, metodo)
        )
    """)

    # ── G. hardware_config ───────────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS hardware_config (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo          TEXT NOT NULL UNIQUE
                              CHECK(tipo IN ('impresora','cajon','scanner','bascula')),
            habilitado    INTEGER NOT NULL DEFAULT 0,
            configuracion TEXT NOT NULL DEFAULT '{}',
            descripcion   TEXT,
            actualizado_en DATETIME DEFAULT (datetime('now'))
        )
    """)
    hw_defaults = [
        ("impresora", 0, '{"tipo":"escpos","puerto":"USB","ancho_mm":80}', "Impresora térmica ESC/POS"),
        ("cajon",     0, '{"metodo":"escpos","pin":"kick1"}',              "Cajón de dinero"),
        ("scanner",   0, '{"debounce_ms":80,"min_len":3}',                 "Lector de código de barras"),
        ("bascula",   0, '{"puerto":"COM3","baud":9600}',                  "Báscula serial"),
    ]
    for tipo, hab, cfg, desc in hw_defaults:
        conn.execute(
            "INSERT OR IGNORE INTO hardware_config (tipo, habilitado, configuracion, descripcion) "
            "VALUES (?,?,?,?)",
            (tipo, hab, cfg, desc)
        )

    # ── H. ticket_design_config ──────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticket_design_config (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            tipo        TEXT NOT NULL DEFAULT 'ticket'
                            CHECK(tipo IN ('ticket','etiqueta')),
            nombre      TEXT NOT NULL,
            elementos   TEXT NOT NULL DEFAULT '[]',
            activo      INTEGER NOT NULL DEFAULT 0,
            ancho_mm    INTEGER DEFAULT 80,
            alto_mm     INTEGER DEFAULT 0,
            creado_en   DATETIME DEFAULT (datetime('now')),
            UNIQUE(tipo, nombre)
        )
    """)
    # Diseño ticket por defecto
    import json
    ticket_default = json.dumps([
        {"tipo": "texto", "id": "header_empresa", "variable": "empresa",
         "fuente_size": 14, "bold": True, "align": "center", "y_pos": 0},
        {"tipo": "texto", "id": "header_sucursal", "variable": "sucursal",
         "fuente_size": 10, "bold": False, "align": "center", "y_pos": 1},
        {"tipo": "separador", "id": "sep1", "y_pos": 2},
        {"tipo": "texto", "id": "fecha", "variable": "fecha",
         "fuente_size": 9, "bold": False, "align": "left", "y_pos": 3},
        {"tipo": "texto", "id": "folio", "variable": "folio",
         "fuente_size": 9, "bold": False, "align": "left", "y_pos": 4},
        {"tipo": "texto", "id": "cajero", "variable": "cajero",
         "fuente_size": 9, "bold": False, "align": "left", "y_pos": 5},
        {"tipo": "separador", "id": "sep2", "y_pos": 6},
        {"tipo": "tabla_items", "id": "items", "y_pos": 7},
        {"tipo": "separador", "id": "sep3", "y_pos": 8},
        {"tipo": "totales", "id": "totales", "y_pos": 9},
        {"tipo": "texto", "id": "footer", "variable": "footer",
         "fuente_size": 9, "bold": False, "align": "center", "y_pos": 10},
    ])
    conn.execute(
        "INSERT OR IGNORE INTO ticket_design_config (tipo, nombre, elementos, activo) "
        "VALUES ('ticket', 'Default', ?, 1)",
        (ticket_default,)
    )
    etiqueta_default = json.dumps([
        {"tipo": "texto",    "id": "nombre",    "variable": "nombre",
         "fuente_size": 12, "bold": True,  "align": "center", "y_pos": 0},
        {"tipo": "precio",   "id": "precio",    "variable": "precio",
         "fuente_size": 18, "bold": True,  "align": "center", "y_pos": 1},
        {"tipo": "qr",       "id": "qr",        "variable": "codigo_barras",
         "size": 80,                                           "y_pos": 2},
        {"tipo": "barcode",  "id": "barcode",   "variable": "codigo_barras",
         "height": 40,                                         "y_pos": 3},
    ])
    conn.execute(
        "INSERT OR IGNORE INTO ticket_design_config (tipo, nombre, elementos, activo) "
        "VALUES ('etiqueta', 'Default', ?, 1)",
        (etiqueta_default,)
    )

    # ── I. compras_inventariables ─────────────────────────────────────────────
    conn.execute("""
        CREATE TABLE IF NOT EXISTS compras_inventariables (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            uuid             TEXT UNIQUE NOT NULL,
            gasto_id         INTEGER REFERENCES gastos(id),
            producto_id      INTEGER NOT NULL REFERENCES productos(id),
            batch_id_global  INTEGER REFERENCES chicken_batches(id),
            proveedor        TEXT,
            volumen          REAL NOT NULL DEFAULT 0,
            unidad           TEXT DEFAULT 'kg',
            costo_unitario   REAL NOT NULL DEFAULT 0,
            costo_total      REAL NOT NULL DEFAULT 0,
            forma_pago       TEXT DEFAULT 'EFECTIVO',
            es_credito       INTEGER DEFAULT 0,
            monto_pagado     REAL DEFAULT 0,
            saldo_pendiente  REAL DEFAULT 0,
            fecha_vencimiento DATE,
            estado           TEXT DEFAULT 'pagado'
                                 CHECK(estado IN ('pagado','credito','parcial')),
            notas            TEXT,
            sucursal_id      INTEGER DEFAULT 1,
            usuario          TEXT,
            fecha            DATETIME DEFAULT (datetime('now'))
        )
    """)

    # ── Índices v14 ───────────────────────────────────────────────────────────
    idx = [
        ("idx_cb_batch_estado",  "card_batches(estado)"),
        ("idx_tf_batch",         "tarjetas_fidelidad(batch_id)"),
        ("idx_tf_estado",        "tarjetas_fidelidad(estado)"),
        ("idx_cah_tarjeta",      "card_assignment_history(tarjeta_id)"),
        ("idx_cah_cliente",      "card_assignment_history(cliente_id_nuevo)"),
        ("idx_ls_cliente",       "loyalty_scores(cliente_id)"),
        ("idx_ls_nivel",         "loyalty_scores(nivel)"),
        ("idx_fc_prod_fecha",    "forecast_cache(producto_id, fecha_prediccion)"),
        ("idx_ci_producto",      "compras_inventariables(producto_id)"),
        ("idx_ci_fecha",         "compras_inventariables(fecha)"),
    ]
    for name, defn in idx:
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {defn}")
        except Exception:
            pass

    conn.commit()


def down(conn: sqlite3.Connection) -> None:
    # Reversión parcial segura
    for table in [
        "compras_inventariables", "ticket_design_config",
        "hardware_config", "forecast_cache",
        "loyalty_config", "loyalty_scores",
        "card_assignment_history",
    ]:
        try:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception:
            pass
    conn.commit()

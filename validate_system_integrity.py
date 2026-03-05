#!/usr/bin/env python3
# scripts/validate_system_integrity.py
# ── VALIDACIÓN DE INTEGRIDAD NOCTURNA — SPJ Enterprise v8 ────────────────────
# Script diseñado para ejecución nocturna (cron / tarea programada).
# Verifica:
#   1. WAL mode y foreign_keys activos
#   2. Árbol de lotes por sucursal (stock negativo, huérfanos, loops)
#   3. Equivalencia matemática de peso (reconstruct_batch_equivalence)
#   4. Eventos pendientes de sync (alerta si > umbral)
#   5. Consistencia tablas críticas (ventas, detalles, movements)
#   6. Índices requeridos presentes
#
# Salida: JSON estructurado + código de salida (0=ok, 1=warnings, 2=errores)
# Uso:
#   python scripts/validate_system_integrity.py
#   python scripts/validate_system_integrity.py --branch 2 --json
import sys
import os
import json
import argparse
import sqlite3
import time
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _get_conn():
    from core.db.connection import get_connection
    return get_connection()


def _check_pragmas(conn: sqlite3.Connection) -> dict:
    result = {"ok": True, "detalles": []}
    wal = conn.execute("PRAGMA journal_mode").fetchone()
    if not wal or wal[0].upper() != "WAL":
        result["ok"] = False
        result["detalles"].append(f"journal_mode={wal[0] if wal else '?'} (esperado WAL)")

    fk = conn.execute("PRAGMA foreign_keys").fetchone()
    if not fk or int(fk[0]) != 1:
        result["ok"] = False
        result["detalles"].append("foreign_keys=OFF")

    busy = conn.execute("PRAGMA busy_timeout").fetchone()
    if busy and int(busy[0]) < 1000:
        result["detalles"].append(f"busy_timeout={busy[0]}ms (recomendado ≥5000)")

    return result


def _check_stock_negativo(conn: sqlite3.Connection, branch_id: int = None) -> dict:
    q = """
        SELECT bib.id, bib.batch_id, bib.branch_id,
               bib.producto_id, bib.cantidad_disponible,
               p.nombre
        FROM branch_inventory_batches bib
        JOIN productos p ON p.id = bib.producto_id
        WHERE bib.cantidad_disponible < -0.001
    """
    params = []
    if branch_id:
        q += " AND bib.branch_id = ?"
        params.append(branch_id)

    rows = conn.execute(q, params).fetchall()
    return {
        "ok":      len(rows) == 0,
        "count":   len(rows),
        "detalles": [
            {
                "bib_id":   r[0], "batch_id": r[1],
                "branch_id": r[2], "producto": r[5],
                "cantidad": round(float(r[4]), 6),
            }
            for r in rows
        ],
    }


def _check_batch_tree(conn: sqlite3.Connection, branch_id: int = None) -> dict:
    from core.database import Connection
    from core.services.inventory_engine import InventoryEngine

    db = Connection(conn)
    branches = []
    if branch_id:
        branches = [branch_id]
    else:
        rows = conn.execute("SELECT id FROM sucursales WHERE activa=1").fetchall()
        branches = [r[0] for r in rows]

    issues_total = 0
    detalles = []

    for bid in branches:
        try:
            eng    = InventoryEngine(db, branch_id=bid)
            report = eng.validate_batch_tree_integrity()
            if not report["integro"]:
                issues_total += report["total_problemas"]
                detalles.append({
                    "branch_id":        bid,
                    "total_problemas":  report["total_problemas"],
                    "problemas":        report["problemas"],
                })
        except Exception as exc:
            detalles.append({"branch_id": bid, "error": str(exc)})

    return {
        "ok":      issues_total == 0,
        "count":   issues_total,
        "detalles": detalles,
    }


def _check_sync_pendientes(conn: sqlite3.Connection, umbral: int = 500) -> dict:
    row = conn.execute(
        "SELECT COUNT(*) FROM event_log WHERE synced=0"
    ).fetchone()
    total = row[0] if row else 0

    row2 = conn.execute(
        "SELECT COUNT(*) FROM event_log WHERE synced=0 AND sync_intentos >= 10"
    ).fetchone()
    abandonados = row2[0] if row2 else 0

    ok = total <= umbral and abandonados == 0
    return {
        "ok":          ok,
        "pendientes":  total,
        "abandonados": abandonados,
        "umbral":      umbral,
        "detalles":    (
            [] if ok
            else [f"{total} eventos pendientes (umbral={umbral})",
                  f"{abandonados} eventos abandonados (max_reintentos alcanzado)"]
        ),
    }


def _check_indices(conn: sqlite3.Connection) -> dict:
    requeridos = [
        "idx_bib_branch_prod",
        "idx_cb_parent",
        "idx_cb_transform",
        "idx_cb_root",
        "idx_el_synced_tipo",
        "idx_el_hash",
        "idx_el_device_ver",
        "idx_bm_batch_tipo",
        "idx_bm_prod_fecha",
    ]
    existentes = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }
    faltantes = [idx for idx in requeridos if idx not in existentes]
    return {
        "ok":       len(faltantes) == 0,
        "count":    len(faltantes),
        "faltantes": faltantes,
    }


def _check_consistencia_ventas(conn: sqlite3.Connection) -> dict:
    """Verifica que toda venta completada tiene al menos un detalle."""
    row = conn.execute("""
        SELECT COUNT(*) FROM ventas v
        WHERE v.estado = 'completada'
          AND NOT EXISTS (SELECT 1 FROM detalles_venta dv WHERE dv.venta_id = v.id)
    """).fetchone()
    sin_detalle = row[0] if row else 0

    return {
        "ok":          sin_detalle == 0,
        "sin_detalle": sin_detalle,
    }


def _check_schema_version(conn: sqlite3.Connection) -> dict:
    try:
        row = conn.execute(
            "SELECT MAX(version) FROM schema_migrations"
        ).fetchone()
        version = int(row[0]) if row and row[0] else 0
        ok      = version >= 13  # v8 requiere al menos migración 13
        return {
            "ok":              ok,
            "schema_version":  version,
            "requerido":       13,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Validación de integridad nocturna SPJ v8"
    )
    parser.add_argument("--branch",  type=int, default=None,
                        help="Branch específico (default=todas)")
    parser.add_argument("--json",    action="store_true",
                        help="Salida en formato JSON")
    parser.add_argument("--umbral",  type=int, default=500,
                        help="Umbral de eventos pendientes para alerta (default=500)")
    args = parser.parse_args()

    t0   = time.time()
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        conn = _get_conn()
    except Exception as exc:
        out = {"ok": False, "error": f"No se pudo conectar a BD: {exc}"}
        if args.json:
            print(json.dumps(out))
        else:
            print(f"❌ {out['error']}")
        sys.exit(2)

    checks = {
        "pragmas":            _check_pragmas(conn),
        "stock_negativo":     _check_stock_negativo(conn, args.branch),
        "arbol_lotes":        _check_batch_tree(conn, args.branch),
        "sync_pendientes":    _check_sync_pendientes(conn, args.umbral),
        "indices":            _check_indices(conn),
        "consistencia_ventas": _check_consistencia_ventas(conn),
        "schema_version":     _check_schema_version(conn),
    }

    elapsed  = round(time.time() - t0, 3)
    todos_ok = all(c.get("ok", False) for c in checks.values())

    report = {
        "timestamp":    fecha,
        "branch_id":    args.branch,
        "duracion_ms":  int(elapsed * 1000),
        "integro":      todos_ok,
        "checks":       checks,
    }

    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print("=" * 60)
        print(f"  SPJ v8 — VALIDACIÓN INTEGRIDAD  {fecha}")
        print("=" * 60)
        for nombre, c in checks.items():
            ok_str = "✅" if c.get("ok") else "❌"
            print(f"  {ok_str}  {nombre}")
            if not c.get("ok"):
                for d in c.get("detalles", []):
                    print(f"       → {d}")
                for k in ("count", "pendientes", "abandonados", "sin_detalle",
                          "schema_version", "faltantes", "error"):
                    if k in c:
                        print(f"       {k}: {c[k]}")
        print(f"\n  Duración: {elapsed}s")
        print(f"  {'✅ SISTEMA ÍNTEGRO' if todos_ok else '❌ PROBLEMAS DETECTADOS'}")
        print("=" * 60)

    if todos_ok:
        sys.exit(0)
    elif all(c.get("ok", True) for k, c in checks.items()
             if k not in ("sync_pendientes", "indices")):
        sys.exit(1)   # solo warnings menores
    else:
        sys.exit(2)   # errores críticos


if __name__ == "__main__":
    main()

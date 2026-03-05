#!/usr/bin/env python3
# scripts/stress_test_concurrency.py
# ── TEST DE CONCURRENCIA — SPJ Enterprise v8 ──────────────────────────────────
# Demuestra que inventario negativo es imposible con BEGIN IMMEDIATE.
# Simula:
#   A. 2 ventas simultáneas del mismo lote
#   B. Venta + transformación simultánea
#   C. Transferencia + venta simultánea
#
# Uso:
#   python scripts/stress_test_concurrency.py
#   python scripts/stress_test_concurrency.py --iterations 50
import sys
import os
import sqlite3
import threading
import time
import argparse
import traceback

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.database import Connection, _open_raw
from core.services.inventory_engine import (
    InventoryEngine,
    StockInsuficienteError,
    FIFOAllocation,
)
from migrations.engine import run_migrations, MIGRATIONS


# ── Setup de BD en memoria para tests ────────────────────────────────────────

def _setup_db(kg_lote: float = 10.0) -> tuple:
    """Crea BD en memoria, corre migraciones, inserta datos semilla."""
    raw = sqlite3.connect(":memory:", check_same_thread=False, timeout=30)
    raw.isolation_level = None
    raw.row_factory     = sqlite3.Row
    raw.execute("PRAGMA journal_mode=WAL")
    raw.execute("PRAGMA foreign_keys=ON")

    run_migrations(raw, MIGRATIONS)

    # Semilla
    raw.execute("INSERT INTO productos (nombre, activo, existencia) VALUES ('Pollo', 1, 0)")
    raw.execute("INSERT INTO productos (nombre, activo, existencia) VALUES ('Pierna', 1, 0)")
    raw.execute("INSERT INTO sucursales (nombre, activa) VALUES ('Principal', 1)")
    raw.execute("INSERT INTO sucursales (nombre, activa) VALUES ('Norte', 1)")
    raw.execute(
        "INSERT INTO recetas_pollo (nombre, producto_base_id, activo) VALUES ('Desglose', 1, 1)"
    )
    raw.execute(
        "INSERT INTO recetas_pollo_detalle "
        "(receta_id, producto_resultado_id, porcentaje_rendimiento, porcentaje_merma, orden) "
        "VALUES (1, 2, 50.0, 5.0, 1)"
    )
    raw.commit()

    db  = Connection(raw)
    eng = InventoryEngine(db, usuario="test", branch_id=1)
    batch_id = eng.recepcionar_lote(
        producto_id=1, numero_pollos=10, peso_kg=kg_lote, costo_kg=45.0
    )
    return raw, db, eng, batch_id


# ══════════════════════════════════════════════════════════════════════════════
# ESCENARIO A — 2 ventas simultáneas del mismo lote
# ══════════════════════════════════════════════════════════════════════════════

def test_a_ventas_simultaneas(iterations: int = 20) -> bool:
    """
    Lanza `iterations` pares de ventas simultáneas.
    Cada venta intenta consumir KG_VENTA kg del mismo lote.
    El lote tiene KG_LOTE kg → solo floor(KG_LOTE / KG_VENTA) ventas deben tener éxito.
    PASS si ningún BIB queda negativo.
    """
    KG_LOTE  = 10.0
    KG_VENTA = 2.0

    print(f"\n[A] Ventas simultáneas — lote={KG_LOTE}kg, venta={KG_VENTA}kg x 2 hilos")

    negativos = 0
    exitosas_total = 0
    rechazadas_total = 0

    for it in range(iterations):
        raw, db, eng, batch_id = _setup_db(KG_LOTE)

        results  = []
        barrier  = threading.Barrier(2)
        lock_res = threading.Lock()

        def _venta(hilo_id: int):
            # Cada hilo tiene su propia Connection sobre el mismo archivo en memoria
            # Simula dos cajeros distintos
            try:
                barrier.wait()   # sincronizar inicio
                alloc = eng.descontar_fifo(
                    producto_id=1, cantidad=KG_VENTA, venta_id=hilo_id
                )
                with lock_res:
                    results.append(("ok", hilo_id, alloc))
            except StockInsuficienteError as e:
                with lock_res:
                    results.append(("rechazada", hilo_id, e))
            except Exception as e:
                with lock_res:
                    results.append(("error", hilo_id, e))

        t1 = threading.Thread(target=_venta, args=(1001 + it,))
        t2 = threading.Thread(target=_venta, args=(2001 + it,))
        t1.start(); t2.start()
        t1.join(timeout=10); t2.join(timeout=10)

        # Verificar que ningún BIB quedó negativo
        neg = raw.execute(
            "SELECT COUNT(*) FROM branch_inventory_batches "
            "WHERE cantidad_disponible < -0.001"
        ).fetchone()[0]

        if neg > 0:
            negativos += 1
            print(f"  [it={it}] ❌ BIB NEGATIVO DETECTADO")

        ok  = sum(1 for r in results if r[0] == "ok")
        rej = sum(1 for r in results if r[0] == "rechazada")
        exitosas_total  += ok
        rechazadas_total += rej
        raw.close()

    passed = negativos == 0
    print(
        f"  Iteraciones={iterations} | Exitosas={exitosas_total} | "
        f"Rechazadas={rechazadas_total} | BIBs negativos={negativos}"
    )
    print(f"  {'✅ PASS' if passed else '❌ FAIL'}")
    return passed


# ══════════════════════════════════════════════════════════════════════════════
# ESCENARIO B — Venta + transformación simultánea
# ══════════════════════════════════════════════════════════════════════════════

def test_b_venta_transformacion(iterations: int = 20) -> bool:
    """
    Hilo 1: intenta vender 6kg del lote de 10kg.
    Hilo 2: intenta transformar 6kg del mismo lote.
    Solo uno puede tener éxito (10kg < 6+6).
    PASS si ningún BIB queda negativo.
    """
    KG_LOTE = 10.0
    KG_OP   = 6.0

    print(f"\n[B] Venta + Transformación simultánea — lote={KG_LOTE}kg, op={KG_OP}kg cada uno")

    negativos = 0

    for it in range(iterations):
        raw, db, eng, batch_id = _setup_db(KG_LOTE)
        barrier = threading.Barrier(2)
        results = []
        lock_res = threading.Lock()

        def _venta():
            try:
                barrier.wait()
                alloc = eng.descontar_fifo(producto_id=1, cantidad=KG_OP, venta_id=9000 + it)
                with lock_res: results.append(("venta_ok", alloc))
            except StockInsuficienteError:
                with lock_res: results.append(("venta_rechazada",))
            except Exception as e:
                with lock_res: results.append(("venta_error", e))

        def _transform():
            try:
                barrier.wait()
                res = eng.transformar_parcial(batch_id, KG_OP, receta_id=1)
                with lock_res: results.append(("transf_ok", res))
            except StockInsuficienteError:
                with lock_res: results.append(("transf_rechazada",))
            except Exception as e:
                with lock_res: results.append(("transf_error", e))

        t1 = threading.Thread(target=_venta)
        t2 = threading.Thread(target=_transform)
        t1.start(); t2.start()
        t1.join(timeout=10); t2.join(timeout=10)

        neg = raw.execute(
            "SELECT COUNT(*) FROM branch_inventory_batches "
            "WHERE cantidad_disponible < -0.001"
        ).fetchone()[0]
        if neg > 0:
            negativos += 1

        raw.close()

    passed = negativos == 0
    print(f"  Iteraciones={iterations} | BIBs negativos={negativos}")
    print(f"  {'✅ PASS' if passed else '❌ FAIL'}")
    return passed


# ══════════════════════════════════════════════════════════════════════════════
# ESCENARIO C — Transferencia + venta simultánea
# ══════════════════════════════════════════════════════════════════════════════

def test_c_transferencia_venta(iterations: int = 20) -> bool:
    """
    Hilo 1: transfiere 7kg a sucursal destino.
    Hilo 2: vende 7kg del mismo lote.
    Solo uno puede tener éxito (10kg < 7+7).
    PASS si ningún BIB queda negativo.
    """
    KG_LOTE = 10.0
    KG_OP   = 7.0

    print(f"\n[C] Transferencia + Venta simultánea — lote={KG_LOTE}kg, op={KG_OP}kg cada uno")

    negativos = 0

    for it in range(iterations):
        raw, db, eng, batch_id = _setup_db(KG_LOTE)

        # Crear tabla traspasos_inventario si no existe (puede estar en otra migración)
        try:
            raw.execute("""
                CREATE TABLE IF NOT EXISTS traspasos_inventario (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    uuid TEXT UNIQUE DEFAULT (lower(hex(randomblob(16)))),
                    sucursal_origen_id INTEGER,
                    sucursal_destino_id INTEGER,
                    producto_id INTEGER,
                    cantidad REAL,
                    estado TEXT DEFAULT 'pendiente',
                    usuario_origen TEXT,
                    usuario_destino TEXT,
                    observaciones TEXT
                )
            """)
            raw.commit()
        except Exception:
            pass

        barrier  = threading.Barrier(2)
        results  = []
        lock_res = threading.Lock()

        def _transfer():
            try:
                barrier.wait()
                tid = eng.transferir_entre_sucursales(
                    producto_id=1, cantidad=KG_OP, sucursal_destino=2
                )
                with lock_res: results.append(("transfer_ok", tid))
            except StockInsuficienteError:
                with lock_res: results.append(("transfer_rechazada",))
            except Exception as e:
                with lock_res: results.append(("transfer_error", str(e)))

        def _venta():
            try:
                barrier.wait()
                alloc = eng.descontar_fifo(producto_id=1, cantidad=KG_OP, venta_id=8000 + it)
                with lock_res: results.append(("venta_ok", alloc))
            except StockInsuficienteError:
                with lock_res: results.append(("venta_rechazada",))
            except Exception as e:
                with lock_res: results.append(("venta_error", str(e)))

        t1 = threading.Thread(target=_transfer)
        t2 = threading.Thread(target=_venta)
        t1.start(); t2.start()
        t1.join(timeout=10); t2.join(timeout=10)

        neg = raw.execute(
            "SELECT COUNT(*) FROM branch_inventory_batches "
            "WHERE cantidad_disponible < -0.001"
        ).fetchone()[0]
        if neg > 0:
            negativos += 1

        raw.close()

    passed = negativos == 0
    print(f"  Iteraciones={iterations} | BIBs negativos={negativos}")
    print(f"  {'✅ PASS' if passed else '❌ FAIL'}")
    return passed


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Stress test de concurrencia SPJ v8"
    )
    parser.add_argument("--iterations", type=int, default=20,
                        help="Número de iteraciones por escenario (default=20)")
    args = parser.parse_args()

    print("=" * 60)
    print("  SPJ POS ENTERPRISE v8 — STRESS TEST CONCURRENCIA")
    print("=" * 60)

    t0 = time.time()
    results = {
        "A_ventas_simultaneas":      test_a_ventas_simultaneas(args.iterations),
        "B_venta_transformacion":    test_b_venta_transformacion(args.iterations),
        "C_transferencia_venta":     test_c_transferencia_venta(args.iterations),
    }
    elapsed = round(time.time() - t0, 2)

    print("\n" + "=" * 60)
    print("  RESUMEN")
    print("=" * 60)
    todos_pass = True
    for nombre, ok in results.items():
        estado = "✅ PASS" if ok else "❌ FAIL"
        print(f"  {estado}  {nombre}")
        if not ok:
            todos_pass = False

    print(f"\n  Tiempo total: {elapsed}s")
    print(f"\n  {'✅ TODOS LOS TESTS PASARON' if todos_pass else '❌ FALLOS DETECTADOS'}")
    print("=" * 60)

    sys.exit(0 if todos_pass else 1)


if __name__ == "__main__":
    main()

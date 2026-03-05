# core/services/compras_inventariables_engine.py
# ── Motor de Compras Inventariables SPJ v9 ────────────────────────────────
# Registra compras de productos abarrotes/desechables como:
#   → gasto contable en tabla gastos
#   → lote global en chicken_batches (InventoryEngine)
#   → compra_inventariable para trazabilidad
# Motor idempotente via UUID.
from __future__ import annotations

import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, date
from typing import List, Optional

from core.database import Database

logger = logging.getLogger("spj.compras_inv")


@dataclass
class CompraInventariableResult:
    compra_id:      int
    gasto_id:       int
    batch_id:       Optional[int]
    producto_nombre: str
    volumen:        float
    unidad:         str
    costo_total:    float
    estado:         str   # pagado | credito | parcial


@dataclass
class CompraInventariable:
    id:              int
    uuid:            str
    producto_id:     int
    producto_nombre: str
    proveedor:       str
    volumen:         float
    unidad:          str
    costo_unitario:  float
    costo_total:     float
    forma_pago:      str
    es_credito:      int
    monto_pagado:    float
    saldo_pendiente: float
    estado:          str
    fecha:           str
    usuario:         str


@dataclass
class Proveedor:
    id:        int
    nombre:    str
    contacto:  str
    telefono:  str
    email:     str
    rfc:       str
    activo:    int


@dataclass
class CuentaPorPagar:
    id:               int
    compra_id:        int
    proveedor:        str
    producto_nombre:  str
    monto_total:      float
    monto_pagado:     float
    saldo_pendiente:  float
    fecha_vencimiento: Optional[str]
    estado:           str
    fecha:            str


class ComprasInventariablesEngine:
    """
    Motor central para compras de inventario no-pollo.

    Flujo de registro:
        1. Insertar en gastos (registro contable)
        2. Insertar en compras_inventariables (trazabilidad)
        3. Si producto tiene tipo_batch=True → crear lote en branch_inventory_batches
        4. Actualizar existencia en productos

    Uso:
        eng = ComprasInventariablesEngine(conn, sucursal_id=1, usuario="admin")
        result = eng.registrar_compra(producto_id=5, volumen=10.0, costo_unitario=25.0, ...)
    """

    def __init__(
        self,
        conn: sqlite3.Connection,
        sucursal_id: int = 1,
        usuario: str = "Sistema",
    ) -> None:
        self.conn        = conn
        self.sucursal_id = sucursal_id
        self.usuario     = usuario

    # ── Registro principal ────────────────────────────────────────────────────

    def registrar_compra(
        self,
        producto_id:      int,
        volumen:          float,
        costo_unitario:   float,
        unidad:           str = "kg",
        proveedor:        str = "",
        forma_pago:       str = "EFECTIVO",
        es_credito:       bool = False,
        monto_pagado:     float = 0.0,
        fecha_vencimiento: Optional[str] = None,
        notas:            str = "",
    ) -> CompraInventariableResult:
        if volumen <= 0:
            raise ValueError("volumen debe ser > 0")
        if costo_unitario < 0:
            raise ValueError("costo_unitario no puede ser negativo")

        costo_total     = round(volumen * costo_unitario, 4)
        saldo_pendiente = 0.0
        estado          = "pagado"

        if forma_pago == "CRÉDITO":
            es_credito      = True
            monto_pagado    = 0.0
            saldo_pendiente = costo_total
            estado          = "credito"
        elif forma_pago == "PARCIAL" or (es_credito and 0 < monto_pagado < costo_total):
            es_credito      = True
            saldo_pendiente = round(costo_total - monto_pagado, 4)
            estado          = "parcial"
        else:
            monto_pagado    = costo_total
            saldo_pendiente = 0.0

        compra_uuid = str(uuid.uuid4())

        # Nombre del producto
        row_prod = self.conn.execute(
            "SELECT nombre FROM productos WHERE id=?", (producto_id,)
        ).fetchone()
        if not row_prod:
            raise ValueError(f"Producto id={producto_id} no encontrado")
        producto_nombre = row_prod[0]

        with self.conn:
            # 1. Registro contable en gastos
            self.conn.execute(
                """
                INSERT INTO gastos (
                    fecha, categoria, descripcion, monto, monto_pagado,
                    metodo_pago, estado, proveedor_id, usuario, notas
                ) VALUES (?, 'COMPRA_INVENTARIO', ?, ?, ?, ?, ?, NULL, ?, ?)
                """,
                (
                    date.today().isoformat(),
                    f"Compra inventario: {producto_nombre} — {volumen} {unidad}",
                    costo_total,
                    monto_pagado,
                    forma_pago,
                    estado.upper(),
                    self.usuario,
                    notas,
                ),
            )
            gasto_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            # 2. Registro en compras_inventariables
            self.conn.execute(
                """
                INSERT INTO compras_inventariables (
                    uuid, gasto_id, producto_id, proveedor,
                    volumen, unidad, costo_unitario, costo_total,
                    forma_pago, es_credito, monto_pagado, saldo_pendiente,
                    fecha_vencimiento, estado, sucursal_id, usuario, notas
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    compra_uuid, gasto_id, producto_id, proveedor,
                    volumen, unidad, costo_unitario, costo_total,
                    forma_pago, int(es_credito), monto_pagado, saldo_pendiente,
                    fecha_vencimiento, estado, self.sucursal_id, self.usuario, notas,
                ),
            )
            compra_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

            # 3. Actualizar existencia en productos
            self.conn.execute(
                """
                UPDATE productos
                SET existencia = COALESCE(existencia, 0) + ?,
                    precio_compra = ?,
                    fecha_actualizacion = datetime('now')
                WHERE id = ?
                """,
                (volumen, costo_unitario, producto_id),
            )

            # 4. Registrar movimiento de inventario
            old_row = self.conn.execute(
                "SELECT existencia FROM productos WHERE id=?", (producto_id,)
            ).fetchone()
            existencia_nueva = float(old_row[0]) if old_row else volumen
            self.conn.execute(
                """
                INSERT INTO movimientos_inventario (
                    producto_id, tipo, tipo_movimiento, cantidad,
                    existencia_anterior, existencia_nueva,
                    costo_unitario, costo_total,
                    descripcion, usuario, sucursal_id, fecha,
                    uuid
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'),?)
                """,
                (
                    producto_id, "ENTRADA", "compra_inventario", volumen,
                    existencia_nueva - volumen, existencia_nueva,
                    costo_unitario, costo_total,
                    f"Compra inventariable #{compra_id} de {proveedor or 'Sin proveedor'}",
                    self.usuario, self.sucursal_id,
                    str(uuid.uuid4()),
                ),
            )

        logger.info(
            "Compra inventariable #%d: prod=%d volumen=%.3f %s costo=$%.2f estado=%s",
            compra_id, producto_id, volumen, unidad, costo_total, estado,
        )

        return CompraInventariableResult(
            compra_id=compra_id,
            gasto_id=gasto_id,
            batch_id=None,
            producto_nombre=producto_nombre,
            volumen=volumen,
            unidad=unidad,
            costo_total=costo_total,
            estado=estado,
        )

    # ── Listados ──────────────────────────────────────────────────────────────

    def listar_compras(
        self,
        desde: Optional[str] = None,
        hasta: Optional[str] = None,
        producto_id: Optional[int] = None,
        estado: Optional[str] = None,
        limit: int = 200,
    ) -> List[CompraInventariable]:
        q = """
            SELECT ci.id, ci.uuid, ci.producto_id, p.nombre,
                   COALESCE(ci.proveedor,''), ci.volumen, ci.unidad,
                   ci.costo_unitario, ci.costo_total, ci.forma_pago,
                   ci.es_credito, ci.monto_pagado, ci.saldo_pendiente,
                   ci.estado, ci.fecha, ci.usuario
            FROM compras_inventariables ci
            JOIN productos p ON p.id = ci.producto_id
            WHERE 1=1
        """
        params: list = []
        if desde:
            q += " AND DATE(ci.fecha) >= ?"; params.append(desde)
        if hasta:
            q += " AND DATE(ci.fecha) <= ?"; params.append(hasta)
        if producto_id:
            q += " AND ci.producto_id = ?"; params.append(producto_id)
        if estado:
            q += " AND ci.estado = ?"; params.append(estado)
        q += " ORDER BY ci.fecha DESC LIMIT ?"
        params.append(limit)

        rows = self.conn.execute(q, params).fetchall()
        return [
            CompraInventariable(
                id=r[0], uuid=r[1], producto_id=r[2], producto_nombre=r[3],
                proveedor=r[4], volumen=r[5], unidad=r[6],
                costo_unitario=r[7], costo_total=r[8], forma_pago=r[9],
                es_credito=r[10], monto_pagado=r[11], saldo_pendiente=r[12],
                estado=r[13], fecha=str(r[14]), usuario=r[15],
            )
            for r in rows
        ]

    def cuentas_por_pagar(self) -> List[CuentaPorPagar]:
        """Retorna compras con saldo_pendiente > 0 (crédito o parcial)."""
        rows = self.conn.execute(
            """
            SELECT ci.id, ci.id, COALESCE(ci.proveedor,''), p.nombre,
                   ci.costo_total, ci.monto_pagado, ci.saldo_pendiente,
                   ci.fecha_vencimiento, ci.estado, ci.fecha
            FROM compras_inventariables ci
            JOIN productos p ON p.id = ci.producto_id
            WHERE ci.saldo_pendiente > 0
            ORDER BY ci.fecha_vencimiento ASC NULLS LAST, ci.fecha DESC
            """
        ).fetchall()
        return [
            CuentaPorPagar(
                id=r[0], compra_id=r[1], proveedor=r[2],
                producto_nombre=r[3], monto_total=r[4], monto_pagado=r[5],
                saldo_pendiente=r[6], fecha_vencimiento=r[7],
                estado=r[8], fecha=str(r[9]),
            )
            for r in rows
        ]

    def registrar_pago_cxp(self, compra_id: int, monto: float) -> dict:
        """Aplica pago a una cuenta por pagar."""
        row = self.conn.execute(
            "SELECT saldo_pendiente, monto_pagado FROM compras_inventariables WHERE id=?",
            (compra_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"Compra id={compra_id} no encontrada")
        saldo = float(row[0])
        pagado = float(row[1])
        if monto <= 0 or monto > saldo:
            raise ValueError(f"Monto inválido: {monto} (saldo={saldo})")

        nuevo_saldo  = round(saldo - monto, 4)
        nuevo_pagado = round(pagado + monto, 4)
        nuevo_estado = "pagado" if nuevo_saldo <= 0 else "parcial"

        with self.conn:
            self.conn.execute(
                """
                UPDATE compras_inventariables
                SET saldo_pendiente = ?, monto_pagado = ?, estado = ?
                WHERE id = ?
                """,
                (nuevo_saldo, nuevo_pagado, nuevo_estado, compra_id),
            )
            # Actualizar gasto vinculado
            self.conn.execute(
                """
                UPDATE gastos SET monto_pagado = ?, estado = ?
                WHERE id = (SELECT gasto_id FROM compras_inventariables WHERE id=?)
                """,
                (nuevo_pagado, nuevo_estado.upper(), compra_id),
            )

        logger.info("Pago CXP compra#%d: $%.2f → saldo=$%.2f", compra_id, monto, nuevo_saldo)
        return {
            "compra_id": compra_id,
            "monto_aplicado": monto,
            "nuevo_saldo": nuevo_saldo,
            "estado": nuevo_estado,
        }

    # ── Proveedores ───────────────────────────────────────────────────────────

    def listar_proveedores(self) -> List[Proveedor]:
        rows = self.conn.execute(
            "SELECT id, nombre, COALESCE(contacto,''), COALESCE(telefono,''), "
            "COALESCE(email,''), COALESCE(rfc,''), activo "
            "FROM proveedores ORDER BY nombre"
        ).fetchall()
        return [
            Proveedor(
                id=r[0], nombre=r[1], contacto=r[2], telefono=r[3],
                email=r[4], rfc=r[5], activo=r[6],
            )
            for r in rows
        ]

    def crear_proveedor(
        self,
        nombre: str,
        contacto: str = "",
        telefono: str = "",
        email: str = "",
        rfc: str = "",
    ) -> int:
        if not nombre.strip():
            raise ValueError("nombre requerido")
        with self.conn:
            self.conn.execute(
                "INSERT INTO proveedores (nombre, contacto, telefono, email, rfc) "
                "VALUES (?,?,?,?,?)",
                (nombre.strip(), contacto, telefono, email, rfc),
            )
            pid = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        logger.info("Proveedor #%d creado: %s", pid, nombre)
        return pid

    def actualizar_proveedor(
        self,
        proveedor_id: int,
        nombre: str,
        contacto: str = "",
        telefono: str = "",
        email: str = "",
        rfc: str = "",
        activo: int = 1,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                UPDATE proveedores
                SET nombre=?, contacto=?, telefono=?, email=?, rfc=?, activo=?
                WHERE id=?
                """,
                (nombre, contacto, telefono, email, rfc, activo, proveedor_id),
            )

# repositories/caja.py
# ── CajaRepository — Enterprise Repository Layer ─────────────────────────────
# All cash-drawer operations go through this class.
# No SQL in UI modules. Enforces atomic writes, operation_id idempotency,
# branch filtering, and immediate caja accumulator update.
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional

from core.events.event_bus import EventBus

logger = logging.getLogger("spj.repositories.caja")

CAJA_MOVIMIENTO = "CAJA_MOVIMIENTO"

ALLOWED_TYPES = frozenset({"INGRESO", "EGRESO", "APERTURA", "CIERRE", "AJUSTE"})


class CajaError(Exception):
    pass


class CajaDuplicadaError(CajaError):
    pass


class CajaMontoInvalidoError(CajaError):
    pass


class CajaRepository:

    def __init__(self, db):
        self.db = db

    def _now(self) -> str:
        return datetime.utcnow().isoformat()

    # ── Read ──────────────────────────────────────────────────────────────────

    def get_movimientos(
        self,
        branch_id: int,
        *,
        fecha_desde: Optional[str] = None,
        fecha_hasta: Optional[str] = None,
        tipo: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict]:
        conditions = ["co.branch_id = ?"]
        params: List = [branch_id]
        if fecha_desde:
            conditions.append("co.created_at >= ?")
            params.append(fecha_desde)
        if fecha_hasta:
            conditions.append("co.created_at <= ?")
            params.append(fecha_hasta)
        if tipo and tipo != "Todos":
            conditions.append("co.operation_type = ?")
            params.append(tipo)
        where = "WHERE " + " AND ".join(conditions)
        params.append(limit)
        rows = self.db.fetchall(f"""
            SELECT co.id, co.operation_id, co.operation_type,
                   co.amount, co.usuario, co.reference,
                   co.forma_pago, co.venta_id, co.notes,
                   co.created_at
            FROM caja_operations co
            {where}
            ORDER BY co.created_at DESC
            LIMIT ?
        """, params)
        return [dict(r) for r in rows]

    def get_resumen_dia(self, branch_id: int, fecha: str) -> Dict:
        fecha_ini = f"{fecha} 00:00:00"
        fecha_fin = f"{fecha} 23:59:59"
        rows = self.db.fetchall("""
            SELECT operation_type,
                   SUM(amount)  AS total,
                   COUNT(*)     AS count
            FROM caja_operations
            WHERE branch_id = ?
              AND created_at BETWEEN ? AND ?
            GROUP BY operation_type
        """, (branch_id, fecha_ini, fecha_fin))
        resumen: Dict = {t: {"total": 0.0, "count": 0} for t in ALLOWED_TYPES}
        for row in rows:
            t = row["operation_type"]
            if t in resumen:
                resumen[t] = {
                    "total": float(row["total"] or 0),
                    "count": int(row["count"] or 0),
                }
        ingresos = resumen["INGRESO"]["total"]
        egresos  = resumen["EGRESO"]["total"]
        resumen["balance_neto"] = round(ingresos - egresos, 2)
        return resumen

    def get_resumen_periodo(
        self, branch_id: int, fecha_desde: str, fecha_hasta: str
    ) -> Dict:
        rows = self.db.fetchall("""
            SELECT operation_type,
                   SUM(amount) AS total,
                   COUNT(*)    AS count
            FROM caja_operations
            WHERE branch_id = ?
              AND DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY operation_type
        """, (branch_id, fecha_desde, fecha_hasta))
        resumen: Dict = {t: {"total": 0.0, "count": 0} for t in ALLOWED_TYPES}
        for row in rows:
            t = row["operation_type"]
            if t in resumen:
                resumen[t] = {
                    "total": float(row["total"] or 0),
                    "count": int(row["count"] or 0),
                }
        ingresos = resumen["INGRESO"]["total"]
        egresos  = resumen["EGRESO"]["total"]
        resumen["balance_neto"] = round(ingresos - egresos, 2)
        return resumen

    def get_forma_pago_breakdown(
        self, branch_id: int, fecha_desde: str, fecha_hasta: str
    ) -> List[Dict]:
        rows = self.db.fetchall("""
            SELECT COALESCE(forma_pago, 'No especificado') AS forma_pago,
                   SUM(amount) AS total,
                   COUNT(*)    AS count
            FROM caja_operations
            WHERE branch_id = ?
              AND operation_type = 'INGRESO'
              AND DATE(created_at) BETWEEN DATE(?) AND DATE(?)
            GROUP BY forma_pago
            ORDER BY total DESC
        """, (branch_id, fecha_desde, fecha_hasta))
        return [dict(r) for r in rows]

    def has_apertura_today(self, branch_id: int) -> bool:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        row = self.db.fetchone("""
            SELECT COUNT(*) AS c FROM caja_operations
            WHERE branch_id = ? AND operation_type = 'APERTURA'
              AND DATE(created_at) = ?
        """, (branch_id, today))
        return bool(row and row["c"] > 0)

    # ── Write ─────────────────────────────────────────────────────────────────

    def registrar_movimiento(
        self,
        branch_id: int,
        operation_type: str,
        amount: float,
        usuario: str,
        *,
        reference: str = "",
        forma_pago: str = "",
        venta_id: Optional[int] = None,
        notes: str = "",
        operation_id: Optional[str] = None,
    ) -> str:
        if operation_type not in ALLOWED_TYPES:
            raise CajaError(f"TIPO_INVALIDO: {operation_type}")
        if amount < 0:
            raise CajaMontoInvalidoError("MONTO_NEGATIVO")
        op_id = operation_id or str(uuid.uuid4())

        # Idempotency check
        existing = self.db.fetchone(
            "SELECT id FROM caja_operations WHERE operation_id = ?", (op_id,)
        )
        if existing:
            raise CajaDuplicadaError(f"OPERACION_DUPLICADA: {op_id}")

        with self.db.transaction("CAJA_MOVIMIENTO"):
            self.db.execute("""
                INSERT INTO caja_operations (
                    branch_id, operation_id, operation_type,
                    amount, usuario, reference,
                    forma_pago, venta_id, notes, created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                branch_id, op_id, operation_type,
                abs(amount), usuario, reference or "",
                forma_pago or "", venta_id, notes or "",
                self._now(),
            ))
            # Mirror to movimientos_caja (legacy compatibility)
            self.db.execute("""
                INSERT INTO movimientos_caja (
                    tipo, monto, descripcion, forma_pago,
                    usuario, sucursal_id, operation_id, fecha
                ) VALUES (?,?,?,?,?,?,?,datetime('now'))
            """, (
                operation_type, abs(amount),
                reference or notes or "",
                forma_pago or "", usuario,
                branch_id, op_id,
            ))

        EventBus.publish(CAJA_MOVIMIENTO, {
            "branch_id":      branch_id,
            "operation_id":   op_id,
            "operation_type": operation_type,
            "amount":         abs(amount),
            "usuario":        usuario,
        })
        logger.info(
            "caja_operation branch=%d type=%s amount=%.2f op=%s",
            branch_id, operation_type, abs(amount), op_id
        )
        return op_id

    def apertura_caja(
        self,
        branch_id: int,
        usuario: str,
        monto_inicial: float = 0.0,
        notes: str = "",
    ) -> str:
        if self.has_apertura_today(branch_id):
            raise CajaError("APERTURA_YA_REGISTRADA_HOY")
        return self.registrar_movimiento(
            branch_id=branch_id,
            operation_type="APERTURA",
            amount=monto_inicial,
            usuario=usuario,
            reference="APERTURA DE CAJA",
            notes=notes,
        )

    def cierre_caja(
        self,
        branch_id: int,
        usuario: str,
        monto_final: float,
        notes: str = "",
    ) -> str:
        return self.registrar_movimiento(
            branch_id=branch_id,
            operation_type="CIERRE",
            amount=monto_final,
            usuario=usuario,
            reference="CIERRE DE CAJA",
            notes=notes,
        )

    def egreso(
        self,
        branch_id: int,
        usuario: str,
        monto: float,
        concepto: str,
        notes: str = "",
    ) -> str:
        if monto <= 0:
            raise CajaMontoInvalidoError("EGRESO_MONTO_INVALIDO")
        return self.registrar_movimiento(
            branch_id=branch_id,
            operation_type="EGRESO",
            amount=monto,
            usuario=usuario,
            reference=concepto,
            notes=notes,
        )

    def ajuste(
        self,
        branch_id: int,
        usuario: str,
        monto: float,
        motivo: str,
    ) -> str:
        return self.registrar_movimiento(
            branch_id=branch_id,
            operation_type="AJUSTE",
            amount=abs(monto),
            usuario=usuario,
            reference=motivo,
        )

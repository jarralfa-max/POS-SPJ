# repositories/ventas.py
# ── VentaRepository — Enterprise Repository Layer ────────────────────────────
# Enforces: operation_id idempotency, credit validation,
#           immediate caja accumulator update, no orphan sales.
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional

from core.events.event_bus import EventBus

logger = logging.getLogger("spj.repositories.ventas")

VENTA_COMPLETADA = "VENTA_COMPLETADA"


class VentaError(Exception):
    pass


class CreditoInsuficienteError(VentaError):
    pass


class VentaDuplicadaError(VentaError):
    pass


class VentaRepository:

    def __init__(self, db):
        self.db = db

    def _now(self) -> str:
        return datetime.utcnow().isoformat()

    # ── Credit validation ─────────────────────────────────────────────────────

    def validate_credit(self, cliente_id: int, total: float) -> None:
        """Raises CreditoInsuficienteError if credit sale is not allowed."""
        row = self.db.fetchone("""
            SELECT allows_credit, credit_limit, credit_balance
            FROM clientes WHERE id = ?
        """, (cliente_id,))
        if not row:
            raise VentaError("CLIENTE_NOT_FOUND")
        if not row["allows_credit"]:
            raise CreditoInsuficienteError("CLIENTE_NO_PERMITE_CREDITO")
        available = float(row["credit_limit"] or 0) - float(row["credit_balance"] or 0)
        if total > available:
            raise CreditoInsuficienteError(
                f"LIMITE_CREDITO_EXCEDIDO: disponible={available:.2f} solicitado={total:.2f}"
            )

    def is_credit_enabled(self) -> bool:
        row = self.db.fetchone("""
            SELECT value FROM system_constants WHERE key = 'CREDIT_VALIDATION_ENABLED'
        """)
        return str(row["value"]).strip() in ("1", "true", "True") if row else True

    # ── Read ─────────────────────────────────────────────────────────────────

    def get_today(self, branch_id: int, date: Optional[str] = None) -> List[Dict]:
        if not date:
            date = datetime.utcnow().strftime("%Y-%m-%d")
        rows = self.db.fetchall("""
            SELECT v.id, v.folio, v.usuario, v.total, v.subtotal,
                   v.descuento, v.iva, v.forma_pago, v.estado,
                   v.fecha, v.operation_id,
                   COALESCE(c.nombre,'') || ' ' || COALESCE(c.apellido,'') AS cliente_nombre
            FROM ventas v
            LEFT JOIN clientes c ON c.id = v.cliente_id
            WHERE v.sucursal_id = ?
              AND DATE(v.fecha) = DATE(?)
              AND v.estado != 'cancelada'
            ORDER BY v.fecha DESC
        """, (branch_id, date))
        return [dict(r) for r in rows]

    def get_by_id(self, venta_id: int) -> Optional[Dict]:
        row = self.db.fetchone("SELECT * FROM ventas WHERE id = ?", (venta_id,))
        return dict(row) if row else None

    def get_items(self, venta_id: int) -> List[Dict]:
        rows = self.db.fetchall("""
            SELECT dv.id, dv.producto_id, p.nombre AS producto_nombre,
                   dv.cantidad, dv.precio_unitario, dv.subtotal,
                   dv.costo_unitario, dv.margen_real
            FROM detalles_venta dv
            JOIN productos p ON p.id = dv.producto_id
            WHERE dv.venta_id = ?
        """, (venta_id,))
        return [dict(r) for r in rows]

    # ── Write ────────────────────────────────────────────────────────────────

    def create_sale(self, sale_data: Dict) -> Dict:
        """
        sale_data keys:
            branch_id, usuario, cliente_id (optional),
            forma_pago, items (list of dicts),
            operation_id (optional — for idempotency),
            efectivo_recibido, descuento, is_credit (bool)
        items: [{producto_id, cantidad, precio_unitario, costo_unitario}]
        Returns: {venta_id, folio, total}
        """
        operation_id = sale_data.get("operation_id") or str(uuid.uuid4())

        # Idempotency guard
        existing = self.db.fetchone(
            "SELECT id, folio, total FROM ventas WHERE operation_id = ?",
            (operation_id,)
        )
        if existing:
            raise VentaDuplicadaError(
                f"VENTA_DUPLICADA: operation_id={operation_id} already processed"
            )

        branch_id  = sale_data["branch_id"]
        usuario    = sale_data["usuario"]
        cliente_id = sale_data.get("cliente_id")
        forma_pago = sale_data.get("forma_pago", "Efectivo")
        is_credit  = bool(sale_data.get("is_credit", False))
        items      = sale_data.get("items", [])

        if not items:
            raise VentaError("NO_ITEMS")

        # Compute totals
        subtotal = sum(
            Decimal(str(i["precio_unitario"])) * Decimal(str(i["cantidad"]))
            for i in items
        )
        descuento = Decimal(str(sale_data.get("descuento", 0)))
        iva_rate  = Decimal("0")  # IVA included in price per config
        iva       = (subtotal - descuento) * iva_rate
        total     = subtotal - descuento + iva

        if total <= 0:
            raise VentaError("TOTAL_INVALIDO")

        # Credit validation
        if is_credit and cliente_id and self.is_credit_enabled():
            self.validate_credit(cliente_id, float(total))

        with self.db.transaction("VENTA_CREATE"):
            # Generate folio using AUTOINCREMENT (after INSERT)
            self.db.execute("""
                INSERT INTO ventas (
                    uuid, sucursal_id, usuario, cliente_id,
                    subtotal, descuento, iva, total,
                    forma_pago, efectivo_recibido, cambio,
                    estado, operation_id, credit_approved, fecha
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
            """, (
                str(uuid.uuid4()),
                branch_id, usuario, cliente_id,
                float(subtotal), float(descuento), float(iva), float(total),
                forma_pago,
                float(sale_data.get("efectivo_recibido", total)),
                max(0, float(sale_data.get("efectivo_recibido", total)) - float(total)),
                "completada",
                operation_id,
                0 if is_credit else 1,
            ))

            venta_row = self.db.fetchone(
                "SELECT id FROM ventas WHERE operation_id = ?", (operation_id,)
            )
            venta_id = venta_row["id"]

            # Generate folio
            folio = f"V{datetime.utcnow().strftime('%Y%m%d')}-{venta_id:06d}"
            self.db.execute(
                "UPDATE ventas SET folio = ? WHERE id = ?", (folio, venta_id)
            )

            # Insert items
            for item in items:
                p_id  = item["producto_id"]
                qty   = float(item["cantidad"])
                price = float(item["precio_unitario"])
                cost  = float(item.get("costo_unitario", 0))
                sub   = qty * price
                margin = (sub - qty * cost) / sub if sub > 0 else 0

                self.db.execute("""
                    INSERT INTO detalles_venta (
                        venta_id, producto_id, cantidad,
                        precio_unitario, subtotal,
                        costo_unitario, margen_real
                    ) VALUES (?,?,?,?,?,?,?)
                """, (venta_id, p_id, qty, price, sub, cost, margin))

                # Deduct inventory
                self.db.execute("""
                    UPDATE productos SET existencia = existencia - ?
                    WHERE id = ?
                """, (qty, p_id))
                self.db.execute("""
                    UPDATE branch_inventory SET quantity = quantity - ?
                    WHERE branch_id = ? AND product_id = ?
                """, (qty, branch_id, p_id))

            # Update credit balance if credit sale
            if is_credit and cliente_id:
                self.db.execute("""
                    UPDATE clientes SET credit_balance = credit_balance + ?
                    WHERE id = ?
                """, (float(total), cliente_id))

            # Update caja accumulator immediately
            self._update_caja(branch_id, usuario, float(total), forma_pago, venta_id)

        result = {
            "venta_id": venta_id,
            "folio": folio,
            "total": float(total),
            "operation_id": operation_id,
        }

        EventBus.publish(VENTA_COMPLETADA, {
            "venta_id": venta_id,
            "folio": folio,
            "branch_id": branch_id,
            "total": float(total),
            "cliente_id": cliente_id,
            "items": items,
        })
        return result

    def cancel_sale(self, venta_id: int, usuario: str, reason: str = "") -> None:
        venta = self.get_by_id(venta_id)
        if not venta:
            raise VentaError("VENTA_NOT_FOUND")
        if venta["estado"] == "cancelada":
            raise VentaError("ALREADY_CANCELLED")

        items = self.get_items(venta_id)
        branch_id = venta["sucursal_id"]

        with self.db.transaction("VENTA_CANCEL"):
            for item in items:
                qty = float(item["cantidad"])
                p_id = item["producto_id"]
                self.db.execute(
                    "UPDATE productos SET existencia = existencia + ? WHERE id = ?",
                    (qty, p_id)
                )
                self.db.execute("""
                    UPDATE branch_inventory SET quantity = quantity + ?
                    WHERE branch_id = ? AND product_id = ?
                """, (qty, branch_id, p_id))

            self.db.execute("""
                UPDATE ventas SET estado = 'cancelada',
                    observations = ?
                WHERE id = ?
            """, (reason or "Cancelled", venta_id))

            # Reverse caja entry
            self._update_caja(
                branch_id, usuario, -float(venta["total"]),
                venta["forma_pago"], venta_id, note=f"CANCEL:{venta_id}"
            )

            # Reverse credit if applicable
            if venta.get("cliente_id") and venta.get("credit_approved") == 0:
                self.db.execute("""
                    UPDATE clientes SET credit_balance = credit_balance - ?
                    WHERE id = ?
                """, (float(venta["total"]), venta["cliente_id"]))

    # ── Caja sync ─────────────────────────────────────────────────────────────

    def _update_caja(self, branch_id: int, usuario: str,
                     amount: float, forma_pago: str,
                     venta_id: int, note: str = "") -> None:
        op_id = str(uuid.uuid4())
        try:
            self.db.execute("""
                INSERT INTO caja_operations (
                    branch_id, operation_id, operation_type,
                    amount, usuario, reference, created_at
                ) VALUES (?,?,?,?,?,?,?)
            """, (
                branch_id, op_id,
                "INGRESO" if amount >= 0 else "EGRESO",
                abs(amount), usuario,
                note or f"VENTA:{venta_id}",
                self._now(),
            ))
            # Update movimientos_caja for cash drawer reporting
            self.db.execute("""
                INSERT INTO movimientos_caja (
                    tipo, monto, descripcion, forma_pago, usuario, fecha
                ) VALUES (?,?,?,?,?,datetime('now'))
            """, (
                "INGRESO" if amount >= 0 else "EGRESO",
                abs(amount),
                note or f"Venta #{venta_id}",
                forma_pago,
                usuario,
            ))
        except Exception as exc:
            logger.error("caja update failed for venta %d: %s", venta_id, exc)
            raise VentaError(f"CAJA_UPDATE_FAILED: {exc}") from exc

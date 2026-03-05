# repositories/transferencias.py
# ── TransferRepository — Enterprise Repository Layer ─────────────────────────
# Two-phase transfer flow: Dispatch → Reception.
# Prevents: over-reception, duplicate reception, post-receipt editing,
#           sending without stock.
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Dict, List, Optional

from core.events.event_bus import EventBus

logger = logging.getLogger("spj.repositories.transferencias")

TRANSFER_DISPATCHED = "TRASPASO_INICIADO"
TRANSFER_RECEIVED   = "TRASPASO_CONFIRMADO"
TRANSFER_CANCELLED  = "TRASPASO_CANCELADO"

MAX_DIFFERENCE_KG = 0.5  # overridden by system_constants


class TransferError(Exception):
    pass


class TransferStockError(TransferError):
    pass


class TransferAlreadyReceivedError(TransferError):
    pass


class TransferOverReceptionError(TransferError):
    pass


class TransferRepository:

    def __init__(self, db):
        self.db = db

    def _now(self) -> str:
        return datetime.utcnow().isoformat()

    def _get_max_diff(self) -> float:
        row = self.db.fetchone("""
            SELECT value FROM system_constants WHERE key = 'TRANSFER_MAX_DIFFERENCE_KG'
        """)
        try:
            return float(row["value"]) if row else MAX_DIFFERENCE_KG
        except (TypeError, ValueError):
            return MAX_DIFFERENCE_KG

    # ── Read ─────────────────────────────────────────────────────────────────

    def get_all(self, *, branch_id: Optional[int] = None,
                status: Optional[str] = None) -> List[Dict]:
        conditions = []
        params: List = []
        if branch_id:
            conditions.append(
                "(t.branch_origin_id = ? OR t.branch_dest_id = ?)"
            )
            params.extend([branch_id, branch_id])
        if status:
            conditions.append("t.status = ?")
            params.append(status)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        rows = self.db.fetchall(f"""
            SELECT t.id, t.branch_origin_id, t.branch_dest_id,
                   t.origin_type, t.destination_type, t.status,
                   t.dispatched_by, t.dispatched_at,
                   t.received_by, t.received_at,
                   t.observations, t.difference_kg, t.created_at,
                   COUNT(ti.id) AS item_count
            FROM transfers t
            LEFT JOIN transfer_items ti ON ti.transfer_id = t.id
            {where}
            GROUP BY t.id
            ORDER BY t.created_at DESC
        """, params)
        return [dict(r) for r in rows]

    def get_by_id(self, transfer_id: str) -> Optional[Dict]:
        row = self.db.fetchone("SELECT * FROM transfers WHERE id = ?", (transfer_id,))
        return dict(row) if row else None

    def get_items(self, transfer_id: str) -> List[Dict]:
        rows = self.db.fetchall("""
            SELECT ti.id, ti.transfer_id, ti.product_id,
                   p.nombre AS product_nombre, p.unidad,
                   ti.quantity_sent, ti.quantity_received,
                   ti.batch_id, ti.notes
            FROM transfer_items ti
            JOIN productos p ON p.id = ti.product_id
            WHERE ti.transfer_id = ?
            ORDER BY p.nombre
        """, (transfer_id,))
        return [dict(r) for r in rows]

    def get_pending_for_branch(self, dest_branch_id: int) -> List[Dict]:
        return self.get_all(branch_id=dest_branch_id, status="DISPATCHED")

    # ── Phase 1: Dispatch ────────────────────────────────────────────────────

    def dispatch(self, origin_branch_id: int, dest_branch_id: int,
                 items: List[Dict], dispatched_by: str,
                 origin_type: str = "BRANCH",
                 destination_type: str = "BRANCH",
                 observations: str = "") -> str:
        """
        items: list of dicts with keys:
            product_id, quantity_sent, unit, batch_id (optional), notes (optional)

        Validates: origin != dest, stock available, quantities > 0.
        Atomically deducts stock from origin.
        Returns transfer_id.
        """
        if origin_branch_id == dest_branch_id:
            raise TransferError("SAME_BRANCH")
        if not items:
            raise TransferError("NO_ITEMS")

        transfer_id = str(uuid.uuid4())
        operation_id = str(uuid.uuid4())

        with self.db.transaction("TRANSFER_DISPATCH"):

            # Validate and deduct stock atomically
            for item in items:
                product_id = item["product_id"]
                qty = float(item.get("quantity_sent", 0))
                if qty <= 0:
                    raise TransferError(f"QUANTITY_MUST_BE_POSITIVE: product {product_id}")

                # Check available stock at origin
                stock_row = self.db.fetchone("""
                    SELECT quantity FROM branch_inventory
                    WHERE branch_id = ? AND product_id = ?
                """, (origin_branch_id, product_id))
                available = float(stock_row["quantity"]) if stock_row else 0.0
                if available < qty:
                    prod_row = self.db.fetchone(
                        "SELECT nombre FROM productos WHERE id = ?", (product_id,)
                    )
                    nombre = prod_row["nombre"] if prod_row else str(product_id)
                    raise TransferStockError(
                        f"INSUFFICIENT_STOCK: {nombre} available={available:.3f} needed={qty:.3f}"
                    )

                # Deduct from origin
                self.db.execute("""
                    UPDATE branch_inventory
                    SET quantity = quantity - ?
                    WHERE branch_id = ? AND product_id = ?
                """, (qty, origin_branch_id, product_id))

                # Log movement
                self.db.execute("""
                    INSERT INTO batch_movements (
                        id, branch_id, product_id,
                        operation_type, quantity, operation_id, created_at
                    ) VALUES (?,?,?,'TRANSFER_OUT',?,?,?)
                """, (
                    str(uuid.uuid4()), origin_branch_id, product_id,
                    -qty, operation_id, self._now(),
                ))

            # Insert transfer header
            self.db.execute("""
                INSERT INTO transfers (
                    id, branch_origin_id, branch_dest_id,
                    origin_type, destination_type, status,
                    dispatched_by, dispatched_at,
                    observations, operation_id, created_at
                ) VALUES (?,?,?,?,?,'DISPATCHED',?,?,?,?,?)
            """, (
                transfer_id, origin_branch_id, dest_branch_id,
                origin_type, destination_type,
                dispatched_by, self._now(),
                observations, operation_id, self._now(),
            ))

            # Insert items
            for item in items:
                self.db.execute("""
                    INSERT INTO transfer_items (
                        id, transfer_id, product_id,
                        quantity_sent, unit, batch_id, notes
                    ) VALUES (?,?,?,?,?,?,?)
                """, (
                    str(uuid.uuid4()),
                    transfer_id,
                    item["product_id"],
                    float(item["quantity_sent"]),
                    item.get("unit", "kg"),
                    item.get("batch_id"),
                    item.get("notes", ""),
                ))

        EventBus.publish(TRANSFER_DISPATCHED, {
            "transfer_id": transfer_id,
            "origin_branch_id": origin_branch_id,
            "dest_branch_id": dest_branch_id,
            "item_count": len(items),
        })
        return transfer_id

    # ── Phase 2: Reception ───────────────────────────────────────────────────

    def receive(self, transfer_id: str, received_by: str,
                received_items: List[Dict],
                observations: str = "") -> Dict:
        """
        received_items: list of dicts with keys:
            product_id, quantity_received (actual received qty), notes (optional)

        Returns: {"transfer_id": ..., "total_difference": ..., "items": [...]}
        Raises:
            TransferAlreadyReceivedError — if already received
            TransferOverReceptionError  — if received > sent for any item
        """
        transfer = self.get_by_id(transfer_id)
        if not transfer:
            raise TransferError("TRANSFER_NOT_FOUND")
        if transfer["status"] == "RECEIVED":
            raise TransferAlreadyReceivedError("ALREADY_RECEIVED")
        if transfer["status"] == "CANCELLED":
            raise TransferError("TRANSFER_CANCELLED")
        if transfer["status"] != "DISPATCHED":
            raise TransferError(f"INVALID_STATUS: {transfer['status']}")

        dest_branch_id = transfer["branch_dest_id"]
        sent_items = {i["product_id"]: i for i in self.get_items(transfer_id)}
        max_diff = self._get_max_diff()

        # Build received_map from request
        received_map = {
            item["product_id"]: float(item.get("quantity_received", 0))
            for item in received_items
        }

        total_difference = 0.0
        result_items = []

        with self.db.transaction("TRANSFER_RECEIVE"):
            for product_id, sent_item in sent_items.items():
                qty_sent = float(sent_item["quantity_sent"])
                qty_recv = received_map.get(product_id, qty_sent)  # default = sent

                if qty_recv < 0:
                    raise TransferError(f"NEGATIVE_RECEIVED: product {product_id}")
                if qty_recv > qty_sent:
                    raise TransferOverReceptionError(
                        f"RECEIVED_EXCEEDS_SENT: product {product_id} "
                        f"sent={qty_sent:.3f} received={qty_recv:.3f}"
                    )

                difference = qty_recv - qty_sent
                total_difference += abs(difference)

                # Add to destination inventory
                existing = self.db.fetchone("""
                    SELECT quantity FROM branch_inventory
                    WHERE branch_id = ? AND product_id = ?
                """, (dest_branch_id, product_id))
                if existing:
                    self.db.execute("""
                        UPDATE branch_inventory
                        SET quantity = quantity + ?
                        WHERE branch_id = ? AND product_id = ?
                    """, (qty_recv, dest_branch_id, product_id))
                else:
                    self.db.execute("""
                        INSERT INTO branch_inventory (branch_id, product_id, quantity)
                        VALUES (?,?,?)
                    """, (dest_branch_id, product_id, qty_recv))

                # Log movement
                self.db.execute("""
                    INSERT INTO batch_movements (
                        id, branch_id, product_id,
                        operation_type, quantity, operation_id, created_at
                    ) VALUES (?,?,?,'TRANSFER_IN',?,?,?)
                """, (
                    str(uuid.uuid4()), dest_branch_id, product_id,
                    qty_recv, transfer_id, self._now(),
                ))

                # Update transfer item received qty
                self.db.execute("""
                    UPDATE transfer_items
                    SET quantity_received = ?
                    WHERE transfer_id = ? AND product_id = ?
                """, (qty_recv, transfer_id, product_id))

                result_items.append({
                    "product_id": product_id,
                    "quantity_sent": qty_sent,
                    "quantity_received": qty_recv,
                    "difference": difference,
                })

            # Update transfer header
            self.db.execute("""
                UPDATE transfers SET
                    status = 'RECEIVED',
                    received_by = ?,
                    received_at = ?,
                    observations = COALESCE(? || ' | ' || COALESCE(observations,''), observations),
                    difference_kg = ?
                WHERE id = ?
            """, (
                received_by, self._now(),
                observations or None,
                total_difference,
                transfer_id,
            ))

        result = {
            "transfer_id": transfer_id,
            "total_difference": total_difference,
            "items": result_items,
        }

        EventBus.publish(TRANSFER_RECEIVED, {
            "transfer_id": transfer_id,
            "dest_branch_id": dest_branch_id,
            "total_difference": total_difference,
        })
        return result

    # ── Cancel ───────────────────────────────────────────────────────────────

    def cancel(self, transfer_id: str, cancelled_by: str,
               reason: str = "") -> None:
        transfer = self.get_by_id(transfer_id)
        if not transfer:
            raise TransferError("TRANSFER_NOT_FOUND")
        if transfer["status"] in ("RECEIVED", "CANCELLED"):
            raise TransferError(f"CANNOT_CANCEL: {transfer['status']}")

        origin_branch_id = transfer["branch_origin_id"]
        items = self.get_items(transfer_id)

        with self.db.transaction("TRANSFER_CANCEL"):
            # Restore stock to origin
            for item in items:
                qty = float(item["quantity_sent"])
                self.db.execute("""
                    UPDATE branch_inventory
                    SET quantity = quantity + ?
                    WHERE branch_id = ? AND product_id = ?
                """, (qty, origin_branch_id, item["product_id"]))
                self.db.execute("""
                    INSERT INTO batch_movements (
                        id, branch_id, product_id,
                        operation_type, quantity, operation_id, created_at
                    ) VALUES (?,?,?,'TRANSFER_CANCEL',?,?,?)
                """, (
                    str(uuid.uuid4()), origin_branch_id, item["product_id"],
                    qty, transfer_id, self._now(),
                ))

            self.db.execute("""
                UPDATE transfers SET
                    status = 'CANCELLED',
                    observations = COALESCE(? || ' | CANCELLED BY: ' || ?, 'CANCELLED BY: ' || ?)
                WHERE id = ?
            """, (reason, cancelled_by, cancelled_by, transfer_id))

        EventBus.publish(TRANSFER_CANCELLED, {"transfer_id": transfer_id})

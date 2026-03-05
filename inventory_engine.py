import uuid
from datetime import datetime


TOLERANCE = 0.01
MAX_TREE_DEPTH = 50


class InventarioError(Exception):
    pass


class StockInsuficienteError(InventarioError):
    pass


class LockActivoError(InventarioError):
    pass


class CycleDetectedError(InventarioError):
    pass


class RecetaNoEncontradaError(InventarioError):
    pass


class MixConsumptionResult:
    def __init__(self, consumed):
        self.consumed = consumed


class RootBatchIdMismatchError(InventarioError):
    pass


class InventoryEngine:

    def __init__(self, db, branch_id, usuario=None):
        self.db = db
        self.branch_id = branch_id
        self.usuario = usuario

    def _now(self):
        return datetime.utcnow().isoformat()

    def deduct_stock(self, product_id, delta):
        if delta <= 0:
            raise InventarioError("DELTA_MUST_BE_POSITIVE")
        with self.db.transaction("DEDUCT_STOCK"):
            row = self.db.fetchone("""
                SELECT quantity FROM branch_inventory
                WHERE branch_id = ? AND product_id = ?
            """, (self.branch_id, product_id))
            qty = float(row["quantity"]) if row else 0.0
            if qty - delta < 0:
                raise StockInsuficienteError("NEGATIVE_INVENTORY_BLOCKED")
            self.db.execute("""
                UPDATE branch_inventory
                SET quantity = quantity - ?
                WHERE branch_id = ? AND product_id = ?
            """, (delta, self.branch_id, product_id))
            self.db.execute("""
                INSERT INTO batch_movements(
                    id, branch_id, product_id,
                    operation_type, quantity, created_at
                ) VALUES (?, ?, ?, 'DEDUCT', ?, ?)
            """, (
                str(uuid.uuid4()),
                self.branch_id,
                product_id,
                delta,
                self._now()
            ))

    def add_stock(self, product_id, delta):
        if delta <= 0:
            raise InventarioError("DELTA_MUST_BE_POSITIVE")
        with self.db.transaction("ADD_STOCK"):
            existing = self.db.fetchone("""
                SELECT quantity FROM branch_inventory
                WHERE branch_id = ? AND product_id = ?
            """, (self.branch_id, product_id))
            if existing:
                self.db.execute("""
                    UPDATE branch_inventory
                    SET quantity = quantity + ?
                    WHERE branch_id = ? AND product_id = ?
                """, (delta, self.branch_id, product_id))
            else:
                self.db.execute("""
                    INSERT INTO branch_inventory(branch_id, product_id, quantity)
                    VALUES (?, ?, ?)
                """, (self.branch_id, product_id, delta))
            self.db.execute("""
                INSERT INTO batch_movements(
                    id, branch_id, product_id,
                    operation_type, quantity, created_at
                ) VALUES (?, ?, ?, 'ADD', ?, ?)
            """, (
                str(uuid.uuid4()),
                self.branch_id,
                product_id,
                delta,
                self._now()
            ))

    def adjust_stock(self, product_id, new_quantity):
        if new_quantity < 0:
            raise InventarioError("ADJUST_NEGATIVE_BLOCKED")
        with self.db.transaction("ADJUST_STOCK"):
            existing = self.db.fetchone("""
                SELECT quantity FROM branch_inventory
                WHERE branch_id = ? AND product_id = ?
            """, (self.branch_id, product_id))
            if existing:
                self.db.execute("""
                    UPDATE branch_inventory
                    SET quantity = ?
                    WHERE branch_id = ? AND product_id = ?
                """, (new_quantity, self.branch_id, product_id))
            else:
                self.db.execute("""
                    INSERT INTO branch_inventory(branch_id, product_id, quantity)
                    VALUES (?, ?, ?)
                """, (self.branch_id, product_id, new_quantity))
            self.db.execute("""
                INSERT INTO batch_movements(
                    id, branch_id, product_id,
                    operation_type, quantity, created_at
                ) VALUES (?, ?, ?, 'ADJUST', ?, ?)
            """, (
                str(uuid.uuid4()),
                self.branch_id,
                product_id,
                new_quantity,
                self._now()
            ))

    def transfer_stock(self, product_id, delta, destination_branch_id):
        if delta <= 0:
            raise InventarioError("DELTA_MUST_BE_POSITIVE")
        if destination_branch_id == self.branch_id:
            raise InventarioError("TRANSFER_SAME_BRANCH")
        with self.db.transaction("TRANSFER_STOCK"):
            src_row = self.db.fetchone("""
                SELECT quantity FROM branch_inventory
                WHERE branch_id = ? AND product_id = ?
            """, (self.branch_id, product_id))
            src_qty = float(src_row["quantity"]) if src_row else 0.0
            if src_qty - delta < 0:
                raise StockInsuficienteError("TRANSFER_INSUFFICIENT_STOCK")
            self.db.execute("""
                UPDATE branch_inventory
                SET quantity = quantity - ?
                WHERE branch_id = ? AND product_id = ?
            """, (delta, self.branch_id, product_id))
            dst_row = self.db.fetchone("""
                SELECT quantity FROM branch_inventory
                WHERE branch_id = ? AND product_id = ?
            """, (destination_branch_id, product_id))
            if dst_row:
                self.db.execute("""
                    UPDATE branch_inventory
                    SET quantity = quantity + ?
                    WHERE branch_id = ? AND product_id = ?
                """, (delta, destination_branch_id, product_id))
            else:
                self.db.execute("""
                    INSERT INTO branch_inventory(branch_id, product_id, quantity)
                    VALUES (?, ?, ?)
                """, (destination_branch_id, product_id, delta))
            transfer_id = str(uuid.uuid4())
            now = self._now()
            self.db.execute("""
                INSERT INTO batch_movements(
                    id, branch_id, product_id,
                    operation_type, quantity, created_at
                ) VALUES (?, ?, ?, 'TRANSFER_OUT', ?, ?)
            """, (transfer_id, self.branch_id, product_id, delta, now))
            self.db.execute("""
                INSERT INTO batch_movements(
                    id, branch_id, product_id,
                    operation_type, quantity, created_at
                ) VALUES (?, ?, ?, 'TRANSFER_IN', ?, ?)
            """, (str(uuid.uuid4()), destination_branch_id, product_id, delta, now))

    def _detect_cycle(self, candidate_child_id, candidate_parent_id):
        visited = set()
        current = candidate_parent_id
        depth = 0
        while current:
            if depth > MAX_TREE_DEPTH:
                raise CycleDetectedError("DEPTH_LIMIT_EXCEEDED")
            if current == candidate_child_id:
                raise CycleDetectedError("CYCLE_DETECTED")
            if current in visited:
                raise CycleDetectedError("CYCLE_DETECTED")
            visited.add(current)
            row = self.db.fetchone(
                "SELECT parent_batch_id FROM batches WHERE id = ?",
                (current,)
            )
            if not row:
                break
            current = row["parent_batch_id"]
            depth += 1

    def _validate_root_batch_id(self, parent_id, expected_root_id):
        row = self.db.fetchone(
            "SELECT root_batch_id FROM batches WHERE id = ?",
            (parent_id,)
        )
        if not row:
            raise InventarioError("PARENT_NOT_FOUND_FOR_ROOT_VALIDATION")
        actual_root = row["root_batch_id"] or parent_id
        if actual_root != expected_root_id:
            raise RootBatchIdMismatchError(
                f"ROOT_BATCH_ID_MISMATCH: expected {expected_root_id}, got {actual_root}"
            )

    def transform_batch(self, parent_batch_id, outputs, transformation_group_id=None):
        with self.db.transaction("TRANSFORM_BATCH"):
            parent = self.db.fetchone("""
                SELECT id, weight, root_batch_id, parent_batch_id FROM batches
                WHERE id = ?
            """, (parent_batch_id,))

            if not parent:
                raise InventarioError("PARENT_NOT_FOUND")

            children_count = self.db.fetchone("""
                SELECT COUNT(*) as c FROM batches
                WHERE parent_batch_id = ?
            """, (parent_batch_id,))

            if children_count["c"] > 0:
                raise InventarioError("ALREADY_TRANSFORMED")

            if transformation_group_id:
                existing_group = self.db.fetchone("""
                    SELECT id FROM batches
                    WHERE transformation_group_id = ?
                    LIMIT 1
                """, (transformation_group_id,))
                if existing_group:
                    raise InventarioError("TRANSFORMATION_GROUP_NOT_UNIQUE")

            for o in outputs:
                self._detect_cycle(str(o.get("product_id")), parent_batch_id)

            if not outputs:
                raise InventarioError("OUTPUTS_EMPTY")

            total_out = sum(float(o["weight"]) for o in outputs)
            parent_weight = float(parent["weight"])

            if abs(parent_weight - total_out) > TOLERANCE:
                raise InventarioError("WEIGHT_MISMATCH")

            root_id = parent["root_batch_id"] or parent_batch_id

            if parent["root_batch_id"] is not None:
                self._validate_root_batch_id(parent_batch_id, root_id)

            for o in outputs:
                self.db.execute("""
                    INSERT INTO batches(
                        id,
                        product_id,
                        weight,
                        parent_batch_id,
                        root_batch_id,
                        transformation_group_id,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()),
                    o["product_id"],
                    float(o["weight"]),
                    parent_batch_id,
                    root_id,
                    transformation_group_id,
                    self._now()
                ))

            reconstructed = self.db.fetchone("""
                SELECT COALESCE(SUM(weight), 0) as total
                FROM batches
                WHERE root_batch_id = ? AND parent_batch_id IS NOT NULL
            """, (root_id,))

            reconstructed_val = float(reconstructed["total"])
            root_row = self.db.fetchone(
                "SELECT weight FROM batches WHERE id = ?", (root_id,)
            )
            root_weight = float(root_row["weight"]) if root_row else parent_weight

            if abs(reconstructed_val - root_weight) > TOLERANCE:
                raise InventarioError("POST_INSERT_WEIGHT_MISMATCH")

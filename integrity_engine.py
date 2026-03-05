from datetime import datetime
import uuid


DEFAULT_TOLERANCE = 0.01
DEFAULT_MAX_DEPTH = 50


class IntegrityEngine:

    def __init__(self, db):
        self.db = db

    def _now(self):
        return datetime.utcnow().isoformat()

    def _get_config_float(self, key, default):
        try:
            row = self.db.fetchone(
                "SELECT valor FROM configuracion WHERE clave = ?", (key,)
            )
            if row and row["valor"]:
                return float(row["valor"])
        except Exception:
            pass
        return default

    def _get_config_int(self, key, default):
        try:
            row = self.db.fetchone(
                "SELECT valor FROM configuracion WHERE clave = ?", (key,)
            )
            if row and row["valor"]:
                return int(row["valor"])
        except Exception:
            pass
        return default

    def check_negative_inventory(self):
        row = self.db.fetchone("""
            SELECT COUNT(*) as c FROM branch_inventory
            WHERE quantity < 0
        """)
        return row["c"] == 0

    def _load_tree(self, root_id):
        nodes = self.db.fetchall("""
            SELECT id, parent_batch_id, weight
            FROM batches
            WHERE root_batch_id = ?
        """, (root_id,))
        children_map = {}
        weight_map = {}
        for n in nodes:
            nid = n["id"]
            weight_map[nid] = float(n["weight"])
            pid = n["parent_batch_id"]
            if pid is not None:
                children_map.setdefault(pid, []).append(nid)
        return children_map, weight_map

    def _dfs_detect_cycle(self, node_id, children_map, visiting, visited, depth, max_depth):
        if depth > max_depth:
            return True
        if node_id in visiting:
            return True
        if node_id in visited:
            return False
        visiting.add(node_id)
        for child in children_map.get(node_id, []):
            if self._dfs_detect_cycle(child, children_map, visiting, visited, depth + 1, max_depth):
                return True
        visiting.discard(node_id)
        visited.add(node_id)
        return False

    def _sum_leaf_weights(self, node_id, children_map, weight_map, depth, max_depth):
        if depth > max_depth:
            raise RecursionError("DEPTH_LIMIT_EXCEEDED")
        children = children_map.get(node_id, [])
        if not children:
            return weight_map.get(node_id, 0.0)
        total = 0.0
        for child in children:
            total += self._sum_leaf_weights(child, children_map, weight_map, depth + 1, max_depth)
        return total

    def _validate_intermediate_nodes(self, node_id, children_map, weight_map, tolerance, depth, max_depth):
        if depth > max_depth:
            raise RecursionError("DEPTH_LIMIT_EXCEEDED")
        children = children_map.get(node_id, [])
        if not children:
            return True
        child_total = sum(weight_map.get(c, 0.0) for c in children)
        node_weight = weight_map.get(node_id, 0.0)
        if abs(child_total - node_weight) > tolerance:
            return False
        for child in children:
            if not self._validate_intermediate_nodes(child, children_map, weight_map, tolerance, depth + 1, max_depth):
                return False
        return True

    def check_batch_trees(self):
        tolerance = self._get_config_float("integrity_tolerance_kg", DEFAULT_TOLERANCE)
        max_depth = self._get_config_int("integrity_max_depth", DEFAULT_MAX_DEPTH)

        roots = self.db.fetchall("""
            SELECT id, weight FROM batches
            WHERE parent_batch_id IS NULL
        """)

        all_pass = True

        for r in roots:
            root_id = r["id"]
            original_weight = float(r["weight"])

            children_map, weight_map = self._load_tree(root_id)

            visiting = set()
            visited = set()
            has_cycle = self._dfs_detect_cycle(
                root_id, children_map, visiting, visited, 0, max_depth
            )

            try:
                reconstructed = self._sum_leaf_weights(
                    root_id, children_map, weight_map, 0, max_depth
                )
            except RecursionError:
                reconstructed = 0.0
                has_cycle = True

            try:
                intermediates_valid = self._validate_intermediate_nodes(
                    root_id, children_map, weight_map, tolerance, 0, max_depth
                )
            except RecursionError:
                intermediates_valid = False

            difference = abs(original_weight - reconstructed)
            passed = (
                not has_cycle
                and intermediates_valid
                and difference <= tolerance
            )

            audit_id = str(uuid.uuid4())
            self.db.execute("""
                INSERT INTO batch_tree_audits(
                    audit_uuid,
                    root_batch_id,
                    original_weight,
                    reconstructed_weight,
                    difference,
                    has_cycle,
                    passed,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                audit_id,
                root_id,
                original_weight,
                reconstructed,
                difference,
                1 if has_cycle else 0,
                1 if passed else 0,
                self._now()
            ))

            if not passed:
                all_pass = False

        return all_pass

    def check_unsynced_events(self):
        row = self.db.fetchone("""
            SELECT COUNT(*) as c FROM events
            WHERE synced = 0
            AND created_at <= datetime('now','-72 hours')
        """)
        return row["c"] == 0

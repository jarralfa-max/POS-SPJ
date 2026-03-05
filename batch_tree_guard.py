class CycleDetectedError(Exception):
    pass


class BatchTreeGuard:

    def __init__(self, db):
        self.db = db

    def _get_all_descendants(self, batch_id):
        descendants = set()
        stack = [batch_id]
        while stack:
            current = stack.pop()
            if current in descendants:
                continue
            descendants.add(current)
            children = self.db.fetchall(
                "SELECT id FROM batches WHERE parent_batch_id = ?",
                (current,)
            )
            for child in children:
                if child["id"] not in descendants:
                    stack.append(child["id"])
        return descendants

    def validate_no_cycle(self, parent_batch_id, candidate_child_id):
        descendants_of_child = self._get_all_descendants(candidate_child_id)
        if parent_batch_id in descendants_of_child:
            raise CycleDetectedError("CYCLE_DETECTED: parent is a descendant of candidate child")

        visited = set()
        current = parent_batch_id
        while current:
            if current == candidate_child_id:
                raise CycleDetectedError("CYCLE_DETECTED: candidate child is an ancestor of parent")
            if current in visited:
                raise CycleDetectedError("CYCLE_DETECTED: existing cycle in ancestor chain")
            visited.add(current)
            row = self.db.fetchone(
                "SELECT parent_batch_id FROM batches WHERE id = ?",
                (current,)
            )
            if not row:
                break
            current = row["parent_batch_id"]

    def reconstruct_tree_weight(self, root_batch_id):
        batches = self.db.fetchall(
            "SELECT id, weight, parent_batch_id FROM batches WHERE root_batch_id = ?",
            (root_batch_id,)
        )

        children_map = {}
        weight_map = {}
        for b in batches:
            nid = b["id"]
            weight_map[nid] = float(b["weight"])
            pid = b["parent_batch_id"]
            if pid is not None:
                children_map.setdefault(pid, []).append(nid)

        def sum_leaves(node_id):
            children = children_map.get(node_id, [])
            if not children:
                return weight_map.get(node_id, 0.0)
            return sum(sum_leaves(c) for c in children)

        return sum_leaves(root_batch_id)

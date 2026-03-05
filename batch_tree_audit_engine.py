from utils.operation_context import generate_operation_id, now_iso

class BatchTreeAuditEngine:

    def __init__(self, db):
        self.db = db

    def validate_all_batch_trees(self):
        operation_id = generate_operation_id()

        with self.db.transaction("BATCH_TREE_AUDIT"):
            roots = self.db.fetchall("""
                SELECT id, weight
                FROM batches
                WHERE parent_batch_id IS NULL
            """)

            for root in roots:
                root_id = root["id"]
                original_weight = float(root["weight"])

                all_nodes = self.db.fetchall("""
                    SELECT weight FROM batches
                    WHERE root_batch_id = ?
                """, (root_id,))

                reconstructed = 0.0
                for node in all_nodes:
                    reconstructed += float(node["weight"])

                difference = abs(original_weight - reconstructed)

                if difference > 0.01:
                    self.db.execute("""
                        INSERT INTO batch_tree_audits(
                            root_batch_id,
                            original_weight,
                            reconstructed_weight,
                            difference,
                            created_at
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (
                        root_id,
                        original_weight,
                        reconstructed,
                        difference,
                        now_iso()
                    ))
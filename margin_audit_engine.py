import uuid
from datetime import datetime


MARGIN_ANOMALY_THRESHOLD = 0.0


class MarginAuditEngine:

    def __init__(self, db):
        self.db = db

    def _now(self):
        return datetime.utcnow().isoformat()

    def detect_negative_margin(self, branch_id, week_label, conn=None, product_id=None):
        _db = conn if conn is not None else self.db

        row = _db.fetchone("""
            SELECT
                COALESCE(SUM(total_amount), 0) AS revenue,
                COALESCE(SUM(total_cost), 0)   AS cost
            FROM sales
            WHERE branch_id = ?
              AND strftime('%Y-%W', created_at) = ?
        """, (branch_id, week_label))

        revenue = float(row["revenue"]) if row else 0.0
        cost = float(row["cost"]) if row else 0.0

        if revenue <= 0:
            margin = 0.0
        else:
            margin = (revenue - cost) / revenue

        if margin < MARGIN_ANOMALY_THRESHOLD:
            existing = None
            if product_id is not None:
                existing = _db.fetchone("""
                    SELECT id FROM margin_anomalies
                    WHERE product_id = ? AND branch_id = ? AND week_label = ?
                """, (product_id, branch_id, week_label))
            else:
                existing = _db.fetchone("""
                    SELECT id FROM margin_anomalies
                    WHERE product_id IS NULL AND branch_id = ? AND week_label = ?
                """, (branch_id, week_label))

            if existing:
                return {
                    "anomaly_detected": True,
                    "anomaly_id": existing["id"],
                    "margin": margin,
                    "duplicate_skipped": True,
                }

            anomaly_id = str(uuid.uuid4())
            _db.execute("""
                INSERT INTO margin_anomalies(
                    id,
                    product_id,
                    branch_id,
                    week_label,
                    revenue,
                    cost,
                    margin,
                    detected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                anomaly_id,
                product_id,
                branch_id,
                week_label,
                revenue,
                cost,
                margin,
                self._now()
            ))
            return {"anomaly_detected": True, "anomaly_id": anomaly_id, "margin": margin}

        return {"anomaly_detected": False, "margin": margin}

    def detect_negative_margin_in_transaction(self, branch_id, week_label, db_conn, product_id=None):
        return self.detect_negative_margin(
            branch_id, week_label, conn=db_conn, product_id=product_id
        )

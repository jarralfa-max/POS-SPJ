from datetime import datetime

class ConflictResolver:

    SERVER_AUTHORITATIVE = "SERVER_AUTHORITATIVE"
    LAST_WRITE_WINS = "LAST_WRITE_WINS"
    MANUAL_REVIEW = "MANUAL_REVIEW"

    def __init__(self, db):
        self.db = db

    def resolve(self, event_id, local_payload, remote_payload, policy):
        if policy == self.SERVER_AUTHORITATIVE:
            resolution = remote_payload

        elif policy == self.LAST_WRITE_WINS:
            local_time = local_payload.get("updated_at")
            remote_time = remote_payload.get("updated_at")
            resolution = remote_payload if remote_time >= local_time else local_payload

        else:
            self.db.execute("""
                INSERT INTO sync_conflicts(
                    event_id,
                    local_payload,
                    remote_payload,
                    resolution,
                    resolved_at
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                event_id,
                str(local_payload),
                str(remote_payload),
                self.MANUAL_REVIEW,
                datetime.utcnow().isoformat()
            ))
            return None

        return resolution
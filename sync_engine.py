import json
import uuid
from datetime import datetime
from core.services.event_hashing import deterministic_hash


BATCH_SIZE = 100
MAX_PAYLOAD_BYTES = 1_048_576  # 1 MB configurable default
MAX_PAYLOAD_KEYS = 512


class MalformedPayloadError(Exception):
    pass


class SyncConflictError(Exception):
    pass


def _validate_payload(payload, max_bytes=MAX_PAYLOAD_BYTES):
    if not isinstance(payload, dict):
        raise MalformedPayloadError("PAYLOAD_NOT_DICT")
    if len(payload) > MAX_PAYLOAD_KEYS:
        raise MalformedPayloadError("PAYLOAD_TOO_MANY_KEYS")
    try:
        serialized = json.dumps(payload, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise MalformedPayloadError("PAYLOAD_NOT_SERIALIZABLE") from exc
    if len(serialized.encode("utf-8")) > max_bytes:
        raise MalformedPayloadError("PAYLOAD_TOO_LARGE")
    return serialized


def _deserialize_payload(raw):
    if not isinstance(raw, str):
        raise MalformedPayloadError("RAW_PAYLOAD_NOT_STRING")
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        raise MalformedPayloadError("PAYLOAD_PARSE_ERROR") from exc
    if not isinstance(parsed, dict):
        raise MalformedPayloadError("PAYLOAD_NOT_DICT_AFTER_PARSE")
    return parsed


class SyncEngine:

    def __init__(self, db, device_id):
        self.db = db
        self.device_id = device_id

    def _get_payload_size_limit(self):
        row = self.db.fetchone(
            "SELECT valor FROM configuracion WHERE clave = 'sync_max_payload_bytes'"
        )
        if row and row["valor"]:
            try:
                return int(row["valor"])
            except (ValueError, TypeError):
                pass
        return MAX_PAYLOAD_BYTES

    def register_event(self, event_type, payload):
        if not isinstance(event_type, str) or not event_type.strip():
            raise MalformedPayloadError("INVALID_EVENT_TYPE")

        max_bytes = self._get_payload_size_limit()
        payload_json = _validate_payload(payload, max_bytes=max_bytes)

        operation_id = str(uuid.uuid4())
        payload["updated_at"] = datetime.utcnow().isoformat()
        payload_json = json.dumps(payload, sort_keys=True)
        event_hash = deterministic_hash(payload)

        with self.db.transaction("SYNC_REGISTER", operation_id):
            self.db.execute("""
                INSERT INTO events(
                    id, type, payload, version,
                    hash, origin_device_id,
                    synced, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 0, ?)
            """, (
                operation_id,
                event_type,
                payload_json,
                1,
                event_hash,
                self.device_id,
                datetime.utcnow().isoformat()
            ))

    def get_unsynced_batch(self):
        rows = self.db.fetchall("""
            SELECT id, type, payload, version,
                   hash, origin_device_id, created_at
            FROM events
            WHERE synced = 0
            ORDER BY created_at ASC
            LIMIT ?
        """, (BATCH_SIZE,))

        result = []
        for r in rows:
            record = dict(r)
            if isinstance(record.get("payload"), str):
                record["payload"] = _deserialize_payload(record["payload"])
            result.append(record)
        return result

    def receive_remote_event(self, event_id, event_type, payload, version, event_hash):
        if not isinstance(event_id, str):
            raise MalformedPayloadError("EVENT_ID_NOT_STRING")

        max_bytes = self._get_payload_size_limit()
        _validate_payload(payload, max_bytes=max_bytes)

        computed_hash = deterministic_hash(payload)
        if computed_hash != event_hash:
            self._record_conflict(
                event_id=event_id,
                conflict_type="HASH_MISMATCH",
                local_version=None,
                remote_version=version,
                remote_hash=event_hash,
                computed_hash=computed_hash,
            )
            raise SyncConflictError("HASH_MISMATCH")

        with self.db.transaction("SYNC_RECEIVE"):
            existing = self.db.fetchone(
                "SELECT id, version, hash FROM events WHERE id = ?",
                (event_id,)
            )
            if existing:
                if existing["version"] != version or existing["hash"] != event_hash:
                    self._record_conflict(
                        event_id=event_id,
                        conflict_type="VERSION_MISMATCH",
                        local_version=existing["version"],
                        remote_version=version,
                        remote_hash=event_hash,
                        computed_hash=computed_hash,
                    )
                    raise SyncConflictError("VERSION_MISMATCH")
                return

            payload_json = json.dumps(payload, sort_keys=True)
            self.db.execute("""
                INSERT INTO events(
                    id, type, payload, version,
                    hash, origin_device_id,
                    synced, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            """, (
                event_id,
                event_type,
                payload_json,
                version,
                event_hash,
                "REMOTE",
                datetime.utcnow().isoformat()
            ))

    def _record_conflict(
        self,
        event_id,
        conflict_type,
        local_version,
        remote_version,
        remote_hash,
        computed_hash,
    ):
        try:
            self.db.execute("""
                INSERT OR IGNORE INTO sync_conflicts(
                    id, event_id, conflict_type,
                    local_version, remote_version,
                    remote_hash, computed_hash,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(uuid.uuid4()),
                event_id,
                conflict_type,
                local_version,
                remote_version,
                remote_hash,
                computed_hash,
                datetime.utcnow().isoformat()
            ))
        except Exception:
            pass

    def mark_as_synced(self, confirmed_ids):
        if not confirmed_ids:
            return
        if not isinstance(confirmed_ids, (list, tuple)):
            raise MalformedPayloadError("CONFIRMED_IDS_NOT_LIST")
        with self.db.transaction("SYNC_CONFIRM"):
            for eid in confirmed_ids:
                if not isinstance(eid, str):
                    raise MalformedPayloadError("EVENT_ID_NOT_STRING")
                self.db.execute("""
                    UPDATE events
                    SET synced = 1
                    WHERE id = ? AND synced = 0
                """, (eid,))

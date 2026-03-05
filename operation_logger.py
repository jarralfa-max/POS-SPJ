import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

LOG_FILE = "logs/operations.json"

logger = logging.getLogger("operations_logger")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,
    backupCount=5
)

formatter = logging.Formatter('%(message)s')
handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(handler)

def log_operation(operation_id, branch_id, engine, action, status, duration_ms):
    payload = {
        "operation_id": operation_id,
        "branch_id": branch_id,
        "engine": engine,
        "action": action,
        "status": status,
        "duration_ms": duration_ms,
        "timestamp": datetime.utcnow().isoformat()
    }
    logger.info(json.dumps(payload, separators=(",", ":")))
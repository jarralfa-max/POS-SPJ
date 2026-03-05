import uuid
import time
from contextvars import ContextVar

_operation_id_ctx: ContextVar[str] = ContextVar("operation_id", default=None)

def generate_operation_id() -> str:
    op_id = str(uuid.uuid4())
    _operation_id_ctx.set(op_id)
    return op_id

def get_operation_id() -> str:
    return _operation_id_ctx.get()

def clear_operation_id():
    _operation_id_ctx.set(None)

def now_iso():
    return time.strftime("%Y-%m-%d %H:%M:%S")
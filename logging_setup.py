# utils/logging_setup.py
# ── ENTERPRISE LOGGING — SPJ v3.2 ────────────────────────────────────────────
# logs/app.log     → INFO+  (rotación 5MB × 5 archivos)
# logs/errors.log  → ERROR+ (rotación 5MB × 5 archivos)
# Consola          → DEBUG (solo en desarrollo)
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
from datetime import datetime

_LOG_MAX_BYTES  = 5 * 1024 * 1024   # 5 MB
_LOG_BACKUP     = 5                  # 5 archivos de respaldo


def setup_logging(
    log_dir:        str  = None,
    level:          int  = logging.INFO,
    console:        bool = True,
    max_bytes:      int  = _LOG_MAX_BYTES,
    backup_count:   int  = _LOG_BACKUP,
) -> logging.Logger:
    """
    Configura logging enterprise:
    - logs/app.log    → INFO+  (rotación 5MB × 5 archivos)
    - logs/errors.log → ERROR+ (rotación 5MB × 5 archivos)
    - Consola         → DEBUG  (solo en desarrollo)

    Retorna el logger raíz 'spj'.
    """
    if log_dir is None:
        if getattr(sys, "frozen", False):
            base = os.path.dirname(sys.executable)
        else:
            base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_dir = os.path.join(base, "logs")

    os.makedirs(log_dir, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fmt_error = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(name)s %(filename)s:%(lineno)d\n"
        "  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root = logging.getLogger("spj")
    root.setLevel(logging.DEBUG)   # handlers filtran su propio nivel

    # Evitar duplicar handlers si se llama varias veces
    if root.handlers:
        return root

    # ── Handler app.log (INFO+) ───────────────────────────────────────────────
    app_log = os.path.join(log_dir, "app.log")
    fh_app = RotatingFileHandler(
        app_log, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    fh_app.setFormatter(fmt)
    fh_app.setLevel(level)
    root.addHandler(fh_app)

    # ── Handler errors.log (ERROR+) ───────────────────────────────────────────
    err_log = os.path.join(log_dir, "errors.log")
    fh_err = RotatingFileHandler(
        err_log, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
    )
    fh_err.setFormatter(fmt_error)
    fh_err.setLevel(logging.ERROR)
    root.addHandler(fh_err)

    # ── Consola (DEBUG, solo dev) ─────────────────────────────────────────────
    if console:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        ch.setLevel(logging.DEBUG)
        root.addHandler(ch)

    # Silenciar libs ruidosas
    for noisy in ("PIL", "urllib3", "PyQt5"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    root.info("Logging enterprise iniciado → app=%s | errors=%s", app_log, err_log)
    return root


class DBLogHandler(logging.Handler):
    """
    Handler que escribe logs CRÍTICOS también en la tabla logs de SQLite.
    Solo para nivel WARNING+, para no saturar la BD.
    """

    def __init__(self, conn_factory):
        super().__init__(level=logging.WARNING)
        self.conn_factory = conn_factory

    def emit(self, record: logging.LogRecord) -> None:
        try:
            conn = self.conn_factory()
            conn.execute(
                "INSERT INTO logs (nivel, modulo, mensaje, fecha) VALUES (?,?,?,datetime('now'))",
                (record.levelname, record.name, self.format(record))
            )
            conn.commit()
        except Exception:
            pass  # Logging no debe causar crash

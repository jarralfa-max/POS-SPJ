# security/auth.py
# Módulo de autenticación enterprise
# - Bcrypt obligatorio para contraseñas nuevas
# - Auto-migración transparente de texto plano → bcrypt al primer login exitoso
# - Rate limiting básico para prevenir brute-force
# - Sanitización de inputs
from __future__ import annotations
import sqlite3
import logging
import time
import threading
from dataclasses import dataclass, field
from typing import Optional, List

logger = logging.getLogger("spj.security")

# ── Constantes ────────────────────────────────────────────────────────────────
BCRYPT_ROUNDS    = 12
MAX_ATTEMPTS     = 5        # bloqueo temporal tras 5 intentos fallidos
LOCKOUT_SECONDS  = 300      # 5 minutos de bloqueo
MIN_PASSWORD_LEN = 8


# ── Importación bcrypt con fallback ──────────────────────────────────────────
try:
    import bcrypt
    HAS_BCRYPT = True
except ImportError:
    HAS_BCRYPT = False
    logger.warning("bcrypt no disponible. Instalar: pip install bcrypt")


# ── Rate limiter en memoria (por usuario) ────────────────────────────────────
_rate_lock  = threading.Lock()
_attempts:   dict[str, list[float]] = {}   # usuario → [timestamps]
_lockouts:   dict[str, float]       = {}   # usuario → unlock_timestamp


def _check_rate_limit(usuario: str) -> None:
    """Lanza AuthError si el usuario está bajo bloqueo temporal."""
    now = time.time()
    with _rate_lock:
        unlock = _lockouts.get(usuario, 0)
        if now < unlock:
            remaining = int(unlock - now)
            raise AuthError(
                f"Cuenta temporalmente bloqueada. Intente en {remaining}s."
            )
        # Limpiar intentos viejos
        ats = [t for t in _attempts.get(usuario, []) if now - t < LOCKOUT_SECONDS]
        _attempts[usuario] = ats


def _record_failure(usuario: str) -> None:
    now = time.time()
    with _rate_lock:
        ats = _attempts.setdefault(usuario, [])
        ats.append(now)
        if len(ats) >= MAX_ATTEMPTS:
            _lockouts[usuario] = now + LOCKOUT_SECONDS
            _attempts[usuario] = []
            logger.warning("Usuario '%s' bloqueado por %ds tras %d fallos",
                           usuario, LOCKOUT_SECONDS, MAX_ATTEMPTS)


def _clear_failures(usuario: str) -> None:
    with _rate_lock:
        _attempts.pop(usuario, None)
        _lockouts.pop(usuario, None)


# ── Excepciones ──────────────────────────────────────────────────────────────

class AuthError(Exception):
    pass

class UsuarioInactivoError(AuthError):
    pass

class PasswordDebilError(AuthError):
    pass


# ── Funciones públicas ───────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    """Hashea contraseña con bcrypt. Lanza PasswordDebilError si es muy corta."""
    _validar_password(password)
    if not HAS_BCRYPT:
        raise RuntimeError("bcrypt no instalado. Ejecutar: pip install bcrypt")
    salt = bcrypt.gensalt(BCRYPT_ROUNDS)
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


def verify_password(raw: str, stored: str) -> bool:
    """
    Verifica contraseña. Soporta:
    - bcrypt ($2b$, $2a$): comparación segura
    - Texto plano (legacy): comparación directa (solo para migración)
    """
    if not raw or not stored:
        return False
    if stored.startswith(("$2b$", "$2a$", "$2y$")):
        if not HAS_BCRYPT:
            return False
        try:
            return bcrypt.checkpw(raw.encode("utf-8"), stored.encode("utf-8"))
        except Exception:
            return False
    # Legacy texto plano
    return raw == stored


def autenticar(
    conn: sqlite3.Connection,
    usuario: str,
    password: str
) -> dict:
    """
    Autentica un usuario. Retorna dict con datos del usuario.
    Lanza AuthError (o subclases) en caso de fallo.
    Migra automáticamente passwords legacy a bcrypt tras login exitoso.
    """
    usuario = sanitize_string(usuario, max_len=50)

    _check_rate_limit(usuario)

    row = conn.execute(
        "SELECT id, usuario, contrasena, nombre, rol, activo "
        "FROM usuarios WHERE usuario = ?",
        (usuario,)
    ).fetchone()

    if not row:
        _record_failure(usuario)
        raise AuthError("Usuario o contraseña incorrectos.")

    if not row["activo"]:
        raise UsuarioInactivoError("Cuenta desactivada. Contacte al administrador.")

    if not verify_password(password, row["contrasena"]):
        _record_failure(usuario)
        raise AuthError("Usuario o contraseña incorrectos.")

    # Login exitoso
    _clear_failures(usuario)

    # Actualizar último acceso
    try:
        conn.execute(
            "UPDATE usuarios SET ultimo_acceso=datetime('now') WHERE id=?",
            (row["id"],)
        )
        conn.commit()
    except Exception as e:
        logger.warning("No se pudo actualizar ultimo_acceso: %s", e)

    # Migrar password legacy → bcrypt
    if not row["contrasena"].startswith(("$2b$", "$2a$", "$2y$")):
        _migrar_password_a_bcrypt(conn, row["id"], password)

    # Obtener módulos permitidos
    modulos = _get_modulos(conn, row["id"])

    logger.info("Login exitoso: usuario=%s rol=%s", usuario, row["rol"])

    return {
        "id":      row["id"],
        "usuario": row["usuario"],
        "nombre":  row["nombre"],
        "rol":     row["rol"],
        "modulos": modulos,
    }


def crear_usuario(
    conn: sqlite3.Connection,
    usuario: str,
    password: str,
    nombre: str,
    rol: str,
    modulos: List[str],
    creado_por: str = "Sistema"
) -> int:
    """Crea un usuario con password hasheado. Retorna el ID creado."""
    usuario  = sanitize_string(usuario, max_len=50)
    nombre   = sanitize_string(nombre, max_len=100)
    password_hash = hash_password(password)

    try:
        cur = conn.execute(
            "INSERT INTO usuarios (usuario, contrasena, nombre, rol) VALUES (?,?,?,?)",
            (usuario, password_hash, nombre, rol)
        )
        uid = cur.lastrowid
        for mod in modulos:
            conn.execute(
                "INSERT OR IGNORE INTO usuario_modulos (usuario_id, modulo) VALUES (?,?)",
                (uid, mod)
            )
        # Actualizar columna legacy para compatibilidad transitoria
        import json
        conn.execute(
            "UPDATE usuarios SET modulos_permitidos=? WHERE id=?",
            (json.dumps(modulos), uid)
        )
        conn.commit()
        logger.info("Usuario '%s' creado por %s", usuario, creado_por)
        return uid
    except sqlite3.IntegrityError:
        raise AuthError(f"El usuario '{usuario}' ya existe.")


def cambiar_password(
    conn: sqlite3.Connection,
    usuario_id: int,
    password_actual: str,
    password_nuevo: str
) -> None:
    """Cambia password verificando el actual."""
    row = conn.execute(
        "SELECT contrasena FROM usuarios WHERE id=?", (usuario_id,)
    ).fetchone()
    if not row or not verify_password(password_actual, row["contrasena"]):
        raise AuthError("Contraseña actual incorrecta.")
    nuevo_hash = hash_password(password_nuevo)
    conn.execute(
        "UPDATE usuarios SET contrasena=? WHERE id=?",
        (nuevo_hash, usuario_id)
    )
    conn.commit()


# ── Helpers privados ─────────────────────────────────────────────────────────

def _validar_password(password: str) -> None:
    if not password or len(password) < MIN_PASSWORD_LEN:
        raise PasswordDebilError(
            f"La contraseña debe tener al menos {MIN_PASSWORD_LEN} caracteres."
        )


def _migrar_password_a_bcrypt(conn: sqlite3.Connection, uid: int, pwd_plano: str) -> None:
    """Migración silenciosa en background. Fallo no crítico."""
    if not HAS_BCRYPT:
        return
    try:
        hashed = hash_password(pwd_plano)
        conn.execute("UPDATE usuarios SET contrasena=? WHERE id=?", (hashed, uid))
        conn.commit()
        logger.info("Password usuario#%d migrado a bcrypt", uid)
    except Exception as e:
        logger.warning("No se pudo migrar password bcrypt: %s", e)


def _get_modulos(conn: sqlite3.Connection, usuario_id: int) -> List[str]:
    """Obtiene módulos desde tabla normalizada con fallback a columna legacy."""
    rows = conn.execute(
        "SELECT modulo FROM usuario_modulos WHERE usuario_id=?", (usuario_id,)
    ).fetchall()
    if rows:
        return [r["modulo"] for r in rows]
    # Fallback legacy JSON/CSV
    import json
    row = conn.execute(
        "SELECT modulos_permitidos FROM usuarios WHERE id=?", (usuario_id,)
    ).fetchone()
    if row and row["modulos_permitidos"]:
        raw = row["modulos_permitidos"]
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except (json.JSONDecodeError, ValueError):
            return [m.strip() for m in raw.split(",") if m.strip()]
    return []


def sanitize_string(value: str, max_len: int = 255) -> str:
    """Sanitiza string: strip, limita longitud, elimina caracteres de control."""
    if not isinstance(value, str):
        return ""
    clean = "".join(c for c in value if c.isprintable())
    return clean.strip()[:max_len]

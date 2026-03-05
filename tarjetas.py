# repositories/tarjetas.py
# ── TarjetaRepository — Enterprise Repository Layer ──────────────────────────
# Fixes silent failure persistence issues in ModuloTarjetas.
# Enforces: JSON serialization, explicit commit, visible errors,
#           no direct UI SQL.
from __future__ import annotations

import json
import logging
import random
import uuid
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger("spj.repositories.tarjetas")


class TarjetaError(Exception):
    pass


class TarjetaNotFoundError(TarjetaError):
    pass


class TarjetaYaAsignadaError(TarjetaError):
    pass


class TarjetaRepository:

    def __init__(self, db):
        self.db = db

    def _now(self) -> str:
        return datetime.utcnow().isoformat()

    # ── ID and QR generation ─────────────────────────────────────────────────

    def _generate_unique_id(self) -> int:
        for _ in range(100):
            candidate = random.randint(1000, 9999)
            existing = self.db.fetchone(
                "SELECT id FROM tarjetas_fidelidad WHERE id = ?", (candidate,)
            )
            if not existing:
                return candidate
        raise TarjetaError("NO_UNIQUE_ID_AVAILABLE")

    def _generate_unique_qr(self) -> str:
        for _ in range(100):
            code = f"QR-{random.randint(1000, 9999):04d}"
            existing = self.db.fetchone(
                "SELECT id FROM tarjetas_fidelidad WHERE codigo_qr LIKE ?",
                (f'%{code}%',)
            )
            if not existing:
                return code
        raise TarjetaError("NO_UNIQUE_QR_AVAILABLE")

    def _build_qr_payload(self, qr_code: str, card_id: int) -> str:
        payload = {
            "codigo_qr": qr_code,
            "card_id": card_id,
            "emision": datetime.utcnow().strftime("%Y-%m-%d"),
        }
        return json.dumps(payload, ensure_ascii=False)

    # ── Read ─────────────────────────────────────────────────────────────────

    def get_all(self, search: str = "") -> List[Dict]:
        params: List = []
        where_extra = ""
        if search:
            where_extra = """
                AND (
                    CAST(tf.id AS TEXT) LIKE ?
                    OR c.nombre LIKE ?
                    OR c.apellido LIKE ?
                    OR tf.estado LIKE ?
                )
            """
            like = f"%{search}%"
            params.extend([like, like, like, like])
        rows = self.db.fetchall(f"""
            SELECT tf.id, tf.codigo_qr,
                   COALESCE(c.nombre || ' ' || COALESCE(c.apellido,''), 'Sin asignar') AS cliente,
                   tf.estado, tf.puntos_actuales,
                   tf.es_pregenerada, tf.fecha_creacion, tf.fecha_asignacion,
                   tf.id_cliente
            FROM tarjetas_fidelidad tf
            LEFT JOIN clientes c ON c.id = tf.id_cliente
            WHERE 1=1 {where_extra}
            ORDER BY tf.fecha_creacion DESC
        """, params)
        return [dict(r) for r in rows]

    def get_by_id(self, tarjeta_id: int) -> Optional[Dict]:
        row = self.db.fetchone("""
            SELECT tf.*,
                   COALESCE(c.nombre || ' ' || COALESCE(c.apellido,''), 'Sin asignar') AS cliente_nombre
            FROM tarjetas_fidelidad tf
            LEFT JOIN clientes c ON c.id = tf.id_cliente
            WHERE tf.id = ?
        """, (tarjeta_id,))
        if not row:
            return None
        result = dict(row)
        # Deserialize QR JSON safely
        if isinstance(result.get("codigo_qr"), str):
            try:
                result["qr_data"] = json.loads(result["codigo_qr"])
            except (json.JSONDecodeError, ValueError):
                result["qr_data"] = {"codigo_qr": result["codigo_qr"]}
        return result

    def get_historial(self, tarjeta_id: int) -> List[Dict]:
        rows = self.db.fetchall("""
            SELECT tipo_cambio, valor_anterior, nuevo_valor, usuario, fecha
            FROM historico_tarjetas
            WHERE id_tarjeta = ?
            ORDER BY fecha DESC
        """, (tarjeta_id,))
        return [dict(r) for r in rows]

    def get_design_config(self) -> Dict:
        rows = self.db.fetchall("SELECT clave, valor FROM config_diseno_tarjetas")
        config = {r["clave"]: r["valor"] for r in rows}
        defaults = {
            "color_fondo": "#FFFFFF",
            "color_texto": "#000000",
            "font_path": "",
            "logo_path": "",
            "contenido_html": "<p>Tarjeta de Fidelidad</p>",
        }
        defaults.update({k: v for k, v in config.items() if v is not None})
        return defaults

    # ── Write ────────────────────────────────────────────────────────────────

    def generate_pregeneradas(self, cantidad: int,
                               usuario: str) -> List[Dict]:
        """Generate `cantidad` unassigned pre-generated cards."""
        if cantidad <= 0 or cantidad > 1000:
            raise TarjetaError("CANTIDAD_INVALIDA")

        generated = []
        with self.db.transaction("TARJETA_GENERATE"):
            for _ in range(cantidad):
                card_id = self._generate_unique_id()
                qr_code = self._generate_unique_qr()
                qr_payload = self._build_qr_payload(qr_code, card_id)

                self.db.execute("""
                    INSERT INTO tarjetas_fidelidad (
                        id, codigo_qr, estado, es_pregenerada,
                        puntos_iniciales, puntos_actuales, fecha_creacion
                    ) VALUES (?,?,'disponible',1,0,0,?)
                """, (card_id, qr_payload, self._now()))

                self._write_history(
                    card_id, "CREACION", None, qr_code, usuario
                )
                generated.append({"id": card_id, "qr_code": qr_code,
                                   "codigo_qr": qr_payload})

        logger.info("Generated %d pre-generated cards by %s", cantidad, usuario)
        return generated

    def assign_to_client(self, tarjeta_id: int, cliente_id: int,
                          usuario: str) -> None:
        tarjeta = self.get_by_id(tarjeta_id)
        if not tarjeta:
            raise TarjetaNotFoundError("TARJETA_NOT_FOUND")
        if tarjeta["estado"] == "asignada" and tarjeta["id_cliente"] == cliente_id:
            return  # Idempotent
        if tarjeta["estado"] not in ("disponible", "inactiva"):
            raise TarjetaYaAsignadaError(
                f"TARJETA_ESTADO_INVALIDO: {tarjeta['estado']}"
            )

        # Verify client exists
        client_row = self.db.fetchone(
            "SELECT nombre, puntos FROM clientes WHERE id = ? AND activo = 1",
            (cliente_id,)
        )
        if not client_row:
            raise TarjetaError("CLIENTE_NOT_FOUND")

        new_qr_code = self._generate_unique_qr()
        new_qr_payload = self._build_qr_payload(new_qr_code, tarjeta_id)

        with self.db.transaction("TARJETA_ASSIGN"):
            self.db.execute("""
                UPDATE tarjetas_fidelidad SET
                    codigo_qr = ?,
                    id_cliente = ?,
                    estado = 'asignada',
                    fecha_asignacion = ?,
                    es_pregenerada = 0,
                    puntos_iniciales = ?,
                    puntos_actuales = ?
                WHERE id = ?
            """, (
                new_qr_payload,
                cliente_id,
                self._now(),
                int(client_row["puntos"] or 0),
                int(client_row["puntos"] or 0),
                tarjeta_id,
            ))
            self._write_history(
                tarjeta_id, "ASIGNACION",
                tarjeta.get("codigo_qr"), new_qr_payload, usuario
            )

    def update_card(self, tarjeta_id: int, data: Dict, usuario: str) -> None:
        tarjeta = self.get_by_id(tarjeta_id)
        if not tarjeta:
            raise TarjetaNotFoundError("TARJETA_NOT_FOUND")

        allowed_estados = ("activa", "inactiva", "perdida", "bloqueada",
                           "disponible", "asignada")
        nuevo_estado = data.get("estado", tarjeta["estado"])
        if nuevo_estado not in allowed_estados:
            raise TarjetaError(f"ESTADO_INVALIDO: {nuevo_estado}")

        nuevo_puntos = int(data.get("puntos_actuales", tarjeta["puntos_actuales"]))
        if nuevo_puntos < 0:
            raise TarjetaError("NEGATIVE_POINTS")

        with self.db.transaction("TARJETA_UPDATE"):
            self.db.execute("""
                UPDATE tarjetas_fidelidad SET
                    estado = ?,
                    puntos_actuales = ?,
                    observaciones = ?
                WHERE id = ?
            """, (
                nuevo_estado,
                nuevo_puntos,
                data.get("observaciones", tarjeta.get("observaciones", "")),
                tarjeta_id,
            ))
            self._write_history(
                tarjeta_id, "MODIFICACION",
                f"puntos={tarjeta['puntos_actuales']} estado={tarjeta['estado']}",
                f"puntos={nuevo_puntos} estado={nuevo_estado}",
                usuario,
            )

    def reassign(self, tarjeta_id: int,
                  nuevo_cliente_id: Optional[int],
                  usuario: str) -> None:
        tarjeta = self.get_by_id(tarjeta_id)
        if not tarjeta:
            raise TarjetaNotFoundError("TARJETA_NOT_FOUND")

        prev_cliente_id = tarjeta.get("id_cliente")

        new_qr_code = self._generate_unique_qr()
        new_qr_payload = self._build_qr_payload(new_qr_code, tarjeta_id)

        with self.db.transaction("TARJETA_REASSIGN"):
            self.db.execute("""
                UPDATE tarjetas_fidelidad SET
                    id_cliente = ?,
                    codigo_qr = ?,
                    estado = ?,
                    fecha_asignacion = ?
                WHERE id = ?
            """, (
                nuevo_cliente_id,
                new_qr_payload,
                "asignada" if nuevo_cliente_id else "disponible",
                self._now() if nuevo_cliente_id else None,
                tarjeta_id,
            ))
            # Detach old client's QR reference
            if prev_cliente_id:
                self.db.execute(
                    "UPDATE clientes SET codigo_qr = NULL WHERE id = ?",
                    (prev_cliente_id,)
                )
            # Attach new client
            if nuevo_cliente_id:
                self.db.execute(
                    "UPDATE clientes SET codigo_qr = ? WHERE id = ?",
                    (new_qr_code, nuevo_cliente_id)
                )
            self._write_history(
                tarjeta_id, "REASIGNACION",
                str(prev_cliente_id) if prev_cliente_id else "NULL",
                str(nuevo_cliente_id) if nuevo_cliente_id else "NULL",
                usuario,
            )

    def save_design_config(self, config: Dict, usuario: str) -> None:
        required_keys = (
            "color_fondo", "color_texto", "font_path",
            "logo_path", "contenido_html",
        )
        for key in required_keys:
            if key not in config:
                raise TarjetaError(f"MISSING_CONFIG_KEY: {key}")

        with self.db.transaction("TARJETA_DESIGN_SAVE"):
            self.db.execute("DELETE FROM config_diseno_tarjetas")
            for clave, valor in config.items():
                self.db.execute("""
                    INSERT INTO config_diseno_tarjetas (clave, valor)
                    VALUES (?,?)
                """, (clave, str(valor) if valor is not None else ""))

        logger.info("Card design config saved by %s", usuario)

    # ── Internals ─────────────────────────────────────────────────────────────

    def _write_history(self, tarjeta_id: int, tipo: str,
                        valor_anterior: Optional[str],
                        nuevo_valor: Optional[str],
                        usuario: str) -> None:
        try:
            self.db.execute("""
                INSERT INTO historico_tarjetas (
                    id_tarjeta, tipo_cambio, valor_anterior, nuevo_valor,
                    usuario, fecha
                ) VALUES (?,?,?,?,?,?)
            """, (tarjeta_id, tipo, valor_anterior, nuevo_valor,
                  usuario, self._now()))
        except Exception as exc:
            logger.warning("historico_tarjetas write failed: %s", exc)

    # ── Additional methods required by ModuloTarjetas ────────────────────────

    def get_all(self, search: str = "", estado: str = "") -> List[Dict]:
        """Overload that accepts estado filter and normalizes field names."""
        params: List = []
        conditions = ["1=1"]
        if search:
            conditions.append(
                "(CAST(tf.id AS TEXT) LIKE ? OR c.nombre LIKE ? OR c.apellido LIKE ? OR tf.estado LIKE ?)"
            )
            like = f"%{search}%"
            params.extend([like, like, like, like])
        if estado:
            conditions.append("tf.estado = ?")
            params.append(estado)
        where = " AND ".join(conditions)
        rows = self.db.fetchall(f"""
            SELECT tf.id,
                   COALESCE(c.nombre || ' ' || COALESCE(c.apellido,''), NULL) AS cliente_nombre,
                   tf.estado,
                   COALESCE(tf.puntos_actuales, 0) AS puntos,
                   COALESCE(tf.nivel, 'Bronce') AS nivel,
                   tf.fecha_creacion AS fecha_emision,
                   tf.fecha_asignacion AS updated_at,
                   tf.id_cliente,
                   tf.codigo_qr,
                   tf.observaciones
            FROM tarjetas_fidelidad tf
            LEFT JOIN clientes c ON c.id = tf.id_cliente
            WHERE {where}
            ORDER BY tf.fecha_creacion DESC
        """, params)
        return [dict(r) for r in rows]

    def create(self, data: Dict) -> int:
        """Create a new card. Returns new card id."""
        estado = data.get("estado", "sin_asignar")
        puntos = int(data.get("puntos", 0))
        nivel  = data.get("nivel", "Bronce")
        obs    = data.get("observaciones", "")
        created_by = data.get("created_by", "Sistema")

        card_id = self._generate_unique_id()
        qr_code = self._generate_unique_qr()
        qr_payload = self._build_qr_payload(qr_code, card_id)

        with self.db.transaction("TARJETA_CREATE"):
            self.db.execute("""
                INSERT INTO tarjetas_fidelidad (
                    id, codigo_qr, estado, es_pregenerada,
                    puntos_iniciales, puntos_actuales, nivel,
                    observaciones, fecha_creacion
                ) VALUES (?,?,?,1,?,?,?,?,?)
            """, (card_id, qr_payload, estado, puntos, puntos,
                  nivel, obs, self._now()))
            self._write_history(card_id, "CREACION", None, estado, created_by)

        logger.info("Card %d created by %s", card_id, created_by)
        return card_id

    def update(self, tarjeta_id: int, data: Dict, usuario: str) -> None:
        """Alias for update_card with normalized field names."""
        card = self.get_by_id(tarjeta_id)
        if not card:
            raise TarjetaNotFoundError("TARJETA_NOT_FOUND")
        allowed_estados = ("activa", "inactiva", "perdida", "bloqueada",
                           "disponible", "asignada", "sin_asignar")
        nuevo_estado = data.get("estado", card.get("estado", "activa"))
        if nuevo_estado not in allowed_estados:
            raise TarjetaError(f"ESTADO_INVALIDO: {nuevo_estado}")
        nuevo_puntos = int(data.get("puntos", card.get("puntos_actuales", 0)))
        if nuevo_puntos < 0:
            raise TarjetaError("NEGATIVE_POINTS")
        nivel = data.get("nivel", card.get("nivel", "Bronce"))
        obs   = data.get("observaciones", card.get("observaciones", ""))
        with self.db.transaction("TARJETA_UPDATE"):
            self.db.execute("""
                UPDATE tarjetas_fidelidad SET
                    estado = ?, puntos_actuales = ?, nivel = ?, observaciones = ?
                WHERE id = ?
            """, (nuevo_estado, nuevo_puntos, nivel, obs, tarjeta_id))
            self._write_history(tarjeta_id, "MODIFICACION",
                                card.get("estado"), nuevo_estado, usuario)

    def set_status(self, tarjeta_id: int, estado: str, usuario: str) -> None:
        """Set card status."""
        card = self.get_by_id(tarjeta_id)
        if not card:
            raise TarjetaNotFoundError("TARJETA_NOT_FOUND")
        allowed = ("activa", "inactiva", "bloqueada", "disponible",
                   "asignada", "sin_asignar", "perdida")
        if estado not in allowed:
            raise TarjetaError(f"ESTADO_INVALIDO: {estado}")
        with self.db.transaction("TARJETA_STATUS"):
            self.db.execute(
                "UPDATE tarjetas_fidelidad SET estado = ? WHERE id = ?",
                (estado, tarjeta_id)
            )
            self._write_history(tarjeta_id, "CAMBIO_ESTADO",
                                card.get("estado"), estado, usuario)

    def get_stats(self) -> Dict:
        """Return aggregate card statistics."""
        row = self.db.fetchone("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN estado = 'activa' THEN 1 ELSE 0 END) AS activas,
                SUM(CASE WHEN estado = 'inactiva' THEN 1 ELSE 0 END) AS inactivas,
                SUM(CASE WHEN estado IN ('disponible','sin_asignar')
                         AND id_cliente IS NULL THEN 1 ELSE 0 END) AS sin_asignar
            FROM tarjetas_fidelidad
        """)
        return dict(row) if row else {"total": 0, "activas": 0, "inactivas": 0, "sin_asignar": 0}

    def get_stats_by_level(self) -> List[Dict]:
        """Return point/count stats grouped by loyalty level."""
        rows = self.db.fetchall("""
            SELECT
                COALESCE(nivel, 'Bronce') AS nivel,
                COUNT(*) AS count,
                COALESCE(SUM(puntos_actuales), 0) AS total_puntos,
                COALESCE(AVG(puntos_actuales), 0) AS avg_puntos
            FROM tarjetas_fidelidad
            WHERE id_cliente IS NOT NULL
            GROUP BY nivel
            ORDER BY CASE nivel
                WHEN 'Platino' THEN 1 WHEN 'Oro' THEN 2
                WHEN 'Plata' THEN 3 ELSE 4 END
        """)
        return [dict(r) for r in rows]

    def get_clientes_disponibles(self) -> List[Dict]:
        """Return active clients for assignment dropdowns."""
        rows = self.db.fetchall("""
            SELECT c.id, c.nombre, c.apellido, c.telefono
            FROM clientes c
            WHERE c.activo = 1
            ORDER BY c.nombre, c.apellido
        """)
        return [dict(r) for r in rows]

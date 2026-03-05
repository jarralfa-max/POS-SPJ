# core/services/card_batch_engine.py
# ── Motor de Lotes de Tarjetas SPJ v9 ────────────────────────────────────
# Maneja el ciclo completo de tarjetas de fidelidad:
#   generacion_lote → generada → impresa → libre → asignada / bloqueada
# Genera QR individual y exporta PDF de lote para imprenta.
# Registro completo en card_assignment_history.
from __future__ import annotations

import hashlib
import io
import json
import logging
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

logger = logging.getLogger("spj.card_batch")

try:
    import qrcode
    HAS_QR = True
except ImportError:
    HAS_QR = False

try:
    from reportlab.lib.pagesizes import mm, A4
    from reportlab.pdfgen import canvas as rl_canvas
    from reportlab.lib.utils import ImageReader
    HAS_REPORTLAB = True
except ImportError:
    HAS_REPORTLAB = False


# ── DTOs ──────────────────────────────────────────────────────────────────────

@dataclass
class CardBatch:
    id:               int
    uuid:             str
    nombre:           str
    codigo_inicio:    str
    codigo_fin:       str
    cantidad:         int
    cantidad_libres:  int
    cantidad_asignadas: int
    estado:           str
    notas:            Optional[str]
    fecha_generacion: str


@dataclass
class Tarjeta:
    id:               int
    numero:           str
    batch_id:         Optional[int]
    estado:           str
    id_cliente:       Optional[int]
    puntos_actuales:  int
    nivel:            str
    codigo_qr:        Optional[str]
    fecha_creacion:   str


@dataclass
class AsignacionResult:
    tarjeta_id:   int
    cliente_id:   int
    accion:       str
    exito:        bool
    mensaje:      str


# ── CardBatchEngine ───────────────────────────────────────────────────────────

class CardBatchEngine:
    """
    Motor de gestión de lotes de tarjetas de fidelidad.

    Flujo estándar:
        eng = CardBatchEngine(conn, usuario="admin")
        batch = eng.crear_lote("Lote Enero 2026", prefijo="J", cantidad=500)
        eng.marcar_impreso(batch.id)
        eng.asignar_tarjeta(tarjeta_id, cliente_id)
        eng.bloquear_tarjeta(tarjeta_id, motivo="Extraviada")
    """

    PREFIJO_DEFAULT = "SPJ"

    def __init__(self, conn: sqlite3.Connection, usuario: str = "sistema") -> None:
        self.conn    = conn
        self.usuario = usuario

    # ── Lotes ─────────────────────────────────────────────────────────────────

    def crear_lote(
        self,
        nombre:    str,
        prefijo:   str = None,
        cantidad:  int = 100,
        notas:     str = "",
    ) -> CardBatch:
        """
        Genera un nuevo lote de N tarjetas con numeración secuencial.
        Formato: {prefijo}{timestamp_corto}{seq:05d}
        Ej: SPJ260001, SPJ260002 ...
        """
        if cantidad <= 0 or cantidad > 10000:
            raise ValueError(f"Cantidad debe ser 1-10000, recibido: {cantidad}")

        pfx = (prefijo or self.PREFIJO_DEFAULT).upper()[:6]
        ts  = datetime.now().strftime("%y%m")  # 2602 para feb 2026

        # Buscar secuencia disponible
        last_row = self.conn.execute(
            "SELECT codigo_fin FROM card_batches ORDER BY id DESC LIMIT 1"
        ).fetchone()

        start_seq = 1
        if last_row:
            try:
                # Extraer número del último código
                last_code = last_row[0]
                num_part  = last_code.replace(pfx, "").replace(ts, "")
                if num_part.isdigit():
                    start_seq = int(num_part) + 1
            except Exception:
                pass

        cod_inicio = f"{pfx}{ts}{start_seq:05d}"
        cod_fin    = f"{pfx}{ts}{start_seq + cantidad - 1:05d}"
        batch_uuid = str(uuid.uuid4())

        # INSERT lote
        cur = self.conn.execute(
            """
            INSERT INTO card_batches
                (uuid, nombre, codigo_inicio, codigo_fin, cantidad,
                 cantidad_libres, estado, notas, generado_por)
            VALUES (?,?,?,?,?,?,?,?,?)
            """,
            (batch_uuid, nombre, cod_inicio, cod_fin, cantidad,
             cantidad, "activo", notas, self.usuario)
        )
        batch_id = cur.lastrowid

        # Generar tarjetas individuales
        for i in range(cantidad):
            numero = f"{pfx}{ts}{start_seq + i:05d}"
            qr_str = self._generar_qr_string(numero)
            self.conn.execute(
                """
                INSERT INTO tarjetas_fidelidad
                    (numero, codigo_qr, batch_id, estado, activa,
                     es_pregenerada, puntos_actuales, nivel)
                VALUES (?,?,?,'generada',1,1,0,'Bronce')
                ON CONFLICT(numero) DO NOTHING
                """,
                (numero, qr_str, batch_id)
            )

        self.conn.commit()

        logger.info(
            "crear_lote batch=%d nombre='%s' cantidad=%d inicio=%s fin=%s",
            batch_id, nombre, cantidad, cod_inicio, cod_fin
        )

        return self._load_batch(batch_id)

    def _generar_qr_string(self, numero: str) -> str:
        """Genera código hash SHA-256 corto (16 hex) para QR."""
        return hashlib.sha256(numero.encode()).hexdigest()[:16].upper()

    def _load_batch(self, batch_id: int) -> CardBatch:
        row = self.conn.execute(
            """
            SELECT id, uuid, nombre, codigo_inicio, codigo_fin,
                   cantidad, cantidad_libres, cantidad_asignadas,
                   estado, notas, fecha_generacion
            FROM card_batches WHERE id = ?
            """,
            (batch_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"Batch {batch_id} no encontrado")
        return CardBatch(*row)

    def listar_lotes(self, estado: str = None) -> List[CardBatch]:
        q = "SELECT id, uuid, nombre, codigo_inicio, codigo_fin, " \
            "cantidad, cantidad_libres, cantidad_asignadas, " \
            "estado, notas, fecha_generacion FROM card_batches"
        params: list = []
        if estado:
            q += " WHERE estado = ?"
            params.append(estado)
        q += " ORDER BY id DESC"
        return [CardBatch(*r) for r in self.conn.execute(q, params).fetchall()]

    # ── Estados lote ──────────────────────────────────────────────────────────

    def marcar_impreso(self, batch_id: int) -> int:
        """Marca todas las tarjetas del lote como 'impresa'. Retorna n afectadas."""
        n = self.conn.execute(
            "UPDATE tarjetas_fidelidad SET estado='impresa' "
            "WHERE batch_id = ? AND estado = 'generada'",
            (batch_id,)
        ).rowcount
        self.conn.commit()
        logger.info("marcar_impreso batch=%d n=%d", batch_id, n)
        return n

    def liberar_lote(self, batch_id: int) -> int:
        """Marca tarjetas impresas del lote como 'libre' (disponibles para asignar)."""
        n = self.conn.execute(
            "UPDATE tarjetas_fidelidad SET estado='libre' "
            "WHERE batch_id = ? AND estado IN ('generada','impresa')",
            (batch_id,)
        ).rowcount
        self.conn.commit()
        logger.info("liberar_lote batch=%d n=%d", batch_id, n)
        return n

    def cerrar_lote(self, batch_id: int) -> None:
        self.conn.execute(
            "UPDATE card_batches SET estado='cerrado', fecha_cierre=datetime('now') "
            "WHERE id = ?", (batch_id,)
        )
        self.conn.commit()

    # ── Tarjetas ──────────────────────────────────────────────────────────────

    def _load_tarjeta(self, tarjeta_id: int) -> Tarjeta:
        row = self.conn.execute(
            """
            SELECT id, numero, batch_id, estado, id_cliente,
                   puntos_actuales, COALESCE(nivel,'Bronce'), codigo_qr, fecha_creacion
            FROM tarjetas_fidelidad WHERE id = ?
            """,
            (tarjeta_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"Tarjeta {tarjeta_id} no encontrada")
        return Tarjeta(*row)

    def buscar_tarjeta(self, numero_o_qr: str) -> Optional[Tarjeta]:
        """Busca por número exacto o código QR."""
        row = self.conn.execute(
            """
            SELECT id, numero, batch_id, estado, id_cliente,
                   puntos_actuales, COALESCE(nivel,'Bronce'), codigo_qr, fecha_creacion
            FROM tarjetas_fidelidad
            WHERE numero = ? OR codigo_qr = ?
            LIMIT 1
            """,
            (numero_o_qr, numero_o_qr)
        ).fetchone()
        return Tarjeta(*row) if row else None

    def tarjetas_libres(self, batch_id: int = None, limit: int = 50) -> List[Tarjeta]:
        q = """
            SELECT id, numero, batch_id, estado, id_cliente,
                   puntos_actuales, COALESCE(nivel,'Bronce'), codigo_qr, fecha_creacion
            FROM tarjetas_fidelidad WHERE estado = 'libre'
        """
        params: list = []
        if batch_id is not None:
            q += " AND batch_id = ?"
            params.append(batch_id)
        q += f" ORDER BY id LIMIT {limit}"
        return [Tarjeta(*r) for r in self.conn.execute(q, params).fetchall()]

    # ── Asignación ────────────────────────────────────────────────────────────

    def asignar_tarjeta(
        self,
        tarjeta_id: int,
        cliente_id: int,
        motivo:     str = "asignacion_normal",
    ) -> AsignacionResult:
        """Asigna tarjeta libre a cliente. Registra en historial."""
        try:
            tarjeta = self._load_tarjeta(tarjeta_id)
            if tarjeta.estado == "asignada":
                return AsignacionResult(
                    tarjeta_id, cliente_id, "asignacion",
                    False, f"Tarjeta ya asignada al cliente {tarjeta.id_cliente}"
                )
            if tarjeta.estado == "bloqueada":
                return AsignacionResult(
                    tarjeta_id, cliente_id, "asignacion",
                    False, "Tarjeta bloqueada, no se puede asignar"
                )

            accion = "reasignacion" if tarjeta.id_cliente else "asignacion"
            cliente_prev = tarjeta.id_cliente

            self.conn.execute(
                """
                UPDATE tarjetas_fidelidad
                SET id_cliente = ?, estado = 'asignada',
                    fecha_asignacion = datetime('now')
                WHERE id = ?
                """,
                (cliente_id, tarjeta_id)
            )
            self._log_asignacion(tarjeta_id, cliente_prev, cliente_id, accion, motivo)
            # Actualizar contadores del lote
            if tarjeta.batch_id:
                self._sync_contadores_lote(tarjeta.batch_id)
            self.conn.commit()

            logger.info("asignar_tarjeta id=%d cliente=%d", tarjeta_id, cliente_id)
            return AsignacionResult(tarjeta_id, cliente_id, accion, True, "Tarjeta asignada correctamente")

        except Exception as exc:
            logger.error("asignar_tarjeta: %s", exc)
            return AsignacionResult(tarjeta_id, cliente_id, "asignacion", False, str(exc))

    def liberar_tarjeta(self, tarjeta_id: int, motivo: str = "") -> AsignacionResult:
        """Desvincula tarjeta de cliente → estado libre."""
        try:
            tarjeta = self._load_tarjeta(tarjeta_id)
            cliente_prev = tarjeta.id_cliente
            self.conn.execute(
                "UPDATE tarjetas_fidelidad SET id_cliente=NULL, estado='libre' WHERE id=?",
                (tarjeta_id,)
            )
            self._log_asignacion(tarjeta_id, cliente_prev, None, "liberacion", motivo)
            if tarjeta.batch_id:
                self._sync_contadores_lote(tarjeta.batch_id)
            self.conn.commit()
            return AsignacionResult(tarjeta_id, 0, "liberacion", True, "Tarjeta liberada")
        except Exception as exc:
            return AsignacionResult(tarjeta_id, 0, "liberacion", False, str(exc))

    def bloquear_tarjeta(
        self, tarjeta_id: int, motivo: str, bloqueado_por: str = None
    ) -> AsignacionResult:
        bp = bloqueado_por or self.usuario
        try:
            tarjeta = self._load_tarjeta(tarjeta_id)
            self.conn.execute(
                "UPDATE tarjetas_fidelidad SET estado='bloqueada', activa=0, "
                "bloqueado_por=?, motivo_bloqueo=? WHERE id=?",
                (bp, motivo, tarjeta_id)
            )
            self._log_asignacion(tarjeta_id, tarjeta.id_cliente, tarjeta.id_cliente,
                                  "bloqueo", motivo)
            self.conn.commit()
            return AsignacionResult(tarjeta_id, 0, "bloqueo", True, "Tarjeta bloqueada")
        except Exception as exc:
            return AsignacionResult(tarjeta_id, 0, "bloqueo", False, str(exc))

    def desbloquear_tarjeta(self, tarjeta_id: int, motivo: str = "") -> AsignacionResult:
        try:
            tarjeta = self._load_tarjeta(tarjeta_id)
            nuevo_estado = "asignada" if tarjeta.id_cliente else "libre"
            self.conn.execute(
                "UPDATE tarjetas_fidelidad SET estado=?, activa=1, "
                "bloqueado_por=NULL, motivo_bloqueo=NULL WHERE id=?",
                (nuevo_estado, tarjeta_id)
            )
            self._log_asignacion(tarjeta_id, tarjeta.id_cliente, tarjeta.id_cliente,
                                  "desbloqueo", motivo)
            self.conn.commit()
            return AsignacionResult(tarjeta_id, 0, "desbloqueo", True, "Tarjeta desbloqueada")
        except Exception as exc:
            return AsignacionResult(tarjeta_id, 0, "desbloqueo", False, str(exc))

    # ── Historial ─────────────────────────────────────────────────────────────

    def historial_tarjeta(self, tarjeta_id: int) -> List[dict]:
        rows = self.conn.execute(
            """
            SELECT h.accion, h.motivo, h.usuario, h.fecha,
                   c_prev.nombre AS cliente_prev,
                   c_new.nombre  AS cliente_nuevo
            FROM card_assignment_history h
            LEFT JOIN clientes c_prev ON c_prev.id = h.cliente_id_prev
            LEFT JOIN clientes c_new  ON c_new.id  = h.cliente_id_nuevo
            WHERE h.tarjeta_id = ?
            ORDER BY h.fecha DESC
            """,
            (tarjeta_id,)
        ).fetchall()
        return [
            {"accion": r[0], "motivo": r[1], "usuario": r[2], "fecha": r[3],
             "cliente_prev": r[4], "cliente_nuevo": r[5]}
            for r in rows
        ]

    def historial_cliente(self, cliente_id: int) -> List[dict]:
        rows = self.conn.execute(
            """
            SELECT h.accion, h.motivo, h.usuario, h.fecha,
                   tf.numero AS numero_tarjeta
            FROM card_assignment_history h
            JOIN tarjetas_fidelidad tf ON tf.id = h.tarjeta_id
            WHERE h.cliente_id_nuevo = ? OR h.cliente_id_prev = ?
            ORDER BY h.fecha DESC
            """,
            (cliente_id, cliente_id)
        ).fetchall()
        return [
            {"accion": r[0], "motivo": r[1], "usuario": r[2], "fecha": r[3],
             "numero_tarjeta": r[4]}
            for r in rows
        ]

    # ── QR Image ─────────────────────────────────────────────────────────────

    def generar_qr_imagen(self, numero: str, size_px: int = 200) -> Optional[bytes]:
        """Genera imagen PNG del QR en bytes. None si qrcode no disponible."""
        if not HAS_QR:
            return None
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=10,
            border=2,
        )
        qr.add_data(numero)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    # ── Exportar PDF lote ────────────────────────────────────────────────────

    def exportar_pdf_lote(
        self,
        batch_id:  int,
        ruta_pdf:  str,
        cols:      int = 4,
        rows_page: int = 8,
    ) -> int:
        """
        Genera PDF con todas las tarjetas del lote para imprenta.
        Cada tarjeta muestra: número, QR, nivel Bronce.
        Retorna cantidad de tarjetas incluidas.
        """
        if not HAS_REPORTLAB:
            raise RuntimeError("reportlab no instalado — no se puede generar PDF de lote")

        tarjetas = self.conn.execute(
            """
            SELECT numero, codigo_qr, COALESCE(nivel,'Bronce')
            FROM tarjetas_fidelidad
            WHERE batch_id = ?
            ORDER BY id
            """,
            (batch_id,)
        ).fetchall()

        if not tarjetas:
            raise ValueError(f"Batch {batch_id} sin tarjetas")

        batch = self._load_batch(batch_id)

        c    = rl_canvas.Canvas(ruta_pdf, pagesize=A4)
        W, H = A4
        mg   = 10 * mm
        cell_w = (W - 2 * mg) / cols
        cell_h = (H - 2 * mg) / rows_page

        def dibujar_tarjeta(x, y, numero, qr_code, nivel):
            # Marco
            c.setStrokeColorRGB(0.2, 0.2, 0.2)
            c.setLineWidth(0.5)
            c.rect(x + 1*mm, y + 1*mm, cell_w - 2*mm, cell_h - 2*mm)
            # Número
            c.setFont("Helvetica-Bold", 7)
            c.setFillColorRGB(0, 0, 0)
            c.drawString(x + 3*mm, y + cell_h - 8*mm, numero)
            # Nivel
            c.setFont("Helvetica", 6)
            c.drawString(x + 3*mm, y + cell_h - 13*mm, f"Nivel: {nivel}")
            # QR si disponible
            if HAS_QR and qr_code:
                try:
                    qr_img_data = self.generar_qr_imagen(numero, size_px=80)
                    if qr_img_data:
                        ir = ImageReader(io.BytesIO(qr_img_data))
                        qr_size = min(cell_w - 8*mm, cell_h - 20*mm)
                        c.drawImage(ir, x + (cell_w - qr_size)/2,
                                    y + 2*mm, qr_size, qr_size)
                except Exception:
                    c.setFont("Helvetica", 5)
                    c.drawString(x + 3*mm, y + cell_h/2, qr_code[:16])

        idx = 0
        col = 0
        row = 0

        for numero, qr_code, nivel in tarjetas:
            x = mg + col * cell_w
            y = H - mg - (row + 1) * cell_h
            dibujar_tarjeta(x, y, numero, qr_code, nivel)
            col += 1
            if col >= cols:
                col = 0
                row += 1
            if row >= rows_page:
                c.showPage()
                col = 0
                row = 0
            idx += 1

        if idx > 0:
            c.save()

        logger.info("exportar_pdf_lote batch=%d n=%d ruta=%s", batch_id, idx, ruta_pdf)
        return idx

    # ── Internos ──────────────────────────────────────────────────────────────

    def _log_asignacion(
        self,
        tarjeta_id:     int,
        cliente_prev:   Optional[int],
        cliente_nuevo:  Optional[int],
        accion:         str,
        motivo:         str,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO card_assignment_history
                (tarjeta_id, cliente_id_prev, cliente_id_nuevo,
                 accion, motivo, usuario)
            VALUES (?,?,?,?,?,?)
            """,
            (tarjeta_id, cliente_prev, cliente_nuevo,
             accion, motivo, self.usuario)
        )

    def _sync_contadores_lote(self, batch_id: int) -> None:
        row = self.conn.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE estado = 'libre'),
                COUNT(*) FILTER (WHERE estado = 'asignada')
            FROM tarjetas_fidelidad WHERE batch_id = ?
            """,
            (batch_id,)
        ).fetchone()
        if row:
            self.conn.execute(
                "UPDATE card_batches SET cantidad_libres=?, cantidad_asignadas=? WHERE id=?",
                (row[0], row[1], batch_id)
            )

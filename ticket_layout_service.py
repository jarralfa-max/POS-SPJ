# core/services/ticket_layout_service.py — SPJ Enterprise v9.1
# Fix #11: Layouts de tickets persistentes con versionado.
from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime
from typing import List, Optional

from core.domain.models import TicketLayout

logger = logging.getLogger("spj.ticket_layout")


class TicketLayoutService:
    """
    Fix #11 — Persistencia y versionado de diseños de ticket/etiqueta.

    Cada cambio de layout genera una nueva versión (no sobreescribe).
    Solo un layout puede estar activo por tipo a la vez.
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn
        self._ensure_version_col()

    def _ensure_version_col(self) -> None:
        """Agrega columna version si tabla existía sin ella."""
        try:
            self.conn.execute(
                "ALTER TABLE ticket_design_config ADD COLUMN version INTEGER DEFAULT 1"
            )
        except sqlite3.OperationalError:
            pass
        try:
            self.conn.execute(
                "ALTER TABLE ticket_design_config ADD COLUMN modificado_en DATETIME"
            )
        except sqlite3.OperationalError:
            pass

    # ── Lecturas ──────────────────────────────────────────────────────────────

    def get_activo(self, tipo: str = "ticket") -> Optional[TicketLayout]:
        row = self.conn.execute(
            "SELECT id, tipo, nombre, COALESCE(version,1), activo, "
            "COALESCE(ancho_mm,80), COALESCE(alto_mm,0), elementos, "
            "creado_en, modificado_en "
            "FROM ticket_design_config WHERE tipo=? AND activo=1 LIMIT 1",
            (tipo,),
        ).fetchone()
        if not row:
            return None
        return self._row_to_layout(row)

    def listar(self, tipo: str = "ticket") -> List[TicketLayout]:
        rows = self.conn.execute(
            "SELECT id, tipo, nombre, COALESCE(version,1), activo, "
            "COALESCE(ancho_mm,80), COALESCE(alto_mm,0), elementos, "
            "creado_en, modificado_en "
            "FROM ticket_design_config WHERE tipo=? ORDER BY version DESC",
            (tipo,),
        ).fetchall()
        return [self._row_to_layout(r) for r in rows]

    def get_version(self, layout_id: int) -> Optional[TicketLayout]:
        row = self.conn.execute(
            "SELECT id, tipo, nombre, COALESCE(version,1), activo, "
            "COALESCE(ancho_mm,80), COALESCE(alto_mm,0), elementos, "
            "creado_en, modificado_en "
            "FROM ticket_design_config WHERE id=?",
            (layout_id,),
        ).fetchone()
        return self._row_to_layout(row) if row else None

    # ── Escrituras ────────────────────────────────────────────────────────────

    def guardar_nueva_version(self, layout: TicketLayout) -> int:
        """
        Siempre inserta una nueva fila (no UPDATE).
        Incrementa versión desde la última existente.
        """
        ultima = self.conn.execute(
            "SELECT COALESCE(MAX(version), 0) FROM ticket_design_config WHERE tipo=? AND nombre=?",
            (layout.tipo, layout.nombre),
        ).fetchone()
        nueva_version = int(ultima[0]) + 1

        self.conn.execute(
            """
            INSERT INTO ticket_design_config
                (tipo, nombre, elementos, activo, ancho_mm, alto_mm,
                 version, creado_en, modificado_en)
            VALUES (?,?,?,0,?,?,?,datetime('now'),datetime('now'))
            """,
            (
                layout.tipo, layout.nombre,
                json.dumps(layout.elementos, ensure_ascii=False),
                layout.ancho_mm, layout.alto_mm,
                nueva_version,
            ),
        )
        new_id = self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        self.conn.commit()
        logger.info("Layout '%s/%s' v%d guardado (id=%d)", layout.tipo, layout.nombre, nueva_version, new_id)
        return new_id

    def activar(self, layout_id: int) -> None:
        """Desactiva todos los layouts del mismo tipo y activa este."""
        row = self.conn.execute(
            "SELECT tipo FROM ticket_design_config WHERE id=?", (layout_id,)
        ).fetchone()
        if not row:
            raise ValueError(f"Layout id={layout_id} no encontrado")
        tipo = row[0]
        with self.conn:
            self.conn.execute(
                "UPDATE ticket_design_config SET activo=0 WHERE tipo=?", (tipo,)
            )
            self.conn.execute(
                "UPDATE ticket_design_config SET activo=1 WHERE id=?", (layout_id,)
            )
        logger.info("Layout id=%d activado (tipo=%s)", layout_id, tipo)

    def rollback_a_version(self, layout_id: int) -> None:
        """Activa una versión anterior sin borrar las más nuevas."""
        self.activar(layout_id)
        logger.info("Rollback a layout id=%d", layout_id)

    # ── Helper ────────────────────────────────────────────────────────────────

    def _row_to_layout(self, row) -> TicketLayout:
        elementos = []
        try:
            elementos = json.loads(row[7] or "[]")
        except Exception:
            pass
        return TicketLayout(
            id=row[0], tipo=row[1], nombre=row[2],
            version=int(row[3]), activo=bool(row[4]),
            ancho_mm=int(row[5]), alto_mm=int(row[6]),
            elementos=elementos,
            creado_en=datetime.fromisoformat(str(row[8])) if row[8] else datetime.now(),
            modificado_en=datetime.fromisoformat(str(row[9])) if row[9] else None,
        )

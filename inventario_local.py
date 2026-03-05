# modulos/inventario_local.py
# ── Módulo Inventario Local SPJ v9 ────────────────────────────────────────
# Dos pestañas:
#   1. Inventario Local  — stock actual de la sucursal (products + qty)
#   2. Recepción de Transferencias — aceptar traspasos pendientes de otra sucursal
#
# Diseñado para el cajero / responsable de sucursal.
# SIN lógica de compras globales (eso es ModuloInventarioEnterprise).
from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from datetime import datetime
from typing import Optional

from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QDate
from PyQt5.QtGui import QColor, QFont
from PyQt5.QtWidgets import (
    QDialog, QDialogButtonBox, QFormLayout, QFrame,
    QGroupBox, QHBoxLayout, QHeaderView, QLabel,
    QLineEdit, QMessageBox, QPushButton, QSpinBox,
    QDoubleSpinBox, QSplitter, QStackedWidget, QTabWidget,
    QTableWidget, QTableWidgetItem, QTextEdit, QVBoxLayout,
    QWidget, QComboBox, QAbstractItemView, QSizePolicy,
)

from .base import ModuloBase

logger = logging.getLogger("spj.inventario_local")


# ── helpers ───────────────────────────────────────────────────────────────────
def _item(text: str, align=Qt.AlignLeft | Qt.AlignVCenter) -> QTableWidgetItem:
    it = QTableWidgetItem(str(text))
    it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
    it.setTextAlignment(align)
    return it


def _item_num(value: float, decimals: int = 3) -> QTableWidgetItem:
    fmt = f"{value:,.{decimals}f}"
    it = QTableWidgetItem(fmt)
    it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
    it.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
    return it


# =============================================================================
# TAB 1 — INVENTARIO LOCAL (stock sucursal)
# =============================================================================
class TabInventarioLocal(QWidget):
    """Muestra el stock disponible en la sucursal activa."""

    def __init__(self, parent: "ModuloInventarioLocal") -> None:
        super().__init__(parent)
        self._parent = parent
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        # ── Encabezado + controles ────────────────────────────────────────────
        top = QHBoxLayout()
        self._lbl_titulo = QLabel("📦 Stock en sucursal")
        self._lbl_titulo.setProperty("class", "sectionTitle")
        top.addWidget(self._lbl_titulo)
        top.addStretch()

        self._lbl_sucursal = QLabel("")
        self._lbl_sucursal.setProperty("class", "sucursalBadge")
        top.addWidget(self._lbl_sucursal)

        btn_ref = QPushButton("🔄 Actualizar")
        btn_ref.setFixedHeight(32)
        btn_ref.setProperty("class", "secondaryBtn")
        btn_ref.clicked.connect(self.refrescar)
        top.addWidget(btn_ref)
        layout.addLayout(top)

        # ── Filtro por categoría ──────────────────────────────────────────────
        fil = QHBoxLayout()
        fil.addWidget(QLabel("Categoría:"))
        self._combo_cat = QComboBox()
        self._combo_cat.setMinimumWidth(160)
        self._combo_cat.currentIndexChanged.connect(self.refrescar)
        fil.addWidget(self._combo_cat)
        self._txt_buscar = QLineEdit()
        self._txt_buscar.setPlaceholderText("Buscar producto...")
        self._txt_buscar.setMaximumWidth(220)
        self._txt_buscar.textChanged.connect(self.refrescar)
        fil.addWidget(self._txt_buscar)
        fil.addStretch()
        self._lbl_resumen = QLabel("")
        self._lbl_resumen.setProperty("class", "summaryLabel")
        fil.addWidget(self._lbl_resumen)
        layout.addLayout(fil)

        # ── Tabla ─────────────────────────────────────────────────────────────
        self._tabla = QTableWidget()
        self._tabla.setColumnCount(6)
        self._tabla.setHorizontalHeaderLabels([
            "ID", "Producto", "Categoría", "Disponible", "Unidad", "Stock Mín."
        ])
        self._tabla.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tabla.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tabla.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self._tabla.verticalHeader().setVisible(False)
        self._tabla.setAlternatingRowColors(True)
        self._tabla.setProperty("class", "inventoryTable")
        layout.addWidget(self._tabla)

        # ── Leyenda ───────────────────────────────────────────────────────────
        ley = QHBoxLayout()
        for color, texto in [
            ("#e74c3c", "⬛ Por debajo del mínimo"),
            ("#f39c12", "⬛ Stock bajo (< 2× mínimo)"),
            ("#27ae60", "⬛ Stock OK"),
        ]:
            lbl = QLabel(texto)
            lbl.setStyleSheet(f"color: {color}; font-size: 11px;")
            ley.addWidget(lbl)
        ley.addStretch()
        layout.addLayout(ley)

    def refrescar(self) -> None:
        """Recarga datos de la BD según sucursal activa."""
        suc_id = self._parent.sucursal_id
        self._lbl_sucursal.setText(f"🏪 {self._parent.sucursal_nombre}")

        # Cargar categorías (una sola vez)
        if self._combo_cat.count() == 0:
            try:
                cats = self._parent.conexion.execute(
                    "SELECT DISTINCT COALESCE(categoria,'Sin categoría') FROM productos "
                    "WHERE activo=1 ORDER BY 1"
                ).fetchall()
                self._combo_cat.blockSignals(True)
                self._combo_cat.addItem("Todas", None)
                for (c,) in cats:
                    self._combo_cat.addItem(c, c)
                self._combo_cat.blockSignals(False)
            except Exception:
                pass

        cat_filtro = self._combo_cat.currentData()
        busqueda   = self._txt_buscar.text().strip().lower()

        q = """
            SELECT p.id,
                   p.nombre,
                   COALESCE(p.categoria, 'Sin categoría'),
                   COALESCE(p.existencia, 0),
                   COALESCE(p.unidad, 'pza'),
                   COALESCE(p.stock_minimo, 0)
            FROM productos p
            WHERE p.activo = 1
              AND p.oculto = 0
        """
        params = []
        if cat_filtro:
            q += " AND p.categoria = ?"; params.append(cat_filtro)
        if busqueda:
            q += " AND lower(p.nombre) LIKE ?"; params.append(f"%{busqueda}%")
        q += " ORDER BY p.nombre"

        try:
            rows = self._parent.conexion.execute(q, params).fetchall()
        except Exception as exc:
            logger.warning("TabInventarioLocal.refrescar: %s", exc)
            rows = []

        self._tabla.setRowCount(len(rows))
        total_items = 0
        bajo_minimo = 0

        for i, row in enumerate(rows):
            pid, nombre, cat, qty, unidad, stock_min = row
            qty      = float(qty)
            stk_min  = float(stock_min)

            self._tabla.setItem(i, 0, _item(str(pid)))
            self._tabla.setItem(i, 1, _item(nombre))
            self._tabla.setItem(i, 2, _item(cat))
            it_qty = _item_num(qty)
            self._tabla.setItem(i, 3, it_qty)
            self._tabla.setItem(i, 4, _item(unidad))
            self._tabla.setItem(i, 5, _item_num(stk_min))

            # Color según stock
            if stk_min > 0:
                if qty <= stk_min:
                    color = QColor("#e74c3c")
                    bajo_minimo += 1
                elif qty <= stk_min * 2:
                    color = QColor("#f39c12")
                else:
                    color = QColor("#27ae60")
                for col in range(6):
                    cell = self._tabla.item(i, col)
                    if cell:
                        cell.setForeground(color)

            total_items += 1

        self._lbl_resumen.setText(
            f"<b>{total_items}</b> productos  |  "
            f"<b style='color:#e74c3c'>{bajo_minimo}</b> bajo mínimo"
        )


# =============================================================================
# TAB 2 — RECEPCIÓN DE TRANSFERENCIAS
# =============================================================================
class TabRecepcionTransferencias(QWidget):
    """
    Lista traspasos_inventario con estado='pendiente' destinados a esta sucursal.
    Permite al encargado confirmar recepción, registrando la entrada al stock.
    """

    transferencia_recibida = pyqtSignal()

    def __init__(self, parent: "ModuloInventarioLocal") -> None:
        super().__init__(parent)
        self._parent = parent
        self._build_ui()

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(8)

        # ── Header ────────────────────────────────────────────────────────────
        top = QHBoxLayout()
        lbl = QLabel("📥 Transferencias Pendientes")
        lbl.setProperty("class", "sectionTitle")
        top.addWidget(lbl)
        top.addStretch()
        self._lbl_suc = QLabel("")
        self._lbl_suc.setProperty("class", "sucursalBadge")
        top.addWidget(self._lbl_suc)
        btn_ref = QPushButton("🔄 Actualizar")
        btn_ref.setFixedHeight(32)
        btn_ref.setProperty("class", "secondaryBtn")
        btn_ref.clicked.connect(self.refrescar)
        top.addWidget(btn_ref)
        layout.addLayout(top)

        # ── Info ──────────────────────────────────────────────────────────────
        info = QLabel("Seleccione una transferencia y confirme para ingresar el stock al inventario local.")
        info.setWordWrap(True)
        info.setProperty("class", "infoLabel")
        layout.addWidget(info)

        # ── Tabla ─────────────────────────────────────────────────────────────
        self._tabla = QTableWidget()
        self._tabla.setColumnCount(7)
        self._tabla.setHorizontalHeaderLabels([
            "ID", "Origen", "Producto", "Cantidad", "Unidad", "Fecha Solicitud", "Estado"
        ])
        self._tabla.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tabla.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tabla.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self._tabla.verticalHeader().setVisible(False)
        self._tabla.setAlternatingRowColors(True)
        self._tabla.itemSelectionChanged.connect(self._on_selection)
        layout.addWidget(self._tabla)

        # ── Panel detalle ─────────────────────────────────────────────────────
        self._grp_detalle = QGroupBox("Detalle de la transferencia seleccionada")
        det_lay = QFormLayout(self._grp_detalle)

        self._lbl_det_id       = QLabel("—")
        self._lbl_det_origen   = QLabel("—")
        self._lbl_det_producto = QLabel("—")
        self._lbl_det_qty      = QLabel("—")
        self._lbl_det_obs      = QLabel("—")
        self._txt_obs_recep    = QLineEdit()
        self._txt_obs_recep.setPlaceholderText("Observaciones de recepción (opcional)")

        det_lay.addRow("Folio:", self._lbl_det_id)
        det_lay.addRow("Sucursal origen:", self._lbl_det_origen)
        det_lay.addRow("Producto:", self._lbl_det_producto)
        det_lay.addRow("Cantidad:", self._lbl_det_qty)
        det_lay.addRow("Observaciones envío:", self._lbl_det_obs)
        det_lay.addRow("Observaciones recepción:", self._txt_obs_recep)

        self._btn_confirmar = QPushButton("✅ Confirmar Recepción")
        self._btn_confirmar.setMinimumHeight(38)
        self._btn_confirmar.setEnabled(False)
        self._btn_confirmar.setProperty("class", "primaryBtn")
        self._btn_confirmar.clicked.connect(self._confirmar_recepcion)

        det_lay_v = QVBoxLayout()
        det_lay_v.addWidget(self._grp_detalle)
        det_lay_v.addWidget(self._btn_confirmar)
        layout.addLayout(det_lay_v)

        self._traspaso_id_actual: Optional[int] = None

    def refrescar(self) -> None:
        suc_id = self._parent.sucursal_id
        self._lbl_suc.setText(f"🏪 {self._parent.sucursal_nombre}")

        try:
            rows = self._parent.conexion.execute(
                """
                SELECT t.id,
                       COALESCE(s.nombre, 'Desconocida'),
                       p.nombre,
                       t.cantidad,
                       COALESCE(p.unidad,'pza'),
                       t.fecha_solicitud,
                       t.estado
                FROM traspasos_inventario t
                JOIN productos p   ON p.id = t.producto_id
                LEFT JOIN sucursales s ON s.id = t.sucursal_origen_id
                WHERE t.sucursal_destino_id = ?
                  AND t.estado IN ('pendiente','enviado')
                ORDER BY t.fecha_solicitud DESC
                """,
                (suc_id,)
            ).fetchall()
        except Exception as exc:
            logger.warning("TabRecepcionTransferencias.refrescar: %s", exc)
            rows = []

        self._tabla.setRowCount(len(rows))
        for i, (tid, origen, prod, qty, unidad, fecha, estado) in enumerate(rows):
            self._tabla.setItem(i, 0, _item(str(tid)))
            self._tabla.setItem(i, 1, _item(origen))
            self._tabla.setItem(i, 2, _item(prod))
            self._tabla.setItem(i, 3, _item_num(float(qty)))
            self._tabla.setItem(i, 4, _item(unidad))
            self._tabla.setItem(i, 5, _item(str(fecha)[:16] if fecha else ""))
            color_estado = QColor("#f39c12") if estado == "pendiente" else QColor("#27ae60")
            it_estado = _item(estado.capitalize())
            it_estado.setForeground(color_estado)
            self._tabla.setItem(i, 6, it_estado)

        self._btn_confirmar.setEnabled(False)
        self._traspaso_id_actual = None

    def _on_selection(self) -> None:
        rows = self._tabla.selectedItems()
        if not rows:
            self._btn_confirmar.setEnabled(False)
            self._traspaso_id_actual = None
            return
        row = self._tabla.currentRow()
        tid_item = self._tabla.item(row, 0)
        if not tid_item:
            return
        tid = int(tid_item.text())
        self._traspaso_id_actual = tid

        try:
            t = self._parent.conexion.execute(
                """
                SELECT t.id,
                       COALESCE(s.nombre,'?'),
                       p.nombre,
                       t.cantidad,
                       COALESCE(p.unidad,'pza'),
                       COALESCE(t.observaciones,'—')
                FROM traspasos_inventario t
                JOIN productos p ON p.id = t.producto_id
                LEFT JOIN sucursales s ON s.id = t.sucursal_origen_id
                WHERE t.id = ?
                """,
                (tid,)
            ).fetchone()
            if t:
                self._lbl_det_id.setText(f"#{t[0]}")
                self._lbl_det_origen.setText(t[1])
                self._lbl_det_producto.setText(t[2])
                self._lbl_det_qty.setText(f"{float(t[3]):,.3f} {t[4]}")
                self._lbl_det_obs.setText(t[5])
                self._btn_confirmar.setEnabled(True)
        except Exception as exc:
            logger.warning("_on_selection traspaso: %s", exc)

    def _confirmar_recepcion(self) -> None:
        tid = self._traspaso_id_actual
        if not tid:
            return

        obs = self._txt_obs_recep.text().strip()
        usuario = self._parent.usuario_actual or "Sistema"
        suc_id  = self._parent.sucursal_id

        try:
            t = self._parent.conexion.execute(
                "SELECT producto_id, cantidad, estado FROM traspasos_inventario WHERE id=?",
                (tid,)
            ).fetchone()
            if not t:
                QMessageBox.warning(self, "Error", "Traspaso no encontrado.")
                return
            prod_id, qty, estado = t[0], float(t[1]), t[2]

            if estado not in ("pendiente", "enviado"):
                QMessageBox.information(self, "Aviso", f"Este traspaso ya está en estado '{estado}'.")
                return

            confirm = QMessageBox.question(
                self, "Confirmar recepción",
                f"¿Confirmar recepción de {qty:,.3f} unidades?\n"
                "Esto sumará la cantidad al inventario local.",
                QMessageBox.Yes | QMessageBox.No,
            )
            if confirm != QMessageBox.Yes:
                return

            with self._parent.conexion:
                # Actualizar existencia en productos
                self._parent.conexion.execute(
                    "UPDATE productos SET existencia = COALESCE(existencia,0) + ? WHERE id=?",
                    (qty, prod_id)
                )
                old = self._parent.conexion.execute(
                    "SELECT existencia FROM productos WHERE id=?", (prod_id,)
                ).fetchone()
                exist_nueva = float(old[0]) if old else qty

                # Movimiento inventario
                self._parent.conexion.execute(
                    """
                    INSERT INTO movimientos_inventario (
                        producto_id, tipo, tipo_movimiento, cantidad,
                        existencia_anterior, existencia_nueva,
                        descripcion, usuario, sucursal_id, fecha, uuid
                    ) VALUES (?,?,?,?,?,?,?,?,?,datetime('now'),?)
                    """,
                    (
                        prod_id, "ENTRADA", "recepcion_transferencia", qty,
                        exist_nueva - qty, exist_nueva,
                        f"Recepción traspaso #{tid}" + (f" — {obs}" if obs else ""),
                        usuario, suc_id, str(uuid.uuid4()),
                    )
                )
                # Actualizar estado del traspaso
                self._parent.conexion.execute(
                    """
                    UPDATE traspasos_inventario
                    SET estado='recibido', fecha_recepcion=datetime('now'),
                        usuario_destino=?, observaciones=?
                    WHERE id=?
                    """,
                    (usuario, obs or None, tid)
                )

            QMessageBox.information(
                self, "✅ Recepción confirmada",
                f"Transferencia #{tid} recibida correctamente.\n"
                f"Stock actualizado en {self._parent.sucursal_nombre}."
            )
            self._txt_obs_recep.clear()
            self.refrescar()
            self.transferencia_recibida.emit()

        except Exception as exc:
            logger.error("_confirmar_recepcion: %s", exc)
            QMessageBox.critical(self, "Error", f"No se pudo confirmar la recepción:\n{exc}")


# =============================================================================
# MÓDULO PRINCIPAL — INVENTARIO LOCAL
# =============================================================================
class ModuloInventarioLocal(ModuloBase):
    """
    Módulo de menú lateral: INVENTARIO
    Contiene dos pestañas:
      - Inventario Local: vista de stock actual de la sucursal
      - Recepción de Transferencias: aceptar traspasos pendientes
    """

    def __init__(self, conexion: sqlite3.Connection, parent=None) -> None:
        super().__init__(conexion, parent)
        self.sucursal_id     = 1
        self.sucursal_nombre = "Principal"
        self.usuario_actual  = None
        self.rol_usuario     = None
        self._build_ui()

    def set_sucursal(self, sucursal_id: int, sucursal_nombre: str) -> None:
        self.sucursal_id     = sucursal_id
        self.sucursal_nombre = sucursal_nombre
        if hasattr(self, "_tab_local"):
            self._tab_local.refrescar()
        if hasattr(self, "_tab_transferencias"):
            self._tab_transferencias.refrescar()

    def set_usuario_actual(self, usuario: str, rol: str = None) -> None:
        self.usuario_actual = usuario
        self.rol_usuario    = rol

    def set_sesion(self, usuario: str, rol: str = None) -> None:
        self.set_usuario_actual(usuario, rol)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # ── Header ────────────────────────────────────────────────────────────
        header = QFrame()
        header.setProperty("class", "moduleHeader")
        header.setFixedHeight(52)
        h_lay = QHBoxLayout(header)
        h_lay.setContentsMargins(16, 0, 16, 0)
        title = QLabel("📦 Inventario Local")
        title.setProperty("class", "moduleTitle")
        h_lay.addWidget(title)
        h_lay.addStretch()
        layout.addWidget(header)

        # ── Tabs ──────────────────────────────────────────────────────────────
        self._tabs = QTabWidget()
        self._tabs.setProperty("class", "inventarioTabs")
        self._tabs.currentChanged.connect(self._on_tab_changed)

        self._tab_local = TabInventarioLocal(self)
        self._tabs.addTab(self._tab_local, "📦 Inventario Local")

        self._tab_transferencias = TabRecepcionTransferencias(self)
        self._tab_transferencias.transferencia_recibida.connect(self._tab_local.refrescar)
        self._tabs.addTab(self._tab_transferencias, "📥 Recepción de Transferencias")

        layout.addWidget(self._tabs)

    def _on_tab_changed(self, idx: int) -> None:
        if idx == 0:
            self._tab_local.refrescar()
        elif idx == 1:
            self._tab_transferencias.refrescar()

    def refrescar(self) -> None:
        self._on_tab_changed(self._tabs.currentIndex())

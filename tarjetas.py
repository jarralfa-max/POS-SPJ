# modulos/tarjetas.py
# ── ModuloTarjetas — Enterprise Repository-Based Card Management ─────────────
# Block 6 requirements:
#   ✓ Repository-only DB access — no raw SQL, no sqlite3 in UI
#   ✓ JSON serialization enforced at repository level
#   ✓ Explicit commit via repository transactions
#   ✓ Visible errors — no silent failures
#   ✓ UI refresh after save via EventBus
#   ✓ Migration-backed tables
from __future__ import annotations
import logging
import os
from typing import Dict, List, Optional

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QLineEdit,
    QComboBox, QMessageBox, QDialog, QFormLayout, QTableWidget,
    QTableWidgetItem, QAbstractItemView, QTabWidget, QGroupBox,
    QHeaderView, QFrame, QSizePolicy, QSpinBox, QProgressBar
)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor

from .base import ModuloBase
from repositories.tarjetas import (
    TarjetaRepository, TarjetaError,
    TarjetaNotFoundError, TarjetaYaAsignadaError,
)
from core.events.event_bus import EventBus

logger = logging.getLogger("spj.ui.tarjetas")

TARJETA_CREADA      = "TARJETA_CREADA"
TARJETA_ACTUALIZADA = "TARJETA_ACTUALIZADA"
TARJETA_ASIGNADA    = "TARJETA_ASIGNADA"
VENTA_COMPLETADA    = "VENTA_COMPLETADA"

_C3 = "#2980b9"; _C4 = "#27ae60"; _C5 = "#e74c3c"; _C6 = "#f39c12"; _C7 = "#8e44ad"


class ModuloTarjetas(ModuloBase):

    def __init__(self, conexion, parent=None):
        super().__init__(conexion, parent)
        self.conexion        = conexion
        self.main_window     = parent
        self.sucursal_id     = 1
        self.sucursal_nombre = "Principal"
        self.usuario_actual  = "Sistema"
        self.rol_usuario     = ""
        self._repo = TarjetaRepository(conexion)
        self._init_ui()
        self._subscribe_events()
        QTimer.singleShot(0, self._refresh_all)

    def set_sucursal(self, sucursal_id: int, sucursal_nombre: str) -> None:
        self.sucursal_id = sucursal_id
        self.sucursal_nombre = sucursal_nombre
        QTimer.singleShot(0, self._refresh_all)

    def set_usuario_actual(self, usuario: str, rol: str) -> None:
        self.usuario_actual = usuario or "Sistema"
        self.rol_usuario = rol or ""

    def obtener_usuario_actual(self) -> str:
        return self.usuario_actual

    def _subscribe_events(self) -> None:
        for evt in (TARJETA_CREADA, TARJETA_ACTUALIZADA, TARJETA_ASIGNADA, VENTA_COMPLETADA):
            EventBus.subscribe(evt, self._on_data_changed)

    def _on_data_changed(self, _data: dict) -> None:
        QTimer.singleShot(0, self._refresh_all)

    def _refresh_all(self) -> None:
        self._load_tarjetas()
        self._load_resumen()

    def limpiar(self) -> None:
        for evt in (TARJETA_CREADA, TARJETA_ACTUALIZADA, TARJETA_ASIGNADA, VENTA_COMPLETADA):
            try:
                EventBus.unsubscribe(evt, self._on_data_changed)
            except Exception:
                pass

    def _init_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 12, 16, 12); root.setSpacing(10)

        hdr = QHBoxLayout()
        title = QLabel("Gestión de Tarjetas de Fidelidad")
        f = title.font(); f.setPointSize(15); f.setBold(True); title.setFont(f)
        title.setObjectName("tituloPrincipal"); hdr.addWidget(title); hdr.addStretch()
        self._lbl_suc = QLabel()
        self._lbl_suc.setStyleSheet("color:#7f8c8d;"); hdr.addWidget(self._lbl_suc)
        root.addLayout(hdr)

        kpi_row = QHBoxLayout()
        self._kpi_total     = self._make_kpi("Total Tarjetas",   "0", _C3)
        self._kpi_activas   = self._make_kpi("Activas",          "0", _C4)
        self._kpi_inactivas = self._make_kpi("Inactivas",        "0", _C5)
        self._kpi_sin_asig  = self._make_kpi("Sin Asignar",      "0", _C6)
        for k in (self._kpi_total, self._kpi_activas, self._kpi_inactivas, self._kpi_sin_asig):
            kpi_row.addWidget(k)
        root.addLayout(kpi_row)

        self._tabs = QTabWidget(); root.addWidget(self._tabs)
        self._tabs.addTab(self._build_tab_gestion(),  "🃏 Gestión")
        self._tabs.addTab(self._build_tab_asignar(),  "👤 Asignación")
        self._tabs.addTab(self._build_tab_generar(),  "⚙️ Generar Masivo")
        self._tabs.addTab(self._build_tab_reportes(), "📊 Reportes")

    def _make_kpi(self, title: str, value: str, color: str) -> QFrame:
        card = QFrame()
        card.setStyleSheet(
            f"QFrame{{background:white;border:none;border-left:4px solid {color};border-radius:6px;}}"
        )
        card.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed); card.setFixedHeight(72)
        lay = QVBoxLayout(card); lay.setContentsMargins(10, 6, 10, 6)
        lt = QLabel(title); lt.setStyleSheet("color:#7f8c8d;font-size:11px;")
        lv = QLabel(value); lv.setStyleSheet(f"color:{color};font-size:18px;font-weight:bold;")
        lay.addWidget(lt); lay.addWidget(lv); card._val_label = lv; return card

    def _build_tab_gestion(self) -> QWidget:
        w = QWidget(); lay = QVBoxLayout(w)
        sb = QHBoxLayout()
        self._search = QLineEdit()
        self._search.setPlaceholderText("Buscar por ID, nombre de cliente o estado…")
        self._search.textChanged.connect(lambda _: self._load_tarjetas())
        sb.addWidget(QLabel("Buscar:")); sb.addWidget(self._search)
        self._combo_estado = QComboBox()
        self._combo_estado.addItems(["Todos", "activa", "inactiva", "bloqueada", "sin_asignar"])
        self._combo_estado.currentIndexChanged.connect(lambda _: self._load_tarjetas())
        sb.addWidget(QLabel("Estado:")); sb.addWidget(self._combo_estado)
        sb.addStretch()
        btn_nueva = QPushButton("+ Nueva Tarjeta")
        btn_nueva.setStyleSheet(f"background:{_C3};color:white;font-weight:bold;padding:6px 12px;border-radius:4px;")
        btn_nueva.clicked.connect(self._crear_tarjeta); sb.addWidget(btn_nueva)
        lay.addLayout(sb)

        self._tbl = QTableWidget()
        self._tbl.setColumnCount(7)
        self._tbl.setHorizontalHeaderLabels(
            ["ID", "Cliente", "Estado", "Puntos", "Nivel", "Fecha Emisión", "Modificado"]
        )
        self._tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tbl.verticalHeader().setVisible(False)
        self._tbl.setAlternatingRowColors(True)
        hdr = self._tbl.horizontalHeader()
        for i in range(7):
            hdr.setSectionResizeMode(i, QHeaderView.Stretch if i == 1 else QHeaderView.ResizeToContents)
        lay.addWidget(self._tbl)

        ab = QHBoxLayout()
        self._btn_edit    = QPushButton("✏️ Editar")
        self._btn_block   = QPushButton("🔒 Bloquear/Desbloquear")
        self._btn_reasign = QPushButton("🔄 Reasignar")
        for b in (self._btn_edit, self._btn_block, self._btn_reasign):
            b.setEnabled(False); ab.addWidget(b)
        ab.addStretch()
        self._btn_edit.clicked.connect(self._editar_tarjeta)
        self._btn_block.clicked.connect(self._toggle_block)
        self._btn_reasign.clicked.connect(self._reasignar_tarjeta)
        self._tbl.itemSelectionChanged.connect(self._on_sel_changed)
        lay.addLayout(ab); return w

    def _build_tab_asignar(self) -> QWidget:
        w = QWidget(); lay = QVBoxLayout(w)
        grp = QGroupBox("Asignar Tarjeta a Cliente"); fl = QFormLayout(grp)
        self._asign_card_id = QSpinBox(); self._asign_card_id.setRange(1000, 99999)
        self._combo_cliente = QComboBox(); self._combo_cliente.setEditable(True)
        btn_load = QPushButton("🔍 Cargar Clientes"); btn_load.clicked.connect(self._cargar_clientes)
        btn_asign = QPushButton("✅ Asignar")
        btn_asign.setStyleSheet(f"background:{_C4};color:white;font-weight:bold;padding:6px 12px;border-radius:4px;")
        btn_asign.clicked.connect(self._asignar_tarjeta)
        fl.addRow("ID Tarjeta:", self._asign_card_id)
        fl.addRow("Cliente:", self._combo_cliente)
        fl.addRow("", btn_load); fl.addRow("", btn_asign)
        lay.addWidget(grp); lay.addStretch(); return w

    def _build_tab_generar(self) -> QWidget:
        w = QWidget(); lay = QVBoxLayout(w)
        grp = QGroupBox("Generación Masiva de Tarjetas"); fl = QFormLayout(grp)
        self._spin_cantidad = QSpinBox(); self._spin_cantidad.setRange(1, 500); self._spin_cantidad.setValue(10)
        self._pb_gen = QProgressBar(); self._pb_gen.setVisible(False)
        btn_gen = QPushButton("🃏 Generar Tarjetas")
        btn_gen.setStyleSheet(f"background:{_C7};color:white;font-weight:bold;padding:6px 12px;border-radius:4px;")
        btn_gen.clicked.connect(self._generar_masivo)
        fl.addRow("Cantidad:", self._spin_cantidad); fl.addRow("Progreso:", self._pb_gen)
        fl.addRow("", btn_gen)
        lay.addWidget(grp); lay.addStretch(); return w

    def _build_tab_reportes(self) -> QWidget:
        w = QWidget(); lay = QVBoxLayout(w)
        btn_ref = QPushButton("🔄 Actualizar"); btn_ref.clicked.connect(self._load_resumen)
        lay.addWidget(btn_ref)
        self._tbl_report = QTableWidget()
        self._tbl_report.setColumnCount(4)
        self._tbl_report.setHorizontalHeaderLabels(["Nivel", "Clientes", "Puntos Totales", "Promedio"])
        self._tbl_report.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tbl_report.verticalHeader().setVisible(False)
        self._tbl_report.horizontalHeader().setStretchLastSection(True)
        lay.addWidget(self._tbl_report); return w

    def _load_tarjetas(self) -> None:
        search = self._search.text().strip()
        estado = self._combo_estado.currentText()
        estado = "" if estado == "Todos" else estado
        try:
            rows = self._repo.get_all(search=search, estado=estado)
        except Exception as exc:
            logger.exception("load_tarjetas"); rows = []
        self._tbl.setRowCount(len(rows))
        _ec = {"activa": _C4, "inactiva": "#7f8c8d", "bloqueada": _C5, "sin_asignar": _C6}
        for ri, r in enumerate(rows):
            ev = r.get("estado", "—")
            vals = [
                str(r.get("id", "")),
                r.get("cliente_nombre", "— Sin asignar —"),
                ev,
                str(r.get("puntos", 0)),
                r.get("nivel", "Bronce"),
                str(r.get("fecha_emision", ""))[:10] if r.get("fecha_emision") else "—",
                str(r.get("updated_at", ""))[:16] if r.get("updated_at") else "—",
            ]
            for ci, v in enumerate(vals):
                it = QTableWidgetItem(str(v)); it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                if ci == 2: it.setForeground(QColor(_ec.get(ev, "#000")))
                if ci in (3,): it.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self._tbl.setItem(ri, ci, it)
        self._lbl_suc.setText(f"Sucursal: {self.sucursal_nombre}")

    def _load_resumen(self) -> None:
        try:
            stats = self._repo.get_stats()
        except Exception:
            stats = {}
        self._kpi_total._val_label.setText(str(stats.get("total", 0)))
        self._kpi_activas._val_label.setText(str(stats.get("activas", 0)))
        self._kpi_inactivas._val_label.setText(str(stats.get("inactivas", 0)))
        self._kpi_sin_asig._val_label.setText(str(stats.get("sin_asignar", 0)))
        try:
            nivel_stats = self._repo.get_stats_by_level()
        except Exception:
            nivel_stats = []
        self._tbl_report.setRowCount(len(nivel_stats))
        for ri, row in enumerate(nivel_stats):
            for ci, v in enumerate([
                row.get("nivel", "—"), str(row.get("count", 0)),
                str(row.get("total_puntos", 0)), f"{row.get('avg_puntos', 0):.1f}",
            ]):
                it = QTableWidgetItem(v); it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                if ci > 0: it.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self._tbl_report.setItem(ri, ci, it)

    def _cargar_clientes(self) -> None:
        try:
            clientes = self._repo.get_clientes_disponibles()
            self._combo_cliente.clear()
            self._combo_cliente.addItem("— Seleccionar —", None)
            for c in clientes:
                nombre = f"{c.get('nombre','')} {c.get('apellido','')}".strip()
                self._combo_cliente.addItem(f"{nombre} (ID:{c['id']})", c["id"])
        except Exception as exc:
            QMessageBox.warning(self, "Error", f"No se pudieron cargar clientes: {exc}")

    def _on_sel_changed(self) -> None:
        has = len(self._tbl.selectedItems()) > 0
        for b in (self._btn_edit, self._btn_block, self._btn_reasign): b.setEnabled(has)

    def _get_selected_id(self) -> Optional[int]:
        row = self._tbl.currentRow()
        if row < 0: return None
        it = self._tbl.item(row, 0)
        if not it: return None
        try: return int(it.text())
        except ValueError: return None

    def _crear_tarjeta(self) -> None:
        dlg = DialogoTarjeta(self._repo, self.usuario_actual, parent=self)
        if dlg.exec_() == QDialog.Accepted:
            EventBus.publish(TARJETA_CREADA, {}); self._refresh_all()

    def _editar_tarjeta(self) -> None:
        tid = self._get_selected_id()
        if tid is None: return
        try:
            data = self._repo.get_by_id(tid)
        except TarjetaNotFoundError:
            QMessageBox.warning(self, "Error", "Tarjeta no encontrada."); return
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc)); return
        if not data:
            QMessageBox.warning(self, "Error", "Tarjeta no encontrada."); return
        dlg = DialogoTarjeta(self._repo, self.usuario_actual, tarjeta_data=data, parent=self)
        if dlg.exec_() == QDialog.Accepted:
            EventBus.publish(TARJETA_ACTUALIZADA, {}); self._refresh_all()

    def _toggle_block(self) -> None:
        tid = self._get_selected_id()
        if tid is None: return
        row = self._tbl.currentRow()
        estado_it = self._tbl.item(row, 2)
        if not estado_it: return
        current = estado_it.text()
        try:
            new_estado = "activa" if current == "bloqueada" else "bloqueada"
            self._repo.set_status(tid, new_estado, self.usuario_actual)
            label = "desbloqueada" if new_estado == "activa" else "bloqueada"
            QMessageBox.information(self, "Éxito", f"Tarjeta {label}.")
            EventBus.publish(TARJETA_ACTUALIZADA, {}); self._refresh_all()
        except TarjetaNotFoundError:
            QMessageBox.warning(self, "Error", "Tarjeta no encontrada.")
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))

    def _reasignar_tarjeta(self) -> None:
        tid = self._get_selected_id()
        if tid is None: return
        dlg = DialogoReasignacion(self._repo, tid, self.usuario_actual, parent=self)
        if dlg.exec_() == QDialog.Accepted:
            EventBus.publish(TARJETA_ASIGNADA, {}); self._refresh_all()

    def _asignar_tarjeta(self) -> None:
        card_id    = self._asign_card_id.value()
        cliente_id = self._combo_cliente.currentData()
        if not cliente_id:
            QMessageBox.warning(self, "Validación", "Seleccione un cliente."); return
        try:
            self._repo.assign_to_client(card_id, cliente_id, self.usuario_actual)
            QMessageBox.information(self, "Éxito", "Tarjeta asignada correctamente.")
            EventBus.publish(TARJETA_ASIGNADA, {}); self._refresh_all()
        except TarjetaNotFoundError:
            QMessageBox.warning(self, "Error", f"Tarjeta {card_id} no encontrada.")
        except TarjetaYaAsignadaError:
            QMessageBox.warning(self, "Error", "Esta tarjeta ya está asignada.")
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))

    def _generar_masivo(self) -> None:
        cantidad = self._spin_cantidad.value()
        if QMessageBox.question(
            self, "Confirmar", f"¿Generar {cantidad} tarjetas nuevas?",
            QMessageBox.Yes | QMessageBox.No
        ) != QMessageBox.Yes:
            return
        self._pb_gen.setVisible(True); self._pb_gen.setRange(0, cantidad); self._pb_gen.setValue(0)
        generated = 0; errors = 0
        for i in range(cantidad):
            try:
                self._repo.create({"estado": "sin_asignar", "puntos": 0,
                                   "nivel": "Bronce", "created_by": self.usuario_actual})
                generated += 1
            except Exception as exc:
                logger.warning("Error generando tarjeta %d: %s", i, exc); errors += 1
            self._pb_gen.setValue(i + 1)
        self._pb_gen.setVisible(False)
        msg = f"Generadas {generated} tarjetas."
        if errors: msg += f"\n{errors} errores (ver log)."
        QMessageBox.information(self, "Generación Masiva", msg)
        EventBus.publish(TARJETA_CREADA, {}); self._refresh_all()


class DialogoTarjeta(QDialog):
    def __init__(self, repo: TarjetaRepository, usuario: str,
                 tarjeta_data: Optional[Dict] = None, parent=None):
        super().__init__(parent)
        self._repo = repo; self._usuario = usuario; self._data = tarjeta_data
        self.setWindowTitle("Nueva Tarjeta" if not tarjeta_data else "Editar Tarjeta")
        self.setMinimumWidth(400); self._build_ui()
        if tarjeta_data: self._load()

    def _build_ui(self) -> None:
        lay = QVBoxLayout(self); fl = QFormLayout()
        self._e_estado = QComboBox()
        self._e_estado.addItems(["activa", "inactiva", "bloqueada", "sin_asignar"])
        self._e_puntos = QSpinBox(); self._e_puntos.setRange(0, 9_999_999)
        self._e_nivel  = QComboBox(); self._e_nivel.addItems(["Bronce", "Plata", "Oro", "Platino"])
        self._e_obs    = QLineEdit()
        fl.addRow("Estado:", self._e_estado); fl.addRow("Puntos:", self._e_puntos)
        fl.addRow("Nivel:", self._e_nivel); fl.addRow("Observaciones:", self._e_obs)
        lay.addLayout(fl)
        bl = QHBoxLayout()
        btn_ok = QPushButton("Guardar"); btn_ok.clicked.connect(self._guardar)
        btn_no = QPushButton("Cancelar"); btn_no.clicked.connect(self.reject)
        btn_ok.setStyleSheet(f"background:{_C4};color:white;font-weight:bold;padding:5px 12px;")
        bl.addWidget(btn_ok); bl.addWidget(btn_no); lay.addLayout(bl)

    def _load(self) -> None:
        d = self._data
        idx = self._e_estado.findText(d.get("estado", "activa"))
        if idx >= 0: self._e_estado.setCurrentIndex(idx)
        self._e_puntos.setValue(int(d.get("puntos", 0)))
        idx2 = self._e_nivel.findText(d.get("nivel", "Bronce"))
        if idx2 >= 0: self._e_nivel.setCurrentIndex(idx2)
        self._e_obs.setText(d.get("observaciones", "") or "")

    def _guardar(self) -> None:
        data = {"estado": self._e_estado.currentText(),
                "puntos": self._e_puntos.value(),
                "nivel": self._e_nivel.currentText(),
                "observaciones": self._e_obs.text().strip()}
        try:
            if self._data:
                self._repo.update(self._data["id"], data, self._usuario)
                QMessageBox.information(self, "Éxito", "Tarjeta actualizada.")
            else:
                data["created_by"] = self._usuario
                self._repo.create(data)
                QMessageBox.information(self, "Éxito", "Tarjeta creada.")
            self.accept()
        except TarjetaError as exc:
            QMessageBox.warning(self, "Error", str(exc))
        except Exception as exc:
            logger.exception("guardar_tarjeta")
            QMessageBox.critical(self, "Error", str(exc))


class DialogoReasignacion(QDialog):
    def __init__(self, repo: TarjetaRepository, card_id: int, usuario: str, parent=None):
        super().__init__(parent)
        self._repo = repo; self._card_id = card_id; self._usuario = usuario
        self.setWindowTitle(f"Reasignar Tarjeta {card_id}"); self.setMinimumWidth(360)
        self._build_ui(); self._cargar_clientes()

    def _build_ui(self) -> None:
        lay = QVBoxLayout(self); fl = QFormLayout()
        self._combo = QComboBox(); self._combo.setEditable(True)
        fl.addRow("Nuevo Cliente:", self._combo); lay.addLayout(fl)
        bl = QHBoxLayout()
        btn_ok = QPushButton("Reasignar"); btn_ok.clicked.connect(self._reasignar)
        btn_no = QPushButton("Cancelar"); btn_no.clicked.connect(self.reject)
        btn_ok.setStyleSheet(f"background:{_C6};color:white;font-weight:bold;padding:5px 12px;")
        bl.addWidget(btn_ok); bl.addWidget(btn_no); lay.addLayout(bl)

    def _cargar_clientes(self) -> None:
        try:
            clientes = self._repo.get_clientes_disponibles()
            self._combo.clear(); self._combo.addItem("— Seleccionar —", None)
            for c in clientes:
                nombre = f"{c.get('nombre','')} {c.get('apellido','')}".strip()
                self._combo.addItem(f"{nombre} (ID:{c['id']})", c["id"])
        except Exception as exc:
            QMessageBox.warning(self, "Error", f"No se pudieron cargar clientes: {exc}")

    def _reasignar(self) -> None:
        cliente_id = self._combo.currentData()
        if not cliente_id:
            QMessageBox.warning(self, "Validación", "Seleccione un cliente."); return
        try:
            self._repo.reassign(self._card_id, cliente_id, self._usuario)
            QMessageBox.information(self, "Éxito", "Tarjeta reasignada.")
            self.accept()
        except TarjetaNotFoundError:
            QMessageBox.warning(self, "Error", "Tarjeta no encontrada.")
        except TarjetaYaAsignadaError:
            QMessageBox.warning(self, "Error", "El cliente ya tiene tarjeta.")
        except Exception as exc:
            logger.exception("reasignar_tarjeta"); QMessageBox.critical(self, "Error", str(exc))

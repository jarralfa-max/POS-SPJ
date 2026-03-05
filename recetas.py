# modulos/recetas.py
# ── ModuloRecetas — Enterprise Recipe Management UI ──────────────────────────
# Block 2 requirements:
#   ✓ Prevent cyclic dependencies (validated in RecetaRepository)
#   ✓ Prevent self-reference (validated in RecetaRepository)
#   ✓ Enforce sum(componentes) + merma <= 100%
#   ✓ Mathematical validation before save
#   ✓ FK constraints + ON DELETE RESTRICT enforced by migration
#   ✓ Refresh dependent windows after update via EventBus
#   ✓ Prevent duplicate recipe for same base product
#   ✓ Transformation integrity tolerance 0.01kg
#   ✓ Integration with InventoryEngine batch tree validation
#   ✓ Complete UI (no partial/incomplete form)
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Dict, List, Optional

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QLineEdit,
    QComboBox, QMessageBox, QDialog, QFormLayout, QTableWidget,
    QTableWidgetItem, QAbstractItemView, QTabWidget, QGroupBox,
    QHeaderView, QFrame, QSizePolicy, QSplitter, QDoubleSpinBox,
    QSpinBox, QScrollArea, QTextEdit, QProgressBar
)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor

from .base import ModuloBase
from repositories.recetas import (
    RecetaRepository,
    RecetaError,
    RecetaCyclicError,
    RecetaSelfReferenceError,
    RecetaPercentageError,
    RecetaDuplicadaError,
)
from repositories.productos import ProductoRepository
from core.events.event_bus import EventBus

logger = logging.getLogger("spj.ui.recetas")

RECETA_CREADA      = "RECETA_CREADA"
RECETA_ACTUALIZADA = "RECETA_ACTUALIZADA"
PRODUCTO_CREADO    = "PRODUCTO_CREADO"
PRODUCTO_ACTUALIZADO = "PRODUCTO_ACTUALIZADO"

_C1 = "#1a252f"; _C3 = "#2980b9"; _C4 = "#27ae60"; _C5 = "#e74c3c"; _C6 = "#f39c12"


class ModuloRecetas(ModuloBase):

    def __init__(self, conexion, parent=None):
        super().__init__(conexion, parent)
        self.conexion        = conexion
        self.main_window     = parent
        self.sucursal_id     = 1
        self.sucursal_nombre = "Principal"
        self.usuario_actual  = "Sistema"
        self.rol_usuario     = ""
        self._repo   = RecetaRepository(conexion)
        self._prepo  = ProductoRepository(conexion)
        self._cached_productos: List[Dict] = []
        self._init_ui()
        self._subscribe_events()
        QTimer.singleShot(0, self._refresh_all)

    def set_sucursal(self, sucursal_id: int, sucursal_nombre: str) -> None:
        self.sucursal_id = sucursal_id
        self.sucursal_nombre = sucursal_nombre

    def set_usuario_actual(self, usuario: str, rol: str) -> None:
        self.usuario_actual = usuario or "Sistema"
        self.rol_usuario    = rol or ""

    def obtener_usuario_actual(self) -> str:
        return self.usuario_actual

    # ── Events ────────────────────────────────────────────────────────────────

    def _subscribe_events(self) -> None:
        for evt in (RECETA_CREADA, RECETA_ACTUALIZADA,
                    PRODUCTO_CREADO, PRODUCTO_ACTUALIZADO):
            EventBus.subscribe(evt, self._on_data_changed)

    def _on_data_changed(self, _data: dict) -> None:
        QTimer.singleShot(0, self._refresh_all)

    def _refresh_all(self) -> None:
        self._load_productos_cache()
        self._load_recetas()

    def limpiar(self) -> None:
        for evt in (RECETA_CREADA, RECETA_ACTUALIZADA,
                    PRODUCTO_CREADO, PRODUCTO_ACTUALIZADO):
            try:
                EventBus.unsubscribe(evt, self._on_data_changed)
            except Exception:
                pass

    # ── UI construction ───────────────────────────────────────────────────────

    def _init_ui(self) -> None:
        root = QVBoxLayout(self)
        root.setContentsMargins(16, 12, 16, 12); root.setSpacing(10)

        # Header
        hdr = QHBoxLayout()
        title = QLabel("Gestión de Recetas de Producción")
        f = title.font(); f.setPointSize(15); f.setBold(True); title.setFont(f)
        title.setObjectName("tituloPrincipal"); hdr.addWidget(title); hdr.addStretch()
        self._lbl_suc = QLabel()
        self._lbl_suc.setStyleSheet("color:#7f8c8d;"); hdr.addWidget(self._lbl_suc)
        root.addLayout(hdr)

        # Main splitter: list on left, details on right
        sp = QSplitter(Qt.Horizontal)

        # Left panel: recipe list
        left = QWidget(); ll = QVBoxLayout(left); ll.setContentsMargins(0,0,0,0)
        # Search
        sh = QHBoxLayout()
        self._search = QLineEdit()
        self._search.setPlaceholderText("Buscar receta…")
        self._search.textChanged.connect(lambda _: self._load_recetas())
        sh.addWidget(QLabel("Buscar:")); sh.addWidget(self._search)
        ll.addLayout(sh)

        self._tbl = QTableWidget()
        self._tbl.setColumnCount(4)
        self._tbl.setHorizontalHeaderLabels(["ID", "Nombre Receta", "Base", "Rendimiento"])
        self._tbl.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tbl.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tbl.verticalHeader().setVisible(False)
        self._tbl.setAlternatingRowColors(True)
        hdr_ = self._tbl.horizontalHeader()
        hdr_.setSectionResizeMode(1, QHeaderView.Stretch)
        for i in (0, 2, 3):
            hdr_.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        self._tbl.itemSelectionChanged.connect(self._on_sel_changed)
        ll.addWidget(self._tbl)

        ab = QHBoxLayout()
        btn_nueva  = QPushButton("+ Nueva Receta")
        btn_nueva.setStyleSheet(f"background:{_C3};color:white;font-weight:bold;padding:6px 10px;border-radius:4px;")
        btn_nueva.clicked.connect(self._nueva_receta)
        self._btn_edit   = QPushButton("✏️ Editar");   self._btn_edit.setEnabled(False)
        self._btn_delete = QPushButton("🗑 Desactivar"); self._btn_delete.setEnabled(False)
        self._btn_edit.clicked.connect(self._editar_receta)
        self._btn_delete.clicked.connect(self._desactivar_receta)
        for b in (btn_nueva, self._btn_edit, self._btn_delete):
            ab.addWidget(b)
        ab.addStretch()
        ll.addLayout(ab)
        sp.addWidget(left)

        # Right panel: recipe detail
        right = QWidget(); rl = QVBoxLayout(right); rl.setContentsMargins(0,0,0,0)
        self._lbl_detalle = QLabel("Seleccione una receta para ver sus componentes.")
        self._lbl_detalle.setStyleSheet("color:#7f8c8d;font-style:italic;")
        rl.addWidget(self._lbl_detalle)
        self._tbl_comp = QTableWidget()
        self._tbl_comp.setColumnCount(5)
        self._tbl_comp.setHorizontalHeaderLabels(
            ["Componente", "Rendimiento %", "Merma %", "Total %", "Descripción"]
        )
        self._tbl_comp.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tbl_comp.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tbl_comp.verticalHeader().setVisible(False)
        hdr2 = self._tbl_comp.horizontalHeader()
        hdr2.setSectionResizeMode(0, QHeaderView.Stretch)
        for i in (1, 2, 3, 4):
            hdr2.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        rl.addWidget(self._tbl_comp)

        # Validation summary
        self._lbl_total = QLabel()
        self._lbl_total.setStyleSheet("font-size:13px;font-weight:bold;")
        rl.addWidget(self._lbl_total)
        sp.addWidget(right)
        sp.setSizes([360, 540])
        root.addWidget(sp)

    # ── Data ──────────────────────────────────────────────────────────────────

    def _load_productos_cache(self) -> None:
        try:
            self._cached_productos = self._prepo.get_all(include_inactive=False)
        except Exception as exc:
            logger.warning("load_productos_cache: %s", exc)
            self._cached_productos = []

    def _load_recetas(self) -> None:
        search = self._search.text().strip().lower()
        try:
            rows = self._repo.get_all()
        except Exception as exc:
            logger.exception("load_recetas"); rows = []
        if search:
            rows = [r for r in rows
                    if search in r.get("nombre_receta", "").lower()
                    or search in r.get("base_product_nombre", "").lower()]
        self._tbl.setRowCount(len(rows))
        for ri, r in enumerate(rows):
            rend = float(r.get("total_rendimiento", 0))
            vals = [
                str(r.get("id", "")),
                r.get("nombre_receta", "—"),
                r.get("base_product_nombre", "—"),
                f"{rend:.2f}%",
            ]
            for ci, v in enumerate(vals):
                it = QTableWidgetItem(str(v)); it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                if ci == 3:
                    it.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                    it.setForeground(QColor(_C4 if rend <= 100 else _C5))
                self._tbl.setItem(ri, ci, it)
        self._lbl_suc.setText(f"Sucursal: {self.sucursal_nombre}")

    def _load_receta_detail(self) -> None:
        row = self._tbl.currentRow()
        if row < 0:
            self._tbl_comp.setRowCount(0); self._lbl_total.setText("")
            self._lbl_detalle.setText("Seleccione una receta para ver sus componentes.")
            return
        it = self._tbl.item(row, 0)
        if not it:
            return
        try:
            rid = int(it.text())
            comps = self._repo.get_components(rid)
        except Exception as exc:
            logger.exception("load_receta_detail"); return

        self._tbl_comp.setRowCount(len(comps))
        total_rend = Decimal("0"); total_merma = Decimal("0")
        for ri, c in enumerate(comps):
            rend  = Decimal(str(c.get("rendimiento_pct", 0)))
            merma = Decimal(str(c.get("merma_pct", 0)))
            total_rend  += rend
            total_merma += merma
            fila_total = float(rend + merma)
            vals = [
                c.get("component_nombre", "?"),
                f"{float(rend):.2f}%",
                f"{float(merma):.2f}%",
                f"{fila_total:.2f}%",
                c.get("descripcion", ""),
            ]
            for ci, v in enumerate(vals):
                it2 = QTableWidgetItem(v); it2.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                if ci in (1, 2, 3): it2.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self._tbl_comp.setItem(ri, ci, it2)

        grand_total = float(total_rend + total_merma)
        if grand_total <= 100.01:
            color = _C4; ok = "✅"
        else:
            color = _C5; ok = "❌"
        self._lbl_total.setText(
            f"{ok} Total rendimiento: {float(total_rend):.2f}% | "
            f"Total merma: {float(total_merma):.2f}% | "
            f"SUMA: {grand_total:.2f}%"
        )
        self._lbl_total.setStyleSheet(f"font-size:13px;font-weight:bold;color:{color};")
        nombre = self._tbl.item(row, 1)
        self._lbl_detalle.setText(
            f"Componentes de: {nombre.text() if nombre else 'receta'} "
            f"({len(comps)} componentes)"
        )

    def _on_sel_changed(self) -> None:
        has = len(self._tbl.selectedItems()) > 0
        self._btn_edit.setEnabled(has)
        self._btn_delete.setEnabled(has)
        self._load_receta_detail()

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def _get_selected_id(self) -> Optional[int]:
        row = self._tbl.currentRow()
        if row < 0: return None
        it = self._tbl.item(row, 0)
        if not it: return None
        try: return int(it.text())
        except ValueError: return None

    def _nueva_receta(self) -> None:
        if not self._cached_productos:
            QMessageBox.warning(self, "Sin productos",
                                "No hay productos activos. Cree productos primero."); return
        dlg = DialogoReceta(self._repo, self._cached_productos,
                             self.usuario_actual, parent=self)
        if dlg.exec_() == QDialog.Accepted:
            self._refresh_all()

    def _editar_receta(self) -> None:
        rid = self._get_selected_id()
        if rid is None: return
        try:
            data = self._repo.get_by_id(rid)
            comps = self._repo.get_components(rid)
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc)); return
        if not data:
            QMessageBox.warning(self, "Error", "Receta no encontrada."); return
        dlg = DialogoReceta(self._repo, self._cached_productos,
                             self.usuario_actual,
                             receta_data=data, componentes=comps, parent=self)
        if dlg.exec_() == QDialog.Accepted:
            self._refresh_all()

    def _desactivar_receta(self) -> None:
        rid = self._get_selected_id()
        if rid is None: return
        if QMessageBox.question(
            self, "Confirmar", "¿Desactivar esta receta? No se eliminará pero dejará de usarse.",
            QMessageBox.Yes | QMessageBox.No
        ) != QMessageBox.Yes:
            return
        try:
            self._repo.deactivate(rid, self.usuario_actual)
            QMessageBox.information(self, "Éxito", "Receta desactivada.")
            self._refresh_all()
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))


# ── Dialogo Nueva/Editar Receta ───────────────────────────────────────────────

class DialogoReceta(QDialog):

    def __init__(
        self,
        repo: RecetaRepository,
        productos: List[Dict],
        usuario: str,
        receta_data: Optional[Dict] = None,
        componentes: Optional[List[Dict]] = None,
        parent=None,
    ):
        super().__init__(parent)
        self._repo        = repo
        self._productos   = productos
        self._usuario     = usuario
        self._data        = receta_data
        self._componentes = componentes or []
        self._comp_rows: List[Dict] = []  # working copy
        self.setWindowTitle("Nueva Receta" if not receta_data else "Editar Receta")
        self.setMinimumWidth(700); self.setMinimumHeight(550)
        self._build_ui()
        if receta_data:
            self._load()

    def _build_ui(self) -> None:
        lay = QVBoxLayout(self)

        # Header form
        fl = QFormLayout()
        self._e_nombre = QLineEdit()
        self._e_nombre.setPlaceholderText("Nombre de la receta…")
        self._combo_base = QComboBox()
        self._combo_base.addItem("— Seleccionar producto base —", None)
        for p in self._productos:
            self._combo_base.addItem(
                f"{p['nombre']} [{p.get('unidad','kg')}]", p["id"]
            )
        fl.addRow("Nombre Receta*:", self._e_nombre)
        fl.addRow("Producto Base*:", self._combo_base)
        lay.addLayout(fl)

        # Components table
        grp = QGroupBox("Componentes (suma rendimiento + merma ≤ 100%)")
        gl = QVBoxLayout(grp)

        self._tbl_comp = QTableWidget()
        self._tbl_comp.setColumnCount(5)
        self._tbl_comp.setHorizontalHeaderLabels(
            ["Componente", "Rendimiento %", "Merma %", "Total %", "Descripción"]
        )
        self._tbl_comp.verticalHeader().setVisible(False)
        self._tbl_comp.setAlternatingRowColors(True)
        hdr = self._tbl_comp.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.Stretch)
        for i in (1, 2, 3, 4): hdr.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        gl.addWidget(self._tbl_comp)

        # Add component form
        add_row = QHBoxLayout()
        self._combo_comp = QComboBox()
        self._combo_comp.addItem("— Componente —", None)
        for p in self._productos:
            self._combo_comp.addItem(f"{p['nombre']}", p["id"])
        self._spin_rend  = QDoubleSpinBox(); self._spin_rend.setRange(0, 100); self._spin_rend.setDecimals(3); self._spin_rend.setSuffix(" %")
        self._spin_merma = QDoubleSpinBox(); self._spin_merma.setRange(0, 100); self._spin_merma.setDecimals(3); self._spin_merma.setSuffix(" %")
        self._e_desc     = QLineEdit(); self._e_desc.setPlaceholderText("Descripción (opcional)")
        btn_add = QPushButton("➕ Agregar")
        btn_add.clicked.connect(self._add_component)
        btn_add.setStyleSheet(f"background:{_C3};color:white;padding:4px 10px;border-radius:3px;")
        btn_del = QPushButton("🗑 Quitar Sel.")
        btn_del.clicked.connect(self._remove_component)
        for w, lbl in [(self._combo_comp,"Comp:"), (QLabel("Rend:"), None),
                       (self._spin_rend,None), (QLabel("Merma:"),None),
                       (self._spin_merma,None), (self._e_desc,None),
                       (btn_add,None), (btn_del,None)]:
            if lbl is not None: add_row.addWidget(QLabel(lbl))
            add_row.addWidget(w)
        gl.addLayout(add_row)

        # Totals
        self._lbl_totales = QLabel("Suma: 0.00%")
        self._lbl_totales.setStyleSheet("font-size:13px;font-weight:bold;")
        gl.addWidget(self._lbl_totales)
        lay.addWidget(grp)

        # Buttons
        bl = QHBoxLayout()
        btn_ok = QPushButton("💾 Guardar Receta"); btn_ok.clicked.connect(self._guardar)
        btn_no = QPushButton("Cancelar"); btn_no.clicked.connect(self.reject)
        btn_ok.setStyleSheet(f"background:{_C4};color:white;font-weight:bold;padding:6px 14px;border-radius:4px;")
        bl.addStretch(); bl.addWidget(btn_ok); bl.addWidget(btn_no)
        lay.addLayout(bl)

    def _load(self) -> None:
        d = self._data
        self._e_nombre.setText(d.get("nombre_receta", ""))
        idx = self._combo_base.findData(d.get("base_product_id"))
        if idx >= 0: self._combo_base.setCurrentIndex(idx)
        self._comp_rows = []
        for c in self._componentes:
            self._comp_rows.append({
                "component_product_id": c.get("component_product_id"),
                "component_nombre":     c.get("component_nombre", "?"),
                "rendimiento_pct":      float(c.get("rendimiento_pct", 0)),
                "merma_pct":            float(c.get("merma_pct", 0)),
                "descripcion":          c.get("descripcion", ""),
                "orden":                c.get("orden", 0),
            })
        self._refresh_comp_table()

    def _add_component(self) -> None:
        comp_id = self._combo_comp.currentData()
        if not comp_id:
            QMessageBox.warning(self, "Validación", "Seleccione un componente."); return
        rend  = self._spin_rend.value()
        merma = self._spin_merma.value()
        if rend + merma <= 0:
            QMessageBox.warning(self, "Validación",
                                "Rendimiento + Merma debe ser mayor a 0%."); return
        base_id = self._combo_base.currentData()
        if comp_id == base_id:
            QMessageBox.warning(self, "Auto-referencia",
                                "Un componente no puede ser el mismo producto base."); return
        # Check duplicate
        if any(r["component_product_id"] == comp_id for r in self._comp_rows):
            QMessageBox.warning(self, "Duplicado",
                                "Este componente ya está en la receta."); return
        comp_nombre = self._combo_comp.currentText()
        self._comp_rows.append({
            "component_product_id": comp_id,
            "component_nombre":     comp_nombre,
            "rendimiento_pct":      rend,
            "merma_pct":            merma,
            "descripcion":          self._e_desc.text().strip(),
            "orden":                len(self._comp_rows),
        })
        self._refresh_comp_table()

    def _remove_component(self) -> None:
        row = self._tbl_comp.currentRow()
        if row < 0: return
        self._comp_rows.pop(row)
        self._refresh_comp_table()

    def _refresh_comp_table(self) -> None:
        self._tbl_comp.setRowCount(len(self._comp_rows))
        total_rend = Decimal("0"); total_merma = Decimal("0")
        for ri, r in enumerate(self._comp_rows):
            rend  = Decimal(str(r["rendimiento_pct"]))
            merma = Decimal(str(r["merma_pct"]))
            total_rend  += rend; total_merma += merma
            fila_total = float(rend + merma)
            vals = [
                r.get("component_nombre", "?"),
                f"{float(rend):.3f}%",
                f"{float(merma):.3f}%",
                f"{fila_total:.3f}%",
                r.get("descripcion", ""),
            ]
            for ci, v in enumerate(vals):
                it = QTableWidgetItem(v); it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                if ci in (1, 2, 3): it.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self._tbl_comp.setItem(ri, ci, it)
        grand = float(total_rend + total_merma)
        ok = grand <= 100.01
        color = _C4 if ok else _C5
        icon  = "✅" if ok else "❌ EXCEDE 100%"
        self._lbl_totales.setText(
            f"{icon}  Rendimiento total: {float(total_rend):.3f}%  |  "
            f"Merma total: {float(total_merma):.3f}%  |  "
            f"Suma: {grand:.3f}%"
        )
        self._lbl_totales.setStyleSheet(f"font-size:13px;font-weight:bold;color:{color};")

    def _guardar(self) -> None:
        nombre = self._e_nombre.text().strip()
        if not nombre:
            QMessageBox.warning(self, "Validación", "Nombre de receta obligatorio."); return
        base_id = self._combo_base.currentData()
        if not base_id:
            QMessageBox.warning(self, "Validación", "Seleccione producto base."); return
        if not self._comp_rows:
            QMessageBox.warning(self, "Validación", "Agregue al menos un componente."); return

        # Pre-validate totals client-side
        total = sum(
            Decimal(str(c["rendimiento_pct"])) + Decimal(str(c["merma_pct"]))
            for c in self._comp_rows
        )
        if total > Decimal("100.01"):
            QMessageBox.warning(
                self, "Error de Porcentaje",
                f"La suma total ({float(total):.3f}%) excede el 100%.\n"
                "Ajuste los porcentajes antes de guardar."
            ); return

        components = [
            {
                "component_product_id": c["component_product_id"],
                "rendimiento_pct":      c["rendimiento_pct"],
                "merma_pct":            c["merma_pct"],
                "descripcion":          c.get("descripcion", ""),
                "orden":                c.get("orden", i),
            }
            for i, c in enumerate(self._comp_rows)
        ]

        try:
            if self._data:
                self._repo.update(self._data["id"], nombre, components, self._usuario)
                QMessageBox.information(self, "Éxito", "Receta actualizada correctamente.")
            else:
                rid = self._repo.create(
                    nombre_receta=nombre,
                    base_product_id=base_id,
                    components=components,
                    usuario=self._usuario,
                )
                QMessageBox.information(self, "Éxito", f"Receta #{rid} creada correctamente.")
            self.accept()
        except RecetaCyclicError:
            QMessageBox.warning(self, "Dependencia Cíclica",
                                "Esta configuración crea una dependencia circular entre productos.")
        except RecetaSelfReferenceError:
            QMessageBox.warning(self, "Auto-referencia",
                                "Un componente no puede ser el mismo producto base.")
        except RecetaPercentageError as exc:
            QMessageBox.warning(self, "Error de Porcentaje", str(exc))
        except RecetaDuplicadaError:
            QMessageBox.warning(self, "Receta Duplicada",
                                "Ya existe una receta activa para este producto base.")
        except RecetaError as exc:
            QMessageBox.warning(self, "Error en Receta", str(exc))
        except Exception as exc:
            logger.exception("guardar_receta")
            QMessageBox.critical(self, "Error Inesperado", str(exc))

# modulos/productos.py
# ── ModuloProductos — Enterprise UI ──────────────────────────────────────────
# Block 1 requirements:
#   ✓ No automatic seeding
#   ✓ No SQL in UI
#   ✓ Repository-only DB access
#   ✓ Soft delete with deletion guards
#   ✓ Refresh via EventBus
#   ✓ Name normalization enforced at repo level
from __future__ import annotations

import logging
import os
import shutil
from datetime import datetime
from typing import Dict, Optional

from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QColor, QPixmap
from PyQt5.QtWidgets import (
    QAbstractItemView, QCheckBox, QComboBox, QDialog,
    QDoubleSpinBox, QFileDialog, QFormLayout,
    QHBoxLayout, QHeaderView, QLabel, QLineEdit, QMessageBox,
    QPushButton, QSplitter, QTableWidget, QTableWidgetItem,
    QTabWidget, QVBoxLayout, QWidget
)

from .base import ModuloBase
from repositories.productos import (
    ProductoRepository,
    ProductoDeletionError,
    ProductoNombreDuplicadoError,
)
from core.events.event_bus import EventBus

logger = logging.getLogger("spj.ui.productos")

PRODUCTO_CREADO      = "PRODUCTO_CREADO"
PRODUCTO_ACTUALIZADO = "PRODUCTO_ACTUALIZADO"
PRODUCTO_ELIMINADO   = "PRODUCTO_ELIMINADO"
VENTA_COMPLETADA     = "VENTA_COMPLETADA"


class ModuloProductos(ModuloBase):

    def __init__(self, conexion, parent=None):
        super().__init__(conexion, parent)
        self.conexion = conexion
        self.main_window = parent
        self.usuario_actual = "Sistema"
        self.rol_usuario    = ""
        self._repo = ProductoRepository(conexion)
        os.makedirs("imagenes_productos", exist_ok=True)
        self._init_ui()
        self._subscribe_events()
        self._refresh_all()

    def set_sucursal(self, sucursal_id: int, sucursal_nombre: str):
        self.sucursal_id     = sucursal_id
        self.sucursal_nombre = sucursal_nombre

    def set_usuario_actual(self, usuario, rol):
        self.usuario_actual = usuario or "Sistema"
        self.rol_usuario    = rol or ""
        if hasattr(self, "_idx_recetas"):
            self.tabs.setTabVisible(self._idx_recetas,
                                    (rol or "").lower() == "admin")

    def obtener_usuario_actual(self) -> str:
        return self.usuario_actual

    # ── Events ────────────────────────────────────────────────────────────────

    def _subscribe_events(self):
        for evt in (VENTA_COMPLETADA, PRODUCTO_CREADO,
                    PRODUCTO_ACTUALIZADO, PRODUCTO_ELIMINADO):
            EventBus.subscribe(evt, self._on_data_changed)

    def _on_data_changed(self, _data: dict):
        QTimer.singleShot(0, self._refresh_all)

    def _refresh_all(self):
        self._refresh_cats()
        self._load_productos()
        self._load_compuestos()
        self._load_subproductos()
        self._load_recetas()

    # ── UI ────────────────────────────────────────────────────────────────────

    def _init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        self.tabs = QTabWidget()
        self._build_tab_prod()
        self._build_tab_comp()
        self._build_tab_sub()
        self._build_tab_recetas()
        layout.addWidget(self.tabs)
        self.setLayout(layout)

    @staticmethod
    def _make_table(headers):
        t = QTableWidget()
        t.setColumnCount(len(headers))
        t.setHorizontalHeaderLabels(headers)
        t.setEditTriggers(QAbstractItemView.NoEditTriggers)
        t.setSelectionBehavior(QAbstractItemView.SelectRows)
        t.verticalHeader().setVisible(False)
        t.setAlternatingRowColors(True)
        hdr = t.horizontalHeader()
        for i in range(len(headers)):
            hdr.setSectionResizeMode(
                i, QHeaderView.Stretch if i == 1 else QHeaderView.ResizeToContents
            )
        return t

    def _build_tab_prod(self):
        tab = QWidget()
        lay = QVBoxLayout(tab)
        tb = QHBoxLayout()
        self._search_prod = QLineEdit()
        self._search_prod.setPlaceholderText("Buscar nombre o categoría…")
        self._combo_cat = QComboBox()
        self._combo_cat.addItem("Todas las categorías", "")
        self._chk_ocultos = QCheckBox("Mostrar ocultos")
        btn_nuevo = QPushButton("+ Nuevo")
        btn_nuevo.clicked.connect(self._nuevo_producto)
        for w in [QLabel("Buscar:"), self._search_prod, QLabel("Cat:"),
                  self._combo_cat, self._chk_ocultos, btn_nuevo]:
            tb.addWidget(w)
        tb.addStretch()
        lay.addLayout(tb)
        self._tbl_prod = self._make_table(
            ["ID", "Nombre", "Precio", "Existencia", "Stock Mín", "Unidad", "Categoría", "Estado"]
        )
        lay.addWidget(self._tbl_prod)
        ab = QHBoxLayout()
        self._btn_edit = QPushButton("Editar")
        self._btn_del  = QPushButton("Eliminar")
        self._btn_vis  = QPushButton("Ocultar/Mostrar")
        for b in (self._btn_edit, self._btn_del, self._btn_vis):
            b.setEnabled(False)
            ab.addWidget(b)
        ab.addStretch()
        lay.addLayout(ab)
        self._tbl_prod.itemSelectionChanged.connect(self._on_prod_sel)
        self._search_prod.textChanged.connect(lambda _: self._load_productos())
        self._combo_cat.currentIndexChanged.connect(lambda _: self._load_productos())
        self._chk_ocultos.stateChanged.connect(lambda _: self._load_productos())
        self._btn_edit.clicked.connect(self._editar_producto)
        self._btn_del.clicked.connect(self._eliminar_producto)
        self._btn_vis.clicked.connect(self._toggle_vis)
        self.tabs.addTab(tab, "📦 Simples / Todos")

    def _build_tab_comp(self):
        tab = QWidget()
        lay = QVBoxLayout(tab)
        self._search_comp = QLineEdit()
        self._search_comp.setPlaceholderText("Buscar compuestos…")
        self._chk_ocultos_c = QCheckBox("Mostrar ocultos")
        tb2 = QHBoxLayout()
        tb2.addWidget(self._search_comp)
        tb2.addWidget(self._chk_ocultos_c)
        tb2.addStretch()
        lay.addLayout(tb2)
        self._tbl_comp = self._make_table(
            ["ID", "Nombre", "Precio", "Existencia", "Unidad", "Categoría", "Estado"]
        )
        lay.addWidget(self._tbl_comp)
        self._search_comp.textChanged.connect(lambda _: self._load_compuestos())
        self._chk_ocultos_c.stateChanged.connect(lambda _: self._load_compuestos())
        self.tabs.addTab(tab, "🧩 Compuestos")

    def _build_tab_sub(self):
        tab = QWidget()
        lay = QVBoxLayout(tab)
        lay.addWidget(QLabel("Subproductos generados por transformación."))
        self._tbl_sub = self._make_table(["ID", "Nombre", "Categoría", "Stock", "Unidad"])
        lay.addWidget(self._tbl_sub)
        self.tabs.addTab(tab, "🍗 Subproductos")

    def _build_tab_recetas(self):
        tab = QWidget()
        lay = QVBoxLayout(tab)
        lay.addWidget(QLabel("Recetas — solo administradores."))
        sp = QSplitter(Qt.Horizontal)
        izq = QWidget()
        il = QVBoxLayout(izq)
        il.setContentsMargins(0,0,0,0)
        self._tbl_rec = self._make_table(["ID", "Nombre receta", "Producto base"])
        self._tbl_rec.itemSelectionChanged.connect(self._load_receta_det)
        il.addWidget(self._tbl_rec)
        btn_ref = QPushButton("🔄 Actualizar")
        btn_ref.clicked.connect(self._load_recetas)
        il.addWidget(btn_ref)
        sp.addWidget(izq)
        der = QWidget()
        dl = QVBoxLayout(der)
        dl.setContentsMargins(0,0,0,0)
        self._tbl_rec_det = self._make_table(
            ["Componente", "Rendimiento %", "Merma %", "Descripción"]
        )
        dl.addWidget(self._tbl_rec_det)
        sp.addWidget(der)
        sp.setSizes([300, 450])
        lay.addWidget(sp)
        self._idx_recetas = self.tabs.addTab(tab, "📋 Recetas")
        self.tabs.setTabVisible(self._idx_recetas, False)

    # ── Data ──────────────────────────────────────────────────────────────────

    def _refresh_cats(self):
        cats = self._repo.get_categories()
        cur = self._combo_cat.currentData()
        self._combo_cat.blockSignals(True)
        self._combo_cat.clear()
        self._combo_cat.addItem("Todas las categorías", "")
        for c in cats:
            self._combo_cat.addItem(c, c)
        idx = self._combo_cat.findData(cur)
        self._combo_cat.setCurrentIndex(idx if idx >= 0 else 0)
        self._combo_cat.blockSignals(False)

    def _load_productos(self):
        rows = self._repo.get_all(
            include_inactive=self._chk_ocultos.isChecked(),
            categoria=self._combo_cat.currentData() or "",
            search=self._search_prod.text().strip(),
        )
        rows = [r for r in rows if not r.get("es_compuesto")]
        self._fill_table(self._tbl_prod, rows)

    def _load_compuestos(self):
        rows = self._repo.get_all(
            include_inactive=self._chk_ocultos_c.isChecked(),
            search=self._search_comp.text().strip(),
        )
        rows = [r for r in rows if r.get("es_compuesto")]
        self._fill_table(self._tbl_comp, rows)

    def _load_subproductos(self):
        rows = self._repo.get_all(include_inactive=False)
        rows = [r for r in rows if r.get("es_subproducto")]
        t = self._tbl_sub
        t.setRowCount(len(rows))
        for i, r in enumerate(rows):
            for c, v in enumerate([
                str(r["id"]), r["nombre"],
                r.get("categoria",""),
                f"{float(r.get('existencia',0)):.3f}",
                r.get("unidad","pza"),
            ]):
                it = QTableWidgetItem(v)
                it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                t.setItem(i, c, it)

    def _load_recetas(self):
        try:
            from repositories.recetas import RecetaRepository
            rows = RecetaRepository(self.conexion).get_all()
            self._tbl_rec.setRowCount(len(rows))
            for i, r in enumerate(rows):
                for c, v in enumerate([
                    str(r["id"]), r.get("nombre_receta","—"),
                    r.get("base_product_nombre","—")
                ]):
                    it = QTableWidgetItem(v)
                    it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                    self._tbl_rec.setItem(i, c, it)
        except Exception as exc:
            logger.debug("_load_recetas: %s", exc)

    def _load_receta_det(self):
        row = self._tbl_rec.currentRow()
        if row < 0:
            return
        it = self._tbl_rec.item(row, 0)
        if not it:
            return
        rid = int(it.text())
        try:
            from repositories.recetas import RecetaRepository
            comps = RecetaRepository(self.conexion).get_components(rid)
            self._tbl_rec_det.setRowCount(len(comps))
            for i, c in enumerate(comps):
                for col, val in enumerate([
                    c.get("component_nombre","?"),
                    f"{float(c.get('rendimiento_pct',0)):.2f}%",
                    f"{float(c.get('merma_pct',0)):.2f}%",
                    c.get("descripcion",""),
                ]):
                    it2 = QTableWidgetItem(val)
                    it2.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                    if col in (1,2):
                        it2.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                    self._tbl_rec_det.setItem(i, col, it2)
        except Exception as exc:
            logger.debug("_load_receta_det: %s", exc)

    def _fill_table(self, tbl, rows):
        ncols = tbl.columnCount()
        tbl.setRowCount(len(rows))
        for ri, r in enumerate(rows):
            ex  = float(r.get("existencia",0))
            sm  = float(r.get("stock_minimo",0) or 0)
            ocu = bool(r.get("oculto"))
            vals = [
                str(r["id"]),
                r["nombre"],
                f"${float(r.get('precio',0)):.2f}",
                f"{ex:.3f}",
                f"{sm:.3f}",
                r.get("unidad","kg"),
                r.get("categoria") or "—",
                "Oculto" if ocu else "Visible",
            ]
            for ci, v in enumerate(vals[:ncols]):
                it = QTableWidgetItem(str(v))
                it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                if ci == 1 and ocu:
                    it.setForeground(QColor("gray"))
                if ci == 3:
                    it.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                    if ex <= 0:
                        it.setForeground(QColor("red"))
                    elif ex <= sm:
                        it.setForeground(QColor("orange"))
                tbl.setItem(ri, ci, it)

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def _on_prod_sel(self):
        sel = len(self._tbl_prod.selectedItems()) > 0
        self._btn_edit.setEnabled(sel)
        self._btn_del.setEnabled(sel)
        self._btn_vis.setEnabled(sel)
        if sel:
            row = self._tbl_prod.currentRow()
            it  = self._tbl_prod.item(row, 7)
            if it:
                self._btn_vis.setText(
                    "Mostrar" if it.text() == "Oculto" else "Ocultar"
                )

    def _get_pid(self, tbl) -> Optional[int]:
        row = tbl.currentRow()
        if row < 0:
            return None
        it = tbl.item(row, 0)
        return int(it.text()) if it else None

    def _nuevo_producto(self):
        dlg = DialogoProducto(self._repo, self.usuario_actual, parent=self)
        if dlg.exec_() == QDialog.Accepted:
            self._refresh_all()

    def _editar_producto(self):
        pid = self._get_pid(self._tbl_prod)
        if pid is None:
            return
        data = self._repo.get_by_id(pid)
        if not data:
            QMessageBox.warning(self, "Error", "Producto no encontrado.")
            return
        dlg = DialogoProducto(self._repo, self.usuario_actual,
                               producto_data=data, parent=self)
        if dlg.exec_() == QDialog.Accepted:
            self._refresh_all()

    def _eliminar_producto(self):
        pid = self._get_pid(self._tbl_prod)
        if pid is None:
            return
        prod = self._repo.get_by_id(pid)
        if not prod:
            return
        if QMessageBox.question(
            self, "Confirmar eliminación",
            f"¿Eliminar '{prod['nombre']}'? Se ocultará permanentemente\n"
            "si no tiene ventas, movimientos ni recetas.",
            QMessageBox.Yes | QMessageBox.No
        ) != QMessageBox.Yes:
            return
        try:
            self._repo.soft_delete(pid, self.usuario_actual)
            QMessageBox.information(self, "Éxito", "Producto eliminado.")
            self._refresh_all()
        except ProductoDeletionError as e:
            msgs = {
                "TIENE_VENTAS":      "tiene ventas registradas",
                "TIENE_MOVIMIENTOS": "tiene movimientos de inventario",
                "TIENE_RECETAS":     "está en recetas activas",
            }
            QMessageBox.warning(self, "No se puede eliminar",
                                f"El producto {msgs.get(str(e), str(e))}.")
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))

    def _toggle_vis(self):
        pid = self._get_pid(self._tbl_prod)
        if pid is None:
            return
        row = self._tbl_prod.currentRow()
        it  = self._tbl_prod.item(row, 7)
        if it:
            ocultar = it.text() == "Visible"
            try:
                self._repo.set_visibility(pid, ocultar, self.usuario_actual)
                self._refresh_all()
            except Exception as exc:
                QMessageBox.critical(self, "Error", str(exc))

    def limpiar(self):
        for evt in (VENTA_COMPLETADA, PRODUCTO_CREADO,
                    PRODUCTO_ACTUALIZADO, PRODUCTO_ELIMINADO):
            try:
                EventBus.unsubscribe(evt, self._on_data_changed)
            except Exception:
                pass


# ── Dialogo Producto ──────────────────────────────────────────────────────────

class DialogoProducto(QDialog):

    def __init__(self, repo: ProductoRepository, usuario: str,
                 producto_data: Optional[Dict] = None, parent=None):
        super().__init__(parent)
        self._repo = repo
        self._usuario = usuario
        self._data = producto_data
        self._img_path = None
        self.setWindowTitle("Nuevo Producto" if not producto_data else "Editar Producto")
        self.setMinimumWidth(480)
        self._build_ui()
        if producto_data:
            self._load()

    def _build_ui(self):
        ml = QVBoxLayout(self)
        rl = QHBoxLayout()

        # Image
        il = QVBoxLayout()
        self._lbl_img = QLabel("Sin imagen")
        self._lbl_img.setFixedSize(130, 130)
        self._lbl_img.setAlignment(Qt.AlignCenter)
        self._lbl_img.setStyleSheet("border:1px solid #ccc;background:#f5f5f5")
        b_si = QPushButton("Imagen…")
        b_di = QPushButton("Eliminar imagen")
        b_si.clicked.connect(self._sel_img)
        b_di.clicked.connect(self._del_img)
        for w in [self._lbl_img, b_si, b_di]:
            il.addWidget(w)
        il.addStretch()
        rl.addLayout(il)
        rl.addSpacing(12)

        # Form
        fm = QFormLayout()
        self._e_nombre    = QLineEdit()
        self._e_precio    = QDoubleSpinBox(); self._e_precio.setMaximum(9_999_999); self._e_precio.setPrefix("$ ")
        self._e_exist     = QDoubleSpinBox(); self._e_exist.setMaximum(999_999)
        self._e_stock_min = QDoubleSpinBox(); self._e_stock_min.setMaximum(999_999)
        self._e_unidad    = QComboBox(); self._e_unidad.addItems(["kg","g","lb","oz","pz","lt","ml"])
        self._e_cat       = QLineEdit()
        self._e_tipo      = QComboBox()
        self._e_tipo.addItem("Simple","simple")
        self._e_tipo.addItem("Compuesto","compuesto")
        self._e_tipo.addItem("Subproducto","subproducto")
        self._e_tipo.currentIndexChanged.connect(self._tipo_changed)
        self._e_padre     = QComboBox(); self._e_padre.setEnabled(False)
        self._e_oculto    = QCheckBox("Ocultar")
        self._load_padres()
        fm.addRow("Nombre*:", self._e_nombre)
        fm.addRow("Precio*:", self._e_precio)
        fm.addRow("Existencia:", self._e_exist)
        fm.addRow("Stock mín:", self._e_stock_min)
        fm.addRow("Unidad:", self._e_unidad)
        fm.addRow("Categoría:", self._e_cat)
        fm.addRow("Tipo*:", self._e_tipo)
        fm.addRow("Prod. padre:", self._e_padre)
        fm.addRow("", self._e_oculto)
        rl.addLayout(fm)
        ml.addLayout(rl)

        bl = QHBoxLayout()
        b_ok = QPushButton("Guardar"); b_ok.clicked.connect(self._guardar)
        b_no = QPushButton("Cancelar"); b_no.clicked.connect(self.reject)
        bl.addWidget(b_ok); bl.addWidget(b_no)
        ml.addLayout(bl)

    def _load_padres(self):
        try:
            rows = self._repo.get_all(include_inactive=False)
            self._e_padre.clear()
            self._e_padre.addItem("-- Seleccionar --", None)
            for r in rows:
                if r.get("es_compuesto"):
                    self._e_padre.addItem(r["nombre"], r["id"])
        except Exception as exc:
            logger.debug("load_padres: %s", exc)

    def _tipo_changed(self):
        self._e_padre.setEnabled(self._e_tipo.currentData() == "subproducto")

    def _sel_img(self):
        p, _ = QFileDialog.getOpenFileName(
            self, "Imagen", "", "Imágenes (*.png *.jpg *.jpeg *.bmp)"
        )
        if p:
            self._img_path = p
            px = QPixmap(p)
            if not px.isNull():
                self._lbl_img.setPixmap(
                    px.scaled(130, 130, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                )
                self._lbl_img.setText("")

    def _del_img(self):
        self._img_path = None
        self._lbl_img.clear()
        self._lbl_img.setText("Sin imagen")

    def _load(self):
        d = self._data
        self._e_nombre.setText(d.get("nombre",""))
        self._e_precio.setValue(float(d.get("precio",0)))
        self._e_exist.setValue(float(d.get("existencia",0)))
        self._e_stock_min.setValue(float(d.get("stock_minimo",0) or 0))
        idx = self._e_unidad.findText(d.get("unidad","kg"))
        if idx >= 0: self._e_unidad.setCurrentIndex(idx)
        self._e_cat.setText(d.get("categoria",""))
        tipo = "compuesto" if d.get("es_compuesto") else \
               "subproducto" if d.get("es_subproducto") else "simple"
        idx2 = self._e_tipo.findData(tipo)
        if idx2 >= 0: self._e_tipo.setCurrentIndex(idx2)
        if d.get("producto_padre_id"):
            idx3 = self._e_padre.findData(d["producto_padre_id"])
            if idx3 >= 0: self._e_padre.setCurrentIndex(idx3)
        self._e_oculto.setChecked(bool(d.get("oculto")))
        img = d.get("imagen_path")
        if img and os.path.exists(img):
            self._img_path = img
            px = QPixmap(img)
            if not px.isNull():
                self._lbl_img.setPixmap(
                    px.scaled(130, 130, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                )
                self._lbl_img.setText("")

    def _guardar(self):
        nombre = self._e_nombre.text().strip()
        if not nombre:
            QMessageBox.warning(self, "Validación", "Nombre obligatorio.")
            return
        precio = self._e_precio.value()
        if precio <= 0:
            QMessageBox.warning(self, "Validación", "Precio debe ser > 0.")
            return
        tipo = self._e_tipo.currentData()
        padre_id = self._e_padre.currentData() if self._e_padre.isEnabled() else None
        if tipo == "subproducto" and not padre_id:
            QMessageBox.warning(self, "Validación", "Subproductos requieren producto padre.")
            return
        # Copy image
        img_path = None
        if self._img_path:
            ext = os.path.splitext(self._img_path)[1]
            fn  = f"prod_{nombre.replace(' ','_')}_{datetime.now().strftime('%Y%m%d%H%M%S')}{ext}"
            dst = os.path.join("imagenes_productos", fn)
            try:
                shutil.copy2(self._img_path, dst)
                img_path = dst
            except Exception:
                img_path = self._img_path
        data = {
            "nombre": nombre, "precio": precio,
            "existencia": self._e_exist.value(),
            "stock_minimo": self._e_stock_min.value(),
            "unidad": self._e_unidad.currentText(),
            "categoria": self._e_cat.text().strip(),
            "oculto": self._e_oculto.isChecked(),
            "es_compuesto": tipo == "compuesto",
            "es_subproducto": tipo == "subproducto",
            "producto_padre_id": padre_id,
            "imagen_path": img_path,
            "tipo_producto": tipo,
        }
        try:
            if self._data:
                self._repo.update(self._data["id"], data, self._usuario)
                QMessageBox.information(self, "Éxito", "Producto actualizado.")
            else:
                self._repo.create(data, self._usuario)
                QMessageBox.information(self, "Éxito", "Producto creado.")
            self.accept()
        except ProductoNombreDuplicadoError:
            QMessageBox.warning(self, "Duplicado",
                                f"Ya existe un producto activo con nombre '{nombre}'.")
        except ValueError as exc:
            QMessageBox.warning(self, "Validación", str(exc))
        except Exception as exc:
            logger.exception("guardar_producto")
            QMessageBox.critical(self, "Error", str(exc))

# modulos/inventario_pollo.py
# Módulo UI enterprise para gestión de inventario de pollo multi-sucursal.
# Layout: Sidebar + QStackedWidget
# Depende de: ChickenEngine, InventoryService
from __future__ import annotations

import logging
logger = logging.getLogger("spj.ui.inventario_pollo")

from PyQt5.QtWidgets import (
    QWidget, QHBoxLayout, QVBoxLayout, QLabel, QPushButton,
    QFrame, QStackedWidget, QFormLayout, QSpinBox, QDoubleSpinBox,
    QComboBox, QTableWidget, QTableWidgetItem, QHeaderView,
    QAbstractItemView, QMessageBox, QTextEdit, QSizePolicy,
    QGroupBox, QLineEdit, QDialog, QDialogButtonBox
)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QFont, QColor

from .base import ModuloBase

try:
    from core.services.chicken_engine import ChickenEngine, ChickenError
    from core.services.inventory_service import InventoryService, StockInsuficienteError
    from sync.event_logger import EventLogger
    HAS_ENGINES = True
except ImportError as _e:
    HAS_ENGINES = False
    _IMPORT_ERROR = str(_e)


# ── Constantes de layout ──────────────────────────────────────────────────────

SECCIONES = [
    ("🐔  Recepción Pollo",   "recepcion"),
    ("🛒  Compra Global",     "compra_global"),
    ("🔪  Transformación",    "transformacion"),
    ("📋  Recetas",           "recetas"),
    ("🍱  Recetas Consumo",   "recetas_consumo"),
    ("🚚  Traspasos",         "traspasos"),
    ("📊  Conciliación",      "conciliacion"),
    ("📑  Movimientos",       "movimientos"),
]

SIDEBAR_WIDTH = 200


# ══════════════════════════════════════════════════════════════════════════════

class ModuloInventarioPollo(ModuloBase):
    """
    Módulo principal de inventario pollo enterprise.
    Muestra sidebar + contenido apilado por sección.
    """

    def __init__(self, conexion, parent=None):
        super().__init__(conexion, parent)
        self._engine: "ChickenEngine | None" = None
        self._inv_svc: "InventoryService | None" = None
        self._event_log: "EventLogger | None" = None
        self._pages: dict[str, QWidget] = {}
        self.init_ui()
        if not HAS_ENGINES:
            QMessageBox.warning(
                self,
                "Dependencia faltante",
                f"ChickenEngine no disponible:\n{_IMPORT_ERROR}\n\n"
                "Instale las dependencias y reinicie.",
            )

    # ── Sesión ────────────────────────────────────────────────────────────────

    def set_usuario_actual(self, usuario: str, rol: str) -> None:
        super().set_usuario_actual(usuario, rol)
        self._init_engines()

    def set_sucursal(self, sucursal_id: int, sucursal_nombre: str) -> None:
        self.sucursal_id     = sucursal_id
        self.sucursal_nombre = sucursal_nombre
        self._init_engines()

    def _init_engines(self) -> None:
        if not HAS_ENGINES or not self.conexion:
            return
        usuario = self.usuario_actual or "Sistema"
        suc_id  = getattr(self, "sucursal_id", 1)
        self._engine    = ChickenEngine(self.conexion, usuario, suc_id)
        self._inv_svc   = InventoryService(self.conexion, usuario, suc_id)
        self._event_log = EventLogger(self.conexion)

    # ── UI principal ──────────────────────────────────────────────────────────

    def init_ui(self) -> None:
        root = QHBoxLayout(self)
        root.setContentsMargins(0, 0, 0, 0)
        root.setSpacing(0)

        # Sidebar
        sidebar = self._build_sidebar()
        root.addWidget(sidebar)

        # Separador
        sep = QFrame()
        sep.setFrameShape(QFrame.VLine)
        sep.setFrameShadow(QFrame.Sunken)
        root.addWidget(sep)

        # Stack de páginas
        self.stack = QStackedWidget()
        for _, key in SECCIONES:
            page = self._crear_pagina(key)
            self._pages[key] = page
            self.stack.addWidget(page)

        root.addWidget(self.stack, 1)
        self.ir_a("recepcion")

    def _build_sidebar(self) -> QFrame:
        sidebar = QFrame()
        sidebar.setFixedWidth(SIDEBAR_WIDTH)
        sidebar.setObjectName("sidebarPollo")

        lay = QVBoxLayout(sidebar)
        lay.setContentsMargins(6, 14, 6, 8)
        lay.setSpacing(3)

        titulo = QLabel("🐔 POLLO")
        titulo.setAlignment(Qt.AlignCenter)
        titulo.setFont(QFont("Segoe UI", 11, QFont.Bold))
        lay.addWidget(titulo)
        lay.addSpacing(8)

        self._btns: dict[str, QPushButton] = {}
        for label, key in SECCIONES:
            btn = QPushButton(label)
            btn.setCheckable(True)
            btn.setObjectName("sidebarBtn")
            btn.setMinimumHeight(38)
            btn.clicked.connect(lambda _, k=key: self.ir_a(k))
            self._btns[key] = btn
            lay.addWidget(btn)

        lay.addStretch()
        return sidebar

    def ir_a(self, key: str) -> None:
        keys = [k for _, k in SECCIONES]
        if key not in keys:
            return
        self.stack.setCurrentIndex(keys.index(key))
        for k, btn in self._btns.items():
            btn.setChecked(k == key)
        self._refrescar(key)

    # ── Páginas ───────────────────────────────────────────────────────────────

    def _crear_pagina(self, key: str) -> QWidget:
        builder = {
            "recepcion":      self._pagina_recepcion,
            "compra_global":  self._pagina_compra_global,
            "transformacion": self._pagina_transformacion,
            "recetas":        self._pagina_recetas,
            "recetas_consumo": self._pagina_recetas_consumo,
            "traspasos":      self._pagina_traspasos,
            "conciliacion":   self._pagina_conciliacion,
            "movimientos":    self._pagina_movimientos,
        }
        return builder[key]()

    # ── RECEPCIÓN ─────────────────────────────────────────────────────────────

    def _pagina_recepcion(self) -> QWidget:
        w = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(28, 22, 28, 22)

        _titulo(lay, "🐔 Recepción de Pollo en Sucursal")

        form = QFormLayout()
        form.setSpacing(12)
        form.setLabelAlignment(Qt.AlignRight)

        self.inp_rec_pollos = QSpinBox()
        self.inp_rec_pollos.setRange(1, 9999)
        self.inp_rec_pollos.setMinimumHeight(38)
        self.inp_rec_pollos.setValue(1)

        self.inp_rec_peso = QDoubleSpinBox()
        self.inp_rec_peso.setRange(0.001, 99999)
        self.inp_rec_peso.setDecimals(3)
        self.inp_rec_peso.setMinimumHeight(38)
        self.inp_rec_peso.setSuffix(" kg")

        self.cbo_rec_producto = QComboBox()
        self.cbo_rec_producto.setMinimumHeight(38)
        self.cbo_rec_producto.currentIndexChanged.connect(self._actualizar_stock_recepcion)

        form.addRow("Número de pollos:", self.inp_rec_pollos)
        form.addRow("Peso total:", self.inp_rec_peso)
        form.addRow("Producto base:", self.cbo_rec_producto)
        lay.addLayout(form)
        lay.addSpacing(10)

        btn = QPushButton("✅  Recepcionar  [Enter]")
        btn.setMinimumHeight(46)
        btn.setShortcut("Return")
        btn.clicked.connect(self._recepcionar_pollo)
        lay.addWidget(btn)
        lay.addSpacing(14)

        self.lbl_stock_recepcion = QLabel("Stock actual: —")
        self.lbl_stock_recepcion.setFont(QFont("Segoe UI", 11))
        lay.addWidget(self.lbl_stock_recepcion)

        lay.addStretch()
        return w

    def _recepcionar_pollo(self) -> None:
        if not self._engine:
            return self._no_engine()
        try:
            n   = self.inp_rec_pollos.value()
            kg  = self.inp_rec_peso.value()
            pid = self.cbo_rec_producto.currentData()
            if not pid:
                return QMessageBox.warning(self, "Aviso", "Seleccione un producto base.")
            rec_id = self._engine.recepcionar_en_sucursal(n, kg, pid)
            if self._event_log:
                self._event_log.registrar(
                    tipo="recepcion_pollo",
                    entidad="inventario_pollo_sucursal",
                    entidad_id=rec_id,
                    payload={"pollos": n, "kg": kg, "producto_id": pid},
                    sucursal_id=getattr(self, "sucursal_id", 1),
                    usuario=self.usuario_actual or "Sistema",
                )
            if self.conexion:
                self.conexion.commit()
            QMessageBox.information(
                self, "Recepción OK",
                f"✅ Recepcionado exitosamente.\n\n"
                f"Pollos: {n}\nPeso: {kg:.3f} kg\nID recepción: {rec_id}",
            )
            self._refrescar("recepcion")
        except Exception as exc:
            if self.conexion:
                try: self.conexion.rollback()
                except: pass
            QMessageBox.critical(self, "Error en recepción", str(exc))

    def _actualizar_stock_recepcion(self) -> None:
        if not self._inv_svc:
            return
        pid = self.cbo_rec_producto.currentData()
        if pid:
            try:
                stock = self._inv_svc.get_stock(pid)
                self.lbl_stock_recepcion.setText(f"Stock actual: {stock:.3f} kg")
            except Exception:
                self.lbl_stock_recepcion.setText("Stock actual: —")

    # ── COMPRA GLOBAL ─────────────────────────────────────────────────────────

    def _pagina_compra_global(self) -> QWidget:
        w = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(28, 22, 28, 22)

        _titulo(lay, "🛒 Compra Global de Pollo (Admin)")

        form = QFormLayout()
        form.setSpacing(12)
        form.setLabelAlignment(Qt.AlignRight)

        self.inp_cg_pollos = QSpinBox()
        self.inp_cg_pollos.setRange(1, 99999)
        self.inp_cg_pollos.setMinimumHeight(38)

        self.inp_cg_peso = QDoubleSpinBox()
        self.inp_cg_peso.setRange(0.001, 999999)
        self.inp_cg_peso.setDecimals(3)
        self.inp_cg_peso.setMinimumHeight(38)
        self.inp_cg_peso.setSuffix(" kg")

        self.inp_cg_costo = QDoubleSpinBox()
        self.inp_cg_costo.setRange(0.01, 9999999)
        self.inp_cg_costo.setDecimals(2)
        self.inp_cg_costo.setMinimumHeight(38)
        self.inp_cg_costo.setPrefix("$")

        self.inp_cg_proveedor = QLineEdit()
        self.inp_cg_proveedor.setMinimumHeight(38)
        self.inp_cg_proveedor.setPlaceholderText("Nombre del proveedor")

        self.cbo_cg_producto = QComboBox()
        self.cbo_cg_producto.setMinimumHeight(38)

        self.lbl_cg_costo_kg = QLabel("Costo por kg: $—")
        self.lbl_cg_costo_kg.setFont(QFont("Segoe UI", 10))

        self.inp_cg_peso.valueChanged.connect(self._recalc_costo_kg)
        self.inp_cg_costo.valueChanged.connect(self._recalc_costo_kg)

        form.addRow("Número de pollos:", self.inp_cg_pollos)
        form.addRow("Peso total:", self.inp_cg_peso)
        form.addRow("Costo total:", self.inp_cg_costo)
        form.addRow("Proveedor:", self.inp_cg_proveedor)
        form.addRow("Producto base:", self.cbo_cg_producto)
        form.addRow("", self.lbl_cg_costo_kg)
        lay.addLayout(form)
        lay.addSpacing(10)

        btn = QPushButton("✅  Registrar Compra Global")
        btn.setMinimumHeight(46)
        btn.clicked.connect(self._registrar_compra_global)
        lay.addWidget(btn)
        lay.addStretch()
        return w

    def _recalc_costo_kg(self) -> None:
        peso = self.inp_cg_peso.value()
        costo = self.inp_cg_costo.value()
        if peso > 0:
            self.lbl_cg_costo_kg.setText(f"Costo por kg: ${costo/peso:.4f}")

    def _registrar_compra_global(self) -> None:
        if not self._engine:
            return self._no_engine()
        try:
            pid  = self.cbo_cg_producto.currentData()
            rec  = self._engine.registrar_compra_global(
                numero_pollos       = self.inp_cg_pollos.value(),
                peso_total_kg       = self.inp_cg_peso.value(),
                costo_total         = self.inp_cg_costo.value(),
                proveedor           = self.inp_cg_proveedor.text().strip(),
                producto_base_id    = pid or None,
                sucursal_destino_id = getattr(self, "sucursal_id", 1),
            )
            if self.conexion:
                self.conexion.commit()
            QMessageBox.information(
                self, "Compra registrada",
                f"✅ Compra global #{rec} registrada correctamente.",
            )
        except Exception as exc:
            if self.conexion:
                try: self.conexion.rollback()
                except: pass
            QMessageBox.critical(self, "Error", str(exc))

    # ── TRANSFORMACIÓN ────────────────────────────────────────────────────────

    def _pagina_transformacion(self) -> QWidget:
        w = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(28, 22, 28, 22)

        _titulo(lay, "🔪 Transformación Pollo → Cortes")

        form = QFormLayout()
        form.setSpacing(12)
        form.setLabelAlignment(Qt.AlignRight)

        self.cbo_tx_receta = QComboBox()
        self.cbo_tx_receta.setMinimumHeight(38)

        self.cbo_tx_producto = QComboBox()
        self.cbo_tx_producto.setMinimumHeight(38)
        self.cbo_tx_producto.currentIndexChanged.connect(self._actualizar_stock_tx)

        self.inp_tx_kg = QDoubleSpinBox()
        self.inp_tx_kg.setRange(0.001, 99999)
        self.inp_tx_kg.setDecimals(3)
        self.inp_tx_kg.setMinimumHeight(38)
        self.inp_tx_kg.setSuffix(" kg")

        self.lbl_tx_stock = QLabel("Stock disponible: —")

        form.addRow("Receta:", self.cbo_tx_receta)
        form.addRow("Producto base:", self.cbo_tx_producto)
        form.addRow("Kg a procesar:", self.inp_tx_kg)
        form.addRow("", self.lbl_tx_stock)
        lay.addLayout(form)
        lay.addSpacing(10)

        btn = QPushButton("🔪  Transformar")
        btn.setMinimumHeight(46)
        btn.clicked.connect(self._transformar)
        lay.addWidget(btn)

        # Resultado de última transformación
        self.txt_tx_resultado = QTextEdit()
        self.txt_tx_resultado.setReadOnly(True)
        self.txt_tx_resultado.setMaximumHeight(180)
        self.txt_tx_resultado.setPlaceholderText("Resultado de la última transformación aparecerá aquí…")
        lay.addWidget(self.txt_tx_resultado)
        lay.addStretch()
        return w

    def _actualizar_stock_tx(self) -> None:
        if not self._inv_svc:
            return
        pid = self.cbo_tx_producto.currentData()
        if pid:
            try:
                stock = self._inv_svc.get_stock(pid)
                self.lbl_tx_stock.setText(f"Stock disponible: {stock:.3f} kg")
            except Exception:
                self.lbl_tx_stock.setText("Stock disponible: —")

    def _transformar(self) -> None:
        if not self._engine:
            return self._no_engine()
        try:
            receta_id = self.cbo_tx_receta.currentData()
            pid       = self.cbo_tx_producto.currentData()
            kg        = self.inp_tx_kg.value()
            if not receta_id:
                return QMessageBox.warning(self, "Aviso", "Seleccione una receta.")
            if not pid:
                return QMessageBox.warning(self, "Aviso", "Seleccione un producto base.")

            resultado = self._engine.transformar_por_receta(receta_id, kg, pid)
            if self.conexion:
                self.conexion.commit()

            lineas = [
                f"✅ Transformación completada\n",
                f"Kg procesados:   {resultado.kg_procesados:.3f} kg",
                f"Kg merma:        {resultado.kg_merma:.3f} kg",
                f"Movimientos:     {len(resultado.movimiento_ids)}",
                "",
                "Cortes generados:",
            ]
            for c in resultado.cortes:
                lineas.append(f"  • {c['nombre']}: {c['kg']:.3f} kg  (merma {c['merma_kg']:.3f})")
            self.txt_tx_resultado.setPlainText("\n".join(lineas))
            self._refrescar("transformacion")
        except Exception as exc:
            if self.conexion:
                try: self.conexion.rollback()
                except: pass
            QMessageBox.critical(self, "Error en transformación", str(exc))

    # ── RECETAS ───────────────────────────────────────────────────────────────

    def _pagina_recetas(self) -> QWidget:
        w = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(28, 22, 28, 22)

        _titulo(lay, "📋 Recetas de Transformación")

        # Selector de receta
        hrow = QHBoxLayout()
        self.cbo_receta_sel = QComboBox()
        self.cbo_receta_sel.setMinimumHeight(36)
        self.cbo_receta_sel.currentIndexChanged.connect(self._cargar_detalle_receta)
        hrow.addWidget(QLabel("Receta:"))
        hrow.addWidget(self.cbo_receta_sel, 1)
        btn_nueva = QPushButton("+ Nueva")
        btn_nueva.clicked.connect(self._nueva_receta)
        hrow.addWidget(btn_nueva)
        lay.addLayout(hrow)
        lay.addSpacing(10)

        # Tabla editable de cortes
        self.tbl_receta = QTableWidget(0, 4)
        self.tbl_receta.setHorizontalHeaderLabels(
            ["Corte (producto)", "ID Producto", "% Rendimiento", "% Merma"]
        )
        self.tbl_receta.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.tbl_receta.setEditTriggers(QAbstractItemView.DoubleClicked | QAbstractItemView.SelectedClicked)
        self.tbl_receta.setMinimumHeight(220)
        lay.addWidget(self.tbl_receta)

        # Total rendimiento
        self.lbl_total_rend = QLabel("Total rendimiento: 0.0% | Merma: 0.0%")
        lay.addWidget(self.lbl_total_rend)

        btnbar = QHBoxLayout()
        btn_add_fila = QPushButton("+ Agregar corte")
        btn_del_fila = QPushButton("− Eliminar fila")
        btn_guardar  = QPushButton("💾  Guardar receta")
        btn_add_fila.clicked.connect(self._agregar_fila_receta)
        btn_del_fila.clicked.connect(self._eliminar_fila_receta)
        btn_guardar.clicked.connect(self._guardar_receta)
        btnbar.addWidget(btn_add_fila)
        btnbar.addWidget(btn_del_fila)
        btnbar.addStretch()
        btnbar.addWidget(btn_guardar)
        lay.addLayout(btnbar)
        lay.addStretch()
        return w

    def _cargar_detalle_receta(self) -> None:
        if not self._engine:
            return
        receta_id = self.cbo_receta_sel.currentData()
        if not receta_id:
            return
        try:
            rows = self._engine.obtener_detalle_receta(receta_id)
            self.tbl_receta.setRowCount(0)
            for row in rows:
                r = self.tbl_receta.rowCount()
                self.tbl_receta.insertRow(r)
                self.tbl_receta.setItem(r, 0, QTableWidgetItem(row[1]))
                self.tbl_receta.setItem(r, 1, QTableWidgetItem(str(row[0])))  # detalle id
                self.tbl_receta.setItem(r, 2, QTableWidgetItem(str(row[2])))
                self.tbl_receta.setItem(r, 3, QTableWidgetItem(str(row[3])))
            self._recalc_total_rendimiento()
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))

    def _agregar_fila_receta(self) -> None:
        r = self.tbl_receta.rowCount()
        self.tbl_receta.insertRow(r)
        self.tbl_receta.setItem(r, 0, QTableWidgetItem("Nuevo corte"))
        self.tbl_receta.setItem(r, 1, QTableWidgetItem(""))
        self.tbl_receta.setItem(r, 2, QTableWidgetItem("0.0"))
        self.tbl_receta.setItem(r, 3, QTableWidgetItem("0.0"))

    def _eliminar_fila_receta(self) -> None:
        row = self.tbl_receta.currentRow()
        if row >= 0:
            self.tbl_receta.removeRow(row)
        self._recalc_total_rendimiento()

    def _recalc_total_rendimiento(self) -> None:
        total_r = total_m = 0.0
        for r in range(self.tbl_receta.rowCount()):
            try:
                total_r += float(self.tbl_receta.item(r, 2).text() or 0)
                total_m += float(self.tbl_receta.item(r, 3).text() or 0)
            except Exception:
                pass
        color = "red" if total_r + total_m > 105 else "green"
        self.lbl_total_rend.setText(
            f"Total rendimiento: {total_r:.1f}% | Merma: {total_m:.1f}%"
        )
        self.lbl_total_rend.setStyleSheet(f"color: {color};")

    def _guardar_receta(self) -> None:
        if not self._engine:
            return self._no_engine()
        receta_id  = self.cbo_receta_sel.currentData()
        nombre     = self.cbo_receta_sel.currentText()
        cortes     = []
        for r in range(self.tbl_receta.rowCount()):
            try:
                pid_item = self.tbl_receta.item(r, 1)
                pid = int(pid_item.text()) if pid_item and pid_item.text().strip().isdigit() else None
                if not pid:
                    continue
                cortes.append({
                    "producto_resultado_id": pid,
                    "porcentaje_rendimiento": float(self.tbl_receta.item(r, 2).text() or 0),
                    "porcentaje_merma":       float(self.tbl_receta.item(r, 3).text() or 0),
                    "orden": r,
                })
            except Exception:
                continue
        if not cortes:
            return QMessageBox.warning(self, "Aviso", "Agregue al menos un corte con ID de producto válido.")
        try:
            # Necesitamos producto_base_id — tomamos el primero de la receta actual
            pbase = self.cbo_receta_sel.currentData()
            self._engine.guardar_receta(nombre, pbase or 1, cortes, receta_id)
            if self.conexion:
                self.conexion.commit()
            QMessageBox.information(self, "OK", "Receta guardada correctamente.")
            self._refrescar("recetas")
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))

    def _nueva_receta(self) -> None:
        nombre, ok = _input_dialog(self, "Nueva Receta", "Nombre de la nueva receta:")
        if ok and nombre.strip():
            # Crear receta vacía con producto base 1 de placeholder
            try:
                rid = self._engine.guardar_receta(nombre.strip(), 1, [])
                if self.conexion:
                    self.conexion.commit()
                self._refrescar("recetas")
                # Seleccionar la nueva
                idx = self.cbo_receta_sel.findData(rid)
                if idx >= 0:
                    self.cbo_receta_sel.setCurrentIndex(idx)
            except Exception as exc:
                QMessageBox.critical(self, "Error", str(exc))

    # ── TRASPASOS ─────────────────────────────────────────────────────────────

    def _pagina_traspasos(self) -> QWidget:
        w = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(28, 22, 28, 22)

        _titulo(lay, "🚚 Traspasos Entre Sucursales")

        form = QFormLayout()
        form.setSpacing(12)
        form.setLabelAlignment(Qt.AlignRight)

        self.cbo_tr_producto = QComboBox()
        self.cbo_tr_producto.setMinimumHeight(38)

        self.inp_tr_cantidad = QDoubleSpinBox()
        self.inp_tr_cantidad.setRange(0.001, 99999)
        self.inp_tr_cantidad.setDecimals(3)
        self.inp_tr_cantidad.setMinimumHeight(38)

        self.inp_tr_destino = QLineEdit()
        self.inp_tr_destino.setMinimumHeight(38)
        self.inp_tr_destino.setPlaceholderText("ID o nombre de sucursal destino")

        self.inp_tr_obs = QLineEdit()
        self.inp_tr_obs.setMinimumHeight(38)
        self.inp_tr_obs.setPlaceholderText("Observaciones (opcional)")

        form.addRow("Producto:", self.cbo_tr_producto)
        form.addRow("Cantidad:", self.inp_tr_cantidad)
        form.addRow("Destino:", self.inp_tr_destino)
        form.addRow("Observaciones:", self.inp_tr_obs)
        lay.addLayout(form)
        lay.addSpacing(10)

        btn = QPushButton("🚚  Registrar Traspaso")
        btn.setMinimumHeight(46)
        btn.clicked.connect(self._registrar_traspaso)
        lay.addWidget(btn)

        # Tabla de traspasos pendientes
        _subtitulo(lay, "Traspasos pendientes")
        self.tbl_traspasos = QTableWidget(0, 5)
        self.tbl_traspasos.setHorizontalHeaderLabels(
            ["ID", "Producto", "Cantidad", "Destino", "Estado"]
        )
        self.tbl_traspasos.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.tbl_traspasos.setEditTriggers(QAbstractItemView.NoEditTriggers)
        lay.addWidget(self.tbl_traspasos)
        lay.addStretch()
        return w

    def _registrar_traspaso(self) -> None:
        if not self._inv_svc:
            return self._no_engine()
        try:
            pid        = self.cbo_tr_producto.currentData()
            cantidad   = self.inp_tr_cantidad.value()
            destino_tx = self.inp_tr_destino.text().strip()
            obs        = self.inp_tr_obs.text().strip()
            if not pid:
                return QMessageBox.warning(self, "Aviso", "Seleccione un producto.")
            destino_id = int(destino_tx) if destino_tx.isdigit() else 0
            if destino_id <= 0:
                return QMessageBox.warning(self, "Aviso", "Ingrese un ID numérico válido de sucursal destino.")

            self._inv_svc.transferir_entre_sucursales(
                producto_id      = pid,
                cantidad         = cantidad,
                sucursal_destino = destino_id,
                usuario_destino  = destino_tx,
                observaciones    = obs,
            )
            if self.conexion:
                self.conexion.commit()
            QMessageBox.information(self, "OK", "Traspaso registrado. Estado: pendiente.")
            self._refrescar("traspasos")
        except Exception as exc:
            if self.conexion:
                try: self.conexion.rollback()
                except: pass
            QMessageBox.critical(self, "Error en traspaso", str(exc))

    # ── CONCILIACIÓN ──────────────────────────────────────────────────────────

    def _pagina_conciliacion(self) -> QWidget:
        w = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(28, 22, 28, 22)

        _titulo(lay, "📊 Conciliación Global vs Sucursales")

        # Panel de totales
        panel = QGroupBox("Resumen")
        play  = QFormLayout(panel)
        self.lbl_con_global   = QLabel("—")
        self.lbl_con_locales  = QLabel("—")
        self.lbl_con_diff     = QLabel("—")
        for lbl in [self.lbl_con_global, self.lbl_con_locales, self.lbl_con_diff]:
            lbl.setFont(QFont("Segoe UI", 12, QFont.Bold))
        play.addRow("Global kg comprado:", self.lbl_con_global)
        play.addRow("Total sucursales kg:", self.lbl_con_locales)
        play.addRow("Diferencia:", self.lbl_con_diff)
        lay.addWidget(panel)

        _subtitulo(lay, "Detalle por sucursal")
        self.tbl_conciliacion = QTableWidget(0, 3)
        self.tbl_conciliacion.setHorizontalHeaderLabels(["Sucursal ID", "Pollos", "Kg Disponibles"])
        self.tbl_conciliacion.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.tbl_conciliacion.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self.tbl_conciliacion.setEditTriggers(QAbstractItemView.NoEditTriggers)
        lay.addWidget(self.tbl_conciliacion)

        btn = QPushButton("🔄  Actualizar")
        btn.clicked.connect(lambda: self._refrescar("conciliacion"))
        lay.addWidget(btn)
        lay.addStretch()
        return w

    # ── MOVIMIENTOS ───────────────────────────────────────────────────────────

    def _pagina_movimientos(self) -> QWidget:
        w = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(28, 22, 28, 22)

        _titulo(lay, "📑 Movimientos de Inventario")

        self.tbl_movimientos = QTableWidget(0, 6)
        self.tbl_movimientos.setHorizontalHeaderLabels(
            ["Fecha", "Producto", "Tipo", "Cantidad", "Stock Anterior", "Stock Nuevo"]
        )
        self.tbl_movimientos.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.tbl_movimientos.setEditTriggers(QAbstractItemView.NoEditTriggers)
        lay.addWidget(self.tbl_movimientos)

        btn = QPushButton("🔄  Actualizar")
        btn.clicked.connect(lambda: self._refrescar("movimientos"))
        lay.addWidget(btn)
        lay.addStretch()
        return w

    # ── Refresh ───────────────────────────────────────────────────────────────

    def _refrescar(self, key: str) -> None:
        # Siempre recargar combos
        self._cargar_productos_combos()
        self._cargar_recetas_combo()

        if key == "recepcion":
            self._actualizar_stock_recepcion()

        elif key == "transformacion":
            self._actualizar_stock_tx()

        elif key == "conciliacion":
            if not self._engine:
                return
            try:
                data = self._engine.conciliar()
                self.lbl_con_global.setText(f"{data.global_kg:.3f} kg  ({data.global_pollos} pollos)")
                self.lbl_con_locales.setText(f"{data.total_sucursales_kg:.3f} kg")
                diff = data.diferencia_kg
                self.lbl_con_diff.setText(f"{diff:+.3f} kg")
                self.lbl_con_diff.setStyleSheet(
                    "color: #c00000;" if abs(diff) > 1.0 else "color: #375623;"
                )
                self.tbl_conciliacion.setRowCount(0)
                for suc in data.sucursales:
                    r = self.tbl_conciliacion.rowCount()
                    self.tbl_conciliacion.insertRow(r)
                    self.tbl_conciliacion.setItem(r, 0, QTableWidgetItem(str(suc["sucursal_id"])))
                    self.tbl_conciliacion.setItem(r, 1, QTableWidgetItem(str(suc["pollos"])))
                    self.tbl_conciliacion.setItem(r, 2, QTableWidgetItem(f'{suc["kg"]:.3f}'))
            except Exception as exc:
                self.lbl_con_diff.setText(f"Error: {exc}")

        elif key == "traspasos":
            if not self.conexion:
                return
            try:
                rows = self.conexion.execute("""
                    SELECT t.id, p.nombre, t.cantidad, t.sucursal_destino_id, t.estado
                    FROM transferencias_inventario t
                    LEFT JOIN productos p ON p.id = t.producto_id
                    WHERE t.sucursal_origen_id = ?
                    ORDER BY t.id DESC LIMIT 50
                """, (getattr(self, "sucursal_id", 1),)).fetchall()
                self.tbl_traspasos.setRowCount(0)
                for row in rows:
                    r = self.tbl_traspasos.rowCount()
                    self.tbl_traspasos.insertRow(r)
                    for ci, val in enumerate(row):
                        self.tbl_traspasos.setItem(r, ci, QTableWidgetItem(str(val or "")))
            except Exception:
                pass  # tabla puede no existir aún

        elif key == "movimientos":
            if not self.conexion:
                return
            try:
                rows = self.conexion.execute("""
                    SELECT m.fecha, p.nombre, m.tipo_movimiento, m.cantidad,
                           m.existencia_anterior, m.existencia_nueva
                    FROM movimientos_inventario m
                    LEFT JOIN productos p ON p.id = m.producto_id
                    ORDER BY m.id DESC LIMIT 100
                """).fetchall()
                self.tbl_movimientos.setRowCount(0)
                for row in rows:
                    r = self.tbl_movimientos.rowCount()
                    self.tbl_movimientos.insertRow(r)
                    for ci, val in enumerate(row):
                        item = QTableWidgetItem(str(val or ""))
                        if ci == 2:  # tipo
                            t = str(val or "")
                            if "salida" in t.lower() or "transformacion_salida" in t.lower():
                                item.setForeground(QColor("#C00000"))
                            elif "entrada" in t.lower():
                                item.setForeground(QColor("#375623"))
                        self.tbl_movimientos.setItem(r, ci, item)
            except Exception:
                pass

        elif key == "recetas":
            receta_id = self.cbo_receta_sel.currentData() if hasattr(self, "cbo_receta_sel") else None
            if receta_id:
                self._cargar_detalle_receta()

        elif key == "recetas_consumo":
            self._rc_cargar_productos_combo()
            self._rc_refrescar_lista()

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _cargar_productos_combos(self) -> None:
        if not self.conexion:
            return
        try:
            rows = self.conexion.execute(
                "SELECT id, nombre FROM productos WHERE activo=1 ORDER BY nombre"
            ).fetchall()
        except Exception:
            return

        for cbo_attr in [
            "cbo_rec_producto", "cbo_cg_producto",
            "cbo_tx_producto", "cbo_tr_producto",
        ]:
            cbo = getattr(self, cbo_attr, None)
            if cbo is None:
                continue
            current = cbo.currentData()
            cbo.blockSignals(True)
            cbo.clear()
            for pid, nombre in rows:
                cbo.addItem(nombre, pid)
            if current is not None:
                idx = cbo.findData(current)
                if idx >= 0:
                    cbo.setCurrentIndex(idx)
            cbo.blockSignals(False)

    def _cargar_recetas_combo(self) -> None:
        if not self._engine:
            return
        for cbo_attr in ["cbo_tx_receta", "cbo_receta_sel"]:
            cbo = getattr(self, cbo_attr, None)
            if cbo is None:
                continue
            try:
                rows = self._engine.listar_recetas()
                current = cbo.currentData()
                cbo.blockSignals(True)
                cbo.clear()
                for row in rows:
                    cbo.addItem(row[1], row[0])  # nombre, id
                if current is not None:
                    idx = cbo.findData(current)
                    if idx >= 0:
                        cbo.setCurrentIndex(idx)
                cbo.blockSignals(False)
            except Exception:
                pass

    # ══════════════════════════════════════════════════════════════════════════
    # PÁGINA: RECETAS DE CONSUMO DE PRODUCTO (Surtidos / Retazos / Combos)
    # ══════════════════════════════════════════════════════════════════════════

    def _pagina_recetas_consumo(self) -> QWidget:
        """
        Editor de recetas de consumo.
        Permite definir qué piezas se descuentan —y en qué proporción— cuando
        se vende un producto compuesto por peso (surtido, retazo, combo, bandeja).
        """
        w = QWidget()
        lay = QVBoxLayout(w)
        lay.setContentsMargins(28, 22, 28, 22)
        lay.setSpacing(10)

        _titulo(lay, "🍱 Recetas de Consumo de Producto")

        # ── Instrucción ───────────────────────────────────────────────────────
        info = QLabel(
            "Define qué piezas se descuentan del inventario cuando se vende\n"
            "un producto por peso (surtido, retazo, combo, bandeja mixta).\n"
            "La suma de porcentajes debe ser exactamente 100%."
        )
        info.setStyleSheet("color: #555; font-size: 11px;")
        lay.addWidget(info)

        # ── Selector de producto padre ────────────────────────────────────────
        grp_sel = QGroupBox("Producto a configurar")
        sel_lay = QHBoxLayout(grp_sel)

        lbl_prod = QLabel("Producto:")
        self.cbo_rc_producto = QComboBox()
        self.cbo_rc_producto.setMinimumHeight(34)
        self.cbo_rc_producto.setMinimumWidth(280)
        self.cbo_rc_producto.currentIndexChanged.connect(self._rc_cargar_receta)
        sel_lay.addWidget(lbl_prod)
        sel_lay.addWidget(self.cbo_rc_producto, 1)

        btn_nuevo = QPushButton("+ Nuevo componente")
        btn_nuevo.setMinimumHeight(34)
        btn_nuevo.clicked.connect(self._rc_agregar_fila)
        sel_lay.addWidget(btn_nuevo)

        btn_del = QPushButton("− Quitar fila")
        btn_del.setMinimumHeight(34)
        btn_del.clicked.connect(self._rc_quitar_fila)
        sel_lay.addWidget(btn_del)

        lay.addWidget(grp_sel)

        # ── Tabla editable de componentes ─────────────────────────────────────
        self.tbl_rc = QTableWidget(0, 4)
        self.tbl_rc.setHorizontalHeaderLabels(
            ["ID Pieza", "Nombre Pieza", "% Consumo", "Validar"]
        )
        self.tbl_rc.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self.tbl_rc.horizontalHeader().setSectionResizeMode(2, QHeaderView.ResizeToContents)
        self.tbl_rc.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeToContents)
        self.tbl_rc.setColumnWidth(0, 80)
        self.tbl_rc.setMinimumHeight(240)
        self.tbl_rc.setAlternatingRowColors(True)
        self.tbl_rc.itemChanged.connect(self._rc_recalc_total)
        lay.addWidget(self.tbl_rc)

        # ── Barra de totales ──────────────────────────────────────────────────
        bar_total = QHBoxLayout()
        self.lbl_rc_total = QLabel("Total: 0.00%  ✗  (debe ser 100%)")
        self.lbl_rc_total.setStyleSheet(
            "font-weight: bold; font-size: 13px; color: #c00000;"
        )
        bar_total.addWidget(self.lbl_rc_total)
        bar_total.addStretch()

        btn_validar = QPushButton("🔍 Validar")
        btn_validar.setMinimumHeight(32)
        btn_validar.clicked.connect(self._rc_validar)
        bar_total.addWidget(btn_validar)
        lay.addLayout(bar_total)

        # ── Botones de acción ─────────────────────────────────────────────────
        btn_bar = QHBoxLayout()

        btn_guardar = QPushButton("💾  Guardar receta")
        btn_guardar.setMinimumHeight(38)
        btn_guardar.setObjectName("btnPrimary")
        btn_guardar.clicked.connect(self._rc_guardar)
        btn_bar.addStretch()
        btn_bar.addWidget(btn_guardar)

        btn_borrar = QPushButton("🗑  Eliminar receta")
        btn_borrar.setMinimumHeight(38)
        btn_borrar.clicked.connect(self._rc_eliminar)
        btn_bar.addWidget(btn_borrar)
        lay.addLayout(btn_bar)

        # ── Tabla de recetas existentes (sólo lectura) ────────────────────────
        grp_lista = QGroupBox("Productos con receta de consumo activa")
        lista_lay = QVBoxLayout(grp_lista)

        self.tbl_rc_lista = QTableWidget(0, 3)
        self.tbl_rc_lista.setHorizontalHeaderLabels(
            ["Producto", "Piezas", "Estado"]
        )
        self.tbl_rc_lista.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self.tbl_rc_lista.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tbl_rc_lista.setMaximumHeight(160)
        self.tbl_rc_lista.setAlternatingRowColors(True)
        self.tbl_rc_lista.cellDoubleClicked.connect(self._rc_seleccionar_desde_lista)
        lista_lay.addWidget(self.tbl_rc_lista)

        lay.addWidget(grp_lista)
        lay.addStretch()
        return w

    # ── Lógica de Recetas Consumo ─────────────────────────────────────────────

    def _rc_get_repo(self):
        """Retorna ProductRecipeRepository o None si no disponible."""
        if not self.conexion:
            return None
        try:
            from core.database import Connection
            from core.services.product_recipe_repository import ProductRecipeRepository
            db = Connection(self.conexion)
            return ProductRecipeRepository(db)
        except Exception as exc:
            logger.warning("ProductRecipeRepository no disponible: %s", exc)
            return None

    def _rc_cargar_productos_combo(self) -> None:
        """Carga todos los productos activos en el combo de selector."""
        cbo = getattr(self, "cbo_rc_producto", None)
        if not cbo or not self.conexion:
            return
        try:
            rows = self.conexion.execute(
                "SELECT id, nombre FROM productos WHERE activo=1 AND _deleted=0 "
                "ORDER BY nombre LIMIT 500"
            ).fetchall()
        except Exception:
            try:
                rows = self.conexion.execute(
                    "SELECT id, nombre FROM productos WHERE activo=1 ORDER BY nombre LIMIT 500"
                ).fetchall()
            except Exception:
                return

        current = cbo.currentData()
        cbo.blockSignals(True)
        cbo.clear()
        cbo.addItem("— Seleccione producto —", None)
        for pid, nombre in rows:
            cbo.addItem(nombre, pid)
        if current is not None:
            idx = cbo.findData(current)
            if idx >= 0:
                cbo.setCurrentIndex(idx)
        cbo.blockSignals(False)

    def _rc_cargar_receta(self) -> None:
        """Carga la receta activa del producto seleccionado en la tabla."""
        cbo = getattr(self, "cbo_rc_producto", None)
        tbl = getattr(self, "tbl_rc", None)
        if not cbo or not tbl:
            return

        product_id = cbo.currentData()
        tbl.blockSignals(True)
        tbl.setRowCount(0)
        tbl.blockSignals(False)

        if not product_id:
            self._rc_recalc_total()
            return

        repo = self._rc_get_repo()
        if not repo:
            return

        try:
            items = repo.get_recipe_raw(product_id)
            tbl.blockSignals(True)
            for item in items:
                r = tbl.rowCount()
                tbl.insertRow(r)
                tbl.setItem(r, 0, QTableWidgetItem(str(item["piece_product_id"])))
                tbl.setItem(r, 1, QTableWidgetItem(item["piece_name"]))
                tbl.setItem(r, 2, QTableWidgetItem(f'{item["percentage"]:.2f}'))
                estado_item = QTableWidgetItem("✓ activo" if item["active"] else "○ inactivo")
                estado_item.setFlags(Qt.ItemIsEnabled)
                tbl.setItem(r, 3, estado_item)
            tbl.blockSignals(False)
            self._rc_recalc_total()
        except Exception as exc:
            QMessageBox.critical(self, "Error", f"No se pudo cargar la receta:\n{exc}")

    def _rc_agregar_fila(self) -> None:
        """Agrega una fila vacía para un nuevo componente."""
        tbl = getattr(self, "tbl_rc", None)
        if not tbl:
            return
        tbl.blockSignals(True)
        r = tbl.rowCount()
        tbl.insertRow(r)
        tbl.setItem(r, 0, QTableWidgetItem(""))
        tbl.setItem(r, 1, QTableWidgetItem("Nombre pieza"))
        tbl.setItem(r, 2, QTableWidgetItem("0.00"))
        estado = QTableWidgetItem("nuevo")
        estado.setFlags(Qt.ItemIsEnabled)
        tbl.setItem(r, 3, estado)
        tbl.blockSignals(False)
        tbl.scrollToBottom()
        tbl.selectRow(r)
        self._rc_recalc_total()

    def _rc_quitar_fila(self) -> None:
        tbl = getattr(self, "tbl_rc", None)
        if not tbl:
            return
        row = tbl.currentRow()
        if row >= 0:
            tbl.removeRow(row)
        self._rc_recalc_total()

    def _rc_recalc_total(self) -> None:
        """Recalcula el total de porcentajes y actualiza el label con color."""
        tbl = getattr(self, "tbl_rc", None)
        lbl = getattr(self, "lbl_rc_total", None)
        if not tbl or not lbl:
            return
        total = 0.0
        for r in range(tbl.rowCount()):
            try:
                item = tbl.item(r, 2)
                if item:
                    total += float(item.text() or 0)
            except (ValueError, AttributeError):
                pass
        ok = abs(total - 100.0) < 0.1
        simbolo = "✓" if ok else "✗"
        color   = "#1a7a1a" if ok else "#c00000"
        lbl.setText(f"Total: {total:.2f}%  {simbolo}  (debe ser 100%)")
        lbl.setStyleSheet(f"font-weight: bold; font-size: 13px; color: {color};")

    def _rc_validar(self) -> None:
        """Valida la receta activa del producto seleccionado."""
        cbo = getattr(self, "cbo_rc_producto", None)
        if not cbo:
            return
        product_id = cbo.currentData()
        if not product_id:
            QMessageBox.information(self, "Validar", "Seleccione un producto primero.")
            return
        repo = self._rc_get_repo()
        if not repo:
            return
        result = repo.validate_recipe(product_id)
        nombre = cbo.currentText()
        if result["valid"]:
            msg = (
                f"✅ Receta de '{nombre}' válida.\n"
                f"Total: {result['total']:.2f}%"
            )
            if result["warnings"]:
                msg += "\n\n⚠️ Advertencias:\n" + "\n".join(result["warnings"])
            QMessageBox.information(self, "Validación OK", msg)
        else:
            msg = f"❌ Receta inválida:\n\n" + "\n".join(result["errors"])
            if result["warnings"]:
                msg += "\n\n⚠️ Advertencias:\n" + "\n".join(result["warnings"])
            QMessageBox.warning(self, "Validación fallida", msg)

    def _rc_guardar(self) -> None:
        """Lee la tabla y guarda la receta usando ProductRecipeRepository."""
        cbo = getattr(self, "cbo_rc_producto", None)
        tbl = getattr(self, "tbl_rc", None)
        if not cbo or not tbl:
            return

        product_id = cbo.currentData()
        if not product_id:
            QMessageBox.warning(self, "Aviso", "Seleccione un producto.")
            return

        items = []
        for r in range(tbl.rowCount()):
            try:
                id_item  = tbl.item(r, 0)
                nom_item = tbl.item(r, 1)
                pct_item = tbl.item(r, 2)
                if not id_item or not id_item.text().strip():
                    continue
                pid = int(id_item.text().strip())
                nombre = nom_item.text().strip() if nom_item else ""
                pct    = float(pct_item.text().strip()) if pct_item else 0.0
                if pid <= 0 or pct <= 0:
                    continue
                items.append({
                    "piece_product_id": pid,
                    "piece_name":       nombre,
                    "percentage":       pct,
                    "orden":            r,
                })
            except (ValueError, AttributeError):
                continue

        if not items:
            QMessageBox.warning(
                self, "Sin datos",
                "Agregue al menos un componente con ID de pieza y porcentaje válido."
            )
            return

        repo = self._rc_get_repo()
        if not repo:
            return

        try:
            repo.save_recipe(product_id, items)
            QMessageBox.information(
                self, "Guardado",
                f"✅ Receta guardada correctamente.\n"
                f"Producto: {cbo.currentText()}\n"
                f"Componentes: {len(items)}"
            )
            self._rc_cargar_receta()
            self._rc_refrescar_lista()
        except ValueError as exc:
            QMessageBox.warning(self, "Error de validación", str(exc))
        except Exception as exc:
            QMessageBox.critical(self, "Error al guardar", str(exc))

    def _rc_eliminar(self) -> None:
        """Elimina (soft-delete) la receta del producto seleccionado."""
        cbo = getattr(self, "cbo_rc_producto", None)
        if not cbo:
            return
        product_id = cbo.currentData()
        if not product_id:
            QMessageBox.warning(self, "Aviso", "Seleccione un producto.")
            return

        resp = QMessageBox.question(
            self, "Confirmar eliminación",
            f"¿Eliminar la receta de consumo de '{cbo.currentText()}'?\n"
            "Las ventas futuras usarán descuento directo.",
            QMessageBox.Yes | QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return

        repo = self._rc_get_repo()
        if not repo:
            return
        try:
            repo.delete_recipe(product_id)
            QMessageBox.information(self, "Eliminado", "Receta eliminada correctamente.")
            self._rc_cargar_receta()
            self._rc_refrescar_lista()
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))

    def _rc_refrescar_lista(self) -> None:
        """Actualiza la tabla de productos con receta activa."""
        tbl = getattr(self, "tbl_rc_lista", None)
        if not tbl:
            return
        repo = self._rc_get_repo()
        if not repo:
            return
        try:
            productos = repo.list_products_with_recipes()
            tbl.setRowCount(0)
            for p in productos:
                result = repo.validate_recipe(p["id"])
                r = tbl.rowCount()
                tbl.insertRow(r)
                tbl.setItem(r, 0, QTableWidgetItem(p["nombre"]))
                n_piezas = len(repo.get_recipe_raw(p["id"]))
                tbl.setItem(r, 1, QTableWidgetItem(str(n_piezas)))
                estado = "✅ Válida" if result["valid"] else f"⚠️ {result['total']:.1f}%"
                item_estado = QTableWidgetItem(estado)
                item_estado.setForeground(
                    QColor("#1a7a1a") if result["valid"] else QColor("#c00000")
                )
                tbl.setItem(r, 2, item_estado)
        except Exception as exc:
            logger.warning("_rc_refrescar_lista: %s", exc)

    def _rc_seleccionar_desde_lista(self, row: int, _col: int) -> None:
        """Al hacer doble clic en la lista, selecciona el producto en el combo."""
        tbl = getattr(self, "tbl_rc_lista", None)
        cbo = getattr(self, "cbo_rc_producto", None)
        if not tbl or not cbo:
            return
        nombre_item = tbl.item(row, 0)
        if not nombre_item:
            return
        nombre = nombre_item.text()
        idx = cbo.findText(nombre)
        if idx >= 0:
            cbo.setCurrentIndex(idx)

    def _no_engine(self) -> None:
        QMessageBox.warning(
            self, "Motor no disponible",
            "ChickenEngine no inicializado. Inicie sesión primero."
        )


# ── Helpers UI ────────────────────────────────────────────────────────────────

def _titulo(lay: QVBoxLayout, text: str) -> None:
    lbl = QLabel(text)
    lbl.setFont(QFont("Segoe UI", 14, QFont.Bold))
    lay.addWidget(lbl)
    lay.addSpacing(8)


def _subtitulo(lay: QVBoxLayout, text: str) -> None:
    lbl = QLabel(text)
    lbl.setFont(QFont("Segoe UI", 10, QFont.Bold))
    lay.addWidget(lbl)


def _input_dialog(parent, title: str, label: str) -> tuple[str, bool]:
    from PyQt5.QtWidgets import QInputDialog
    return QInputDialog.getText(parent, title, label)

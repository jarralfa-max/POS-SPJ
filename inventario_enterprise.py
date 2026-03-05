# modulos/inventario_enterprise.py
# ── MÓDULO INVENTARIO ENTERPRISE — SPJ Pollería v6 ────────────────────────────
# QTabWidget con 5 secciones:
#   1. Inventario Global   — vista administrativa por producto
#   2. Inventario Sucursal — stock local en tiempo real
#   3. Recepción de Pollo  — vendedor registra llegadas
#   4. Traspasos           — movimientos inter-sucursal
#   5. Recetas de Consumo  — editor de rendimiento proporcional
#
# Desacoplado del motor: toda lógica en PolloOperativoEngine.
# NO rompe módulos existentes. Integración vía notificar_evento.
from __future__ import annotations

import logging
import sqlite3
from typing import Optional

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTabWidget, QTableWidget, QTableWidgetItem, QComboBox,
    QDoubleSpinBox, QGroupBox, QFormLayout, QLineEdit,
    QTextEdit, QHeaderView, QAbstractItemView, QSplitter,
    QMessageBox, QFrame, QSizePolicy, QSpacerItem,
)
from PyQt5.QtCore import Qt, QTimer
from PyQt5.QtGui import QFont, QColor

logger = logging.getLogger("spj.inventario_enterprise")


def _get_engine(conn: sqlite3.Connection, usuario: str, sucursal_id: int):
    from core.services.pollo_operativo_engine import PolloOperativoEngine
    return PolloOperativoEngine(conn, usuario=usuario, sucursal_id=sucursal_id)


# ══════════════════════════════════════════════════════════════════════════════
# MÓDULO CONTENEDOR
# ══════════════════════════════════════════════════════════════════════════════

class ModuloInventarioEnterprise(QWidget):
    """
    Módulo de inventario enterprise para pollería.
    Se añade al stacked_widget de MainWindow como cualquier otro módulo.
    """

    def __init__(self, conexion: sqlite3.Connection, main_window=None):
        super().__init__()
        self.conexion      = conexion
        self.main_window   = main_window
        self.usuario       = "Sistema"
        self.sucursal_id   = 1
        self.sucursal_nombre = "Principal"
        self._tabs: dict[str, QWidget] = {}

        self._init_ui()
        # Refrescar cuando otra parte del sistema actualiza inventario
        if main_window and hasattr(main_window, "registrar_evento"):
            main_window.registrar_evento(
                "inventario_actualizado", self._on_inventario_actualizado
            )

    # ── API de sesión (compatible con otros módulos) ──────────────────────────

    def set_usuario_actual(self, usuario: str, rol: str = None) -> None:
        self.usuario = usuario or "Sistema"

    def set_sucursal(self, sucursal_id: int, nombre: str = "") -> None:
        self.sucursal_id     = sucursal_id or 1
        self.sucursal_nombre = nombre or "Principal"
        self._lbl_sucursal.setText(f"🏪 {self.sucursal_nombre}")
        self._refrescar_tab_actual()

    def set_sesion(self, usuario: str, rol: str = None) -> None:
        self.set_usuario_actual(usuario, rol)

    # ── UI principal ──────────────────────────────────────────────────────────

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # ── Header ────────────────────────────────────────────────────────────
        header = QFrame()
        header.setFixedHeight(52)
        header.setProperty("class", "moduleHeader")
        h_lay = QHBoxLayout(header)
        h_lay.setContentsMargins(20, 0, 20, 0)
        h_lay.setSpacing(12)

        ico = QLabel("🐔")
        ico.setFont(QFont("Segoe UI Emoji", 20))
        h_lay.addWidget(ico)

        title = QLabel("INVENTARIO ENTERPRISE")
        title.setProperty("class", "moduleTitle")
        h_lay.addWidget(title)

        h_lay.addStretch()

        self._lbl_sucursal = QLabel("🏪 Principal")
        self._lbl_sucursal.setProperty("class", "sucursalBadge")
        h_lay.addWidget(self._lbl_sucursal)

        btn_refrescar = QPushButton("🔄 Refrescar")
        btn_refrescar.setProperty("class", "actionBtn")
        btn_refrescar.setFixedWidth(110)
        btn_refrescar.clicked.connect(self._refrescar_tab_actual)
        h_lay.addWidget(btn_refrescar)

        layout.addWidget(header)

        # ── Tabs ──────────────────────────────────────────────────────────────
        self._tabwidget = QTabWidget()
        self._tabwidget.setDocumentMode(False)
        self._tabwidget.setTabPosition(QTabWidget.North)
        self._tabwidget.setProperty("class", "inventarioTabs")

        self._tab_global    = TabInventarioGlobal(self)
        self._tab_sucursal  = TabInventarioSucursal(self)
        self._tab_recepcion = TabRecepcionPollo(self)
        self._tab_traspasos = TabTraspasos(self)
        self._tab_recetas   = TabRecetasConsumo(self)

        self._tabwidget.addTab(self._tab_global,    "🌐 Global")
        self._tabwidget.addTab(self._tab_sucursal,  "🏪 Sucursal")
        self._tabwidget.addTab(self._tab_recepcion, "📥 Recepción")
        self._tabwidget.addTab(self._tab_traspasos, "↔️ Traspasos")
        self._tabwidget.addTab(self._tab_recetas,   "🍽️ Recetas")

        self._tabwidget.currentChanged.connect(self._on_tab_changed)

        layout.addWidget(self._tabwidget)

    # ── Navegación de tabs ────────────────────────────────────────────────────

    def _on_tab_changed(self, idx: int) -> None:
        self._refrescar_tab_actual()

    def _refrescar_tab_actual(self) -> None:
        tab = self._tabwidget.currentWidget()
        if tab and hasattr(tab, "refrescar"):
            try:
                tab.refrescar()
            except Exception as exc:
                logger.warning("refrescar tab: %s", exc)

    def _on_inventario_actualizado(self, datos: dict) -> None:
        """Llamado por main_window.notificar_evento cuando ventas actualiza stock."""
        self._refrescar_tab_actual()

    def _engine(self):
        return _get_engine(self.conexion, self.usuario, self.sucursal_id)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — INVENTARIO GLOBAL
# ══════════════════════════════════════════════════════════════════════════════

class TabInventarioGlobal(QWidget):
    """Vista administrativa: stock global por producto + registro de compra."""

    def __init__(self, parent: ModuloInventarioEnterprise):
        super().__init__()
        self._p = parent
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        # ── Tabla ─────────────────────────────────────────────────────────────
        lbl = QLabel("📊 Stock Global por Producto")
        lbl.setProperty("class", "sectionTitle")
        layout.addWidget(lbl)

        self._tabla = QTableWidget()
        self._tabla.setColumnCount(4)
        self._tabla.setHorizontalHeaderLabels(
            ["Producto", "Stock Global (kg)", "Costo/kg", "Último movimiento"]
        )
        self._tabla.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._tabla.horizontalHeader().setSectionResizeMode(1, QHeaderView.Fixed)
        self._tabla.horizontalHeader().setSectionResizeMode(2, QHeaderView.Fixed)
        self._tabla.horizontalHeader().setSectionResizeMode(3, QHeaderView.Fixed)
        self._tabla.setColumnWidth(1, 140)
        self._tabla.setColumnWidth(2, 100)
        self._tabla.setColumnWidth(3, 160)
        self._tabla.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tabla.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tabla.setAlternatingRowColors(True)
        layout.addWidget(self._tabla, stretch=2)

        # ── Formulario de compra ──────────────────────────────────────────────
        grp = QGroupBox("➕ Registrar Compra Global")
        grp.setProperty("class", "formGroup")
        form_lay = QHBoxLayout(grp)
        form_lay.setSpacing(12)

        self._combo_prod = QComboBox()
        self._combo_prod.setMinimumWidth(180)
        self._combo_prod.setPlaceholderText("Seleccionar producto…")
        form_lay.addWidget(QLabel("Producto:"))
        form_lay.addWidget(self._combo_prod)

        self._spin_peso = QDoubleSpinBox()
        self._spin_peso.setRange(0.001, 99999.0)
        self._spin_peso.setDecimals(3)
        self._spin_peso.setSuffix(" kg")
        self._spin_peso.setValue(0.0)
        form_lay.addWidget(QLabel("Peso kg:"))
        form_lay.addWidget(self._spin_peso)

        self._spin_costo = QDoubleSpinBox()
        self._spin_costo.setRange(0, 9999999.0)
        self._spin_costo.setDecimals(2)
        self._spin_costo.setPrefix("$")
        self._spin_costo.setValue(0.0)
        form_lay.addWidget(QLabel("Costo total:"))
        form_lay.addWidget(self._spin_costo)

        self._txt_notas = QLineEdit()
        self._txt_notas.setPlaceholderText("Notas (opcional)")
        self._txt_notas.setMaximumWidth(200)
        form_lay.addWidget(self._txt_notas)

        btn = QPushButton("💾 Registrar Compra")
        btn.setProperty("class", "primaryBtn")
        btn.clicked.connect(self._registrar_compra)
        form_lay.addWidget(btn)

        layout.addWidget(grp)

    def refrescar(self) -> None:
        eng = self._p._engine()
        try:
            productos = eng.productos_activos()
            self._combo_prod.clear()
            for p in productos:
                self._combo_prod.addItem(p["nombre"], p["id"])
        except Exception:
            pass

        try:
            stocks = eng.stock_global()
            self._tabla.setRowCount(len(stocks))
            for i, s in enumerate(stocks):
                self._tabla.setItem(i, 0, _item(s.nombre))
                peso_item = _item(f"{s.peso_global:.3f}")
                peso_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                if s.peso_global <= 0:
                    peso_item.setForeground(QColor("#e74c3c"))
                elif s.peso_global < 10:
                    peso_item.setForeground(QColor("#f39c12"))
                self._tabla.setItem(i, 1, peso_item)
                # costo_por_kg: suma ponderada
                row = self._p.conexion.execute(
                    """SELECT CASE WHEN SUM(peso_kg)>0
                               THEN SUM(costo_total)/SUM(peso_kg) ELSE 0 END
                       FROM inventario_global WHERE producto_id=?""",
                    (s.producto_id,)
                ).fetchone()
                costo = float(row[0]) if row else 0.0
                self._tabla.setItem(i, 2, _item(f"${costo:.2f}"))
                self._tabla.setItem(i, 3, _item("—"))
        except Exception as exc:
            logger.warning("refrescar global: %s", exc)

    def _registrar_compra(self) -> None:
        prod_id = self._combo_prod.currentData()
        if not prod_id:
            QMessageBox.warning(self, "Error", "Selecciona un producto"); return
        peso    = self._spin_peso.value()
        costo   = self._spin_costo.value()
        notas   = self._txt_notas.text().strip()
        if peso <= 0:
            QMessageBox.warning(self, "Error", "El peso debe ser mayor a 0"); return

        try:
            eng = self._p._engine()
            eng.registrar_compra_global(prod_id, peso, costo, notas)
            self._spin_peso.setValue(0); self._spin_costo.setValue(0)
            self._txt_notas.clear()
            self.refrescar()
            QMessageBox.information(
                self, "✅ Compra Registrada",
                f"Se registraron {peso:.3f}kg en inventario global."
            )
        except Exception as exc:
            logger.error("registrar_compra: %s", exc)
            QMessageBox.critical(self, "Error", str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — INVENTARIO SUCURSAL
# ══════════════════════════════════════════════════════════════════════════════

class TabInventarioSucursal(QWidget):
    """Stock operativo de la sucursal actual en tiempo real."""

    def __init__(self, parent: ModuloInventarioEnterprise):
        super().__init__()
        self._p = parent
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)

        # Header con badge de sucursal
        h_lay = QHBoxLayout()
        lbl = QLabel("🏪 Stock en Sucursal")
        lbl.setProperty("class", "sectionTitle")
        h_lay.addWidget(lbl)
        h_lay.addStretch()
        layout.addLayout(h_lay)

        self._tabla = QTableWidget()
        self._tabla.setColumnCount(3)
        self._tabla.setHorizontalHeaderLabels(
            ["Producto", "Stock Sucursal (kg)", "Stock Global (kg)"]
        )
        self._tabla.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._tabla.horizontalHeader().setSectionResizeMode(1, QHeaderView.Fixed)
        self._tabla.horizontalHeader().setSectionResizeMode(2, QHeaderView.Fixed)
        self._tabla.setColumnWidth(1, 160)
        self._tabla.setColumnWidth(2, 150)
        self._tabla.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tabla.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tabla.setAlternatingRowColors(True)
        layout.addWidget(self._tabla)

        # Resumen
        self._lbl_resumen = QLabel("")
        self._lbl_resumen.setAlignment(Qt.AlignRight)
        self._lbl_resumen.setProperty("class", "summaryLabel")
        layout.addWidget(self._lbl_resumen)

    def refrescar(self) -> None:
        try:
            eng = self._p._engine()
            stocks = eng.stock_sucursal()
            self._tabla.setRowCount(len(stocks))
            total_local = 0.0
            for i, s in enumerate(stocks):
                self._tabla.setItem(i, 0, _item(s.nombre))
                local_item = _item(f"{s.peso_sucursal:.3f}")
                local_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                if s.peso_sucursal <= 0:
                    local_item.setForeground(QColor("#e74c3c"))
                elif s.peso_sucursal < 5:
                    local_item.setForeground(QColor("#f39c12"))
                else:
                    local_item.setForeground(QColor("#27ae60"))
                self._tabla.setItem(i, 1, local_item)
                global_item = _item(f"{s.peso_global:.3f}")
                global_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self._tabla.setItem(i, 2, global_item)
                total_local += s.peso_sucursal
            self._lbl_resumen.setText(
                f"Total en sucursal: <b>{total_local:.3f} kg</b>"
            )
        except Exception as exc:
            logger.warning("refrescar sucursal: %s", exc)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — RECEPCIÓN DE POLLO
# ══════════════════════════════════════════════════════════════════════════════

class TabRecepcionPollo(QWidget):
    """Vendedor registra el pollo recibido. Descuenta global, aumenta sucursal."""

    def __init__(self, parent: ModuloInventarioEnterprise):
        super().__init__()
        self._p = parent
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(14)

        # ── Formulario ────────────────────────────────────────────────────────
        grp = QGroupBox("📥 Registrar Recepción de Pollo")
        grp.setProperty("class", "formGroup")
        form = QFormLayout(grp)
        form.setSpacing(10)
        form.setLabelAlignment(Qt.AlignRight)

        self._combo_prod = QComboBox()
        self._combo_prod.setMinimumWidth(220)
        self._combo_prod.currentIndexChanged.connect(self._mostrar_stock_global)
        form.addRow("Producto:", self._combo_prod)

        self._lbl_stock_global = QLabel("—")
        self._lbl_stock_global.setProperty("class", "stockBadge")
        form.addRow("Disponible global:", self._lbl_stock_global)

        self._spin_peso = QDoubleSpinBox()
        self._spin_peso.setRange(0.001, 99999.0)
        self._spin_peso.setDecimals(3)
        self._spin_peso.setSuffix(" kg")
        self._spin_peso.setMinimumWidth(140)
        form.addRow("Peso recibido:", self._spin_peso)

        self._spin_costo = QDoubleSpinBox()
        self._spin_costo.setRange(0, 9999.99)
        self._spin_costo.setDecimals(2)
        self._spin_costo.setPrefix("$")
        self._spin_costo.setSuffix(" /kg")
        form.addRow("Costo/kg (opcional):", self._spin_costo)

        self._txt_proveedor = QLineEdit()
        self._txt_proveedor.setPlaceholderText("Nombre del proveedor")
        self._txt_proveedor.setMaximumWidth(280)
        form.addRow("Proveedor:", self._txt_proveedor)

        self._txt_lote_ref = QLineEdit()
        self._txt_lote_ref.setPlaceholderText("Nº de lote / referencia")
        self._txt_lote_ref.setMaximumWidth(200)
        form.addRow("Lote ref:", self._txt_lote_ref)

        self._txt_notas = QLineEdit()
        self._txt_notas.setPlaceholderText("Observaciones…")
        form.addRow("Notas:", self._txt_notas)

        btn_lay = QHBoxLayout()
        btn = QPushButton("✅ Confirmar Recepción")
        btn.setProperty("class", "primaryBtn")
        btn.setMinimumHeight(40)
        btn.clicked.connect(self._confirmar)
        btn_lay.addStretch()
        btn_lay.addWidget(btn)
        form.addRow("", btn_lay)

        layout.addWidget(grp)

        # ── Historial ─────────────────────────────────────────────────────────
        lbl_hist = QLabel("📋 Recepciones Recientes")
        lbl_hist.setProperty("class", "sectionTitle")
        layout.addWidget(lbl_hist)

        self._tabla_hist = QTableWidget()
        self._tabla_hist.setColumnCount(7)
        self._tabla_hist.setHorizontalHeaderLabels(
            ["#", "Fecha", "Producto", "Peso (kg)", "Proveedor", "Usuario", "Estado"]
        )
        self._tabla_hist.horizontalHeader().setSectionResizeMode(2, QHeaderView.Stretch)
        self._tabla_hist.setColumnWidth(0, 50)
        self._tabla_hist.setColumnWidth(1, 140)
        self._tabla_hist.setColumnWidth(3, 100)
        self._tabla_hist.setColumnWidth(4, 120)
        self._tabla_hist.setColumnWidth(5, 100)
        self._tabla_hist.setColumnWidth(6, 90)
        self._tabla_hist.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tabla_hist.setAlternatingRowColors(True)
        layout.addWidget(self._tabla_hist)

    def refrescar(self) -> None:
        try:
            eng = self._p._engine()
            prods = eng.productos_activos()
            self._combo_prod.clear()
            for p in prods:
                self._combo_prod.addItem(p["nombre"], p["id"])
            self._mostrar_stock_global()

            hist = eng.recepciones_recientes(50)
            self._tabla_hist.setRowCount(len(hist))
            for i, r in enumerate(hist):
                self._tabla_hist.setItem(i, 0, _item(str(r["id"])))
                self._tabla_hist.setItem(i, 1, _item(str(r["fecha"])[:16]))
                self._tabla_hist.setItem(i, 2, _item(r["producto"]))
                peso_item = _item(f"{r['peso_kg']:.3f}")
                peso_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self._tabla_hist.setItem(i, 3, peso_item)
                self._tabla_hist.setItem(i, 4, _item(str(r.get("proveedor", "—") or "—")))
                self._tabla_hist.setItem(i, 5, _item(str(r["usuario"])))
                estado_item = _item(r["estado"])
                if r["estado"] == "confirmada":
                    estado_item.setForeground(QColor("#27ae60"))
                else:
                    estado_item.setForeground(QColor("#e74c3c"))
                self._tabla_hist.setItem(i, 6, estado_item)
        except Exception as exc:
            logger.warning("refrescar recepciones: %s", exc)

    def _mostrar_stock_global(self) -> None:
        prod_id = self._combo_prod.currentData()
        if not prod_id:
            self._lbl_stock_global.setText("—"); return
        try:
            eng = self._p._engine()
            from core.services.pollo_operativo_engine import PolloOperativoEngine
            stock = eng._get_stock_global(prod_id)
            color = "#27ae60" if stock > 0 else "#e74c3c"
            self._lbl_stock_global.setText(
                f'<span style="color:{color};font-weight:bold">{stock:.3f} kg</span>'
            )
        except Exception:
            self._lbl_stock_global.setText("—")

    def _confirmar(self) -> None:
        prod_id = self._combo_prod.currentData()
        if not prod_id:
            QMessageBox.warning(self, "Error", "Selecciona un producto"); return
        peso    = self._spin_peso.value()
        costo   = self._spin_costo.value()
        notas   = self._txt_notas.text().strip()
        if peso <= 0:
            QMessageBox.warning(self, "Error", "El peso debe ser mayor a 0"); return

        nombre = self._combo_prod.currentText()
        prov_txt = self._txt_proveedor.text().strip()
        lote_txt  = self._txt_lote_ref.text().strip()
        detalles_extra = ""
        if prov_txt:
            detalles_extra += f"\nProveedor: {prov_txt}"
        if lote_txt:
            detalles_extra += f"\nLote: {lote_txt}"
        resp = QMessageBox.question(
            self, "Confirmar Recepción",
            f"¿Confirmas la recepción de {peso:.3f}kg de {nombre}?" + detalles_extra,
            QMessageBox.Yes | QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return

        try:
            eng = self._p._engine()
            proveedor = prov_txt
            lote_ref  = self._txt_lote_ref.text().strip()
            rec_id = eng.registrar_recepcion(
                prod_id, peso, costo_kg=costo,
                proveedor=proveedor, lote_ref=lote_ref, notas=notas,
            )
            self._spin_peso.setValue(0)
            self._spin_costo.setValue(0)
            self._txt_proveedor.clear()
            self._txt_lote_ref.clear()
            self._txt_notas.clear()
            self.refrescar()
            QMessageBox.information(
                self, "✅ Recepción Confirmada",
                f"Recepción #{rec_id} registrada.\n{peso:.3f}kg de {nombre} agregados al stock."
            )
        except Exception as exc:
            logger.error("confirmar_recepcion: %s", exc)
            QMessageBox.critical(self, "Error", str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — TRASPASOS
# ══════════════════════════════════════════════════════════════════════════════

class TabTraspasos(QWidget):
    """Movimientos inter-sucursal con validación de stock."""

    def __init__(self, parent: ModuloInventarioEnterprise):
        super().__init__()
        self._p = parent
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(14)

        # ── Formulario de traspaso ────────────────────────────────────────────
        grp = QGroupBox("↔️ Registrar Traspaso")
        grp.setProperty("class", "formGroup")
        form = QFormLayout(grp)
        form.setSpacing(10)
        form.setLabelAlignment(Qt.AlignRight)

        self._combo_prod = QComboBox()
        self._combo_prod.setMinimumWidth(220)
        self._combo_prod.currentIndexChanged.connect(self._mostrar_stock_origen)
        form.addRow("Producto:", self._combo_prod)

        self._lbl_stock_origen = QLabel("—")
        self._lbl_stock_origen.setProperty("class", "stockBadge")
        form.addRow("Stock en sucursal origen:", self._lbl_stock_origen)

        self._combo_destino = QComboBox()
        self._combo_destino.setMinimumWidth(220)
        form.addRow("Sucursal destino:", self._combo_destino)

        self._spin_peso = QDoubleSpinBox()
        self._spin_peso.setRange(0.001, 99999.0)
        self._spin_peso.setDecimals(3)
        self._spin_peso.setSuffix(" kg")
        form.addRow("Peso a traspasar:", self._spin_peso)

        self._txt_obs = QTextEdit()
        self._txt_obs.setPlaceholderText("Observaciones (opcional)")
        self._txt_obs.setMaximumHeight(60)
        form.addRow("Observaciones:", self._txt_obs)

        btn_lay = QHBoxLayout()
        btn = QPushButton("↔️ Confirmar Traspaso")
        btn.setProperty("class", "primaryBtn")
        btn.setMinimumHeight(40)
        btn.clicked.connect(self._confirmar)
        btn_lay.addStretch()
        btn_lay.addWidget(btn)
        form.addRow("", btn_lay)

        layout.addWidget(grp)

        # ── Historial ─────────────────────────────────────────────────────────
        lbl_hist = QLabel("📋 Traspasos Recientes")
        lbl_hist.setProperty("class", "sectionTitle")
        layout.addWidget(lbl_hist)

        self._tabla_hist = QTableWidget()
        self._tabla_hist.setColumnCount(6)
        self._tabla_hist.setHorizontalHeaderLabels(
            ["#", "Fecha", "Producto", "Kg", "Origen → Destino", "Estado"]
        )
        self._tabla_hist.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)
        self._tabla_hist.setColumnWidth(0, 50)
        self._tabla_hist.setColumnWidth(1, 140)
        self._tabla_hist.setColumnWidth(2, 150)
        self._tabla_hist.setColumnWidth(3, 80)
        self._tabla_hist.setColumnWidth(5, 100)
        self._tabla_hist.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tabla_hist.setAlternatingRowColors(True)
        layout.addWidget(self._tabla_hist)

    def refrescar(self) -> None:
        try:
            eng = self._p._engine()

            # Combo productos
            prods = eng.productos_activos()
            self._combo_prod.clear()
            for p in prods:
                self._combo_prod.addItem(p["nombre"], p["id"])

            # Combo destino (todas las sucursales excepto la actual)
            sucursales = eng.sucursales_activas()
            self._combo_destino.clear()
            for s in sucursales:
                if s["id"] != self._p.sucursal_id:
                    self._combo_destino.addItem(s["nombre"], s["id"])
            if self._combo_destino.count() == 0:
                self._combo_destino.addItem("(sin otras sucursales)", -1)

            self._mostrar_stock_origen()

            # Historial
            hist = eng.traspasos_recientes(50)
            self._tabla_hist.setRowCount(len(hist))
            for i, r in enumerate(hist):
                self._tabla_hist.setItem(i, 0, _item(str(r["id"])))
                self._tabla_hist.setItem(i, 1, _item(str(r["fecha"])[:16]))
                self._tabla_hist.setItem(i, 2, _item(r["producto"]))
                kg_item = _item(f"{r['peso_kg']:.3f}")
                kg_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self._tabla_hist.setItem(i, 3, kg_item)
                origen = r["origen"] or f"#{r.get('origen_id','?')}"
                destino = r["destino"] or f"#{r.get('destino_id','?')}"
                self._tabla_hist.setItem(i, 4, _item(f"{origen} → {destino}"))
                est_item = _item(r["estado"])
                color = {"confirmado": "#27ae60", "pendiente": "#f39c12", "anulado": "#e74c3c"}
                est_item.setForeground(QColor(color.get(r["estado"], "#ffffff")))
                self._tabla_hist.setItem(i, 5, est_item)
        except Exception as exc:
            logger.warning("refrescar traspasos: %s", exc)

    def _mostrar_stock_origen(self) -> None:
        prod_id = self._combo_prod.currentData()
        if not prod_id:
            self._lbl_stock_origen.setText("—"); return
        try:
            eng = self._p._engine()
            stock = eng._get_stock_sucursal(prod_id)
            color = "#27ae60" if stock > 0 else "#e74c3c"
            self._lbl_stock_origen.setText(
                f'<span style="color:{color};font-weight:bold">{stock:.3f} kg</span>'
            )
        except Exception:
            self._lbl_stock_origen.setText("—")

    def _confirmar(self) -> None:
        prod_id  = self._combo_prod.currentData()
        dest_id  = self._combo_destino.currentData()
        peso     = self._spin_peso.value()
        obs      = self._txt_obs.toPlainText().strip()

        if not prod_id:
            QMessageBox.warning(self, "Error", "Selecciona un producto"); return
        if not dest_id or dest_id == -1:
            QMessageBox.warning(self, "Error", "Selecciona una sucursal destino"); return
        if peso <= 0:
            QMessageBox.warning(self, "Error", "El peso debe ser mayor a 0"); return

        prod_nombre = self._combo_prod.currentText()
        dest_nombre = self._combo_destino.currentText()
        resp = QMessageBox.question(
            self, "Confirmar Traspaso",
            f"¿Traspasar {peso:.3f}kg de {prod_nombre} a {dest_nombre}?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return

        try:
            eng = self._p._engine()
            result = eng.registrar_traspaso(dest_id, prod_id, peso, obs)
            self._spin_peso.setValue(0)
            self._txt_obs.clear()
            self.refrescar()
            QMessageBox.information(
                self, "✅ Traspaso Confirmado",
                f"Traspaso #{result.traspaso_id} confirmado.\n"
                f"{peso:.3f}kg de {prod_nombre}\n"
                f"{result.origen} → {result.destino}"
            )
        except Exception as exc:
            logger.error("confirmar_traspaso: %s", exc)
            QMessageBox.critical(self, "Error", str(exc))


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — RECETAS DE CONSUMO
# ══════════════════════════════════════════════════════════════════════════════

class TabRecetasConsumo(QWidget):
    """
    Editor de recetas de rendimiento proporcional.
    Producto venta → lista de materias primas con % suma=100.
    """

    def __init__(self, parent: ModuloInventarioEnterprise):
        super().__init__()
        self._p = parent
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)

        # ── Panel izquierdo: lista de recetas ─────────────────────────────────
        left = QVBoxLayout()
        lbl_lista = QLabel("📋 Recetas Activas")
        lbl_lista.setProperty("class", "sectionTitle")
        left.addWidget(lbl_lista)

        self._tabla_lista = QTableWidget()
        self._tabla_lista.setColumnCount(3)
        self._tabla_lista.setHorizontalHeaderLabels(
            ["Producto venta", "Nombre receta", "Ingredientes"]
        )
        self._tabla_lista.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._tabla_lista.setColumnWidth(1, 130)
        self._tabla_lista.setColumnWidth(2, 90)
        self._tabla_lista.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tabla_lista.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tabla_lista.setAlternatingRowColors(True)
        self._tabla_lista.clicked.connect(self._cargar_receta_seleccionada)
        left.addWidget(self._tabla_lista)

        btn_nueva = QPushButton("➕ Nueva receta")
        btn_nueva.setProperty("class", "actionBtn")
        btn_nueva.clicked.connect(self._nueva_receta)
        left.addWidget(btn_nueva)

        left_widget = QWidget()
        left_widget.setLayout(left)
        left_widget.setMaximumWidth(380)
        layout.addWidget(left_widget)

        # ── Panel derecho: editor ─────────────────────────────────────────────
        right = QVBoxLayout()
        lbl_editor = QLabel("✏️ Editor de Receta")
        lbl_editor.setProperty("class", "sectionTitle")
        right.addWidget(lbl_editor)

        form = QFormLayout()
        form.setSpacing(8)
        form.setLabelAlignment(Qt.AlignRight)

        self._combo_prod_venta = QComboBox()
        self._combo_prod_venta.setMinimumWidth(200)
        form.addRow("Producto venta:", self._combo_prod_venta)

        self._txt_nombre_receta = QLineEdit()
        self._txt_nombre_receta.setPlaceholderText("Ej. Surtido estándar")
        form.addRow("Nombre receta:", self._txt_nombre_receta)

        right.addLayout(form)

        # Tabla de ingredientes
        lbl_ing = QLabel("Ingredientes (% debe sumar 100%)")
        lbl_ing.setProperty("class", "subSectionTitle")
        right.addWidget(lbl_ing)

        self._tabla_det = QTableWidget()
        self._tabla_det.setColumnCount(3)
        self._tabla_det.setHorizontalHeaderLabels(
            ["Materia prima", "% Consumo", ""]
        )
        self._tabla_det.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)
        self._tabla_det.setColumnWidth(1, 100)
        self._tabla_det.setColumnWidth(2, 50)
        self._tabla_det.setAlternatingRowColors(True)
        right.addWidget(self._tabla_det, stretch=1)

        # Total
        self._lbl_total = QLabel("Total: 0.00%")
        self._lbl_total.setAlignment(Qt.AlignRight)
        self._lbl_total.setProperty("class", "totalPct")
        right.addWidget(self._lbl_total)

        # Botones de ingrediente
        ing_btns = QHBoxLayout()
        btn_add_ing = QPushButton("➕ Ingrediente")
        btn_add_ing.clicked.connect(self._agregar_fila)
        btn_rem_ing = QPushButton("➖ Quitar")
        btn_rem_ing.clicked.connect(self._quitar_fila)
        ing_btns.addWidget(btn_add_ing)
        ing_btns.addWidget(btn_rem_ing)
        ing_btns.addStretch()
        right.addLayout(ing_btns)

        # Botones de guardado
        btns = QHBoxLayout()
        btn_guardar = QPushButton("💾 Guardar Receta")
        btn_guardar.setProperty("class", "primaryBtn")
        btn_guardar.setMinimumHeight(38)
        btn_guardar.clicked.connect(self._guardar_receta)
        btn_eliminar = QPushButton("🗑 Eliminar")
        btn_eliminar.setProperty("class", "dangerBtn")
        btn_eliminar.clicked.connect(self._eliminar_receta)
        btns.addStretch()
        btns.addWidget(btn_eliminar)
        btns.addWidget(btn_guardar)
        right.addLayout(btns)

        right_widget = QWidget()
        right_widget.setLayout(right)
        layout.addWidget(right_widget, stretch=1)

        # Estado interno para receta en edición
        self._producto_venta_id_actual: Optional[int] = None

    def refrescar(self) -> None:
        try:
            eng = self._p._engine()
            prods = eng.productos_activos()

            # Combos
            self._combo_prod_venta.clear()
            for p in prods:
                self._combo_prod_venta.addItem(p["nombre"], p["id"])

            # Lista de recetas activas
            recetas = eng.recetas_activas()
            self._tabla_lista.setRowCount(len(recetas))
            for i, r in enumerate(recetas):
                self._tabla_lista.setItem(i, 0, _item(r["producto"]))
                self._tabla_lista.setItem(i, 1, _item(r["nombre"] or "—"))
                n_item = _item(str(r["n_lineas"]))
                n_item.setTextAlignment(Qt.AlignCenter)
                self._tabla_lista.setItem(i, 2, n_item)
                self._tabla_lista.item(i, 0).setData(Qt.UserRole, r["receta_id"])
        except Exception as exc:
            logger.warning("refrescar recetas: %s", exc)

        # Cargar materias primas en tabla editor
        self._poblar_combos_detalle()

    def _poblar_combos_detalle(self) -> None:
        """Actualiza los combos en cada fila del editor de detalle."""
        for row in range(self._tabla_det.rowCount()):
            widget = self._tabla_det.cellWidget(row, 0)
            if isinstance(widget, QComboBox):
                self._llenar_combo_mp(widget)

    def _llenar_combo_mp(self, combo: QComboBox, selected_id: int = None) -> None:
        try:
            eng = self._p._engine()
            prods = eng.productos_activos()
            combo.clear()
            for p in prods:
                combo.addItem(p["nombre"], p["id"])
            if selected_id:
                for i in range(combo.count()):
                    if combo.itemData(i) == selected_id:
                        combo.setCurrentIndex(i); break
        except Exception:
            pass

    def _agregar_fila(self) -> None:
        row = self._tabla_det.rowCount()
        self._tabla_det.insertRow(row)

        combo = QComboBox()
        self._llenar_combo_mp(combo)
        self._tabla_det.setCellWidget(row, 0, combo)

        spin = QDoubleSpinBox()
        spin.setRange(0.01, 100.0)
        spin.setDecimals(2)
        spin.setSuffix("%")
        spin.valueChanged.connect(self._recalc_total)
        self._tabla_det.setCellWidget(row, 1, spin)

        btn_del = QPushButton("✕")
        btn_del.setFixedWidth(30)
        btn_del.clicked.connect(lambda _, r=row: self._quitar_fila(r))
        self._tabla_det.setCellWidget(row, 2, btn_del)

        self._recalc_total()

    def _quitar_fila(self, row: int = None) -> None:
        if row is None:
            row = self._tabla_det.currentRow()
        if row < 0: return
        self._tabla_det.removeRow(row)
        self._recalc_total()

    def _recalc_total(self) -> None:
        total = 0.0
        for row in range(self._tabla_det.rowCount()):
            spin = self._tabla_det.cellWidget(row, 1)
            if isinstance(spin, QDoubleSpinBox):
                total += spin.value()
        color = "#27ae60" if abs(total - 100.0) < 0.5 else "#e74c3c"
        self._lbl_total.setText(
            f'<span style="color:{color};font-weight:bold">Total: {total:.2f}%</span>'
        )

    def _nueva_receta(self) -> None:
        """Limpia el editor para crear una nueva receta."""
        self._tabla_det.setRowCount(0)
        self._txt_nombre_receta.clear()
        self._producto_venta_id_actual = None
        self._recalc_total()
        # Agregar 2 filas por defecto
        self._agregar_fila()
        self._agregar_fila()

    def _cargar_receta_seleccionada(self) -> None:
        """Carga la receta seleccionada en la lista para editar."""
        row = self._tabla_lista.currentRow()
        if row < 0: return
        item = self._tabla_lista.item(row, 0)
        if not item: return

        # Buscar el producto_venta_id por nombre
        prod_nombre = item.text()
        try:
            eng = self._p._engine()
            prods = eng.productos_activos()
            prod_id = None
            for p in prods:
                if p["nombre"] == prod_nombre:
                    prod_id = p["id"]; break
            if not prod_id: return

            detalle = eng.detalle_receta(prod_id)
            if not detalle: return

            self._producto_venta_id_actual = prod_id
            self._txt_nombre_receta.setText(detalle["nombre"] or "")

            # Seleccionar producto en combo
            for i in range(self._combo_prod_venta.count()):
                if self._combo_prod_venta.itemData(i) == prod_id:
                    self._combo_prod_venta.setCurrentIndex(i); break

            # Poblar tabla detalle
            self._tabla_det.setRowCount(0)
            for linea in detalle["lineas"]:
                r = self._tabla_det.rowCount()
                self._tabla_det.insertRow(r)

                combo = QComboBox()
                self._llenar_combo_mp(combo, selected_id=linea["materia_prima_id"])
                self._tabla_det.setCellWidget(r, 0, combo)

                spin = QDoubleSpinBox()
                spin.setRange(0.01, 100.0)
                spin.setDecimals(2)
                spin.setSuffix("%")
                spin.setValue(float(linea["porcentaje"]))
                spin.valueChanged.connect(self._recalc_total)
                self._tabla_det.setCellWidget(r, 1, spin)

                btn_del = QPushButton("✕")
                btn_del.setFixedWidth(30)
                _r = r
                btn_del.clicked.connect(lambda _, row=_r: self._quitar_fila(row))
                self._tabla_det.setCellWidget(r, 2, btn_del)

            self._recalc_total()
        except Exception as exc:
            logger.warning("_cargar_receta: %s", exc)

    def _guardar_receta(self) -> None:
        prod_id = self._combo_prod_venta.currentData()
        nombre  = self._txt_nombre_receta.text().strip()
        if not prod_id:
            QMessageBox.warning(self, "Error", "Selecciona el producto de venta"); return
        if not nombre:
            QMessageBox.warning(self, "Error", "Ingresa un nombre para la receta"); return

        lineas = []
        for row in range(self._tabla_det.rowCount()):
            combo = self._tabla_det.cellWidget(row, 0)
            spin  = self._tabla_det.cellWidget(row, 1)
            if not isinstance(combo, QComboBox) or not isinstance(spin, QDoubleSpinBox):
                continue
            mp_id = combo.currentData()
            pct   = spin.value()
            if mp_id and pct > 0:
                lineas.append({
                    "materia_prima_id": mp_id,
                    "porcentaje":       pct,
                    "nombre_mp":        combo.currentText(),
                })

        if not lineas:
            QMessageBox.warning(self, "Error", "Agrega al menos un ingrediente"); return

        try:
            eng = self._p._engine()
            receta_id = eng.guardar_receta(prod_id, nombre, lineas)
            self.refrescar()
            QMessageBox.information(
                self, "✅ Receta Guardada",
                f"Receta '{nombre}' guardada (id={receta_id})."
            )
        except Exception as exc:
            logger.error("guardar_receta: %s", exc)
            QMessageBox.critical(self, "Error", str(exc))

    def _eliminar_receta(self) -> None:
        prod_id = self._combo_prod_venta.currentData()
        if not prod_id:
            QMessageBox.warning(self, "Error", "Selecciona un producto"); return
        nombre = self._combo_prod_venta.currentText()
        resp = QMessageBox.question(
            self, "Eliminar Receta",
            f"¿Desactivar la receta de '{nombre}'?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if resp != QMessageBox.Yes: return
        try:
            eng = self._p._engine()
            eng.eliminar_receta(prod_id)
            self._tabla_det.setRowCount(0)
            self.refrescar()
            QMessageBox.information(self, "✅", "Receta desactivada.")
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))


# ── Helper ────────────────────────────────────────────────────────────────────

def _item(texto: str) -> QTableWidgetItem:
    it = QTableWidgetItem(str(texto))
    it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
    return it

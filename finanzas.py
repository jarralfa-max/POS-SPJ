# modulos/finanzas.py
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *
import sqlite3
from .base import ModuloBase
import os
from datetime import datetime, date

from .base import ModuloBase

class ModuloFinanzas(ModuloBase):
    def __init__(self, conexion, parent=None):
        super().__init__(conexion, parent)
        self.main_window     = parent
        self.usuario_actual  = "admin"
        self.sucursal_id     = 1
        self.sucursal_nombre = "Principal"
        self.init_ui()
        self.conectar_eventos()

    def set_sucursal(self, sucursal_id: int, sucursal_nombre: str):
        """Recibe sucursal activa desde MainWindow tras login."""
        self.sucursal_id     = sucursal_id
        self.sucursal_nombre = sucursal_nombre
        if hasattr(self, 'widget_pollo') and self.widget_pollo.isVisible():
            self.cargar_lotes_pollo()
        
    def set_usuario_actual(self, usuario, rol):
        """Establece el usuario actual para el módulo"""
        self.usuario_actual = usuario
        self.rol_usuario = rol
        
    def obtener_usuario_actual(self):
        """Obtiene el usuario actual para registrar en movimientos"""
        return self.usuario_actual if self.usuario_actual else "Sistema"

    def init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)

        # --- Encabezado ---
        header_layout = QHBoxLayout()
        if os.path.exists("logo.png"):
            logo_label = QLabel()
            pixmap = QPixmap("logo.png")
            if not pixmap.isNull():
                pixmap = pixmap.scaled(50, 50, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                logo_label.setPixmap(pixmap)
            header_layout.addWidget(logo_label)

        title = QLabel("Gestión Financiera")
        title.setObjectName("tituloPrincipal")
        header_layout.addWidget(title)
        header_layout.addStretch()
        layout.addLayout(header_layout)

        # --- Navegación entre secciones ---
        nav_layout = QHBoxLayout()
        self.btn_ver_gastos   = QPushButton("Gastos")
        self.btn_ver_gastos.setObjectName("botonActivo")
        self.btn_ver_personal = QPushButton("Personal")
        self.btn_ver_pollo    = QPushButton("🐔 Pollo")
        self.btn_ver_compras_inv = QPushButton("🛒 Compras Globales")
        self.btn_ver_proveedores = QPushButton("🏢 Proveedores")
        self.btn_ver_cxp         = QPushButton("💸 Cuentas x Pagar")

        nav_layout.addWidget(self.btn_ver_gastos)
        nav_layout.addWidget(self.btn_ver_personal)
        nav_layout.addWidget(self.btn_ver_pollo)
        nav_layout.addWidget(self.btn_ver_compras_inv)
        nav_layout.addWidget(self.btn_ver_proveedores)
        nav_layout.addWidget(self.btn_ver_cxp)
        nav_layout.addStretch()
        layout.addLayout(nav_layout)

        # --- Widgets para cada sección ---
        self.widget_gastos = QWidget()
        self.init_seccion_gastos()
        
        self.widget_personal = QWidget()
        self.init_seccion_personal()

        self.widget_pollo = QWidget()
        self.init_seccion_pollo()

        # ── v9: Compras Inventariables ────────────────────────────────────────
        self.widget_compras_inv = QWidget()
        self.init_seccion_compras_inventariables()

        # ── v9: Proveedores ───────────────────────────────────────────────────
        self.widget_proveedores = QWidget()
        self._init_seccion_proveedores()

        # ── v9: Cuentas por Pagar ─────────────────────────────────────────────
        self.widget_cxp = QWidget()
        self._init_seccion_cxp()

        layout.addWidget(self.widget_gastos)
        layout.addWidget(self.widget_personal)
        layout.addWidget(self.widget_pollo)
        layout.addWidget(self.widget_compras_inv)
        layout.addWidget(self.widget_proveedores)
        layout.addWidget(self.widget_cxp)

        self.setLayout(layout)

        self.btn_ver_gastos.clicked.connect(self.mostrar_gastos)
        self.btn_ver_personal.clicked.connect(self.mostrar_personal)
        self.btn_ver_pollo.clicked.connect(self.mostrar_pollo)
        self.btn_ver_compras_inv.clicked.connect(self.mostrar_compras_inventariables)
        self.btn_ver_proveedores.clicked.connect(self._mostrar_proveedores)
        self.btn_ver_cxp.clicked.connect(self._mostrar_cxp)

        self.mostrar_gastos()

    def aplicar_estilo_boton_activo(self, boton_activo):
        """Aplica estilo para resaltar el botón de sección activa."""
        btns = [self.btn_ver_gastos, self.btn_ver_personal]
        for attr in ('btn_ver_pollo', 'btn_ver_compras_inv', 'btn_ver_proveedores', 'btn_ver_cxp'):
            if hasattr(self, attr):
                btns.append(getattr(self, attr))
        for btn in btns:
            btn.setObjectName("")
            btn.style().unpolish(btn)
            btn.style().polish(btn)
        
        # Aplicar estilo al botón activo
        boton_activo.setObjectName("botonActivo")
        boton_activo.style().unpolish(boton_activo)
        boton_activo.style().polish(boton_activo)
        
    def conectar_eventos(self):
        """Conectar a los eventos del sistema"""
        if hasattr(self.main_window, 'registrar_evento'):
            self.main_window.registrar_evento('venta_realizada', self.on_venta_realizada)
            self.main_window.registrar_evento('cliente_creado', self.on_datos_actualizados)
            self.main_window.registrar_evento('producto_actualizado', self.on_datos_actualizados)

    def desconectar_eventos(self):
        """Desconectar eventos al cerrar el módulo"""
        if hasattr(self.main_window, 'desregistrar_evento'):
            self.main_window.desregistrar_evento('venta_realizada', self.on_venta_realizada)
            self.main_window.desregistrar_evento('cliente_creado', self.on_datos_actualizados)
            self.main_window.desregistrar_evento('producto_actualizado', self.on_datos_actualizados)

    def on_venta_realizada(self, datos):
        """Actualizar cuando se realiza una venta"""
        if datos:
            print(f"Venta #{datos['venta_id']} realizada - actualizando finanzas")
            # Podrías actualizar reportes específicos aquí
            self.actualizar_datos()

    def on_datos_actualizados(self, datos):
        """Actualizar datos generales"""
        self.actualizar_datos()

    def mostrar_gastos(self):
        """Muestra la sección de Gastos."""
        self.aplicar_estilo_boton_activo(self.btn_ver_gastos)
        self.widget_personal.hide()
        self.widget_gastos.show()
        
    def mostrar_pollo(self):
        """Muestra la sección de Pollo."""
        self.aplicar_estilo_boton_activo(self.btn_ver_pollo)
        self.widget_gastos.hide()
        self.widget_personal.hide()
        self.widget_pollo.show() 

    def mostrar_personal(self):
        """Muestra la sección de Personal."""
        self.aplicar_estilo_boton_activo(self.btn_ver_personal)
        self.widget_gastos.hide()
        self.widget_personal.show()
        
    def _mostrar_proveedores(self):
        widgets = [
            self.widget_gastos, self.widget_personal, self.widget_pollo,
            self.widget_compras_inv,
        ]
        if hasattr(self, 'widget_cxp'):
            widgets.append(self.widget_cxp)
        for w in widgets:
            w.hide()
        self.widget_proveedores.show()
        self.aplicar_estilo_boton_activo(self.btn_ver_proveedores)
        self._cargar_proveedores()

    # === SECCIÓN DE GASTOS ===
        # === SECCIÓN DE GASTOS ===
    def init_seccion_gastos(self):
        """Inicializa la interfaz para la gestión de gastos."""
        layout = QVBoxLayout(self.widget_gastos)

        # --- Barra de herramientas de Gastos ---
        toolbar_gastos = QHBoxLayout()
        self.busqueda_gastos = QLineEdit()
        self.busqueda_gastos.setPlaceholderText("Buscar por categoría, proveedor, descripción...")
        self.btn_buscar_gastos = QPushButton()
        self.btn_buscar_gastos.setIcon(self.obtener_icono("search.png"))
        self.btn_buscar_gastos.setToolTip("Buscar Gasto")
        
        # CORRECCIÓN 1: Nombres de atributos consistentes
        self.date_inicio = QDateEdit() 
        self.date_inicio.setDate(QDate.currentDate().addDays(-30)) # Últimos 30 días por defecto
        self.date_inicio.setDisplayFormat("dd/MM/yyyy")
        self.date_inicio.setCalendarPopup(True)
        
        self.date_fin = QDateEdit()
        self.date_fin.setDate(QDate.currentDate())
        self.date_fin.setDisplayFormat("dd/MM/yyyy")
        self.date_fin.setCalendarPopup(True)
        
        self.btn_filtrar_gastos = QPushButton("Filtrar")
        self.btn_filtrar_gastos.setIcon(self.obtener_icono("filter.png"))
        
        self.btn_nuevo_gasto = QPushButton("Nuevo Gasto")
        self.btn_nuevo_gasto.setIcon(self.obtener_icono("add.png"))
        
        toolbar_gastos.addWidget(QLabel("Buscar:"))
        toolbar_gastos.addWidget(self.busqueda_gastos)
        toolbar_gastos.addWidget(self.btn_buscar_gastos)
        toolbar_gastos.addSpacing(20)
        toolbar_gastos.addWidget(QLabel("Desde:"))
        # CORRECCIÓN 2: Agregar widgets con nombres correctos
        toolbar_gastos.addWidget(self.date_inicio) 
        toolbar_gastos.addWidget(QLabel("Hasta:"))
        toolbar_gastos.addWidget(self.date_fin)
        toolbar_gastos.addWidget(self.btn_filtrar_gastos)
        toolbar_gastos.addStretch()
        toolbar_gastos.addWidget(self.btn_nuevo_gasto)
        layout.addLayout(toolbar_gastos)

        # --- Resumen de Gastos ---
        resumen_gastos_group = QGroupBox("Resumen")
        resumen_gastos_layout = QHBoxLayout()
        self.lbl_total_gastos = QLabel("Total: $0.00")
        self.lbl_total_gastos.setObjectName("lblTotalGastos")
        resumen_gastos_layout.addWidget(self.lbl_total_gastos)
        resumen_gastos_layout.addStretch()
        resumen_gastos_group.setLayout(resumen_gastos_layout)
        layout.addWidget(resumen_gastos_group)

        # --- Tabla de Gastos ---
        self.tabla_gastos = QTableWidget()
        self.tabla_gastos.setColumnCount(8)
        self.tabla_gastos.setHorizontalHeaderLabels([
            "ID", "Fecha", "Categoría", "Proveedor", "Monto", "Pagado", "Estado", "Descripción"
        ])
        self.tabla_gastos.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_gastos.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tabla_gastos.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.tabla_gastos)

        # --- Barra de estado/botones de acción de Gastos ---
        acciones_gastos_layout = QHBoxLayout()
        self.btn_editar_gasto = QPushButton("Editar")
        self.btn_editar_gasto.setIcon(self.obtener_icono("edit.png"))
        self.btn_editar_gasto.setEnabled(False)
        
        self.btn_eliminar_gasto = QPushButton("Eliminar")
        self.btn_eliminar_gasto.setIcon(self.obtener_icono("delete.png"))
        self.btn_eliminar_gasto.setEnabled(False)
        
        self.btn_abonar_gasto = QPushButton("Abonar")
        self.btn_abonar_gasto.setIcon(self.obtener_icono("payment.png"))
        self.btn_abonar_gasto.setEnabled(False)
        
        acciones_gastos_layout.addWidget(self.btn_editar_gasto)
        acciones_gastos_layout.addWidget(self.btn_eliminar_gasto)
        acciones_gastos_layout.addWidget(self.btn_abonar_gasto)
        acciones_gastos_layout.addStretch()
        layout.addLayout(acciones_gastos_layout)

        # --- Conexiones de Gastos ---
        self.btn_buscar_gastos.clicked.connect(self.buscar_gastos)
        self.btn_filtrar_gastos.clicked.connect(self.filtrar_gastos)
        self.btn_nuevo_gasto.clicked.connect(self.nuevo_gasto)
        self.btn_editar_gasto.clicked.connect(self.editar_gasto)
        self.btn_eliminar_gasto.clicked.connect(self.eliminar_gasto)
        self.btn_abonar_gasto.clicked.connect(self.abonar_gasto)
        self.tabla_gastos.itemSelectionChanged.connect(self.actualizar_botones_gastos)

        # --- Inicialización de Gastos ---
        self.filtrar_gastos() # Cargar gastos iniciales
        

    # === SECCIÓN DE PERSONAL ===
    def init_seccion_personal(self):
        """Inicializa la interfaz para la gestión de personal."""
        layout = QVBoxLayout(self.widget_personal)

        # --- Barra de herramientas de Personal ---
        toolbar_personal = QHBoxLayout()
        self.busqueda_personal = QLineEdit()
        self.busqueda_personal.setPlaceholderText("Buscar por nombre, puesto...")
        self.btn_buscar_personal = QPushButton()
        self.btn_buscar_personal.setIcon(self.obtener_icono("search.png"))
        self.btn_buscar_personal.setToolTip("Buscar Empleado")
        
        self.combo_filtro_estado = QComboBox()
        self.combo_filtro_estado.addItems(["Todos", "Activos", "Inactivos"])
        
        self.btn_nuevo_empleado = QPushButton("Nuevo Empleado")
        self.btn_nuevo_empleado.setIcon(self.obtener_icono("add.png"))
        
        toolbar_personal.addWidget(QLabel("Buscar:"))
        toolbar_personal.addWidget(self.busqueda_personal)
        toolbar_personal.addWidget(self.btn_buscar_personal)
        toolbar_personal.addSpacing(20)
        toolbar_personal.addWidget(QLabel("Estado:"))
        toolbar_personal.addWidget(self.combo_filtro_estado)
        toolbar_personal.addStretch()
        toolbar_personal.addWidget(self.btn_nuevo_empleado)
        layout.addLayout(toolbar_personal)

        # --- Tabla de Personal ---
        self.tabla_personal = QTableWidget()
        self.tabla_personal.setColumnCount(7)
        self.tabla_personal.setHorizontalHeaderLabels([
            "ID", "Nombre", "Apellidos", "Puesto", "Salario", "Fecha Ingreso", "Estado"
        ])
        self.tabla_personal.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_personal.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tabla_personal.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.tabla_personal)

        # --- Barra de estado/botones de acción de Personal ---
        acciones_personal_layout = QHBoxLayout()
        self.btn_editar_empleado = QPushButton("Editar")
        self.btn_editar_empleado.setIcon(self.obtener_icono("edit.png"))
        self.btn_editar_empleado.setEnabled(False)
        
        self.btn_eliminar_empleado = QPushButton("Eliminar")
        self.btn_eliminar_empleado.setIcon(self.obtener_icono("delete.png"))
        self.btn_eliminar_empleado.setEnabled(False)
        
        acciones_personal_layout.addWidget(self.btn_editar_empleado)
        acciones_personal_layout.addWidget(self.btn_eliminar_empleado)
        acciones_personal_layout.addStretch()
        layout.addLayout(acciones_personal_layout)

        # --- Conexiones de Personal ---
        self.btn_buscar_personal.clicked.connect(self.buscar_personal)
        self.combo_filtro_estado.currentIndexChanged.connect(self.cargar_personal)
        self.btn_nuevo_empleado.clicked.connect(self.nuevo_empleado)
        self.btn_editar_empleado.clicked.connect(self.editar_empleado)
        self.btn_eliminar_empleado.clicked.connect(self.eliminar_empleado)
        self.tabla_personal.itemSelectionChanged.connect(self.actualizar_botones_personal)

        # --- Inicialización de Personal ---
        self.cargar_personal() # Cargar personal inicial
        
    def _init_seccion_proveedores(self):
        from PyQt5.QtWidgets import (QVBoxLayout, QHBoxLayout, QPushButton,
                                     QTableWidget, QTableWidgetItem, QHeaderView,
                                     QAbstractItemView, QLabel, QLineEdit, QFormLayout,
                                     QDialog, QDialogButtonBox, QCheckBox)
        layout = QVBoxLayout(self.widget_proveedores)
        layout.setContentsMargins(12, 12, 12, 12)

        # Toolbar
        top = QHBoxLayout()
        top.addWidget(QLabel("🏢 Directorio de Proveedores"))
        top.addStretch()
        self._btn_nuevo_prov = QPushButton("➕ Nuevo Proveedor")
        self._btn_nuevo_prov.clicked.connect(self._nuevo_proveedor)
        top.addWidget(self._btn_nuevo_prov)
        layout.addLayout(top)

        self._tabla_proveedores = QTableWidget()
        self._tabla_proveedores.setColumnCount(6)
        self._tabla_proveedores.setHorizontalHeaderLabels(
            ["ID", "Nombre", "Contacto", "Teléfono", "Email", "RFC"]
        )
        self._tabla_proveedores.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tabla_proveedores.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tabla_proveedores.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self._tabla_proveedores.verticalHeader().setVisible(False)
        self._tabla_proveedores.setAlternatingRowColors(True)
        layout.addWidget(self._tabla_proveedores)
        
    def _nuevo_proveedor(self):
        from PyQt5.QtWidgets import QDialog, QFormLayout, QLineEdit, QDialogButtonBox, QVBoxLayout
        dlg = QDialog(self)
        dlg.setWindowTitle("Nuevo Proveedor")
        dlg.setMinimumWidth(380)
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        txt_nombre   = QLineEdit(); form.addRow("Nombre *:", txt_nombre)
        txt_contacto = QLineEdit(); form.addRow("Contacto:", txt_contacto)
        txt_tel      = QLineEdit(); form.addRow("Teléfono:", txt_tel)
        txt_email    = QLineEdit(); form.addRow("Email:", txt_email)
        txt_rfc      = QLineEdit(); form.addRow("RFC:", txt_rfc)
        layout.addLayout(form)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        layout.addWidget(btns)
        if dlg.exec_() == QDialog.Accepted:
            nombre = txt_nombre.text().strip()
            if not nombre:
                QMessageBox.warning(self, "Error", "El nombre es obligatorio.")
                return
            try:
                from core.services.compras_inventariables_engine import ComprasInventariablesEngine
                eng = ComprasInventariablesEngine(self.conexion, self.sucursal_id, self.usuario_actual or "admin")
                eng.crear_proveedor(nombre, txt_contacto.text(), txt_tel.text(),
                                    txt_email.text(), txt_rfc.text())
                self._cargar_proveedores()
                QMessageBox.information(self, "✅ Guardado", f"Proveedor '{nombre}' creado correctamente.")
            except Exception as exc:
                QMessageBox.critical(self, "Error", str(exc))

    def _cargar_proveedores(self):
        if not hasattr(self, '_tabla_proveedores'):
            return
        from core.services.compras_inventariables_engine import ComprasInventariablesEngine
        eng = ComprasInventariablesEngine(self.conexion, self.sucursal_id, self.usuario_actual or "admin")
        provs = eng.listar_proveedores()
        self._tabla_proveedores.setRowCount(len(provs))
        for i, p in enumerate(provs):
            for col, val in enumerate([str(p.id), p.nombre, p.contacto, p.telefono, p.email, p.rfc]):
                it = QTableWidgetItem(val)
                it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                self._tabla_proveedores.setItem(i, col, it)


    # === FUNCIONES DE GASTOS ===
    # ── v9: Cuentas por Pagar (CXP) ───────────────────────────────────────────
    def _mostrar_cxp(self):
        widgets = [
            self.widget_gastos, self.widget_personal, self.widget_pollo,
            self.widget_compras_inv,
        ]
        if hasattr(self, 'widget_proveedores'):
            widgets.append(self.widget_proveedores)
        for w in widgets:
            w.hide()
        self.widget_cxp.show()
        self.aplicar_estilo_boton_activo(self.btn_ver_cxp)
        self._cargar_cxp()

    def _init_seccion_cxp(self):
        from PyQt5.QtWidgets import (QVBoxLayout, QHBoxLayout, QPushButton,
                                     QTableWidget, QTableWidgetItem, QHeaderView,
                                     QAbstractItemView, QLabel)
        layout = QVBoxLayout(self.widget_cxp)
        layout.setContentsMargins(12, 12, 12, 12)

        top = QHBoxLayout()
        top.addWidget(QLabel("💸 Cuentas por Pagar — compras con crédito o saldo pendiente"))
        top.addStretch()
        btn_pagar = QPushButton("💵 Registrar Pago")
        btn_pagar.clicked.connect(self._registrar_pago_cxp)
        top.addWidget(btn_pagar)
        btn_ref = QPushButton("🔄 Actualizar")
        btn_ref.clicked.connect(self._cargar_cxp)
        top.addWidget(btn_ref)
        layout.addLayout(top)

        self._lbl_cxp_total = QLabel("")
        layout.addWidget(self._lbl_cxp_total)

        self._tabla_cxp = QTableWidget()
        self._tabla_cxp.setColumnCount(8)
        self._tabla_cxp.setHorizontalHeaderLabels(
            ["ID", "Proveedor", "Producto", "Total", "Pagado", "Saldo", "Vencimiento", "Estado"]
        )
        self._tabla_cxp.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._tabla_cxp.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self._tabla_cxp.horizontalHeader().setSectionResizeMode(1, QHeaderView.Stretch)
        self._tabla_cxp.verticalHeader().setVisible(False)
        self._tabla_cxp.setAlternatingRowColors(True)
        layout.addWidget(self._tabla_cxp)

    def _cargar_cxp(self):
        if not hasattr(self, '_tabla_cxp'):
            return
        from core.services.compras_inventariables_engine import ComprasInventariablesEngine
        eng = ComprasInventariablesEngine(self.conexion, self.sucursal_id, self.usuario_actual or "admin")
        cxps = eng.cuentas_por_pagar()
        self._tabla_cxp.setRowCount(len(cxps))
        total_saldo = sum(c.saldo_pendiente for c in cxps)
        self._lbl_cxp_total.setText(
            f"<b>Total pendiente:</b> ${total_saldo:,.2f}  |  "
            f"<b>{len(cxps)}</b> cuentas abiertas"
        )
        from PyQt5.QtGui import QColor
        for i, c in enumerate(cxps):
            vals = [
                str(c.id), c.proveedor, c.producto_nombre,
                f"${c.monto_total:,.2f}", f"${c.monto_pagado:,.2f}",
                f"${c.saldo_pendiente:,.2f}",
                (c.fecha_vencimiento or "—")[:10], c.estado.capitalize()
            ]
            for col, val in enumerate(vals):
                it = QTableWidgetItem(val)
                it.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                if col in (3, 4, 5):
                    it.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self._tabla_cxp.setItem(i, col, it)
            # Color si está vencida
            if c.fecha_vencimiento:
                from datetime import date
                try:
                    vence = date.fromisoformat(c.fecha_vencimiento)
                    if vence < date.today():
                        for col in range(8):
                            cell = self._tabla_cxp.item(i, col)
                            if cell:
                                cell.setForeground(QColor("#e74c3c"))
                except Exception:
                    pass
    
    def _registrar_pago_cxp(self):
        row = self._tabla_cxp.currentRow()
        if row < 0:
            QMessageBox.warning(self, "Aviso", "Seleccione una cuenta por pagar.")
            return
        cxp_id = int(self._tabla_cxp.item(row, 0).text())
        saldo_str = self._tabla_cxp.item(row, 5).text().replace("$", "").replace(",", "")
        saldo = float(saldo_str)

        from PyQt5.QtWidgets import QDialog, QFormLayout, QDoubleSpinBox, QDialogButtonBox, QVBoxLayout
        dlg = QDialog(self)
        dlg.setWindowTitle("Registrar Pago")
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        spin = QDoubleSpinBox()
        spin.setDecimals(2)
        spin.setRange(0.01, saldo)
        spin.setValue(saldo)
        spin.setPrefix("$")
        form.addRow(f"Monto a pagar (saldo: ${saldo:,.2f}):", spin)
        layout.addLayout(form)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(dlg.accept)
        btns.rejected.connect(dlg.reject)
        layout.addWidget(btns)
        if dlg.exec_() == QDialog.Accepted:
            try:
                from core.services.compras_inventariables_engine import ComprasInventariablesEngine
                eng = ComprasInventariablesEngine(self.conexion, self.sucursal_id, self.usuario_actual or "admin")
                result = eng.registrar_pago_cxp(cxp_id, spin.value())
                self._cargar_cxp()
                QMessageBox.information(
                    self, "✅ Pago registrado",
                    f"Pago de ${spin.value():,.2f} aplicado.\nNuevo saldo: ${result['nuevo_saldo']:,.2f}"
                )
            except Exception as exc:
                QMessageBox.critical(self, "Error", str(exc))
            
                
    def cargar_gastos(self, consulta=None, parametros=None):
        """Carga la lista de gastos en la tabla."""
        try:
            cursor = self.conexion.cursor()
            
            if consulta is None:
                consulta = """
                    SELECT g.id, g.fecha, g.categoria, p.nombre, g.monto, g.monto_pagado, g.estado, g.descripcion
                    FROM gastos g
                    LEFT JOIN proveedores p ON g.proveedor_id = p.id
                    ORDER BY g.fecha DESC
                """
                parametros = []
            
            cursor.execute(consulta, parametros)
            gastos = cursor.fetchall()

            self.tabla_gastos.setRowCount(len(gastos))
            total_gastos = 0.0
            for row, gasto in enumerate(gastos):
                for col, valor in enumerate(gasto):
                    if col in [4, 5]: # Monto y Monto Pagado
                        item = QTableWidgetItem(f"${valor:,.2f}" if valor is not None else "$0.00")
                        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        self.tabla_gastos.setItem(row, col, item)
                        if col == 4: # Sumar al total
                            total_gastos += valor if valor else 0.0
                    elif col == 6: # Estado
                        item = QTableWidgetItem(str(valor) if valor is not None else "")
                        # Aplicar color según estado
                        if valor == "PAGADO":
                            item.setForeground(QColor('green'))
                        elif valor == "PENDIENTE":
                            item.setForeground(QColor('orange'))
                        elif valor == "PARCIAL":
                            item.setForeground(QColor('blue'))
                        self.tabla_gastos.setItem(row, col, item)
                    else:
                        self.tabla_gastos.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))

            self.lbl_total_gastos.setText(f"Total: <b>${total_gastos:,.2f}</b>")

        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al cargar gastos: {str(e)}", QMessageBox.Critical)
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error inesperado: {str(e)}", QMessageBox.Critical)

    def filtrar_gastos(self):
        """Filtra los gastos por rango de fechas."""
        try:
            # CORRECCIÓN 3: Usar nombres de atributos correctos
            fecha_inicio = self.date_inicio.date().toString("yyyy-MM-dd")
            fecha_fin = self.date_fin.date().toString("yyyy-MM-dd")
            
            consulta = """
                SELECT g.id, g.fecha, g.categoria, p.nombre, g.monto, g.monto_pagado, g.estado, g.descripcion
                FROM gastos g
                LEFT JOIN proveedores p ON g.proveedor_id = p.id
                WHERE date(g.fecha) BETWEEN ? AND ?
                ORDER BY g.fecha DESC
            """
            parametros = [fecha_inicio, fecha_fin]
            
            self.cargar_gastos(consulta, parametros)
            
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error al filtrar gastos: {str(e)}", QMessageBox.Critical)
            
    def buscar_gastos(self):
        """Busca gastos según el texto ingresado."""
        texto = self.busqueda_gastos.text().strip()
        if not texto:
            self.filtrar_gastos() # Si no hay texto, aplicar filtro por fechas
            return

        try:
            # CORRECCIÓN 4: Usar nombres de atributos correctos
            fecha_inicio = self.date_inicio.date().toString("yyyy-MM-dd")
            fecha_fin = self.date_fin.date().toString("yyyy-MM-dd")
            
            consulta = """
                SELECT g.id, g.fecha, g.categoria, p.nombre, g.monto, g.monto_pagado, g.estado, g.descripcion
                FROM gastos g
                LEFT JOIN proveedores p ON g.proveedor_id = p.id
                WHERE date(g.fecha) BETWEEN ? AND ?
                  AND (g.categoria LIKE ? OR p.nombre LIKE ? OR g.descripcion LIKE ?)
                ORDER BY g.fecha DESC
            """
            parametros = [fecha_inicio, fecha_fin, f"%{texto}%", f"%{texto}%", f"%{texto}%"]
            
            self.cargar_gastos(consulta, parametros)
            
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error en búsqueda de gastos: {str(e)}", QMessageBox.Critical)
            
    def nuevo_gasto(self):
        """Abre el diálogo para crear un nuevo gasto."""
        dialogo = DialogoGasto(self.conexion, self.usuario_actual, self)
        if dialogo.exec_() == QDialog.Accepted:
            self.filtrar_gastos() # Refrescar la lista
            # Notificar a otros módulos
            if hasattr(self.main_window, 'notificar_evento'):
                self.main_window.notificar_evento('gasto_creado', {})
            # También notificar directamente si es posible
            self.notificar_actualizacion_gastos()

    
    def editar_gasto(self):
        """Abre el diálogo para editar un gasto seleccionado."""
        fila_seleccionada = self.tabla_gastos.currentRow()
        if fila_seleccionada < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un gasto para editar.")
            return

        try:
            id_gasto = int(self.tabla_gastos.item(fila_seleccionada, 0).text())
            cursor = self.conexion.cursor()
            cursor.execute("SELECT * FROM gastos WHERE id = ?", (id_gasto,))
            gasto_data = cursor.fetchone()
            
            if gasto_data:
                columnas = [description[0] for description in cursor.description]
                gasto_dict = dict(zip(columnas, gasto_data))
                
                dialogo = DialogoGasto(self.conexion, self.usuario_actual, self, gasto_dict)
                if dialogo.exec_() == QDialog.Accepted:
                    self.filtrar_gastos()
                    # Notificar a otros módulos
                    if hasattr(self.main_window, 'notificar_evento'):
                        self.main_window.notificar_evento('gasto_actualizado', {'id': id_gasto})
                    self.notificar_actualizacion_gastos()
            else:
                self.mostrar_mensaje("Error", "Gasto no encontrado.")

        except Exception as e:
            self.mostrar_mensaje("Error", f"Error: {str(e)}")
            
    def eliminar_gasto(self):
        """Elimina un gasto."""
        fila_seleccionada = self.tabla_gastos.currentRow()
        if fila_seleccionada < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un gasto para eliminar.")
            return

        try:
            id_gasto = int(self.tabla_gastos.item(fila_seleccionada, 0).text())
            categoria = self.tabla_gastos.item(fila_seleccionada, 2).text() or ""
            monto = self.tabla_gastos.item(fila_seleccionada, 4).text() or "$0.00"
            
            respuesta = self.mostrar_mensaje(
                "Confirmar Eliminación",
                f"¿Está seguro que desea eliminar este gasto?\n\n"
                f"Categoría: {categoria}\nMonto: {monto}\n\n"
                f"Esta acción no se puede deshacer.",
                QMessageBox.Question,
                QMessageBox.Yes | QMessageBox.No
            )
            
            if respuesta == QMessageBox.Yes:
                cursor = self.conexion.cursor()
                cursor.execute("DELETE FROM gastos WHERE id = ?", (id_gasto,))
                self.conexion.commit()
                self.mostrar_mensaje("Éxito", "Gasto eliminado correctamente.")
                self.filtrar_gastos()
                
                # Notificar a otros módulos
                if hasattr(self.main_window, 'notificar_evento'):
                    self.main_window.notificar_evento('gasto_eliminado', {'id': id_gasto})
                self.notificar_actualizacion_gastos()

        except ValueError:
            self.mostrar_mensaje("Error", "ID de gasto inválido.")
        except sqlite3.Error as e:
            self.conexion.rollback()
            self.mostrar_mensaje("Error", f"Error al eliminar gasto: {str(e)}", QMessageBox.Critical)
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error inesperado: {str(e)}", QMessageBox.Critical)

    def abonar_gasto(self):
        """Registra un abono a un gasto."""
        fila_seleccionada = self.tabla_gastos.currentRow()
        if fila_seleccionada < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un gasto para abonar.")
            return

        try:
            id_gasto = int(self.tabla_gastos.item(fila_seleccionada, 0).text())
            estado_actual = self.tabla_gastos.item(fila_seleccionada, 6).text()
            monto_str = self.tabla_gastos.item(fila_seleccionada, 4).text()
            pagado_str = self.tabla_gastos.item(fila_seleccionada, 5).text()
            
            # Parsear montos
            monto_total = float(monto_str.replace('$', '').replace(',', '')) if monto_str else 0.0
            monto_pagado = float(pagado_str.replace('$', '').replace(',', '')) if pagado_str else 0.0
            saldo_pendiente = monto_total - monto_pagado
            
            if estado_actual == "PAGADO":
                self.mostrar_mensaje("Advertencia", "Este gasto ya está pagado.")
                return

            monto_abono, ok = QInputDialog.getDouble(
                self, "Abonar Gasto", 
                f"Saldo pendiente: ${saldo_pendiente:,.2f}\nIngrese el monto a abonar:",
                0, 0, saldo_pendiente, 2
            )
            
            if ok and monto_abono > 0:
                nuevo_pagado = monto_pagado + monto_abono
                nuevo_estado = "PAGADO" if nuevo_pagado >= monto_total else "PARCIAL"
                cursor = self.conexion.cursor()
                cursor.execute("""
                    UPDATE gastos 
                    SET monto_pagado = ?, estado = ?
                    WHERE id = ?
                """, (nuevo_pagado, nuevo_estado, id_gasto))
                
                self.conexion.commit()
                self.mostrar_mensaje("Éxito", f"Abono de ${monto_abono:,.2f} registrado correctamente.")
                self.filtrar_gastos()
                
                # Notificar a otros módulos
                if hasattr(self.main_window, 'notificar_evento'):
                    self.main_window.notificar_evento('gasto_abonado', {
                        'id': id_gasto, 
                        'monto_abono': monto_abono,
                        'nuevo_estado': nuevo_estado
                    })
                self.notificar_actualizacion_gastos()

        except ValueError:
            self.mostrar_mensaje("Error", "Datos de gasto inválidos.")
        except sqlite3.Error as e:
            self.conexion.rollback()
            self.mostrar_mensaje("Error", f"Error al registrar abono: {str(e)}", QMessageBox.Critical)
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error inesperado: {str(e)}", QMessageBox.Critical)
    
    def notificar_actualizacion_gastos(self):
        """Notifica que los gastos han sido actualizados"""
        print("Notificando actualización de gastos...")
        # Emitir una señal personalizada si es necesario
        if hasattr(self, 'gastos_actualizados'):
            self.gastos_actualizados.emit()
            
    def actualizar_botones_gastos(self):
        """Habilita/deshabilita botones de gastos según la selección en la tabla."""
        seleccionado = len(self.tabla_gastos.selectedItems()) > 0
        self.btn_editar_gasto.setEnabled(seleccionado)
        self.btn_eliminar_gasto.setEnabled(seleccionado)
        self.btn_abonar_gasto.setEnabled(seleccionado)

    # === FUNCIONES DE PERSONAL ===
    def cargar_personal(self):
        """Carga la lista de personal en la tabla."""
        try:
            cursor = self.conexion.cursor()
            
            filtro_estado = self.combo_filtro_estado.currentText()
            condicion_estado = ""
            if filtro_estado == "Activos":
                condicion_estado = "WHERE activo = 1"
            elif filtro_estado == "Inactivos":
                condicion_estado = "WHERE activo = 0"

            consulta = f"""
                SELECT id, nombre, apellidos, puesto, salario, fecha_ingreso, 
                       CASE WHEN activo = 1 THEN 'Activo' ELSE 'Inactivo' END as estado
                FROM personal
                {condicion_estado}
                ORDER BY nombre
            """
            
            cursor.execute(consulta)
            empleados = cursor.fetchall()

            self.tabla_personal.setRowCount(len(empleados))
            for row, empleado in enumerate(empleados):
                for col, valor in enumerate(empleado):
                    if col == 4: # Salario
                        item = QTableWidgetItem(f"${valor:,.2f}" if valor is not None else "$0.00")
                        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        self.tabla_personal.setItem(row, col, item)
                    elif col == 6: # Estado
                        item = QTableWidgetItem(str(valor) if valor is not None else "")
                        if valor == "Inactivo":
                            item.setForeground(QColor('red'))
                        self.tabla_personal.setItem(row, col, item)
                    else:
                        self.tabla_personal.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))

        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al cargar personal: {str(e)}", QMessageBox.Critical)
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error inesperado: {str(e)}", QMessageBox.Critical)

    def buscar_personal(self):
        """Busca personal según el texto ingresado."""
        texto = self.busqueda_personal.text().strip()
        if not texto:
            self.cargar_personal()
            return

        try:
            filtro_estado = self.combo_filtro_estado.currentText()
            condicion_estado = ""
            if filtro_estado == "Activos":
                condicion_estado = "AND p.activo = 1"
            elif filtro_estado == "Inactivos":
                condicion_estado = "AND p.activo = 0"

            consulta = f"""
                SELECT p.id, p.nombre, p.apellidos, p.puesto, p.salario, p.fecha_ingreso,
                       CASE WHEN p.activo = 1 THEN 'Activo' ELSE 'Inactivo' END as estado
                FROM personal p
                WHERE (p.nombre LIKE ? OR p.apellidos LIKE ? OR p.puesto LIKE ?)
                {condicion_estado}
                ORDER BY p.nombre
            """
            parametros = [f"%{texto}%", f"%{texto}%", f"%{texto}%"]

            cursor = self.conexion.cursor()
            cursor.execute(consulta, parametros)
            empleados = cursor.fetchall()

            self.tabla_personal.setRowCount(len(empleados))
            for row, empleado in enumerate(empleados):
                for col, valor in enumerate(empleado):
                    if col == 4: # Salario
                        item = QTableWidgetItem(f"${valor:,.2f}" if valor is not None else "$0.00")
                        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        self.tabla_personal.setItem(row, col, item)
                    elif col == 6: # Estado
                        item = QTableWidgetItem(str(valor) if valor is not None else "")
                        if valor == "Inactivo":
                            item.setForeground(QColor('red'))
                        self.tabla_personal.setItem(row, col, item)
                    else:
                        self.tabla_personal.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))

        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error en búsqueda de personal: {str(e)}", QMessageBox.Critical)
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error inesperado en búsqueda: {str(e)}", QMessageBox.Critical)

    def nuevo_empleado(self):
        """Abre el diálogo para crear un nuevo empleado."""
        dialogo = DialogoEmpleado(self.conexion, self)
        if dialogo.exec_() == QDialog.Accepted:
            self.cargar_personal()
            # Notificar a otros módulos si es necesario
            if hasattr(self.main_window, 'notificar_evento'):
                self.main_window.notificar_evento('empleado_creado', {})

    def editar_empleado(self):
        """Abre el diálogo para editar un empleado seleccionado."""
        fila_seleccionada = self.tabla_personal.currentRow()
        if fila_seleccionada < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un empleado para editar.")
            return

        try:
            id_empleado = int(self.tabla_personal.item(fila_seleccionada, 0).text())
            cursor = self.conexion.cursor()
            cursor.execute("SELECT * FROM personal WHERE id = ?", (id_empleado,))
            empleado_data = cursor.fetchone()
            
            if empleado_data:
                # Crear un diccionario con los datos del empleado
                columnas = [description[0] for description in cursor.description]
                empleado_dict = dict(zip(columnas, empleado_data))
                
                dialogo = DialogoEmpleado(self.conexion, self, empleado_dict)
                if dialogo.exec_() == QDialog.Accepted:
                    self.cargar_personal()
                    # Notificar a otros módulos si es necesario
                    if hasattr(self.main_window, 'notificar_evento'):
                        self.main_window.notificar_evento('empleado_actualizado', {'id': id_empleado})
            else:
                self.mostrar_mensaje("Error", "Empleado no encontrado.")

        except ValueError:
            self.mostrar_mensaje("Error", "ID de empleado inválido.")
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al cargar datos del empleado: {str(e)}", QMessageBox.Critical)
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error inesperado: {str(e)}", QMessageBox.Critical)

    def eliminar_empleado(self):
        """Elimina un empleado (lógicamente, cambiando su estado a inactivo)."""
        fila_seleccionada = self.tabla_personal.currentRow()
        if fila_seleccionada < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un empleado para eliminar.")
            return

        try:
            id_empleado = int(self.tabla_personal.item(fila_seleccionada, 0).text())
            nombre_empleado = self.tabla_personal.item(fila_seleccionada, 1).text()
            apellidos_empleado = self.tabla_personal.item(fila_seleccionada, 2).text() if self.tabla_personal.item(fila_seleccionada, 2) else ""
            nombre_completo = f"{nombre_empleado} {apellidos_empleado}".strip()
            
            respuesta = self.mostrar_mensaje(
                "Confirmar Eliminación",
                f"¿Está seguro que desea desactivar al empleado '{nombre_completo}'?\n\n"
                f"Esto lo marcará como inactivo.",
                QMessageBox.Question,
                QMessageBox.Yes | QMessageBox.No
            )
            
            if respuesta == QMessageBox.Yes:
                cursor = self.conexion.cursor()
                # Actualizar a inactivo
                cursor.execute("UPDATE personal SET activo = 0 WHERE id = ?", (id_empleado,))
                self.conexion.commit()
                self.mostrar_mensaje("Éxito", "Empleado desactivado correctamente.")
                self.cargar_personal()
                
                # Notificar a otros módulos si es necesario
                if hasattr(self.main_window, 'notificar_evento'):
                    self.main_window.notificar_evento('empleado_eliminado', {'id': id_empleado})

        except ValueError:
            self.mostrar_mensaje("Error", "ID de empleado inválido.")
        except sqlite3.Error as e:
            self.conexion.rollback()
            self.mostrar_mensaje("Error", f"Error al desactivar empleado: {str(e)}", QMessageBox.Critical)
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error inesperado: {str(e)}", QMessageBox.Critical)

    def actualizar_botones_personal(self):
        """Habilita/deshabilita botones de personal según la selección en la tabla."""
        seleccionado = len(self.tabla_personal.selectedItems()) > 0
        self.btn_editar_empleado.setEnabled(seleccionado)
        self.btn_eliminar_empleado.setEnabled(seleccionado)

    def actualizar_datos(self):
        """Actualiza los datos de todas las secciones."""
        self.filtrar_gastos()
        self.cargar_personal()
        if hasattr(self, 'widget_pollo') and self.widget_pollo.isVisible():
            self.cargar_lotes_pollo()

    # =========================================================================
    # SECCIÓN INVENTARIO POLLO  (Fase 6 — Enterprise)
    # =========================================================================

    def init_seccion_pollo(self):
        """Inicializa la UI de Inventario Pollo dentro del módulo Finanzas."""
        layout = QVBoxLayout(self.widget_pollo)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)

        titulo = QLabel("🐔 Inventario de Pollo — Control por Lotes FIFO")
        titulo.setObjectName("tituloPrincipal")
        layout.addWidget(titulo)

        tabs = QTabWidget()
        layout.addWidget(tabs)

        tab_compra  = QWidget(); self._init_tab_compra(tab_compra)
        tab_transf  = QWidget(); self._init_tab_transformacion(tab_transf)
        tab_inv     = QWidget(); self._init_tab_inventario(tab_inv)
        tab_hist    = QWidget(); self._init_tab_historial(tab_hist)

        tabs.addTab(tab_compra, "📦 Registrar Lote")
        tabs.addTab(tab_transf, "🔪 Transformar")
        tabs.addTab(tab_inv,    "📊 Inventario")
        tabs.addTab(tab_hist,   "📋 Ledger")

    # ── Tab 1: Registrar Lote ─────────────────────────────────────────────────

    def _init_tab_compra(self, parent):
        layout = QVBoxLayout(parent)
        layout.setSpacing(6)

        grp = QGroupBox("Datos del Lote de Compra")
        form = QFormLayout(grp)
        form.setVerticalSpacing(7)

        self.pc_date  = QDateEdit(QDate.currentDate())
        self.pc_date.setDisplayFormat("dd/MM/yyyy")
        self.pc_date.setCalendarPopup(True)

        self.pc_combo_prod = QComboBox()
        self._recargar_productos_pollo()

        self.pc_spin_pollos = QSpinBox()
        self.pc_spin_pollos.setRange(1, 9999)
        self.pc_spin_pollos.setValue(10)
        self.pc_spin_pollos.setSuffix(" pzas")

        self.pc_spin_kg = QDoubleSpinBox()
        self.pc_spin_kg.setRange(0.001, 99999)
        self.pc_spin_kg.setDecimals(3)
        self.pc_spin_kg.setSuffix(" kg")
        self.pc_spin_kg.setValue(20.0)

        self.pc_spin_costo = QDoubleSpinBox()
        self.pc_spin_costo.setRange(0, 9_999_999)
        self.pc_spin_costo.setDecimals(2)
        self.pc_spin_costo.setPrefix("$ ")

        self.pc_lbl_ckg = QLabel("$/kg: $0.00")
        self.pc_lbl_ckg.setStyleSheet("font-weight:bold; color:#27ae60;")

        self.pc_combo_prov = QComboBox()
        self.pc_combo_prov.setEditable(True)
        self._recargar_proveedores_pollo()

        self.pc_combo_estado = QComboBox()
        self.pc_combo_estado.addItems(["PAGADO", "PENDIENTE", "PARCIAL"])

        self.pc_combo_pago = QComboBox()
        self.pc_combo_pago.addItems(["Efectivo", "Transferencia", "Tarjeta", "Crédito"])

        self.pc_desc = QTextEdit()
        self.pc_desc.setMaximumHeight(52)
        self.pc_desc.setPlaceholderText("Notas u observaciones...")

        form.addRow("Fecha:", self.pc_date)
        form.addRow("Producto base*:", self.pc_combo_prod)
        form.addRow("Núm. pollos:", self.pc_spin_pollos)
        form.addRow("Kg totales*:", self.pc_spin_kg)
        form.addRow("Costo total*:", self.pc_spin_costo)
        form.addRow("", self.pc_lbl_ckg)
        form.addRow("Proveedor:", self.pc_combo_prov)
        form.addRow("Estado pago:", self.pc_combo_estado)
        form.addRow("Método pago:", self.pc_combo_pago)
        form.addRow("Descripción:", self.pc_desc)
        layout.addWidget(grp)

        btns = QHBoxLayout()
        btn_preview  = QPushButton("🧮 Preview Subproductos")
        btn_registrar = QPushButton("💾 Registrar Lote")
        btn_registrar.setMinimumHeight(36)
        btn_preview.clicked.connect(self._preview_subproductos)
        btn_registrar.clicked.connect(self._registrar_lote)
        btns.addWidget(btn_preview)
        btns.addWidget(btn_registrar)
        layout.addLayout(btns)

        self.pc_tabla_prev = QTableWidget()
        self.pc_tabla_prev.setColumnCount(4)
        self.pc_tabla_prev.setHorizontalHeaderLabels(
            ["Corte", "Rendimiento %", "Kg estimados", "Costo estimado"])
        self.pc_tabla_prev.setMaximumHeight(160)
        self.pc_tabla_prev.horizontalHeader().setStretchLastSection(True)
        self.pc_tabla_prev.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(QLabel("Vista previa de subproductos (desde receta):"))
        layout.addWidget(self.pc_tabla_prev)

        self.pc_spin_kg.valueChanged.connect(self._update_ckg)
        self.pc_spin_costo.valueChanged.connect(self._update_ckg)

    def _update_ckg(self):
        kg = self.pc_spin_kg.value()
        c  = self.pc_spin_costo.value()
        self.pc_lbl_ckg.setText(f"$/kg: ${c/kg:.2f}" if kg > 0 else "$/kg: —")

    def _recargar_productos_pollo(self):
        try:
            rows = self.conexion.execute("""
                SELECT id, nombre FROM productos
                WHERE activo=1 AND (
                    LOWER(nombre) LIKE '%pollo%' OR
                    LOWER(COALESCE(categoria,'')) LIKE '%pollo%')
                ORDER BY nombre
            """).fetchall()
            self.pc_combo_prod.clear()
            if not rows:
                rows = self.conexion.execute(
                    "SELECT id, nombre FROM productos WHERE activo=1 ORDER BY nombre"
                ).fetchall()
            for pid, nombre in rows:
                self.pc_combo_prod.addItem(nombre, pid)
        except Exception: pass

    def _recargar_proveedores_pollo(self):
        try:
            rows = self.conexion.execute(
                "SELECT DISTINCT proveedor FROM compras_pollo WHERE proveedor IS NOT NULL ORDER BY proveedor"
            ).fetchall()
            self.pc_combo_prov.clear()
            self.pc_combo_prov.addItem("")
            for (p,) in rows:
                if p: self.pc_combo_prov.addItem(p)
        except Exception: pass

    def _preview_subproductos(self):
        kg = self.pc_spin_kg.value()
        costo = self.pc_spin_costo.value()
        ckg = (costo / kg) if kg > 0 else 0
        try:
            rows = self.conexion.execute("""
                SELECT p.nombre, rd.porcentaje_rendimiento
                FROM rendimiento_derivados rd
                JOIN productos p ON rd.producto_derivado_id = p.id
                ORDER BY rd.porcentaje_rendimiento DESC
            """).fetchall()
        except Exception:
            rows = []
        if not rows:
            QMessageBox.information(self, "Sin receta",
                "No hay receta de rendimiento configurada.\n"
                "Agréguela en Inventario → Inventario de Pollo → pestaña Rendimiento.")
            return
        self.pc_tabla_prev.setRowCount(len(rows))
        for i, (nombre, pct) in enumerate(rows):
            pct = pct or 0
            kg_c = round(kg * pct / 100, 3)
            self.pc_tabla_prev.setItem(i, 0, QTableWidgetItem(nombre))
            self.pc_tabla_prev.setItem(i, 1, QTableWidgetItem(f"{pct:.1f}%"))
            self.pc_tabla_prev.setItem(i, 2, QTableWidgetItem(f"{kg_c:.3f} kg"))
            self.pc_tabla_prev.setItem(i, 3, QTableWidgetItem(f"${kg_c*ckg:.2f}"))

    def _registrar_lote(self):
        from core.services.pollo_engine import PolloEngine, PolloEngineError
        prod_id = self.pc_combo_prod.currentData()
        if not prod_id:
            QMessageBox.warning(self, "Error", "Seleccione el producto base."); return
        kg = self.pc_spin_kg.value()
        costo = self.pc_spin_costo.value()
        if kg <= 0:    QMessageBox.warning(self, "Error", "Kg totales requeridos."); return
        if costo <= 0: QMessageBox.warning(self, "Error", "Costo total requerido."); return
        try:
            engine = PolloEngine(self.conexion,
                                 usuario=self.obtener_usuario_actual(),
                                 sucursal_id=self.sucursal_id)
            res = engine.registrar_lote(
                producto_pollo_id   = prod_id,
                numero_pollos       = self.pc_spin_pollos.value(),
                kg_totales          = kg,
                costo_total         = costo,
                proveedor           = self.pc_combo_prov.currentText(),
                fecha               = self.pc_date.date().toPyDate(),
                metodo_pago         = self.pc_combo_pago.currentText(),
                estado              = self.pc_combo_estado.currentText(),
                descripcion         = self.pc_desc.toPlainText(),
                registrar_en_gastos = True,
            )
            self.conexion.commit()
            QMessageBox.information(self, "✅ Lote Registrado",
                f"Folio: {res.folio_lote}\n"
                f"Pollos: {res.numero_pollos}   Kg: {res.kg_totales:.3f}\n"
                f"Costo/kg: ${res.costo_kilo:.2f}\n\n"
                f"Gasto registrado automáticamente en Finanzas.")
            self.pc_spin_costo.setValue(0)
            self.pc_desc.clear()
            self.pc_tabla_prev.setRowCount(0)
            self.cargar_lotes_pollo()
            self.filtrar_gastos()
        except PolloEngineError as e:
            QMessageBox.warning(self, "Error Inventario", str(e))
        except Exception as e:
            self.conexion.rollback()
            QMessageBox.critical(self, "Error", f"Error inesperado:\n{e}")

    # ── Tab 2: Transformar ────────────────────────────────────────────────────

    def _init_tab_transformacion(self, parent):
        layout = QVBoxLayout(parent)
        layout.setSpacing(6)

        grp_lote = QGroupBox("Seleccionar Lote")
        lo = QHBoxLayout(grp_lote)
        self.tr_combo_lote = QComboBox()
        self.tr_combo_lote.setMinimumWidth(340)
        btn_rf = QPushButton("🔄")
        btn_rf.setFixedWidth(34)
        btn_rf.clicked.connect(self._cargar_combo_lotes)
        self.tr_lbl_info = QLabel("Seleccione un lote con stock disponible")
        self.tr_lbl_info.setStyleSheet("color:#555; font-style:italic;")
        lo.addWidget(QLabel("Lote:"))
        lo.addWidget(self.tr_combo_lote)
        lo.addWidget(btn_rf)
        lo.addWidget(self.tr_lbl_info)
        lo.addStretch()
        self.tr_combo_lote.currentIndexChanged.connect(self._on_lote_cambiado)
        layout.addWidget(grp_lote)

        grp_piezas = QGroupBox("Kg Obtenidos por Corte (capture los pesos reales)")
        piezas_layout = QVBoxLayout(grp_piezas)
        self.tr_tabla = QTableWidget()
        self.tr_tabla.setColumnCount(3)
        self.tr_tabla.setHorizontalHeaderLabels(
            ["Corte", "Kg obtenidos", "Costo/kg estimado"])
        self.tr_tabla.horizontalHeader().setStretchLastSection(True)
        self.tr_tabla.setMinimumHeight(160)
        piezas_layout.addWidget(self.tr_tabla)
        layout.addWidget(grp_piezas)

        grp_res = QGroupBox("Kg Entrada y Merma")
        fl = QFormLayout(grp_res)
        self.tr_spin_entrada = QDoubleSpinBox()
        self.tr_spin_entrada.setRange(0.001, 99999)
        self.tr_spin_entrada.setDecimals(3)
        self.tr_spin_entrada.setSuffix(" kg")
        self.tr_spin_merma = QDoubleSpinBox()
        self.tr_spin_merma.setRange(0, 99999)
        self.tr_spin_merma.setDecimals(3)
        self.tr_spin_merma.setSuffix(" kg")
        self.tr_lbl_rend = QLabel("Rendimiento: —")
        self.tr_lbl_rend.setStyleSheet("font-weight:bold; font-size:13px;")
        fl.addRow("Kg de pollo a transformar:", self.tr_spin_entrada)
        fl.addRow("Merma (hueso/vísceras):",    self.tr_spin_merma)
        fl.addRow("", self.tr_lbl_rend)
        layout.addWidget(grp_res)

        btn_exec = QPushButton("🔪 Ejecutar Transformación")
        btn_exec.setMinimumHeight(38)
        btn_exec.clicked.connect(self._ejecutar_transformacion)
        layout.addWidget(btn_exec)

        self.tr_spin_entrada.valueChanged.connect(self._recalc_rendimiento)
        self.tr_spin_merma.valueChanged.connect(self._recalc_rendimiento)

    def _cargar_combo_lotes(self):
        try:
            from core.services.pollo_engine import PolloEngine
            lotes = PolloEngine(self.conexion, sucursal_id=self.sucursal_id).lotes_activos()
            self.tr_combo_lote.clear()
            self.tr_combo_lote.addItem("— Seleccionar lote —", None)
            for l in lotes:
                self.tr_combo_lote.addItem(
                    f"[{l['folio']}] {l['fecha']} | {l['kg_disponibles']:.2f} kg | ${l['costo_kilo']:.2f}/kg",
                    l)
        except Exception as e:
            print(f"Error cargando lotes: {e}")

    def _on_lote_cambiado(self, _):
        lote = self.tr_combo_lote.currentData()
        if not lote:
            self.tr_lbl_info.setText("Seleccione un lote"); return
        self.tr_lbl_info.setText(
            f"📦 {lote['pollos']} pollos | {lote['kg_disponibles']:.3f} kg disponibles | "
            f"Proveedor: {lote['proveedor'] or '—'}")
        self.tr_spin_entrada.setMaximum(lote['kg_disponibles'])
        self.tr_spin_entrada.setValue(min(lote['kg_disponibles'], 15.0))
        self._cargar_tabla_cortes(lote['costo_kilo'])

    def _cargar_tabla_cortes(self, costo_base: float):
        try:
            rows = self.conexion.execute("""
                SELECT p.id, p.nombre, rd.porcentaje_rendimiento
                FROM rendimiento_derivados rd
                JOIN productos p ON rd.producto_derivado_id = p.id
                ORDER BY rd.porcentaje_rendimiento DESC
            """).fetchall()
        except Exception:
            rows = []
        self._cortes = []
        self.tr_tabla.setRowCount(len(rows))
        kg = self.tr_spin_entrada.value()
        for i, (pid, nombre, pct) in enumerate(rows):
            pct = pct or 0
            self.tr_tabla.setItem(i, 0, QTableWidgetItem(nombre))
            self.tr_tabla.item(i, 0).setData(Qt.UserRole, pid)
            spin = QDoubleSpinBox()
            spin.setRange(0, 99999); spin.setDecimals(3); spin.setSuffix(" kg")
            spin.setValue(round(kg * pct / 100, 3))
            spin.valueChanged.connect(self._recalc_rendimiento)
            self.tr_tabla.setCellWidget(i, 1, spin)
            self.tr_tabla.setItem(i, 2, QTableWidgetItem(f"${costo_base:.2f}"))
            self._cortes.append((pid, nombre, spin))

    def _recalc_rendimiento(self):
        if not hasattr(self, '_cortes') or not self._cortes: return
        kg_ent = self.tr_spin_entrada.value()
        if kg_ent <= 0: return
        kg_p   = sum(s.value() for _, _, s in self._cortes)
        merma  = self.tr_spin_merma.value()
        pct_r  = round(kg_p  / kg_ent * 100, 1)
        pct_m  = round(merma / kg_ent * 100, 1)
        color  = "#27ae60" if pct_r >= 85 else "#e67e22" if pct_r >= 70 else "#c0392b"
        self.tr_lbl_rend.setText(
            f"Rendimiento: {pct_r:.1f}%  |  Merma: {pct_m:.1f}%  |  Piezas: {kg_p:.3f} kg")
        self.tr_lbl_rend.setStyleSheet(
            f"font-weight:bold; font-size:13px; color:{color};")

    def _ejecutar_transformacion(self):
        from core.services.pollo_engine import (
            PolloEngine, PiezaTransformacion,
            PolloEngineError, RendimientoInvalidoError, StockInsuficienteError)
        lote = self.tr_combo_lote.currentData()
        if not lote:
            QMessageBox.warning(self, "Error", "Seleccione un lote."); return
        if not hasattr(self, '_cortes') or not self._cortes:
            QMessageBox.warning(self, "Error", "No hay cortes configurados."); return
        kg_ent = self.tr_spin_entrada.value()
        if kg_ent <= 0:
            QMessageBox.warning(self, "Error", "Ingrese kg de entrada."); return

        # Obtener producto_base del lote
        row = self.conexion.execute("""
            SELECT producto_id FROM inventario_subproductos
            WHERE compra_pollo_id=? LIMIT 1
        """, (lote['id'],)).fetchone()
        if not row:
            QMessageBox.warning(self, "Error",
                "No se encontró el producto base del lote.\n"
                "Asegúrese que el lote fue registrado con PolloEngine."); return
        prod_base_id = row[0]

        piezas = [PiezaTransformacion(pid, nombre, spin.value())
                  for pid, nombre, spin in self._cortes if spin.value() > 0]
        if not piezas:
            QMessageBox.warning(self, "Error", "Ingrese al menos un corte con kg > 0."); return

        try:
            engine = PolloEngine(self.conexion,
                                 usuario=self.obtener_usuario_actual(),
                                 sucursal_id=self.sucursal_id)
            res = engine.transformar_lote(
                lote_id          = lote['id'],
                producto_base_id = prod_base_id,
                kg_entrada       = kg_ent,
                piezas           = piezas,
                merma_kg         = self.tr_spin_merma.value(),
            )
            self.conexion.commit()
            QMessageBox.information(self, "✅ Transformación Exitosa",
                f"Entrada: {res.kg_entrada:.3f} kg\n"
                f"Piezas:  {res.kg_piezas:.3f} kg\n"
                f"Merma:   {res.kg_merma:.3f} kg\n"
                f"Rendimiento: {res.pct_rendimiento:.1f}%\n"
                f"Movimientos: {len(res.movimientos_ids)}")
            self._cargar_combo_lotes()
            self.cargar_inventario_pollo()
        except (RendimientoInvalidoError, StockInsuficienteError, PolloEngineError) as e:
            QMessageBox.warning(self, "Error", str(e))
        except Exception as e:
            self.conexion.rollback()
            QMessageBox.critical(self, "Error Inesperado", str(e))

    # ── Tab 3: Inventario actual ──────────────────────────────────────────────

    def _init_tab_inventario(self, parent):
        layout = QVBoxLayout(parent)
        btn = QPushButton("🔄 Actualizar")
        btn.clicked.connect(self.cargar_inventario_pollo)
        layout.addWidget(btn)
        self.inv_p_tabla = QTableWidget()
        self.inv_p_tabla.setColumnCount(6)
        self.inv_p_tabla.setHorizontalHeaderLabels(
            ["Producto", "Stock (kg)", "Stock mín.", "Lotes activos",
             "Costo prom/kg", "Estado"])
        self.inv_p_tabla.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.inv_p_tabla.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.inv_p_tabla.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.inv_p_tabla)

    def cargar_inventario_pollo(self):
        if not hasattr(self, 'inv_p_tabla'): return
        try:
            rows = self.conexion.execute("""
                SELECT p.nombre,
                       COALESCE(p.existencia, 0) as stock,
                       COALESCE(p.existencia_minima, p.stock_minimo, 0) as minimo,
                       (SELECT COUNT(DISTINCT mi.lote_id)
                        FROM movimientos_inventario mi
                        WHERE mi.producto_id=p.id AND mi.lote_id IS NOT NULL
                          AND mi.tipo IN ('ENTRADA_LOTE','TRANSFORMACION_ENTRADA')) as lotes,
                       (SELECT ROUND(AVG(mi.costo_unitario),2)
                        FROM movimientos_inventario mi
                        WHERE mi.producto_id=p.id AND mi.costo_unitario > 0) as costo_prom
                FROM productos p
                WHERE p.activo=1 AND (
                    p.existencia > 0 OR
                    LOWER(p.nombre) LIKE '%pollo%' OR
                    LOWER(COALESCE(p.categoria,'')) LIKE '%pollo%')
                ORDER BY p.existencia DESC
            """).fetchall()
            self.inv_p_tabla.setRowCount(len(rows))
            for i, (nombre, stock, minimo, lotes, costo_p) in enumerate(rows):
                self.inv_p_tabla.setItem(i, 0, QTableWidgetItem(nombre or ""))
                st = QTableWidgetItem(f"{stock:.3f} kg")
                if stock <= (minimo or 0):
                    st.setForeground(QColor("#c0392b"))
                    st.setFont(QFont("", -1, QFont.Bold))
                self.inv_p_tabla.setItem(i, 1, st)
                self.inv_p_tabla.setItem(i, 2, QTableWidgetItem(f"{minimo:.3f} kg"))
                self.inv_p_tabla.setItem(i, 3, QTableWidgetItem(str(lotes or 0)))
                self.inv_p_tabla.setItem(i, 4, QTableWidgetItem(
                    f"${costo_p:.2f}" if costo_p else "—"))
                estado = "⚠️ Bajo" if stock <= (minimo or 0) else "✅ OK"
                ei = QTableWidgetItem(estado)
                ei.setForeground(QColor("#c0392b") if "Bajo" in estado else QColor("#27ae60"))
                self.inv_p_tabla.setItem(i, 5, ei)
        except Exception as e:
            print(f"Error inv pollo: {e}")

    # ── Tab 4: Ledger / Historial ─────────────────────────────────────────────

    def _init_tab_historial(self, parent):
        layout = QVBoxLayout(parent)
        filt = QHBoxLayout()
        self.hl_ini = QDateEdit(QDate.currentDate().addDays(-30))
        self.hl_ini.setDisplayFormat("dd/MM/yyyy")
        self.hl_ini.setCalendarPopup(True)
        self.hl_fin = QDateEdit(QDate.currentDate())
        self.hl_fin.setDisplayFormat("dd/MM/yyyy")
        self.hl_fin.setCalendarPopup(True)
        btn_filt = QPushButton("🔍 Filtrar")
        btn_filt.clicked.connect(self.cargar_historial_ledger)
        filt.addWidget(QLabel("Desde:")); filt.addWidget(self.hl_ini)
        filt.addWidget(QLabel("Hasta:")); filt.addWidget(self.hl_fin)
        filt.addWidget(btn_filt); filt.addStretch()
        layout.addLayout(filt)

        self.hl_tabla = QTableWidget()
        self.hl_tabla.setColumnCount(8)
        self.hl_tabla.setHorizontalHeaderLabels([
            "Fecha", "Producto", "Tipo", "Kg",
            "Stock ant.", "Stock nuevo", "Lote", "Descripción"])
        self.hl_tabla.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.hl_tabla.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.hl_tabla.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.hl_tabla)

    def cargar_historial_ledger(self):
        if not hasattr(self, 'hl_tabla'): return
        try:
            ini = self.hl_ini.date().toString("yyyy-MM-dd")
            fin = self.hl_fin.date().toString("yyyy-MM-dd")
            rows = self.conexion.execute("""
                SELECT mi.fecha, p.nombre, mi.tipo, mi.cantidad,
                       mi.existencia_anterior, mi.existencia_nueva,
                       COALESCE(cp.lote, CAST(mi.lote_id AS TEXT), '—') as folio,
                       mi.descripcion
                FROM movimientos_inventario mi
                JOIN productos p ON mi.producto_id=p.id
                LEFT JOIN compras_pollo cp ON mi.lote_id=cp.id
                WHERE date(mi.fecha) BETWEEN ? AND ?
                ORDER BY mi.fecha DESC LIMIT 500
            """, (ini, fin)).fetchall()
            self.hl_tabla.setRowCount(len(rows))
            COLORS = {
                "ENTRADA_LOTE":          "#27ae60",
                "TRANSFORMACION_ENTRADA":"#2980b9",
                "TRANSFORMACION_SALIDA": "#e67e22",
                "SALIDA_VENTA":          "#c0392b",
                "MERMA":                 "#8e44ad",
                "AJUSTE":                "#16a085",
            }
            for i, row in enumerate(rows):
                for j, val in enumerate(row):
                    item = QTableWidgetItem(str(val or ""))
                    if j == 2:
                        c = COLORS.get(str(val), "#555")
                        item.setForeground(QColor(c))
                        item.setFont(QFont("", -1, QFont.Bold))
                    self.hl_tabla.setItem(i, j, item)
        except Exception as e:
            print(f"Error ledger: {e}")

    def cargar_lotes_pollo(self):
        """Punto de entrada centralizado para refrescar datos de pollo."""
        self._cargar_combo_lotes()
        self.cargar_inventario_pollo()

    # ── v9: COMPRAS INVENTARIABLES ─────────────────────────────────────────────

    def mostrar_compras_inventariables(self):
        """Activa sección de Compras Globales de Inventario."""
        self.widget_gastos.hide()
        self.widget_personal.hide()
        self.widget_pollo.hide()
        self.widget_compras_inv.show()
        self.aplicar_estilo_boton_activo(self.btn_ver_compras_inv)
        self.cargar_compras_inventariables()

    def init_seccion_compras_inventariables(self):
        """
        Sección Compras Globales:
        Permite registrar una compra de inventario → crea gasto + lote global.
        """
        layout = QVBoxLayout(self.widget_compras_inv)
        layout.setContentsMargins(8, 8, 8, 8)

        # Encabezado + botón nueva compra
        top = QHBoxLayout()
        titulo = QLabel("🛒 Compras Globales de Inventario")
        titulo.setObjectName("tituloPrincipal")
        top.addWidget(titulo)
        top.addStretch()
        self.btn_nueva_compra_inv = QPushButton("➕ Nueva Compra")
        self.btn_nueva_compra_inv.clicked.connect(self.nueva_compra_inventariable)
        top.addWidget(self.btn_nueva_compra_inv)
        layout.addLayout(top)

        # Filtros
        filtros = QHBoxLayout()
        self.txt_filtro_compras = QLineEdit()
        self.txt_filtro_compras.setPlaceholderText("Filtrar por producto o proveedor…")
        self.txt_filtro_compras.textChanged.connect(self.cargar_compras_inventariables)
        filtros.addWidget(self.txt_filtro_compras)
        self.combo_estado_compra = QComboBox()
        self.combo_estado_compra.addItems(["Todos", "pagado", "credito", "parcial"])
        self.combo_estado_compra.currentTextChanged.connect(self.cargar_compras_inventariables)
        filtros.addWidget(self.combo_estado_compra)
        layout.addLayout(filtros)

        # Tabla
        cols = ["ID", "Fecha", "Producto", "Proveedor", "Volumen",
                "Costo Unit.", "Costo Total", "Forma Pago", "Estado", "Batch Global"]
        self.tabla_compras_inv = QTableWidget(0, len(cols))
        self.tabla_compras_inv.setHorizontalHeaderLabels(cols)
        self.tabla_compras_inv.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tabla_compras_inv.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_compras_inv.setSelectionBehavior(QAbstractItemView.SelectRows)
        layout.addWidget(self.tabla_compras_inv)

        # Resumen
        self.lbl_resumen_compras = QLabel("Total: $0.00")
        layout.addWidget(self.lbl_resumen_compras)

    def cargar_compras_inventariables(self):
        """Carga tabla de compras inventariables con filtros activos."""
        if not hasattr(self, 'tabla_compras_inv'):
            return
        try:
            filtro  = getattr(self, 'txt_filtro_compras', None)
            estado_w = getattr(self, 'combo_estado_compra', None)
            texto   = filtro.text().strip() if filtro else ""
            estado  = estado_w.currentText() if estado_w else "Todos"

            q = """
                SELECT ci.id, ci.fecha, p.nombre, ci.proveedor,
                       ci.volumen, ci.unidad, ci.costo_unitario, ci.costo_total,
                       ci.forma_pago, ci.estado, ci.batch_id_global
                FROM compras_inventariables ci
                JOIN productos p ON p.id = ci.producto_id
                WHERE 1=1
            """
            params = []
            if texto:
                q += " AND (p.nombre LIKE ? OR ci.proveedor LIKE ?)"
                params += [f"%{texto}%", f"%{texto}%"]
            if estado != "Todos":
                q += " AND ci.estado = ?"
                params.append(estado)
            q += " ORDER BY ci.fecha DESC, ci.id DESC LIMIT 200"

            rows = self.conexion.execute(q, params).fetchall()
            self.tabla_compras_inv.setRowCount(len(rows))
            total = 0.0
            for i, r in enumerate(rows):
                vals = [
                    str(r[0]),
                    str(r[1])[:10],
                    str(r[2]),
                    str(r[3] or "—"),
                    f"{r[4]:.3f} {r[5]}",
                    f"${r[6]:.2f}",
                    f"${r[7]:.2f}",
                    str(r[8]),
                    str(r[9]),
                    f"Batch #{r[10]}" if r[10] else "—",
                ]
                total += float(r[7])
                for j, v in enumerate(vals):
                    item = QTableWidgetItem(v)
                    item.setFlags(Qt.ItemIsSelectable | Qt.ItemIsEnabled)
                    self.tabla_compras_inv.setItem(i, j, item)
            self.lbl_resumen_compras.setText(f"Total período: ${total:,.2f}")
        except Exception as exc:
            import logging
            logging.getLogger('spj.finanzas').warning("cargar_compras_inv: %s", exc)

    def nueva_compra_inventariable(self):
        """Abre diálogo para registrar compra inventariable."""
        dlg = _DialogoCompraInventariable(self.conexion, self.usuario_actual, self)
        if dlg.exec_() == QDialog.Accepted:
            self.cargar_compras_inventariables()
            QMessageBox.information(self, "Compra Registrada",
                f"Compra #{dlg.compra_id} registrada.\n"
                f"Gasto creado y lote global generado.")


# ── Diálogo Compra Inventariable v9 ──────────────────────────────────────────

class _DialogoCompraInventariable(QDialog):
    """
    Registro de compra inventariable:
    - Selección producto
    - Volumen, costo unitario
    - Proveedor, forma de pago, crédito/parcial
    Al aceptar: crea registro en gastos + compras_inventariables
    """

    def __init__(self, conexion, usuario, parent=None):
        super().__init__(parent)
        self.conexion  = conexion
        self.usuario   = usuario
        self.compra_id = None
        self.setWindowTitle("Nueva Compra de Inventario")
        self.setMinimumWidth(500)
        self.setModal(True)
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(18, 18, 18, 18)

        form = QFormLayout()

        # Producto
        self.combo_producto = QComboBox()
        self.combo_producto.setEditable(True)
        try:
            prods = self.conexion.execute(
                "SELECT id, nombre, unidad FROM productos WHERE activo=1 ORDER BY nombre"
            ).fetchall()
            for pid, nombre, unidad in prods:
                self.combo_producto.addItem(f"{nombre} ({unidad})", pid)
        except Exception:
            pass
        form.addRow("Producto *:", self.combo_producto)

        # Proveedor
        self.txt_proveedor = QLineEdit()
        self.txt_proveedor.setPlaceholderText("Nombre del proveedor")
        form.addRow("Proveedor:", self.txt_proveedor)

        # Volumen + unidad
        vol_layout = QHBoxLayout()
        self.spin_volumen = QDoubleSpinBox()
        self.spin_volumen.setDecimals(3)
        self.spin_volumen.setRange(0.001, 999999)
        self.spin_volumen.setValue(1.0)
        self.spin_volumen.valueChanged.connect(self._actualizar_total)
        self.txt_unidad = QLineEdit()
        self.txt_unidad.setText("kg")
        self.txt_unidad.setMaximumWidth(60)
        vol_layout.addWidget(self.spin_volumen)
        vol_layout.addWidget(QLabel("Unidad:"))
        vol_layout.addWidget(self.txt_unidad)
        form.addRow("Volumen *:", vol_layout)

        # Costo unitario
        self.spin_costo_unit = QDoubleSpinBox()
        self.spin_costo_unit.setDecimals(4)
        self.spin_costo_unit.setRange(0, 999999)
        self.spin_costo_unit.setPrefix("$")
        self.spin_costo_unit.valueChanged.connect(self._actualizar_total)
        form.addRow("Costo unitario *:", self.spin_costo_unit)

        # Total calculado
        self.lbl_total = QLabel("$0.00")
        self.lbl_total.setObjectName("tituloPrincipal")
        form.addRow("Total calculado:", self.lbl_total)

        # Forma de pago
        self.combo_pago = QComboBox()
        self.combo_pago.addItems(["EFECTIVO", "TRANSFERENCIA", "CHEQUE", "CRÉDITO", "PARCIAL"])
        self.combo_pago.currentTextChanged.connect(self._on_forma_pago_changed)
        form.addRow("Forma de pago:", self.combo_pago)

        # Pago parcial / crédito
        self.grp_credito = QGroupBox("Crédito / Parcial")
        grp_lay = QFormLayout(self.grp_credito)
        self.spin_monto_pagado = QDoubleSpinBox()
        self.spin_monto_pagado.setDecimals(2)
        self.spin_monto_pagado.setRange(0, 999999)
        self.spin_monto_pagado.setPrefix("$")
        grp_lay.addRow("Monto pagado:", self.spin_monto_pagado)
        self.date_vencimiento = QDateEdit()
        self.date_vencimiento.setCalendarPopup(True)
        from PyQt5.QtCore import QDate
        self.date_vencimiento.setDate(QDate.currentDate().addDays(30))
        grp_lay.addRow("Vence:", self.date_vencimiento)
        self.grp_credito.setVisible(False)

        # Notas
        self.txt_notas = QLineEdit()
        self.txt_notas.setPlaceholderText("Observaciones opcionales")
        form.addRow("Notas:", self.txt_notas)

        layout.addLayout(form)
        layout.addWidget(self.grp_credito)

        # Botones
        botones = QHBoxLayout()
        self.btn_guardar  = QPushButton("💾 Registrar Compra")
        self.btn_cancelar = QPushButton("Cancelar")
        self.btn_guardar.clicked.connect(self._guardar)
        self.btn_cancelar.clicked.connect(self.reject)
        botones.addStretch()
        botones.addWidget(self.btn_guardar)
        botones.addWidget(self.btn_cancelar)
        layout.addLayout(botones)

    def _actualizar_total(self):
        total = self.spin_volumen.value() * self.spin_costo_unit.value()
        self.lbl_total.setText(f"${total:,.2f}")

    def _on_forma_pago_changed(self, forma):
        self.grp_credito.setVisible(forma in ("CRÉDITO", "PARCIAL"))

    def _guardar(self):
        import uuid as _uuid, json as _json
        producto_id = self.combo_producto.currentData()
        if not producto_id:
            QMessageBox.warning(self, "Error", "Seleccione un producto")
            return
        volumen = self.spin_volumen.value()
        if volumen <= 0:
            QMessageBox.warning(self, "Error", "El volumen debe ser mayor a 0")
            return
        costo_unit  = self.spin_costo_unit.value()
        costo_total = round(volumen * costo_unit, 4)
        proveedor   = self.txt_proveedor.text().strip() or None
        unidad      = self.txt_unidad.text().strip() or "kg"
        forma_pago  = self.combo_pago.currentText()
        notas       = self.txt_notas.text().strip()
        es_credito  = 1 if forma_pago in ("CRÉDITO", "PARCIAL") else 0
        monto_pagado = self.spin_monto_pagado.value() if es_credito else costo_total
        saldo       = round(costo_total - monto_pagado, 4) if es_credito else 0.0
        vence       = (self.date_vencimiento.date().toString("yyyy-MM-dd")
                       if es_credito else None)
        estado      = "credito" if forma_pago == "CRÉDITO" else (
                       "parcial" if forma_pago == "PARCIAL" else "pagado")

        try:
            from datetime import datetime as _dt
            hoy = _dt.now().strftime("%Y-%m-%d")

            # Crear gasto
            prod_row = self.conexion.execute(
                "SELECT nombre FROM productos WHERE id=?", (producto_id,)
            ).fetchone()
            prod_nombre = prod_row[0] if prod_row else "Producto"

            cur_g = self.conexion.execute(
                """
                INSERT INTO gastos
                    (fecha, categoria, concepto, monto, monto_pagado,
                     metodo_pago, estado, proveedor_id, usuario, activo)
                VALUES (?,?,?,?,?,'{}',?,NULL,?,1)
                """.replace("'{}'", f"'{forma_pago}'"),
                (
                    hoy, "Compra Inventario",
                    f"Compra {prod_nombre} — {volumen}{unidad} @ ${costo_unit}/{unidad}",
                    costo_total, monto_pagado, estado,
                    self.usuario or "admin",
                )
            )
            gasto_id = cur_g.lastrowid

            # Registrar en compras_inventariables
            ci_uuid = _uuid.uuid4().hex
            cur_ci = self.conexion.execute(
                """
                INSERT INTO compras_inventariables
                    (uuid, gasto_id, producto_id, proveedor,
                     volumen, unidad, costo_unitario, costo_total,
                     forma_pago, es_credito, monto_pagado, saldo_pendiente,
                     fecha_vencimiento, estado, notas,
                     sucursal_id, usuario, fecha)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
                """,
                (
                    ci_uuid, gasto_id, producto_id, proveedor,
                    volumen, unidad, costo_unit, costo_total,
                    forma_pago, es_credito, monto_pagado, saldo,
                    vence, estado, notas,
                    1, self.usuario or "admin",
                )
            )
            self.compra_id = cur_ci.lastrowid

            self.conexion.commit()
            self.accept()

        except Exception as exc:
            self.conexion.rollback()
            QMessageBox.critical(self, "Error", f"No se pudo registrar: {exc}")


# --- Diálogo para Crear/Editar Gasto ---
class DialogoGasto(QDialog):
    def __init__(self, conexion, usuario_actual, parent=None, gasto_data=None):
        super().__init__(parent)
        self.conexion = conexion
        self.usuario_actual = usuario_actual
        self.gasto_data = gasto_data # None para nuevo, dict para editar
        self.setWindowTitle("Nuevo Gasto" if not gasto_data else "Editar Gasto")
        self.setFixedSize(450, 450)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        form_layout = QFormLayout()
        
        self.date_fecha = QDateEdit()
        self.date_fecha.setDate(QDate.currentDate())
        self.date_fecha.setDisplayFormat("dd/MM/yyyy")
        self.date_fecha.setCalendarPopup(True)
        
        self.edit_categoria = QComboBox()
        self.edit_categoria.setEditable(True)
        self.cargar_categorias()
        
        self.edit_proveedor = QComboBox()
        self.edit_proveedor.setEditable(True)
        self.cargar_proveedores()
        
        self.spin_monto = QDoubleSpinBox()
        self.spin_monto.setRange(0.01, 999999.99)
        self.spin_monto.setPrefix("$ ")
        self.spin_monto.setDecimals(2)
        
        self.combo_estado = QComboBox()
        self.combo_estado.addItems(["PENDIENTE", "PAGADO", "PARCIAL"])
        
        self.spin_monto_pagado = QDoubleSpinBox()
        self.spin_monto_pagado.setRange(0.00, 999999.99)
        self.spin_monto_pagado.setPrefix("$ ")
        self.spin_monto_pagado.setDecimals(2)
        
        self.edit_descripcion = QTextEdit()
        self.edit_descripcion.setMaximumHeight(100)
        
        self.edit_metodo_pago = QComboBox()
        self.edit_metodo_pago.addItems(["Efectivo", "Tarjeta", "Transferencia", "Cheque", "Crédito"])

        # Poblar campos si es edición
        if self.gasto_data:
            # Convertir string de fecha a QDate
            fecha_str = self.gasto_data.get('fecha', '')
            if fecha_str:
                fecha_dt = QDate.fromString(fecha_str.split(' ')[0], "yyyy-MM-dd") # Solo la parte de la fecha
                if fecha_dt.isValid():
                    self.date_fecha.setDate(fecha_dt)
            
            categoria = self.gasto_data.get('categoria', '')
            if categoria and self.edit_categoria.findText(categoria) == -1:
                self.edit_categoria.addItem(categoria)
            self.edit_categoria.setCurrentText(categoria)
            
            # Proveedor
            proveedor_id = self.gasto_data.get('proveedor_id')
            if proveedor_id:
                try:
                    cursor = self.conexion.cursor()
                    cursor.execute("SELECT nombre FROM proveedores WHERE id = ?", (proveedor_id,))
                    proveedor = cursor.fetchone()
                    if proveedor:
                        nombre_proveedor = proveedor[0]
                        if self.edit_proveedor.findText(nombre_proveedor) == -1:
                            self.edit_proveedor.addItem(nombre_proveedor)
                        self.edit_proveedor.setCurrentText(nombre_proveedor)
                except sqlite3.Error:
                    pass # Si hay error, dejar el combo vacío
            
            self.spin_monto.setValue(self.gasto_data.get('monto', 0.0))
            self.combo_estado.setCurrentText(self.gasto_data.get('estado', 'PENDIENTE'))
            self.spin_monto_pagado.setValue(self.gasto_data.get('monto_pagado', 0.0))
            self.edit_descripcion.setPlainText(self.gasto_data.get('descripcion', ''))
            metodo_pago = self.gasto_data.get('metodo_pago', 'Efectivo')
            index_metodo = self.edit_metodo_pago.findText(metodo_pago)
            if index_metodo >= 0:
                self.edit_metodo_pago.setCurrentIndex(index_metodo)

        # Conectar señales
        self.combo_estado.currentTextChanged.connect(self.on_estado_changed)
        self.spin_monto.valueChanged.connect(self.on_monto_changed)

        form_layout.addRow("Fecha*:", self.date_fecha)
        form_layout.addRow("Categoría*:", self.edit_categoria)
        form_layout.addRow("Proveedor:", self.edit_proveedor)
        form_layout.addRow("Monto*:", self.spin_monto)
        form_layout.addRow("Estado*:", self.combo_estado)
        form_layout.addRow("Monto Pagado:", self.spin_monto_pagado)
        form_layout.addRow("Método de Pago:", self.edit_metodo_pago)
        form_layout.addRow("Descripción:", self.edit_descripcion)

        btn_layout = QHBoxLayout()
        self.btn_guardar = QPushButton("Guardar")
        self.btn_cancelar = QPushButton("Cancelar")
        btn_layout.addWidget(self.btn_guardar)
        btn_layout.addWidget(self.btn_cancelar)

        layout.addLayout(form_layout)
        layout.addLayout(btn_layout)
        self.setLayout(layout)

        # Conexiones
        self.btn_guardar.clicked.connect(self.guardar)
        self.btn_cancelar.clicked.connect(self.reject)
        
        # Inicializar estado de widgets
        self.on_estado_changed(self.combo_estado.currentText())

    def cargar_categorias(self):
        """Carga las categorías existentes en el combo box."""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT DISTINCT categoria FROM gastos WHERE categoria IS NOT NULL")
            categorias = cursor.fetchall()
            self.edit_categoria.addItem("") # Opción vacía
            for cat in categorias:
                self.edit_categoria.addItem(cat[0])
        except sqlite3.Error:
            pass # Si hay error, el combo queda vacío excepto la opción por defecto

    def cargar_proveedores(self):
        """Carga los proveedores existentes en el combo box."""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT nombre FROM proveedores ORDER BY nombre")
            proveedores = cursor.fetchall()
            self.edit_proveedor.addItem("") # Opción vacía
            for prov in proveedores:
                self.edit_proveedor.addItem(prov[0])
        except sqlite3.Error:
            pass # Si hay error, el combo queda vacío excepto la opción por defecto

    def on_estado_changed(self, estado):
        """Habilita/deshabilita el campo de monto pagado según el estado."""
        if estado == "PAGADO":
            self.spin_monto_pagado.setEnabled(True)
            self.spin_monto_pagado.setValue(self.spin_monto.value())
        elif estado == "PARCIAL":
            self.spin_monto_pagado.setEnabled(True)
        else: # PENDIENTE
            self.spin_monto_pagado.setEnabled(False)
            self.spin_monto_pagado.setValue(0.00)

    def on_monto_changed(self, monto):
        """Actualiza el monto pagado si el estado es PAGADO."""
        if self.combo_estado.currentText() == "PAGADO":
            self.spin_monto_pagado.setValue(monto)

    def validar_formulario(self):
        """Valida los datos del formulario."""
        if self.spin_monto.value() <= 0:
            QMessageBox.warning(self, "Error", "El monto debe ser mayor a cero.")
            return False
        if not self.edit_categoria.currentText().strip():
            QMessageBox.warning(self, "Error", "La categoría es obligatoria.")
            return False
        return True

    def guardar(self):
        """Guarda el gasto en la base de datos."""
        if not self.validar_formulario():
            return

        try:
            cursor = self.conexion.cursor()
            
            fecha = self.date_fecha.date().toString("yyyy-MM-dd")
            categoria = self.edit_categoria.currentText().strip()
            nombre_proveedor = self.edit_proveedor.currentText().strip() or None
            monto = self.spin_monto.value()
            estado = self.combo_estado.currentText()
            monto_pagado = self.spin_monto_pagado.value() if estado in ["PAGADO", "PARCIAL"] else 0.0
            descripcion = self.edit_descripcion.toPlainText().strip() or None
            metodo_pago = self.edit_metodo_pago.currentText()
            usuario = self.usuario_actual
            
            # Obtener ID del proveedor si se proporcionó nombre
            proveedor_id = None
            if nombre_proveedor:
                cursor.execute("SELECT id FROM proveedores WHERE nombre = ?", (nombre_proveedor,))
                prov = cursor.fetchone()
                if prov:
                    proveedor_id = prov[0]
                else:
                    # Crear nuevo proveedor si no existe
                    cursor.execute("INSERT INTO proveedores (nombre) VALUES (?)", (nombre_proveedor,))
                    proveedor_id = cursor.lastrowid

            # CORRECCIÓN: Usar self.gasto_data en lugar de self.gasto
            if self.gasto_data:  # Editar
                id_gasto = self.gasto_data['id']
                
                cursor.execute("""
                    UPDATE gastos 
                    SET fecha = ?, categoria = ?, proveedor_id = ?, monto = ?, 
                        monto_pagado = ?, estado = ?, descripcion = ?, metodo_pago = ?
                    WHERE id = ?
                """, (fecha, categoria, proveedor_id, monto, monto_pagado, estado, descripcion, metodo_pago, id_gasto))
                
                self.conexion.commit()
                QMessageBox.information(self, "Éxito", "Gasto actualizado correctamente.")
                self.accept()
            else:  # Nuevo
                cursor.execute("""
                    INSERT INTO gastos (fecha, categoria, proveedor_id, monto, 
                                        monto_pagado, estado, descripcion, usuario, metodo_pago)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (fecha, categoria, proveedor_id, monto, monto_pagado, estado, descripcion, usuario, metodo_pago))
                
                self.conexion.commit()
                QMessageBox.information(self, "Éxito", "Gasto creado correctamente.")
                self.accept()

        except sqlite3.IntegrityError as e:
            self.conexion.rollback()
            QMessageBox.warning(self, "Error", f"Error de integridad: {str(e)}")
        except sqlite3.Error as e:
            self.conexion.rollback()
            QMessageBox.critical(self, "Error", f"Error en la base de datos: {str(e)}")
        except Exception as e:
            self.conexion.rollback()
            QMessageBox.critical(self, "Error", f"Error inesperado: {str(e)}")

# --- Diálogo para Crear/Editar Empleado ---
class DialogoEmpleado(QDialog):
    def __init__(self, conexion, parent=None, empleado_data=None):
        super().__init__(parent)
        self.conexion = conexion
        self.empleado_data = empleado_data # None para nuevo, dict para editar
        self.setWindowTitle("Nuevo Empleado" if not empleado_data else "Editar Empleado")
        self.setFixedSize(400, 350)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        form_layout = QFormLayout()
        
        self.edit_nombre = QLineEdit()
        self.edit_apellidos = QLineEdit()
        self.edit_puesto = QLineEdit()
        self.spin_salario = QDoubleSpinBox()
        self.spin_salario.setRange(0.01, 999999.99)
        self.spin_salario.setPrefix("$ ")
        self.spin_salario.setDecimals(2)
        self.date_fecha_ingreso = QDateEdit()
        self.date_fecha_ingreso.setDate(QDate.currentDate())
        self.date_fecha_ingreso.setDisplayFormat("dd/MM/yyyy")
        self.date_fecha_ingreso.setCalendarPopup(True)
        self.chk_activo = QCheckBox("Activo")
        self.chk_activo.setChecked(True)

        # Poblar campos si es edición
        if self.empleado_data:
            self.edit_nombre.setText(self.empleado_data.get('nombre', ''))
            self.edit_apellidos.setText(self.empleado_data.get('apellidos', ''))
            self.edit_puesto.setText(self.empleado_data.get('puesto', ''))
            self.spin_salario.setValue(self.empleado_data.get('salario', 0.0))
            
            # Convertir string de fecha a QDate
            fecha_str = self.empleado_data.get('fecha_ingreso', '')
            if fecha_str:
                fecha_dt = QDate.fromString(fecha_str, "yyyy-MM-dd")
                if fecha_dt.isValid():
                    self.date_fecha_ingreso.setDate(fecha_dt)
            
            self.chk_activo.setChecked(self.empleado_data.get('activo', 1) == 1)

        form_layout.addRow("Nombre*:", self.edit_nombre)
        form_layout.addRow("Apellidos:", self.edit_apellidos)
        form_layout.addRow("Puesto:", self.edit_puesto)
        form_layout.addRow("Salario:", self.spin_salario)
        form_layout.addRow("Fecha de Ingreso:", self.date_fecha_ingreso)
        form_layout.addRow(self.chk_activo)

        btn_layout = QHBoxLayout()
        self.btn_guardar = QPushButton("Guardar")
        self.btn_cancelar = QPushButton("Cancelar")
        btn_layout.addWidget(self.btn_guardar)
        btn_layout.addWidget(self.btn_cancelar)

        layout.addLayout(form_layout)
        layout.addLayout(btn_layout)
        self.setLayout(layout)

        # Conexiones
        self.btn_guardar.clicked.connect(self.guardar)
        self.btn_cancelar.clicked.connect(self.reject)

    def validar_formulario(self):
        """Valida los datos del formulario."""
        if not self.edit_nombre.text().strip():
            QMessageBox.warning(self, "Error", "El nombre es obligatorio.")
            return False
        if self.spin_salario.value() <= 0:
            QMessageBox.warning(self, "Error", "El salario debe ser mayor a cero.")
            return False
        return True

    def guardar(self):
        """Guarda el empleado en la base de datos."""
        if not self.validar_formulario():
            return

        try:
            cursor = self.conexion.cursor()
            
            nombre = self.edit_nombre.text().strip()
            apellidos = self.edit_apellidos.text().strip() or None
            puesto = self.edit_puesto.text().strip() or None
            salario = self.spin_salario.value()
            fecha_ingreso = self.date_fecha_ingreso.date().toString("yyyy-MM-dd")
            activo = 1 if self.chk_activo.isChecked() else 0

            # CORRECCIÓN: Usar self.empleado_data en lugar de self.empleado
            if self.empleado_data:  # Editar
                id_empleado = self.empleado_data['id']
                
                cursor.execute("""
                    UPDATE personal 
                    SET nombre = ?, apellidos = ?, puesto = ?, salario = ?, 
                        fecha_ingreso = ?, activo = ?
                    WHERE id = ?
                """, (nombre, apellidos, puesto, salario, fecha_ingreso, activo, id_empleado))
                
                self.conexion.commit()
                QMessageBox.information(self, "Éxito", "Empleado actualizado correctamente.")
                self.accept()
            else:  # Nuevo
                cursor.execute("""
                    INSERT INTO personal (nombre, apellidos, puesto, salario, fecha_ingreso, activo)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (nombre, apellidos, puesto, salario, fecha_ingreso, activo))
                
                self.conexion.commit()
                QMessageBox.information(self, "Éxito", "Empleado creado correctamente.")
                self.accept()

        except sqlite3.IntegrityError as e:
            self.conexion.rollback()
            QMessageBox.warning(self, "Error", f"Error de integridad: {str(e)}")
        except sqlite3.Error as e:
            self.conexion.rollback()
            QMessageBox.critical(self, "Error", f"Error en la base de datos: {str(e)}")
        except Exception as e:
            self.conexion.rollback()
            QMessageBox.critical(self, "Error", f"Error inesperado: {str(e)}")

    
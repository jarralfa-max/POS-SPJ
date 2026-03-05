# main.py  ── SPJ Punto de Venta — VERSIÓN ENTERPRISE INTEGRADA v3.0
# Layout y gráficos 100% preservados del original.
# Mejoras integradas:
#   core/db/connection.py   → WAL + FK + pool por hilo
#   migrations/engine.py    → migraciones versionadas v1..v4
#   security/auth.py        → bcrypt + rate-limit + auto-migración legacy
#   utils/logging_setup.py  → RotatingFileHandler + DBLogHandler
import sys
import os
import sqlite3
import logging
from datetime import datetime

# ── Setup logging PRIMERO ─────────────────────────────────────────────────────
from utils.logging_setup import setup_logging, DBLogHandler
_log_dir = os.path.join(
    os.path.dirname(sys.executable) if getattr(sys, "frozen", False)
    else os.path.dirname(os.path.abspath(__file__)),
    "logs"
)
setup_logging(log_dir=_log_dir, console=not getattr(sys, "frozen", False))
logger = logging.getLogger("spj.main")

from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QLabel, QPushButton, QLineEdit,
    QComboBox, QMessageBox, QDialog, QMainWindow, QHBoxLayout,
    QFrame, QStackedWidget, QListWidget, QTableWidget, QSplitter,
    QTabWidget, QFormLayout, QDateEdit, QCheckBox, QSpinBox,
    QDoubleSpinBox, QTableWidgetItem, QListWidgetItem, QGroupBox,
    QStatusBar, QAction, QMenuBar, QSizePolicy, QActionGroup, QToolButton,
    QTextEdit, QButtonGroup, QRadioButton, QColorDialog, QFileDialog,
    QFontDialog, QProgressDialog, QToolBar, QGridLayout,
    QHeaderView, QAbstractItemView
)
from PyQt5.QtGui import (
    QPixmap, QIcon, QFont, QDoubleValidator, QRegExpValidator, QColor,
    QFontInfo, QImage, QPainter, QTextDocument, QTextOption
)
from PyQt5.QtCore import Qt, QTimer, pyqtSignal, QSize, QDateTime, QLocale
from PyQt5.QtPrintSupport import QPrinter, QPrintDialog

import config
from modulos.ventas import ModuloVentas
from modulos.clientes import ModuloClientes
from modulos.productos import ModuloProductos
from modulos.caja import ModuloCaja
from modulos.reportes import ModuloReportes
from modulos.finanzas import ModuloFinanzas
from modulos.configuracion import ModuloConfiguracion
from modulos.inventario_enterprise import ModuloInventarioEnterprise
from modulos.inventario_local import ModuloInventarioLocal
from modulos.tarjetas import ModuloTarjetas
from modulos.fidelidad import ModuloFidelidad
from modulos.recetas import ModuloRecetas
from modulos.transferencias import ModuloTransferencias

# ── Auth enterprise + fallback legacy ────────────────────────────────────────
from security.auth import autenticar, AuthError, UsuarioInactivoError, HAS_BCRYPT
from database.conexion import verificar_password, migrar_password_a_bcrypt

import json

# ── Validación CRÍTICA: bcrypt obligatorio ────────────────────────────────────
if not HAS_BCRYPT:
    _app_check = QApplication.instance() or QApplication(sys.argv)
    from PyQt5.QtWidgets import QMessageBox
    QMessageBox.critical(
        None,
        "Error Crítico de Seguridad",
        "❌ bcrypt no está instalado.\n\n"
        "Las contraseñas NO pueden protegerse sin esta librería.\n"
        "El sistema no puede arrancar por seguridad.\n\n"
        "Ejecute en la terminal:\n"
        "    pip install bcrypt\n\n"
        "Luego reinicie la aplicación.",
    )
    logger.critical("bcrypt no disponible — arranque abortado por seguridad")
    sys.exit(1)


# =============================================================================
# DIÁLOGO DE LOGIN
# =============================================================================
class DialogoLogin(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Inicio de Sesión")
        self.setModal(True)
        self.setFixedSize(400, 300)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(25, 25, 25, 25)

        titulo = QLabel("🔐 INICIO DE SESIÓN")
        titulo.setAlignment(Qt.AlignCenter)
        titulo.setProperty("class", "dialog-title")
        layout.addWidget(titulo)

        form_layout = QVBoxLayout()
        form_layout.setSpacing(12)

        lbl_usuario = QLabel("👤 Usuario:")
        lbl_usuario.setProperty("class", "form-label")
        self.input_usuario = QLineEdit()
        self.input_usuario.setPlaceholderText("Ingrese su usuario")
        self.input_usuario.setProperty("class", "dialog-input")

        lbl_contrasena = QLabel("🔒 Contraseña:")
        lbl_contrasena.setProperty("class", "form-label")
        self.input_contrasena = QLineEdit()
        self.input_contrasena.setPlaceholderText("Ingrese su contraseña")
        self.input_contrasena.setEchoMode(QLineEdit.Password)
        self.input_contrasena.setProperty("class", "dialog-input")

        form_layout.addWidget(lbl_usuario)
        form_layout.addWidget(self.input_usuario)
        form_layout.addWidget(lbl_contrasena)
        form_layout.addWidget(self.input_contrasena)
        layout.addLayout(form_layout)
        layout.addStretch(1)

        btn_layout = QHBoxLayout()
        self.btn_cancelar = QPushButton("❌ Cancelar")
        self.btn_ingresar = QPushButton("✅ Ingresar")
        self.btn_cancelar.setProperty("class", "cancel-button")
        self.btn_ingresar.setProperty("class", "accept-button")
        btn_layout.addWidget(self.btn_cancelar)
        btn_layout.addWidget(self.btn_ingresar)
        layout.addLayout(btn_layout)

        self.btn_cancelar.clicked.connect(self.reject)
        self.btn_ingresar.clicked.connect(self.aceptar)
        self.input_contrasena.returnPressed.connect(self.aceptar)

    def aceptar(self):
        if not self.input_usuario.text().strip() or not self.input_contrasena.text().strip():
            QMessageBox.warning(self, "Error", "Usuario y contraseña son obligatorios")
            return
        self.accept()

    def get_credenciales(self):
        return self.input_usuario.text().strip(), self.input_contrasena.text().strip()



# =============================================================================
# DIÁLOGO DE SELECCIÓN DE SUCURSAL
# =============================================================================
class DialogoSucursal(QDialog):
    """Se muestra después del login para elegir en qué sucursal trabaja hoy."""

    def __init__(self, conexion: sqlite3.Connection, usuario: str, rol: str, parent=None):
        super().__init__(parent)
        self.conexion  = conexion
        self.usuario   = usuario
        self.rol       = rol
        self.sucursal_id   = 1
        self.sucursal_nombre = "Principal"
        self.setWindowTitle("Seleccionar Sucursal")
        self.setModal(True)
        self.setFixedSize(380, 220)
        self._init_ui()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(14)
        layout.setContentsMargins(24, 24, 24, 24)

        titulo = QLabel("🏪 ¿En qué sucursal trabajas hoy?")
        titulo.setAlignment(Qt.AlignCenter)
        titulo.setProperty("class", "dialog-title")
        layout.addWidget(titulo)

        self.combo = QComboBox()
        self.combo.setMinimumHeight(36)
        self._cargar_sucursales()
        layout.addWidget(self.combo)

        info = QLabel(f"Usuario: <b>{self.usuario}</b>  |  Rol: <b>{self.rol}</b>")
        info.setAlignment(Qt.AlignCenter)
        layout.addWidget(info)

        btn_layout = QHBoxLayout()
        btn_ok = QPushButton("✅ Confirmar")
        btn_ok.setMinimumHeight(36)
        btn_ok.setProperty("class", "accept-button")
        btn_ok.clicked.connect(self._confirmar)
        btn_layout.addWidget(btn_ok)
        layout.addLayout(btn_layout)

    def _cargar_sucursales(self):
        try:
            # Si el usuario tiene sucursal asignada, pre-seleccionarla
            row = self.conexion.execute(
                "SELECT sucursal_id FROM usuarios WHERE usuario=?", (self.usuario,)
            ).fetchone()
            sucursal_fija = row[0] if row and row[0] else None

            sucursales = self.conexion.execute(
                "SELECT id, nombre FROM sucursales WHERE activa=1 ORDER BY id"
            ).fetchall()

            if not sucursales:
                self.combo.addItem("Principal", 1)
                return

            idx_default = 0
            for i, (sid, nombre) in enumerate(sucursales):
                self.combo.addItem(f"🏪 {nombre}", sid)
                if sucursal_fija and sid == sucursal_fija:
                    idx_default = i

            self.combo.setCurrentIndex(idx_default)

            # Si el usuario NO es admin y tiene sucursal fija, bloquear selección
            if sucursal_fija and self.rol.lower() != "admin":
                self.combo.setEnabled(False)

        except Exception as e:
            self.combo.addItem("Principal", 1)

    def _confirmar(self):
        self.sucursal_id      = self.combo.currentData() or 1
        self.sucursal_nombre  = self.combo.currentText().replace("🏪 ", "")
        self.accept()


# =============================================================================
# VENTANA PRINCIPAL
# =============================================================================
class MainWindow(QMainWindow):
    data_updated = pyqtSignal()

    def __init__(self, conexion: sqlite3.Connection):
        super().__init__()
        self.conexion      = conexion
        self.usuario_actual = None
        self.rol_usuario   = None
        self.tema_actual   = "Oscuro Moderno"
        self._eventos      = {}
        self.event_handlers = {}
        self.modulos_activos = {}

        self.setWindowTitle("Surtidora de Pollo Juanis - Sistema POS")
        self.setMinimumSize(1000, 700)

        self.init_ui()
        self.inicializar_modulos()
        self.aplicar_tema_inicial()

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)

        self.crear_panel_izquierdo()
        main_layout.addWidget(self.panel_izquierdo)

        self.stacked_widget = QStackedWidget()
        self.stacked_widget.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.crear_pagina_inicial()
        main_layout.addWidget(self.stacked_widget)

        self.crear_menu()
        self.statusBar().showMessage("Sistema listo - Bienvenido")

    def crear_panel_izquierdo(self):
        self.panel_izquierdo = QFrame()
        self.panel_izquierdo.setFrameShape(QFrame.StyledPanel)
        self.panel_izquierdo.setMinimumWidth(220)
        self.panel_izquierdo.setMaximumWidth(280)
        self.panel_izquierdo.setProperty("class", "panelLateral")

        panel_layout = QVBoxLayout(self.panel_izquierdo)
        panel_layout.setContentsMargins(12, 20, 12, 20)
        panel_layout.setSpacing(3)

        self.logo_panel_label = QLabel()
        self.logo_panel_label.setAlignment(Qt.AlignCenter)
        self.logo_panel_label.setMinimumSize(115, 115)
        self.logo_panel_label.setMaximumSize(200, 200)
        self.logo_panel_label.setProperty("class", "logoCentral")
        if not self.cargar_logo_panel():
            self.logo_panel_label.setText("🍗\nJUANIS")
            self.logo_panel_label.setProperty("class", "logoPlaceholder")
        panel_layout.addWidget(self.logo_panel_label)
        panel_layout.addSpacing(5)

        self.btn_ventas     = self.crear_boton_modulo("💰 VENTAS",       "sales.png")
        self.btn_caja       = self.crear_boton_modulo("💵 CAJA",          "cash.png")
        self.btn_inventario = self.crear_boton_modulo("🐔 INVENTARIO",   "inventory.png")
        self.btn_productos  = self.crear_boton_modulo("📦 PRODUCTOS",     "products.png")
        self.btn_clientes   = self.crear_boton_modulo("👥 CLIENTES",     "clients.png")
        self.btn_reportes   = self.crear_boton_modulo("📊 REPORTES",      "reports.png")
        self.btn_gastos     = self.crear_boton_modulo("💸 GASTOS",        "expenses.png")
        self.btn_tarjetas   = self.crear_boton_modulo("💳 TARJETAS",      "card.png")
        self.btn_config     = self.crear_boton_modulo("⚙️ CONFIGURACIÓN", "config.png")

        # Orden según spec v9: Ventas, Caja, Inventario, Productos, Clientes, Reportes, Gastos, Tarjetas, Config
        self.botones_modulos = {
            "ventas":        self.btn_ventas,
            "caja":          self.btn_caja,
            "inventario":    self.btn_inventario,
            "productos":     self.btn_productos,
            "clientes":      self.btn_clientes,
            "reportes":      self.btn_reportes,
            "gastos":        self.btn_gastos,
            "tarjetas":      self.btn_tarjetas,
            "configuracion": self.btn_config,
        }
        for btn in self.botones_modulos.values():
            panel_layout.addWidget(btn)
            if btn != self.btn_config:
                panel_layout.addSpacing(2)
            btn.setEnabled(False)

        panel_layout.addStretch()

        self.btn_login = QPushButton("🔓 INICIAR SESIÓN")
        self.btn_login.setMinimumHeight(42)
        self.btn_login.setProperty("class", "botonLogin")
        self.btn_login.clicked.connect(self.mostrar_login)
        panel_layout.addWidget(self.btn_login)

        self.btn_ventas.clicked.connect(lambda: self.mostrar_modulo("ventas"))
        self.btn_inventario.clicked.connect(lambda: self.mostrar_modulo("inventario"))
        self.btn_clientes.clicked.connect(lambda: self.mostrar_modulo("clientes"))
        self.btn_productos.clicked.connect(lambda: self.mostrar_modulo("productos"))
        self.btn_caja.clicked.connect(lambda: self.mostrar_modulo("caja"))
        self.btn_reportes.clicked.connect(lambda: self.mostrar_modulo("reportes"))
        self.btn_gastos.clicked.connect(lambda: self.mostrar_modulo("gastos"))
        self.btn_tarjetas.clicked.connect(lambda: self.mostrar_modulo("tarjetas"))
        self.btn_config.clicked.connect(lambda: self.mostrar_modulo("configuracion"))

    def crear_boton_modulo(self, texto, icono):
        btn = QPushButton()
        iconos_emoji = {
            "💰 VENTAS": "💰", "👥 CLIENTES": "👥", "📦 PRODUCTOS": "📦",
            "💵 CAJA": "💵", "📊 REPORTES": "📊", "💸 GASTOS": "💸", "⚙️ CONFIGURACIÓN": "⚙️"
        }
        emoji  = iconos_emoji.get(texto, "📁")
        nombre = texto.split(" ", 1)[1] if " " in texto else texto
        btn.setText(f"{emoji} {nombre}")
        for icon_path in [f"imagenes_productos/{icono}", f"imagenes_productos/icons/{icono}", icono]:
            if os.path.exists(icon_path):
                btn.setIcon(QIcon(icon_path))
                btn.setIconSize(QSize(20, 20))
                btn.setText(nombre)
                break
        btn.setMinimumHeight(45)
        btn.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        btn.setProperty("class", "botonModulo")
        return btn

    def crear_pagina_inicial(self):
        pagina_inicial = QWidget()
        pagina_inicial.setProperty("class", "paginaInicial")
        layout_inicial = QVBoxLayout(pagina_inicial)
        layout_inicial.setAlignment(Qt.AlignCenter)
        layout_inicial.setSpacing(0)
        layout_inicial.setContentsMargins(0, 0, 0, 0)

        contenedor_central = QWidget()
        contenedor_central.setProperty("class", "contenedorCentral")
        contenedor_layout = QVBoxLayout(contenedor_central)
        contenedor_layout.setAlignment(Qt.AlignCenter)
        contenedor_layout.setSpacing(30)

        self.logo_central_label = QLabel()
        self.logo_central_label.setAlignment(Qt.AlignCenter)
        self.logo_central_label.setMinimumSize(300, 300)
        self.logo_central_label.setMaximumSize(400, 400)
        self.logo_central_label.setProperty("class", "logoCentral")
        if not self.cargar_logo_central():
            self.logo_central_label.setText("🍗\nPOLLERÍA\nJUANIS")
            self.logo_central_label.setProperty("class", "logoPlaceholder")
        contenedor_layout.addWidget(self.logo_central_label)

        label_negocio = QLabel("Surtidora de Pollo Juanis")
        label_negocio.setAlignment(Qt.AlignCenter)
        label_negocio.setProperty("class", "labelNegocio")
        label_negocio.setWordWrap(True)
        contenedor_layout.addWidget(label_negocio)

        label_bienvenida = QLabel("BIENVENIDO AL SISTEMA DE GESTIÓN")
        label_bienvenida.setAlignment(Qt.AlignCenter)
        label_bienvenida.setProperty("class", "labelBienvenida")
        label_bienvenida.setWordWrap(True)
        contenedor_layout.addWidget(label_bienvenida)

        label_instruccion = QLabel("Inicie sesión para acceder a los módulos del sistema")
        label_instruccion.setAlignment(Qt.AlignCenter)
        label_instruccion.setProperty("class", "labelInstruccion")
        label_instruccion.setWordWrap(True)
        contenedor_layout.addWidget(label_instruccion)

        layout_inicial.addWidget(contenedor_central)
        self.stacked_widget.addWidget(pagina_inicial)

    # ── Logos ─────────────────────────────────────────────────────────────────
    _LOGO_RUTAS = [
        "imagenes_productos/logo.png", "imagenes_productos/logo.jpg",
        "imagenes_productos/logo.jpeg", "imagenes_productos/logo.svg",
        "logo.png", "logo.jpg", "logo.jpeg", "logo.svg"
    ]

    def _buscar_logo(self, rutas, ancho, alto):
        for ruta in rutas:
            if os.path.exists(ruta):
                try:
                    pm = QPixmap(ruta)
                    if not pm.isNull():
                        return pm.scaled(ancho, alto, Qt.KeepAspectRatio, Qt.SmoothTransformation)
                except Exception:
                    continue
        return None

    def cargar_logo_panel(self):
        pm = self._buscar_logo(self._LOGO_RUTAS, 120, 120)
        if pm:
            self.logo_panel_label.setPixmap(pm); return True
        return False

    def cargar_logo_central(self):
        pm = self._buscar_logo(self._LOGO_RUTAS, 350, 350)
        if pm:
            self.logo_central_label.setPixmap(pm); return True
        return False

    # ── Menú ──────────────────────────────────────────────────────────────────
    def crear_menu(self):
        menubar = self.menuBar()

        # ── Archivo ────────────────────────────────────────────────────────────
        menu_archivo = menubar.addMenu("📁 Archivo")
        exportar_menu = menu_archivo.addMenu("📤 Exportar reportes")
        exportar_pdf   = QAction("📄 Exportar a PDF", self)
        exportar_excel = QAction("📊 Exportar a Excel", self)
        exportar_pdf.triggered.connect(self.exportar_reporte_pdf)
        exportar_excel.triggered.connect(self.exportar_reporte_excel)
        exportar_menu.addAction(exportar_pdf)
        exportar_menu.addAction(exportar_excel)
        menu_archivo.addSeparator()
        backup_action = QAction("💾 Respaldar datos", self)
        backup_action.triggered.connect(self.respaldar_datos)
        menu_archivo.addAction(backup_action)
        menu_archivo.addSeparator()
        salir_action = QAction("🚪 Salir", self)
        salir_action.setShortcut("Ctrl+Q")
        salir_action.triggered.connect(self.close)
        menu_archivo.addAction(salir_action)

        # ── Ajustes ───────────────────────────────────────────────────────────
        menu_ajustes = menubar.addMenu("⚙️ Ajustes")

        # Sub: Temas
        self.crear_menu_temas(menu_ajustes)
        menu_ajustes.addSeparator()

        # Sub: Manual de usuario
        ayuda_action = QAction("📖 Manual de usuario", self)
        ayuda_action.triggered.connect(self.mostrar_manual)
        menu_ajustes.addAction(ayuda_action)
        menu_ajustes.addSeparator()

        # Sub: Configuración (módulo)
        config_action = QAction("🔧 Configuración del sistema", self)
        config_action.triggered.connect(lambda: self.mostrar_modulo("configuracion"))
        menu_ajustes.addAction(config_action)

        # Inventario enterprise (admin)
        inv_enterprise_action = QAction("🏭 Inventario Enterprise (Admin)", self)
        inv_enterprise_action.triggered.connect(lambda: self.mostrar_modulo("inventario_enterprise"))
        menu_ajustes.addAction(inv_enterprise_action)

        menu_ajustes.addSeparator()
        acerca_de_action = QAction("ℹ️ Acerca de", self)
        acerca_de_action.triggered.connect(self.mostrar_acerca_de)
        menu_ajustes.addAction(acerca_de_action)

    def crear_menu_temas(self, menu_config):
        menu_temas = menu_config.addMenu("🎨 Cambiar Tema")
        menu_temas.setObjectName("menuTemas")
        grupo_temas = QActionGroup(self)
        temas_info = {
            "Oscuro Moderno":    "🌙 Tema Oscuro Moderno",
            "Claro Elegante":    "☀️ Tema Claro Elegante",
            "Azul Profesional":  "🔵 Tema Azul Profesional",
            "Verde Naturaleza":  "🍃 Tema Verde Naturaleza",
            "Púrpura Creativo":  "🟣 Tema Púrpura Creativo",
        }
        for tema, nombre in temas_info.items():
            accion = QAction(nombre, self, checkable=True)
            accion.triggered.connect(lambda checked, t=tema: self.cambiar_tema(t))
            if tema == self.tema_actual:
                accion.setChecked(True)
            grupo_temas.addAction(accion)
            menu_temas.addAction(accion)
        menu_temas.addSeparator()
        pers = QAction("🎨 Personalizar temas...", self)
        pers.triggered.connect(self.mostrar_dialogo_personalizacion)
        menu_temas.addAction(pers)

    def mostrar_dialogo_personalizacion(self):
        QMessageBox.information(
            self, "Personalización de Temas",
            "La personalización avanzada estará disponible en futuras versiones.\n\n"
            "Temas disponibles:\n• Oscuro Moderno\n• Claro Elegante\n"
            "• Azul Profesional\n• Verde Naturaleza\n• Púrpura Creativo"
        )

    # ── Módulos ───────────────────────────────────────────────────────────────
    def inicializar_modulos(self):
        self.modulos = {
            "ventas":        ModuloVentas(self.conexion, self),
            "inventario":    ModuloInventarioLocal(self.conexion, self),
            "inventario_enterprise": ModuloInventarioEnterprise(self.conexion, self),
            "clientes":      ModuloClientes(self.conexion, self),
            "productos":     ModuloProductos(self.conexion, self),
            "recetas":       ModuloRecetas(self.conexion, self),
            "transferencias": ModuloTransferencias(self.conexion, self),
            "fidelidad":     ModuloFidelidad(self.conexion, self),
            "caja":          ModuloCaja(self.conexion, self),
            "reportes":      ModuloReportes(self.conexion, self),
            "gastos":        ModuloFinanzas(self.conexion, self),
            "tarjetas":      ModuloTarjetas(self.conexion, self),
            "configuracion": ModuloConfiguracion(self.conexion, self),
        }
        for nombre, modulo in self.modulos.items():
            self.stacked_widget.addWidget(modulo)
            modulo.hide()
            if hasattr(modulo, 'set_usuario_actual'):
                modulo.set_usuario_actual(self.usuario_actual, self.rol_usuario)
        self.configurar_comunicacion_modulos()

    def configurar_comunicacion_modulos(self):
        if hasattr(self.modulos["productos"], 'registrar_actualizacion'):
            self.modulos["productos"].registrar_actualizacion(
                'productos_actualizados',
                lambda: self.modulos["ventas"].cargar_productos()
                        if hasattr(self.modulos["ventas"], 'cargar_productos') else None
            )
        if hasattr(self.modulos["ventas"], 'registrar_actualizacion'):
            self.modulos["ventas"].registrar_actualizacion(
                'venta_realizada',
                lambda datos: [
                    self.modulos["caja"].actualizar_resumen()
                    if hasattr(self.modulos["caja"], 'actualizar_resumen') else None,
                    self.modulos["productos"].actualizar_existencias(datos)
                    if hasattr(self.modulos["productos"], 'actualizar_existencias') else None
                ]
            )
        if hasattr(self.modulos["configuracion"], 'registrar_actualizacion'):
            self.modulos["configuracion"].registrar_actualizacion(
                'configuracion_actualizada',
                lambda: [m.actualizar_configuracion()
                         for m in self.modulos.values()
                         if hasattr(m, 'actualizar_configuracion')]
            )

    def mostrar_modulo(self, nombre_modulo):
        if not self.usuario_actual:
            QMessageBox.warning(self, "Acceso denegado",
                                "Debe iniciar sesión para acceder a los módulos")
            return
        if not self.verificar_permiso_modulo(nombre_modulo):
            QMessageBox.warning(self, "Acceso denegado",
                                "No tiene permisos para acceder a este módulo")
            return
        modulo = self.modulos.get(nombre_modulo)
        if modulo:
            if hasattr(modulo, 'set_sesion'):
                modulo.set_sesion(self.usuario_actual, self.rol_usuario)
            index = self.stacked_widget.indexOf(modulo)
            if index >= 0:
                self.resetear_botones_modulos()
                if nombre_modulo in self.botones_modulos:
                    boton = self.botones_modulos[nombre_modulo]
                    boton.setProperty("class", "botonModuloActivo")
                    boton.style().unpolish(boton); boton.style().polish(boton)
                self.stacked_widget.setCurrentIndex(index)
                modulo.show()
                self.statusBar().showMessage(
                    f"Módulo {nombre_modulo.capitalize()} activo - Usuario: {self.usuario_actual}"
                )

    def resetear_botones_modulos(self):
        for boton in self.botones_modulos.values():
            boton.setProperty("class", "botonModulo")
            boton.style().unpolish(boton); boton.style().polish(boton)

    def verificar_permiso_modulo(self, nombre_modulo):
        if self.rol_usuario and self.rol_usuario.lower() == 'admin':
            return True
        try:
            # Tabla normalizada (nueva)
            uid_row = self.conexion.execute(
                "SELECT id FROM usuarios WHERE usuario=?", (self.usuario_actual,)
            ).fetchone()
            if uid_row:
                rows = self.conexion.execute(
                    "SELECT modulo FROM usuario_modulos WHERE usuario_id=?", (uid_row[0],)
                ).fetchall()
                if rows:
                    return nombre_modulo in [r[0] for r in rows]
        except Exception:
            pass
        # Fallback legacy CSV/JSON
        try:
            resultado = self.conexion.execute(
                "SELECT modulos_permitidos FROM usuarios WHERE usuario=?",
                (self.usuario_actual,)
            ).fetchone()
            if not resultado or not resultado[0]:
                return False
            raw = resultado[0]
            try:
                parsed = json.loads(raw)
                modulos = parsed if isinstance(parsed, list) else []
            except Exception:
                modulos = [m.strip() for m in raw.split(',') if m.strip()]
            return nombre_modulo in modulos
        except Exception:
            return False

    # ── Autenticación ─────────────────────────────────────────────────────────
    def mostrar_login(self):
        if self.usuario_actual:
            self.cerrar_sesion()
            return
        dialogo = DialogoLogin(self)
        if dialogo.exec_() == QDialog.Accepted:
            usuario, contrasena = dialogo.get_credenciales()
            self.verificar_login(usuario, contrasena)

    def verificar_login(self, usuario: str, contrasena: str):
        """Login enterprise con bcrypt + rate-limit. Fallback automático a legacy."""
        try:
            datos = autenticar(self.conexion, usuario, contrasena)
            self.usuario_actual = datos["usuario"]
            self.rol_usuario    = datos["rol"]
            # ── Selección de sucursal ──────────────────────────────────────
            dlg_suc = DialogoSucursal(
                self.conexion, datos["usuario"], datos["rol"], self
            )
            dlg_suc.exec_()          # siempre confirma (no cancelable)
            self.sucursal_id     = dlg_suc.sucursal_id
            self.sucursal_nombre = dlg_suc.sucursal_nombre
            # Propagar sucursal a todos los módulos
            self._propagar_sucursal()
            self.actualizar_ui_sesion()
            QMessageBox.information(
                self, "Éxito",
                f"Bienvenido {datos['nombre']} ({datos['rol']})"
                f"\n🏪 Sucursal: {self.sucursal_nombre}"
            )
            self.stacked_widget.setCurrentIndex(0)
            logger.info("Login OK: %s (%s) → sucursal %s", usuario, datos["rol"], self.sucursal_nombre)

        except UsuarioInactivoError as e:
            QMessageBox.warning(self, "Cuenta Inactiva", str(e))

        except AuthError as e:
            QMessageBox.warning(self, "Acceso Denegado", str(e))
            logger.warning("Login fallido: %s — %s", usuario, e)

        except Exception as e:
            # Fallback legacy si bcrypt no instalado
            logger.warning("Usando fallback legacy login: %s", e)
            try:
                resultado = self.conexion.execute(
                    "SELECT contrasena, rol FROM usuarios WHERE usuario=? AND activo=1",
                    (usuario,)
                ).fetchone()
                if resultado and verificar_password(contrasena, resultado[0]):
                    self.usuario_actual = usuario
                    self.rol_usuario    = resultado[1]
                    migrar_password_a_bcrypt(self.conexion, usuario, contrasena)
                    dlg_suc = DialogoSucursal(
                        self.conexion, usuario, resultado[1], self
                    )
                    dlg_suc.exec_()
                    self.sucursal_id     = dlg_suc.sucursal_id
                    self.sucursal_nombre = dlg_suc.sucursal_nombre
                    self._propagar_sucursal()
                    self.actualizar_ui_sesion()
                    QMessageBox.information(
                        self, "Éxito",
                        f"Bienvenido {usuario} ({resultado[1]})"
                        f"\n🏪 Sucursal: {self.sucursal_nombre}"
                    )
                    self.stacked_widget.setCurrentIndex(0)
                else:
                    QMessageBox.warning(self, "Error", "Usuario o contraseña incorrectos")
            except Exception as e2:
                QMessageBox.critical(self, "Error", f"Error al verificar credenciales: {e2}")

    def _propagar_sucursal(self):
        """Envía sucursal_id y nombre a todos los módulos que la soporten."""
        for modulo in self.modulos.values():
            if hasattr(modulo, 'set_sucursal'):
                modulo.set_sucursal(self.sucursal_id, self.sucursal_nombre)

    def actualizar_ui_sesion(self):
        nombre_corto = (self.usuario_actual[:10] + "..."
                        if len(self.usuario_actual) > 10 else self.usuario_actual)
        self.btn_login.setText(f"🔒 CERRAR SESIÓN\n{nombre_corto}")
        self.btn_login.setProperty("class", "botonLogout")
        self.btn_login.style().unpolish(self.btn_login)
        self.btn_login.style().polish(self.btn_login)
        self.actualizar_usuario_en_modulos()
        self.actualizar_permisos_modulos()
        self.statusBar().showMessage(f"Sesión iniciada - {self.usuario_actual}  |  🏪 {self.sucursal_nombre}")

    def actualizar_usuario_en_modulos(self):
        for modulo in self.modulos.values():
            if hasattr(modulo, 'set_usuario_actual'):
                modulo.set_usuario_actual(self.usuario_actual, self.rol_usuario)

    def actualizar_permisos_modulos(self):
        if self.rol_usuario and self.rol_usuario.lower() == 'admin':
            for btn in self.botones_modulos.values():
                btn.setEnabled(True)
            return
        modulos_permitidos = []
        try:
            uid_row = self.conexion.execute(
                "SELECT id FROM usuarios WHERE usuario=?", (self.usuario_actual,)
            ).fetchone()
            if uid_row:
                rows = self.conexion.execute(
                    "SELECT modulo FROM usuario_modulos WHERE usuario_id=?", (uid_row[0],)
                ).fetchall()
                modulos_permitidos = [r[0] for r in rows]
        except Exception:
            pass
        if not modulos_permitidos:
            try:
                resultado = self.conexion.execute(
                    "SELECT modulos_permitidos FROM usuarios WHERE usuario=?",
                    (self.usuario_actual,)
                ).fetchone()
                if resultado and resultado[0]:
                    raw = resultado[0]
                    try:
                        parsed = json.loads(raw)
                        modulos_permitidos = parsed if isinstance(parsed, list) else []
                    except Exception:
                        modulos_permitidos = [m.strip() for m in raw.split(',') if m.strip()]
            except Exception:
                pass
        for modulo, btn in self.botones_modulos.items():
            btn.setEnabled(modulo in modulos_permitidos)

    def cerrar_sesion(self):
        confirmacion = QMessageBox.question(
            self, "Cerrar Sesión",
            f"¿Está seguro que desea cerrar la sesión de {self.usuario_actual}?",
            QMessageBox.Yes | QMessageBox.No
        )
        if confirmacion == QMessageBox.Yes:
            logger.info("Sesión cerrada: %s", self.usuario_actual)
            self.usuario_actual    = None
            self.rol_usuario      = None
            self.sucursal_id      = 1
            self.sucursal_nombre  = "Principal"
            self.btn_login.setText("🔓 INICIAR SESIÓN")
            self.btn_login.setProperty("class", "botonLogin")
            self.btn_login.style().unpolish(self.btn_login)
            self.btn_login.style().polish(self.btn_login)
            for btn in self.botones_modulos.values():
                btn.setEnabled(False)
            self.stacked_widget.setCurrentIndex(0)
            for modulo in self.modulos.values():
                if hasattr(modulo, 'cerrar_sesion'):
                    modulo.cerrar_sesion()
            self.statusBar().showMessage("Sesión cerrada - Sistema listo")

    # ── Temas ─────────────────────────────────────────────────────────────────
    def aplicar_tema_inicial(self):
        try:
            resultado = self.conexion.execute(
                "SELECT valor FROM configuracion WHERE clave='tema'"
            ).fetchone()
            self.tema_actual = resultado[0] if resultado else "Oscuro Moderno"
        except Exception:
            self.tema_actual = "Oscuro Moderno"
        self.aplicar_tema(self.tema_actual)

    def aplicar_tema(self, nombre_tema: str):
        if nombre_tema in config.TEMAS:
            self.tema_actual = nombre_tema
            QApplication.instance().setStyleSheet(config.TEMAS[nombre_tema])
            try:
                self.conexion.execute(
                    "INSERT OR REPLACE INTO configuracion (clave, valor, descripcion) VALUES (?,?,?)",
                    ('tema', nombre_tema, 'Tema de la interfaz')
                )
                self.conexion.commit()
                for modulo in self.modulos.values():
                    if hasattr(modulo, 'on_tema_cambiado'):
                        modulo.on_tema_cambiado(nombre_tema)
                self.statusBar().showMessage(f"Tema cambiado a: {nombre_tema}")
            except Exception as e:
                logger.warning("Error al guardar tema en BD: %s", e)
        else:
            self.aplicar_tema("Oscuro Moderno")

    def cambiar_tema(self, nombre_tema: str):
        if nombre_tema == self.tema_actual:
            return
        self.aplicar_tema(nombre_tema)
        QMessageBox.information(
            self, "Tema Cambiado",
            f"El tema ha sido cambiado a: {nombre_tema}\n\n"
            "Los cambios se han aplicado a toda la aplicación."
        )

    # ── Inter-módulos ─────────────────────────────────────────────────────────
    def registrar_evento(self, evento, handler):
        if evento not in self.event_handlers:
            self.event_handlers[evento] = []
        self.event_handlers[evento].append(handler)

    def notificar_evento(self, evento, datos):
        for handler in self.event_handlers.get(evento, []):
            try:
                handler(datos)
            except Exception as e:
                logger.warning("Handler evento %s: %s", evento, e)

    # ── Acciones de menú ──────────────────────────────────────────────────────
    def exportar_reporte_pdf(self):
        if not self.usuario_actual:
            QMessageBox.warning(self, "Error", "Debe iniciar sesión primero"); return
        QMessageBox.information(self, "Exportar PDF", "Función de exportación PDF - En desarrollo")

    def exportar_reporte_excel(self):
        if not self.usuario_actual:
            QMessageBox.warning(self, "Error", "Debe iniciar sesión primero"); return
        QMessageBox.information(self, "Exportar Excel", "Función de exportación Excel - En desarrollo")

    def respaldar_datos(self):
        if not self.usuario_actual:
            QMessageBox.warning(self, "Error", "Debe iniciar sesión primero"); return
        try:
            from core.db.connection import DB_PATH
            backup_dir  = os.path.join(os.path.dirname(DB_PATH), "backups")
            os.makedirs(backup_dir, exist_ok=True)
            backup_file = os.path.join(backup_dir, f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
            import shutil
            shutil.copy2(DB_PATH, backup_file)
            logger.info("Respaldo: %s", backup_file)
            QMessageBox.information(self, "Respaldo", f"Respaldo creado:\n{backup_file}")
        except Exception as e:
            logger.error("Error respaldo: %s", e)
            QMessageBox.critical(self, "Error", f"No se pudo crear el respaldo: {str(e)}")

    def mostrar_acerca_de(self):
        QMessageBox.about(
            self, "Acerca de",
            "Surtidora de Pollo Juanis\nSistema de Punto de Venta\n\n"
            "Desarrollado con PyQt5 y SQLite\n"
            f"Versión 3.0 (Enterprise) • {datetime.now().year}\n\n"
            "Arquitectura offline-first multi-sucursal\n"
            "© Todos los derechos reservados"
        )

    def mostrar_manual(self):
        QMessageBox.information(
            self, "Manual de Usuario",
            "Manual de usuario del sistema:\n\n"
            "1. VENTAS: Procesar ventas y cobros\n"
            "2. CLIENTES: Gestionar base de clientes\n"
            "3. PRODUCTOS: Administrar inventario\n"
            "4. CAJA: Control de movimientos de caja\n"
            "5. REPORTES: Generar reportes y estadísticas\n"
            "6. GASTOS: Registrar gastos del negocio\n"
            "7. CONFIGURACIÓN: Ajustes del sistema\n\n"
            "Use los botones del panel lateral para navegar."
        )

    def guardar_configuracion(self):
        try:
            for clave, valor in [
                ('tema', self.tema_actual),
                ('window_size', f"{self.width()},{self.height()}"),
                ('window_position', f"{self.x()},{self.y()}"),
            ]:
                self.conexion.execute(
                    "INSERT OR REPLACE INTO configuracion (clave, valor, descripcion) VALUES (?,?,?)",
                    (clave, valor, f'Configuración automática de {clave}')
                )
            self.conexion.commit()
        except Exception as e:
            logger.warning("Error al guardar configuración: %s", e)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if hasattr(self, 'logo_central_label') and self.logo_central_label.pixmap():
            pm = self.logo_central_label.pixmap()
            scaled = pm.scaled(
                self.logo_central_label.width(), self.logo_central_label.height(),
                Qt.KeepAspectRatio, Qt.SmoothTransformation
            )
            self.logo_central_label.setPixmap(scaled)

    def closeEvent(self, event):
        if self.usuario_actual:
            reply = QMessageBox.question(
                self, 'Confirmar Salida',
                '¿Está seguro que desea salir? Hay una sesión activa.',
                QMessageBox.Yes | QMessageBox.No, QMessageBox.No
            )
            if reply == QMessageBox.No:
                event.ignore(); return
        self.guardar_configuracion()
        if self.conexion:
            try:
                self.conexion.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                self.conexion.close()
            except Exception:
                pass
        event.accept()


# =============================================================================
# INICIALIZACIÓN DEL SISTEMA
# =============================================================================
def inicializar_sistema():
    """
    Arranque enterprise:
    1. Conexión WAL+FK (core/db/connection.py)
    2. Migraciones versionadas (migrations/engine.py)
    3. Migraciones estructurales legacy (database/conexion.py)
    4. Tablas base y datos semilla (modulos/base.py)
    5. DBLogHandler en tabla logs
    """
    try:
        from core.db.connection import get_connection, DB_PATH
        db_conn = get_connection()
        logger.info("BD: %s", DB_PATH)

        from migrations.engine import run_migrations, MIGRATIONS
        n = run_migrations(db_conn, MIGRATIONS)
        if n:
            logger.info("Migraciones aplicadas: %d", n)

        from database.conexion import aplicar_migraciones_estructurales
        aplicar_migraciones_estructurales(db_conn)

        from modulos.base import ModuloBase
        modulo_base = ModuloBase(db_conn)
        print("🔧 Inicializando sistema SPJ Punto de Venta Enterprise...")
        if not modulo_base.inicializar_bd():
            print("❌ Error al inicializar sistema")
            return None

        logging.getLogger("spj").addHandler(
            DBLogHandler(conn_factory=get_connection)
        )

        print("✅ Sistema inicializado correctamente")
        logger.info("Sistema SPJ iniciado OK")
        return db_conn

    except Exception as e:
        print(f"❌ Error crítico en inicialización: {e}")
        import traceback; traceback.print_exc()
        return None


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("SPJ Punto de Venta")
    app.setOrganizationName("Surtidora de Pollo Juanis")

    try:
        db_conn = inicializar_sistema()
        if not db_conn:
            QMessageBox.critical(
                None, "Error Crítico",
                "No se pudo inicializar el sistema.\nVerifique los logs en la consola."
            )
            sys.exit(1)

        # ── Arrancar SyncWorker en background ─────────────────────────────────
        _sync_worker = None
        try:
            from sync.sync_worker import crear_sync_worker
            from core.db.connection import get_connection as _gc

            # Leer config de BD si existe
            _url = ""
            _key = ""
            _suc = 1
            try:
                row_url = db_conn.execute(
                    "SELECT valor FROM configuracion WHERE clave='sync_url'"
                ).fetchone()
                row_key = db_conn.execute(
                    "SELECT valor FROM configuracion WHERE clave='sync_api_key'"
                ).fetchone()
                row_suc = db_conn.execute(
                    "SELECT valor FROM configuracion WHERE clave='sucursal_id'"
                ).fetchone()
                if row_url: _url = row_url[0] or ""
                if row_key: _key = row_key[0] or ""
                if row_suc: _suc = int(row_suc[0] or 1)
            except Exception:
                pass

            _sync_worker = crear_sync_worker(
                _gc, sucursal_id=_suc, url=_url, api_key=_key, intervalo_seg=60,
            )
            _sync_worker.start()
            logger.info("SyncWorker arrancado (sucursal=%d url=%s)", _suc, _url or "(sin configurar)")
        except Exception as sw_exc:
            logger.warning("SyncWorker no pudo arrancar (no crítico): %s", sw_exc)

        ventana_principal = MainWindow(db_conn)
        ventana_principal.show()

        ret = app.exec_()

        # Detener worker al salir
        if _sync_worker:
            try:
                _sync_worker.detener()
            except Exception:
                pass

        sys.exit(ret)

    except Exception as e:
        import traceback
        QMessageBox.critical(
            None, "Error Crítico",
            f"No se pudo iniciar la aplicación:\n{str(e)}\n\n"
            f"Detalles:\n{traceback.format_exc()[:500]}"
        )
        sys.exit(1)


if __name__ == "__main__":
    main()

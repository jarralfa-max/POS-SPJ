# modulos/ventas.py
# MÓDULO DE VENTAS CON LECTURA AUTOMÁTICA DE BÁSCULA Y TEMAS HEREDADOS

import logging
import os
import sqlite3
import time
try:
    import serial
    HAS_SERIAL_MODULE = True
except ImportError:
    serial = None
    HAS_SERIAL_MODULE = False
import json
import re
from datetime import datetime
from typing import List, Dict, Any, Optional

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QPushButton, QLineEdit,
    QComboBox, QMessageBox, QHBoxLayout,
    QFrame, QTableWidget, QTableWidgetItem, QSplitter,
    QGroupBox, QSizePolicy, QAction, QGridLayout,
    QAbstractItemView, QDialog, QCheckBox, QFormLayout, QDoubleSpinBox,
    QHeaderView, QRadioButton, QScrollArea, QListWidget, QListWidgetItem,
    QInputDialog, QGraphicsDropShadowEffect, QDialogButtonBox, QCompleter
)
from PyQt5.QtCore import Qt, QDateTime, QTimer, pyqtSignal, QLocale, QPropertyAnimation, QRect, QUrl, QSize, QStringListModel
from PyQt5.QtGui import QIcon, QDoubleValidator, QPixmap, QImage, QColor, QTextDocument, QFont, QPalette
from PyQt5.QtPrintSupport import QPrinter, QPrintDialog

# Importación de la clase base y utilidades
from .base import ModuloBase

logger = logging.getLogger("spj.ventas") 

# Importar configuración de temas
try:
    from config import TEMAS, CONFIGURACION_POR_DEFECTO, GestorTemas
except ImportError:
    TEMAS = {}
    CONFIGURACION_POR_DEFECTO = {'tema': 'Oscuro Moderno'}
    
    class GestorTemas:
        def __init__(self, conexion):
            self.conexion = conexion
            self.temas = TEMAS
        
        def obtener_tema_actual(self):
            return "Oscuro Moderno"
        
        def aplicar_tema(self, widget, nombre_tema):
            return False

# Hardware utilities centralized imports
try:
    from hardware_utils import (
        HAS_ESC_POS, HAS_WIN32, HAS_SERIAL, HAS_QRCODE,
        safe_print_ticket, safe_serial_read 
    )
except ImportError:
    HAS_ESC_POS = HAS_WIN32 = HAS_SERIAL = HAS_QRCODE = False
    def safe_print_ticket(data, on_success=None, on_error=None): logging.getLogger("spj.ventas").info("[SIMULADO] Ticket impreso (sin hardware)")
    def safe_serial_read(port, baud): return 0.000 

# Constantes de configuración
SERIAL_PORT = "COM3"
SERIAL_BAUD = 9600
TICKETS_FOLDER = "TICKETS"
LOGO_TICKET_PATH = "logo.png"

os.makedirs(TICKETS_FOLDER, exist_ok=True)
os.makedirs("imagenes_productos", exist_ok=True)

# ==============================================================================
# 1. WIDGET DE TARJETA DE PRODUCTO INTERACTIVO (SIN ESTILOS PROPIOS)
# ==============================================================================

class ProductCard(QFrame):
    """Widget interactivo que respeta completamente los temas del sistema."""
    product_selected = pyqtSignal(dict) 

    def __init__(self, producto: Dict[str, Any], parent=None):
        super().__init__(parent)
        self.producto = producto
        self.is_selected = False
        self._is_hovering = False
        self.original_size = QSize(160, 220)
        self.zoom_size = QSize(170, 230)
        
        self.setCursor(Qt.PointingHandCursor)
        self.setFixedSize(self.original_size)
        self.setFrameShape(QFrame.StyledPanel)
        self.setFrameShadow(QFrame.Raised)
        
        # Usar propiedades CSS para heredar estilos del tema
        self.setProperty("class", "product-card")
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(5)

        # 1. Imagen
        self.lbl_imagen = QLabel()
        self.lbl_imagen.setAlignment(Qt.AlignCenter)
        self.lbl_imagen.setFixedSize(140, 120)
        self.lbl_imagen.setProperty("class", "product-image")
        self._load_image()
        
        # 2. Nombre
        self.lbl_nombre = QLabel(self.producto['nombre'])
        self.lbl_nombre.setAlignment(Qt.AlignCenter)
        self.lbl_nombre.setWordWrap(True)
        self.lbl_nombre.setProperty("class", "product-name")
        
        # 3. Precio y Unidad
        self.lbl_precio = QLabel(f"${self.producto['precio']:.2f} / {self.producto['unidad']}")
        self.lbl_precio.setAlignment(Qt.AlignCenter)
        self.lbl_precio.setProperty("class", "product-price")
        
        # 4. Stock
        existencia = self.producto.get('existencia', 0)
        self.lbl_stock = QLabel(f"Stock: {existencia:.2f}")
        self.lbl_stock.setAlignment(Qt.AlignCenter)
        self.lbl_stock.setProperty("class", "product-stock")

        layout.addWidget(self.lbl_imagen)
        layout.addWidget(self.lbl_nombre)
        layout.addWidget(self.lbl_precio)
        layout.addWidget(self.lbl_stock)
        layout.addStretch(1)
        
        # Animación de sombra dinámica
        self.shadow_effect = QGraphicsDropShadowEffect(self)
        self.shadow_effect.setBlurRadius(15)
        self.shadow_effect.setXOffset(2)
        self.shadow_effect.setYOffset(2)
        self.update_shadow_color()
        self.setGraphicsEffect(self.shadow_effect)

    def update_shadow_color(self):
        """Actualiza el color de la sombra según el tema."""
        text_color = QColor(255, 255, 255)
        try:
            text_color = self.palette().color(QPalette.Text)
        except:
            pass
            
        brightness = text_color.red() * 0.299 + text_color.green() * 0.587 + text_color.blue() * 0.114
        
        if brightness > 128:
            self.shadow_effect.setColor(QColor(0, 0, 0, 60))
        else:
            self.shadow_effect.setColor(QColor(0, 0, 0, 100))

    def _load_image(self):
        """Carga la imagen del producto."""
        imagen_path = self.producto.get('imagen_path')
        if imagen_path and os.path.exists(imagen_path):
            pixmap = QPixmap(imagen_path)
            if not pixmap.isNull():
                pixmap = pixmap.scaled(
                    self.lbl_imagen.size(), 
                    Qt.KeepAspectRatio, 
                    Qt.SmoothTransformation
                )
                self.lbl_imagen.setPixmap(pixmap)
                return
        
        self.lbl_imagen.setText("📦\nSin Imagen")
        self.lbl_imagen.setProperty("class", "product-image-placeholder")

    def mousePressEvent(self, event):
        """Maneja el clic para seleccionar y emitir la señal."""
        if event.button() == Qt.LeftButton:
            self.product_selected.emit(self.producto)
            super().mousePressEvent(event)

    def set_selected(self, selected: bool):
        """Método para que el módulo principal controle el resaltado."""
        self.is_selected = selected
        if selected:
            self.setProperty("class", "product-card-selected")
        else:
            self.setProperty("class", "product-card")
        self.style().unpolish(self)
        self.style().polish(self)
        
    def enterEvent(self, event):
        """Efecto al pasar el mouse."""
        self._is_hovering = True
        self.animate_size(self.zoom_size)
        self.setProperty("class", "product-card-hover")
        self.style().unpolish(self)
        self.style().polish(self)
        super().enterEvent(event)

    def leaveEvent(self, event):
        """Quita el efecto al salir el mouse."""
        self._is_hovering = False
        self.animate_size(self.original_size)
        if self.is_selected:
            self.setProperty("class", "product-card-selected")
        else:
            self.setProperty("class", "product-card")
        self.style().unpolish(self)
        self.style().polish(self)
        super().leaveEvent(event)
        
    def animate_size(self, new_size):
        """Animación suave del cambio de tamaño."""
        self.setFixedSize(new_size)

# ==============================================================================
# 2. DIÁLOGO PARA SUSPENDER VENTA
# ==============================================================================

class DialogoSuspender(QDialog):
    """Diálogo para ingresar el nombre de una venta en espera."""
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Suspender Venta")
        self.setModal(True)
        self.setFixedSize(400, 150)
        self.nombre_venta = ""
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # Título
        titulo = QLabel("Asignar nombre a la venta suspendida:")
        titulo.setProperty("class", "dialog-title")
        layout.addWidget(titulo)
        
        # Campo de texto
        self.txt_nombre = QLineEdit()
        self.txt_nombre.setPlaceholderText("Ej: Venta de Juan, Pedido especial, etc.")
        self.txt_nombre.setProperty("class", "dialog-input")
        layout.addWidget(self.txt_nombre)
        
        # Botones
        btn_layout = QHBoxLayout()
        btn_cancelar = QPushButton("Cancelar")
        btn_aceptar = QPushButton("Suspender Venta")
        
        btn_cancelar.setProperty("class", "cancel-button")
        btn_aceptar.setProperty("class", "accept-button")
        
        btn_layout.addWidget(btn_cancelar)
        btn_layout.addWidget(btn_aceptar)
        layout.addLayout(btn_layout)
        
        btn_aceptar.clicked.connect(self.aceptar)
        btn_cancelar.clicked.connect(self.reject)
        
    def aceptar(self):
        nombre = self.txt_nombre.text().strip()
        if not nombre:
            QMessageBox.warning(self, "Advertencia", "Debe ingresar un nombre para la venta en espera.")
            return
        self.nombre_venta = nombre
        self.accept()
        
    def get_nombre_venta(self) -> str:
        return self.nombre_venta

# ==============================================================================
# 3. DIÁLOGO DE PAGO MODAL
# ==============================================================================

class DialogoPago(QDialog):
    """Ventana modal para gestionar el pago de la venta."""
    
    def __init__(self, total_a_pagar: float, parent: QWidget = None):
        super().__init__(parent)
        self.setWindowTitle("Procesar Pago")
        self.setModal(True)
        self.setFixedSize(500, 400)
        self.total_a_pagar = total_a_pagar
        self.efectivo_recibido = 0.0
        self.cambio = 0.0
        self.forma_pago = "Efectivo"
        self.saldo_credito = 0.0
        self.init_ui()
        self.conectar_eventos()
        
    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        
        # Título
        titulo = QLabel("PROCESAR PAGO")
        titulo.setProperty("class", "payment-title")
        layout.addWidget(titulo)
        
        # Totales
        self.lbl_total = QLabel(f"Total a pagar: ${self.total_a_pagar:.2f}")
        self.lbl_total.setProperty("class", "payment-total")
        self.lbl_total.setAlignment(Qt.AlignCenter)
        layout.addWidget(self.lbl_total)
        
        # Formulario
        form_layout = QFormLayout()
        form_layout.setSpacing(10)
        
        # 1. Forma de Pago
        self.cmb_forma_pago = QComboBox()
        self.cmb_forma_pago.addItems(["Efectivo", "Tarjeta", "Transferencia", "Crédito"])
        self.cmb_forma_pago.setProperty("class", "payment-combobox")
        form_layout.addRow("Forma de Pago:", self.cmb_forma_pago)
        
        # 2. Efectivo Recibido (Sólo para Efectivo)
        self.txt_recibido = QDoubleSpinBox()
        self.txt_recibido.setRange(0.00, 99999.00)
        self.txt_recibido.setDecimals(2)
        self.txt_recibido.setValue(self.total_a_pagar)
        self.txt_recibido.setSingleStep(1.0)
        self.txt_recibido.setProperty("class", "payment-spinbox")
        form_layout.addRow("Monto Recibido:", self.txt_recibido)
        
        # 3. Cambio
        self.lbl_cambio = QLabel("Cambio: $0.00")
        self.lbl_cambio.setProperty("class", "payment-change")
        form_layout.addRow("", self.lbl_cambio)
        
        # 4. Saldo Crédito (Sólo para Crédito)
        self.txt_saldo_credito = QDoubleSpinBox()
        self.txt_saldo_credito.setRange(0.00, 99999.00)
        self.txt_saldo_credito.setDecimals(2)
        self.txt_saldo_credito.setValue(self.total_a_pagar)
        self.txt_saldo_credito.setProperty("class", "payment-spinbox")
        form_layout.addRow("Saldo Adeudado:", self.txt_saldo_credito)
        self.txt_saldo_credito.hide()
        
        layout.addLayout(form_layout)
        layout.addStretch(1)
        
        # Botones de Acción
        btn_layout = QHBoxLayout()
        self.btn_cancelar = QPushButton("❌ Cancelar")
        self.btn_aceptar = QPushButton("✅ Confirmar Pago")
        
        self.btn_cancelar.setProperty("class", "payment-cancel-button")
        self.btn_aceptar.setProperty("class", "payment-accept-button")
        
        btn_layout.addWidget(self.btn_cancelar)
        btn_layout.addWidget(self.btn_aceptar)
        layout.addLayout(btn_layout)
        
        self.calcular_cambio()
        
    def conectar_eventos(self):
        self.txt_recibido.valueChanged.connect(self.calcular_cambio)
        self.cmb_forma_pago.currentTextChanged.connect(self.cambiar_forma_pago)
        self.btn_aceptar.clicked.connect(self.accept)
        self.btn_cancelar.clicked.connect(self.reject)
        
    def cambiar_forma_pago(self, forma_pago):
        """Habilita/deshabilita campos según la forma de pago."""
        self.forma_pago = forma_pago
        
        if forma_pago == "Efectivo":
            self.txt_recibido.setEnabled(True)
            self.txt_recibido.setValue(self.total_a_pagar)
            self.lbl_cambio.show()
            self.txt_saldo_credito.hide()
            
        elif forma_pago == "Crédito":
            self.txt_recibido.setEnabled(False)
            self.lbl_cambio.hide()
            self.txt_saldo_credito.show()
            self.txt_saldo_credito.setValue(self.total_a_pagar)
            
        else:
            self.txt_recibido.setEnabled(False)
            self.txt_recibido.setValue(self.total_a_pagar)
            self.lbl_cambio.hide()
            self.txt_saldo_credito.hide()
            
        self.calcular_cambio()

    def calcular_cambio(self):
        """Calcula el cambio y actualiza el label."""
        self.efectivo_recibido = self.txt_recibido.value()
        
        if self.forma_pago == "Efectivo":
            self.cambio = round(self.efectivo_recibido - self.total_a_pagar, 2)
            self.lbl_cambio.setText(f"Cambio: ${self.cambio:.2f}")
            
            if self.cambio < 0:
                self.btn_aceptar.setEnabled(False)
                self.lbl_cambio.setProperty("class", "payment-change-negative")
            else:
                self.btn_aceptar.setEnabled(True)
                self.lbl_cambio.setProperty("class", "payment-change")
        else:
            self.efectivo_recibido = self.total_a_pagar
            self.cambio = 0.0
            self.btn_aceptar.setEnabled(True)

    def get_datos_pago(self) -> Dict[str, Any]:
        """Devuelve los datos de pago al módulo principal."""
        return {
            "forma_pago": self.forma_pago,
            "total_pagado": self.total_a_pagar,
            "efectivo_recibido": self.efectivo_recibido,
            "cambio": self.cambio,
            "saldo_credito": self.txt_saldo_credito.value() if self.forma_pago == "Crédito" else 0.0
        }

# ==============================================================================
# 4. DIÁLOGO PARA AGREGAR CLIENTE
# ==============================================================================

class DialogoAgregarCliente(QDialog):
    """Diálogo para agregar un nuevo cliente rápidamente."""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Agregar Cliente")
        self.setModal(True)
        self.setFixedSize(500, 400)
        self.cliente_data = {}
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # Título
        titulo = QLabel("Agregar Nuevo Cliente")
        titulo.setProperty("class", "client-dialog-title")
        layout.addWidget(titulo)
        
        # Formulario
        form_layout = QFormLayout()
        form_layout.setSpacing(10)
        
        # Nombre
        self.txt_nombre = QLineEdit()
        self.txt_nombre.setPlaceholderText("Nombre completo del cliente")
        self.txt_nombre.setProperty("class", "client-dialog-input")
        form_layout.addRow("Nombre*:", self.txt_nombre)
        
        # Teléfono
        self.txt_telefono = QLineEdit()
        self.txt_telefono.setPlaceholderText("Número de teléfono")
        self.txt_telefono.setProperty("class", "client-dialog-input")
        form_layout.addRow("Teléfono:", self.txt_telefono)
        
        # Email
        self.txt_email = QLineEdit()
        self.txt_email.setPlaceholderText("Correo electrónico")
        self.txt_email.setProperty("class", "client-dialog-input")
        form_layout.addRow("Email:", self.txt_email)
        
        # Dirección
        self.txt_direccion = QLineEdit()
        self.txt_direccion.setPlaceholderText("Dirección completa")
        self.txt_direccion.setProperty("class", "client-dialog-input")
        form_layout.addRow("Dirección:", self.txt_direccion)
        
        # Generar tarjeta de fidelidad
        self.chk_tarjeta = QCheckBox("Generar tarjeta de fidelidad")
        self.chk_tarjeta.setChecked(True)
        self.chk_tarjeta.setProperty("class", "client-dialog-checkbox")
        form_layout.addRow("", self.chk_tarjeta)
        
        layout.addLayout(form_layout)
        layout.addStretch(1)
        
        # Botones
        btn_layout = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btn_layout.setProperty("class", "client-dialog-buttons")
        btn_layout.accepted.connect(self.validar_y_aceptar)
        btn_layout.rejected.connect(self.reject)
        
        layout.addWidget(btn_layout)
        
    def validar_y_aceptar(self):
        """Valida los datos y acepta el diálogo si son correctos."""
        nombre = self.txt_nombre.text().strip()
        if not nombre:
            QMessageBox.warning(self, "Validación", "El nombre del cliente es obligatorio.")
            return
            
        self.cliente_data = {
            'nombre': nombre,
            'telefono': self.txt_telefono.text().strip(),
            'email': self.txt_email.text().strip(),
            'direccion': self.txt_direccion.text().strip(),
            'generar_tarjeta': self.chk_tarjeta.isChecked()
        }
        self.accept()
        
    def get_cliente_data(self):
        return self.cliente_data

# ==============================================================================
# 4b. DIÁLOGO ASIGNAR TARJETA (v9) — aparece cuando tarjeta no está asignada
# ==============================================================================

class _DialogoAsignarTarjeta(QDialog):
    """
    Diálogo modal que aparece al escanear una tarjeta no asignada.
    Opciones:
      A) Asignar a cliente existente (búsqueda)
      B) Crear cliente rápido y asignar
      C) Cancelar (continuar sin tarjeta)
    """

    def __init__(self, tarjeta, conexion, parent=None):
        super().__init__(parent)
        self.tarjeta   = tarjeta
        self.conexion  = conexion
        self.resultado: dict = {}
        self.setWindowTitle("Tarjeta no asignada")
        self.setMinimumWidth(420)
        self.setModal(True)
        self._build_ui()

    def _build_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(14)
        layout.setContentsMargins(18, 18, 18, 18)

        titulo = QLabel(f"💳 Tarjeta {self.tarjeta.numero}")
        titulo.setProperty("class", "dialog-title")
        layout.addWidget(titulo)

        info = QLabel(f"Estado: {self.tarjeta.estado.capitalize()}  |  "
                      f"Puntos: {self.tarjeta.puntos_actuales}")
        layout.addWidget(info)

        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        layout.addWidget(sep)

        # Opción A: cliente existente
        grp_a = QGroupBox("A) Asignar a cliente existente")
        lay_a = QHBoxLayout(grp_a)
        self.txt_buscar_cliente = QLineEdit()
        self.txt_buscar_cliente.setPlaceholderText("Nombre o teléfono…")
        self.btn_buscar_c = QPushButton("Buscar")
        self.btn_buscar_c.clicked.connect(self._buscar_cliente_existente)
        lay_a.addWidget(self.txt_buscar_cliente)
        lay_a.addWidget(self.btn_buscar_c)
        layout.addWidget(grp_a)

        self.lbl_cliente_encontrado = QLabel("")
        self.lbl_cliente_encontrado.setVisible(False)
        layout.addWidget(self.lbl_cliente_encontrado)

        self.btn_asignar_existente = QPushButton("✅ Asignar a este cliente")
        self.btn_asignar_existente.setEnabled(False)
        self.btn_asignar_existente.clicked.connect(self._asignar_existente)
        layout.addWidget(self.btn_asignar_existente)

        sep2 = QFrame()
        sep2.setFrameShape(QFrame.HLine)
        layout.addWidget(sep2)

        # Opción B: cliente rápido
        grp_b = QGroupBox("B) Crear cliente rápido")
        lay_b = QFormLayout(grp_b)
        self.txt_nombre_rapido   = QLineEdit()
        self.txt_telefono_rapido = QLineEdit()
        self.txt_nombre_rapido.setPlaceholderText("Nombre completo *")
        self.txt_telefono_rapido.setPlaceholderText("Teléfono")
        lay_b.addRow("Nombre:", self.txt_nombre_rapido)
        lay_b.addRow("Teléfono:", self.txt_telefono_rapido)
        layout.addWidget(grp_b)

        self.btn_crear_rapido = QPushButton("➕ Crear y asignar")
        self.btn_crear_rapido.clicked.connect(self._crear_y_asignar)
        layout.addWidget(self.btn_crear_rapido)

        # Cancelar
        self.btn_cancelar = QPushButton("✖ Cancelar (continuar sin tarjeta)")
        self.btn_cancelar.clicked.connect(self.reject)
        layout.addWidget(self.btn_cancelar)

        self._cliente_id_sel = None

    def _buscar_cliente_existente(self):
        texto = self.txt_buscar_cliente.text().strip()
        if not texto:
            return
        rows = self.conexion.execute(
            "SELECT id, nombre, telefono FROM clientes "
            "WHERE (nombre LIKE ? OR telefono LIKE ?) AND activo=1 LIMIT 5",
            (f"%{texto}%", f"%{texto}%")
        ).fetchall()
        if not rows:
            self.lbl_cliente_encontrado.setText("❌ No encontrado")
            self.lbl_cliente_encontrado.setVisible(True)
            self._cliente_id_sel = None
            self.btn_asignar_existente.setEnabled(False)
            return
        if len(rows) == 1:
            self._seleccionar_cliente(rows[0])
        else:
            items = [f"{r[1]} — {r[2] or ''}" for r in rows]
            item, ok = QInputDialog.getItem(self, "Seleccionar cliente",
                                             "Múltiples resultados:", items, 0, False)
            if ok:
                idx = items.index(item)
                self._seleccionar_cliente(rows[idx])

    def _seleccionar_cliente(self, row):
        self._cliente_id_sel = row[0]
        self.lbl_cliente_encontrado.setText(f"✓ {row[1]}  {row[2] or ''}")
        self.lbl_cliente_encontrado.setVisible(True)
        self.btn_asignar_existente.setEnabled(True)

    def _asignar_existente(self):
        if not self._cliente_id_sel:
            return
        self.resultado = {'cliente_id': self._cliente_id_sel, 'nuevo': False}
        self.accept()

    def _crear_y_asignar(self):
        nombre = self.txt_nombre_rapido.text().strip()
        if not nombre:
            QMessageBox.warning(self, "Error", "El nombre es obligatorio")
            return
        telefono = self.txt_telefono_rapido.text().strip()
        import uuid as _uuid
        qr_code = _uuid.uuid4().hex[:12].upper()
        try:
            cur = self.conexion.execute(
                "INSERT INTO clientes (nombre, telefono, codigo_qr, activo, puntos) "
                "VALUES (?,?,?,1,0)",
                (nombre, telefono or None, qr_code)
            )
            cliente_id = cur.lastrowid
            self.conexion.commit()
            self.resultado = {'cliente_id': cliente_id, 'nuevo': True}
            self.accept()
        except Exception as exc:
            QMessageBox.critical(self, "Error", f"No se pudo crear cliente: {exc}")


# ==============================================================================
# 5. MODULO PRINCIPAL DE VENTAS (CON BÁSCULA FUNCIONAL Y SIN ESTILOS PROPIOS)
# ==============================================================================

class ModuloVentas(ModuloBase):
    """Módulo principal de Punto de Venta con báscula automática y temas heredados."""

    def __init__(self, conexion: sqlite3.Connection, parent: QWidget = None):
        super().__init__(conexion, parent)
        
        # Estructuras de Venta
        self.compra_actual: List[Dict[str, Any]] = []
        self.cliente_actual: Optional[Dict[str, Any]] = None
        self.producto_seleccionado: Optional[Dict[str, Any]] = None
        self._selected_card: Optional[ProductCard] = None
        self.totales = {"subtotal": 0.0, "impuestos": 0.0, "total_final": 0.0}
        
        # Gestión de Ventas en Espera
        self.ventas_en_espera: Dict[str, Dict[str, Any]] = {}
        
        # Modelo para QCompleter
        self.completer_model = QStringListModel()
        self.productos_cache = []
        
        # Control de báscula
        self.peso_actual = 0.0
        self.peso_estable = 0.0
        self.lecturas_peso = []
        self.bascula_conectada = False
        self.bascula = None
        self.producto_pendiente = None
        self.peso_inicial = 0.0
        self.monitoreo_inicio = 0
        
        # Sucursal activa (se recibe desde MainWindow vía set_sucursal)
        self.sucursal_id     = 1
        self.sucursal_nombre = "Principal"

        # Control de tema
        self._theme_initialized = False
        
        # Gestor de temas
        self.gestor_temas = GestorTemas(conexion)

        # ── SCANNER listener v9 ──────────────────────────────────────────────
        # Buffer para capturar escaneos rápidos (HID keyboard wedge)
        self._scanner_buffer: str = ""
        self._scanner_timer  = QTimer(self)
        self._scanner_timer.setSingleShot(True)
        self._scanner_timer.setInterval(80)  # debounce 80ms
        self._scanner_timer.timeout.connect(self._procesar_buffer_scanner)
        self._scanner_minlen: int = 3

        # ── LoyaltyEnterpriseEngine (Block 5/8) + legacy fallback ──────────
        try:
            from core.services.enterprise.loyalty_enterprise_engine import LoyaltyEnterpriseEngine
            self._loyalty_engine = LoyaltyEnterpriseEngine(conexion)
        except Exception:
            self._loyalty_engine = None
        try:
            from core.services.fidelidad_engine import FidelidadEngine
            self._fidelidad_engine = FidelidadEngine(conexion)
        except Exception:
            self._fidelidad_engine = None

        # ── Hardware config v9 ────────────────────────────────────────────────
        self._hw_impresora_habilitada = False
        self._hw_cajon_habilitado     = False
        self._hw_impresora_cfg: Dict   = {}
        self._hw_cajon_cfg: Dict       = {}
        self._cargar_hardware_config()
        
        # Timers
        self.timer_bascula = QTimer(self)
        self.timer_bascula.setInterval(500)
        
        # Inicialización de la interfaz
        self.init_ui()
        self.conectar_eventos()
        self.cargar_productos_interactivos()
        self.inicializar_completer()
        
        # Intentar conectar eventos del sistema (manejar errores)
        try:
            self.conectar_eventos_sistema()
        except Exception as e:
            logger.warning(f"⚠️ No se pudieron conectar eventos del sistema: {e}")
        
        # Inicializar báscula
        self.inicializar_bascula()
        
        self.aplicar_tema_desde_config()

    def aplicar_tema_desde_config(self):
        """Aplica el tema desde la configuración del sistema."""
        try:
            tema_actual = self.gestor_temas.obtener_tema_actual()
            self._theme_initialized = True
            logger.info(f"✅ Tema '{tema_actual}' aplicado correctamente al módulo de ventas")
        except Exception as e:
            logger.error(f"❌ Error aplicando tema: {e}")

    def set_sucursal(self, sucursal_id: int, sucursal_nombre: str):
        """Recibe la sucursal activa desde MainWindow al iniciar sesión."""
        self.sucursal_id     = sucursal_id
        self.sucursal_nombre = sucursal_nombre
        # Actualizar label de estado si ya existe
        if hasattr(self, "lbl_estado_terminal"):
            self.lbl_estado_terminal.setText(
                f"Terminal: ❌ No disponible  |  🏪 {sucursal_nombre}"
            )
        logger.info(f"✅ Ventas → sucursal activa: {sucursal_nombre} (id={sucursal_id})")

    def inicializar_completer(self):
        """Inicializa el QCompleter para búsqueda de productos."""
        self.completer = QCompleter()
        self.completer.setModel(self.completer_model)
        self.completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.completer.setFilterMode(Qt.MatchContains)
        self.completer.setCompletionMode(QCompleter.PopupCompletion)
        
        self.completer.popup().setProperty("class", "completer-popup")
        
        self.txt_busqueda.setCompleter(self.completer)
        self.actualizar_completer_model()

    def actualizar_completer_model(self):
        """Actualiza el modelo del completer con los productos actuales."""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT nombre, codigo_barras FROM productos WHERE oculto = 0")
            productos = cursor.fetchall()
            
            sugerencias = []
            for nombre, codigo in productos:
                sugerencias.append(nombre)
                if codigo:
                    sugerencias.append(codigo)
            
            self.completer_model.setStringList(sugerencias)
            self.productos_cache = productos
            
        except sqlite3.Error as e:
            logger.error(f"Error actualizando completer: {e}")

    def conectar_eventos(self):
        """Conecta todos los eventos de la interfaz."""
        # Búsqueda
        self.txt_busqueda.returnPressed.connect(self.buscar_productos)
        self.btn_buscar.clicked.connect(self.buscar_productos)
        self.btn_limpiar_busqueda.clicked.connect(self.limpiar_busqueda_productos)
        self.txt_busqueda.textChanged.connect(self.buscar_productos_en_tiempo_real)
        
        # Cliente
        self.txt_cliente.returnPressed.connect(self.buscar_cliente)
        self.btn_buscar_cliente.clicked.connect(self.buscar_cliente)
        self.btn_agregar_cliente.clicked.connect(self.agregar_cliente)
        self.btn_limpiar_cliente.clicked.connect(self.limpiar_cliente)
        
        # Acciones principales
        self.btn_cobrar.clicked.connect(self.procesar_pago)
        self.btn_cancelar.clicked.connect(self.cancelar_venta)
        self.btn_suspender.clicked.connect(self.suspender_venta)
        self.btn_reanudar.clicked.connect(self.mostrar_ventas_espera)
        
        # Báscula - SOLO ESTE TIMER
        self.timer_bascula.timeout.connect(self.leer_peso)
        
    def conectar_eventos_sistema(self):
        """Conecta eventos del sistema para actualizaciones en tiempo real."""
        try:
            if hasattr(self.main_window, 'registrar_evento'):
                self.main_window.registrar_evento('producto_creado', self.on_productos_actualizados)
                self.main_window.registrar_evento('producto_actualizado', self.on_productos_actualizados)
                self.main_window.registrar_evento('producto_eliminado', self.on_productos_actualizados)
                self.main_window.registrar_evento('inventario_actualizado', self.on_productos_actualizados)
                logger.info("✅ Eventos del sistema conectados correctamente")
        except Exception as e:
            logger.warning(f"⚠️ No se pudieron conectar eventos del sistema: {e}")

    def desconectar_eventos_sistema(self):
        """Desconecta eventos del sistema."""
        try:
            if hasattr(self.main_window, 'desregistrar_evento'):
                self.main_window.desregistrar_evento('producto_creado', self.on_productos_actualizados)
                self.main_window.desregistrar_evento('producto_actualizado', self.on_productos_actualizados)
                self.main_window.desregistrar_evento('producto_eliminado', self.on_productos_actualizados)
                self.main_window.desregistrar_evento('inventario_actualizado', self.on_productos_actualizados)
                logger.info("✅ Eventos del sistema desconectados correctamente")
        except Exception as e:
            logger.warning(f"⚠️ No se pudieron desconectar eventos del sistema: {e}")
            
    def on_productos_actualizados(self, datos):
        """Actualiza la lista de productos cuando hay cambios."""
        self.cargar_productos_interactivos()
        self.actualizar_completer_model()

    def init_ui(self):
        self.setWindowTitle("Punto de Venta - Sistema Avanzado")
        main_layout = QHBoxLayout(self)
        main_layout.setContentsMargins(8, 8, 8, 8)
        main_layout.setSpacing(8)
        
        # QSplitter para división de paneles
        splitter = QSplitter(Qt.Horizontal)
        splitter.setProperty("class", "main-splitter")
        splitter.setHandleWidth(3)
        
        # --- PANEL IZQUIERDO (Productos) ---
        panel_izquierdo = QWidget()
        layout_izquierdo = QVBoxLayout(panel_izquierdo)
        layout_izquierdo.setSpacing(8)
        layout_izquierdo.setContentsMargins(5, 5, 5, 5)
        
        # 1. Búsqueda de Productos
        group_busqueda = QGroupBox("🔍 Buscar Producto")
        group_busqueda.setMaximumHeight(80)
        group_busqueda.setProperty("class", "search-group")
        busqueda_layout = QHBoxLayout(group_busqueda)
        busqueda_layout.setContentsMargins(8, 8, 8, 8)
        
        self.txt_busqueda = QLineEdit()
        self.txt_busqueda.setPlaceholderText("Buscar por nombre, código o código de barras...")
        self.txt_busqueda.setProperty("class", "search-input")
        
        self.btn_buscar = QPushButton("Buscar")
        self.btn_buscar.setProperty("class", "search-button")
        
        self.btn_limpiar_busqueda = QPushButton("❌")
        self.btn_limpiar_busqueda.setToolTip("Limpiar búsqueda")
        self.btn_limpiar_busqueda.setFixedWidth(40)
        self.btn_limpiar_busqueda.setProperty("class", "icon-button")
        
        busqueda_layout.addWidget(self.txt_busqueda)
        busqueda_layout.addWidget(self.btn_buscar)
        busqueda_layout.addWidget(self.btn_limpiar_busqueda)
        layout_izquierdo.addWidget(group_busqueda)
        
        # 2. Contenedor de Productos Interactivos
        group_productos = QGroupBox("📦 Productos Disponibles")
        group_productos.setProperty("class", "products-group")
        productos_layout = QVBoxLayout(group_productos)
        
        self.scroll_area_productos = QScrollArea()
        self.scroll_area_productos.setWidgetResizable(True)
        self.scroll_area_productos.setProperty("class", "products-scroll")
        self.scroll_area_productos.setMinimumHeight(300)
        
        self.scroll_content = QWidget()
        self.grid_productos = QGridLayout(self.scroll_content)
        self.grid_productos.setAlignment(Qt.AlignTop | Qt.AlignLeft)
        self.grid_productos.setSpacing(10)
        self.grid_productos.setContentsMargins(10, 10, 10, 10)
        self.scroll_area_productos.setWidget(self.scroll_content)
        productos_layout.addWidget(self.scroll_area_productos)
        
        layout_izquierdo.addWidget(group_productos, 1)
        
        # --- BARRA DE ESTADO INFERIOR ---
        status_layout = QHBoxLayout()
        
        self.lbl_estado_bascula = QLabel("Báscula: ❌ No conectada")
        self.lbl_estado_bascula.setProperty("class", "status-label")
        
        self.lbl_estado_terminal = QLabel("Terminal: ❌ No disponible")
        self.lbl_estado_terminal.setProperty("class", "status-label")
        
        status_layout.addWidget(self.lbl_estado_bascula)
        status_layout.addWidget(self.lbl_estado_terminal)
        status_layout.addStretch()
        
        layout_izquierdo.addLayout(status_layout)
        
        # --- PANEL DERECHO (Carrito y Acciones) ---
        panel_derecho = QWidget()
        panel_derecho.setMinimumWidth(420)
        layout_derecho = QVBoxLayout(panel_derecho)
        layout_derecho.setSpacing(8)
        layout_derecho.setContentsMargins(5, 5, 5, 5)
        
        # 1. Gestión de Cliente
        group_cliente = QGroupBox("👤 Cliente")
        group_cliente.setProperty("class", "client-group")
        cliente_layout = QVBoxLayout(group_cliente)
        cliente_layout.setContentsMargins(6, 6, 6, 6)
        cliente_layout.setSpacing(3)
        
        # INICIALIZAR WIDGETS DE CLIENTE (FALTANTES)
        self.txt_cliente = QLineEdit()
        self.txt_cliente.setPlaceholderText("Buscar por ID, nombre, teléfono o código...")
        self.txt_cliente.setProperty("class", "client-input")
        
        self.btn_buscar_cliente = QPushButton("🔍")
        self.btn_buscar_cliente.setToolTip("Buscar cliente")
        self.btn_buscar_cliente.setFixedWidth(40)
        self.btn_buscar_cliente.setProperty("class", "icon-button")
        
        self.btn_agregar_cliente = QPushButton("➕")
        self.btn_agregar_cliente.setToolTip("Agregar nuevo cliente")
        self.btn_agregar_cliente.setFixedWidth(40)
        self.btn_agregar_cliente.setProperty("class", "icon-button")
        
        self.btn_limpiar_cliente = QPushButton("❌")
        self.btn_limpiar_cliente.setToolTip("Limpiar búsqueda de cliente")
        self.btn_limpiar_cliente.setFixedWidth(40)
        self.btn_limpiar_cliente.setProperty("class", "icon-button")
        
        # Layout para búsqueda de cliente
        busqueda_cliente_layout = QHBoxLayout()
        busqueda_cliente_layout.addWidget(self.txt_cliente)
        busqueda_cliente_layout.addWidget(self.btn_buscar_cliente)
        busqueda_cliente_layout.addWidget(self.btn_agregar_cliente)
        busqueda_cliente_layout.addWidget(self.btn_limpiar_cliente)
        cliente_layout.addLayout(busqueda_cliente_layout)
        
        # Información del cliente (INICIALIZAR FALTANTES)
        self.lbl_nombre_cliente = QLabel("Público General")
        self.lbl_puntos_cliente = QLabel("Puntos: 0")
        self.lbl_telefono_cliente = QLabel("Teléfono: -")  # FALTABA
        self.lbl_email_cliente = QLabel("Email: -")        # FALTABA
        
        self.lbl_nombre_cliente.setProperty("class", "client-info-highlight")
        self.lbl_puntos_cliente.setProperty("class", "client-info-highlight")
        self.lbl_telefono_cliente.setProperty("class", "client-info")
        self.lbl_email_cliente.setProperty("class", "client-info")
        
        # Layout compacto para info del cliente
        cliente_info_layout = QHBoxLayout()
        cliente_info_layout.addWidget(self.lbl_nombre_cliente)
        cliente_info_layout.addStretch()
        cliente_info_layout.addWidget(self.lbl_puntos_cliente)
        cliente_layout.addLayout(cliente_info_layout)
        
        # Info adicional en segunda línea
        cliente_info2_layout = QHBoxLayout()
        cliente_info2_layout.addWidget(self.lbl_telefono_cliente)
        cliente_info2_layout.addStretch()
        cliente_info2_layout.addWidget(self.lbl_email_cliente)
        cliente_layout.addLayout(cliente_info2_layout)
        
        layout_derecho.addWidget(group_cliente)
        
        # 2. Carrito de Compras - CONFIGURADO PARA 4 PRODUCTOS VISIBLES
        group_carrito = QGroupBox("🛒 Carrito de Compra")
        group_carrito.setProperty("class", "venta-group")
        carrito_layout = QVBoxLayout(group_carrito)
        carrito_layout.setContentsMargins(5, 5, 5, 5)

        # TABLA OPTIMIZADA PARA 4 PRODUCTOS VISIBLES
        self.tabla_compra = QTableWidget()
        self.tabla_compra.setProperty("class", "tabla-carrito")  # ✅ NUEVA CLASE CSS
        self.tabla_compra.setColumnCount(6)
        self.tabla_compra.setHorizontalHeaderLabels(["Producto", "Cant.", "Precio", "Total", "", ""])
        
        # Configuración para espacio óptimo
        self.tabla_compra.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_compra.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tabla_compra.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.tabla_compra.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        
        # Altura para 4 filas visibles (calculada automáticamente por CSS)
        self.tabla_compra.verticalHeader().setDefaultSectionSize(40)
        self.tabla_compra.verticalHeader().setVisible(False)
        
        # Distribución de columnas
        self.tabla_compra.setColumnWidth(0, 160)  # Producto
        self.tabla_compra.setColumnWidth(1, 50)   # Cantidad
        self.tabla_compra.setColumnWidth(2, 60)   # Precio
        self.tabla_compra.setColumnWidth(3, 65)   # Total
        self.tabla_compra.setColumnWidth(4, 35)   # Modificar
        self.tabla_compra.setColumnWidth(5, 35)   # Eliminar
        
        self.tabla_compra.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)

        carrito_layout.addWidget(self.tabla_compra)
        
        # Indicador de scroll actualizado para 4 productos
        self.lbl_info_carrito = QLabel("")
        self.lbl_info_carrito.setAlignment(Qt.AlignCenter)
        self.lbl_info_carrito.setProperty("class", "info-label")
        carrito_layout.addWidget(self.lbl_info_carrito)
        
        layout_derecho.addWidget(group_carrito, 1)
    
        # 3. Información de Venta
        group_info_venta = QGroupBox("📊 Resumen")
        group_info_venta.setMaximumHeight(120)
        group_info_venta.setProperty("class", "venta-group")
        info_venta_layout = QGridLayout(group_info_venta)
        info_venta_layout.setContentsMargins(8, 8, 8, 8)
        
        # INICIALIZAR WIDGETS DE INFO VENTA
        self.lbl_peso_bascula = QLabel("Peso: 0.000 kg")
        self.lbl_total = QLabel("TOTAL: $0.00")
        self.lbl_puntos_venta = QLabel("Puntos: 0")
        
        self.lbl_peso_bascula.setProperty("class", "info-box")
        self.lbl_total.setProperty("class", "total-box")
        self.lbl_puntos_venta.setProperty("class", "info-box")
        
        self.lbl_peso_bascula.setAlignment(Qt.AlignCenter)
        self.lbl_total.setAlignment(Qt.AlignCenter)
        self.lbl_puntos_venta.setAlignment(Qt.AlignCenter)
        
        info_venta_layout.addWidget(self.lbl_peso_bascula, 0, 0)
        info_venta_layout.addWidget(self.lbl_total, 0, 1)
        info_venta_layout.addWidget(self.lbl_puntos_venta, 1, 0, 1, 2)
        
        layout_derecho.addWidget(group_info_venta)

        # 4. Botones de Acción
        group_acciones = QGroupBox("⚡ Acciones")
        group_acciones.setMaximumHeight(165)
        group_acciones.setProperty("class", "venta-group")
        acciones_layout = QGridLayout(group_acciones)
        acciones_layout.setContentsMargins(8, 8, 8, 8)
        acciones_layout.setVerticalSpacing(4)
        
        # INICIALIZAR BOTONES
        self.btn_cobrar = QPushButton("💰 Cobrar")
        self.btn_suspender = QPushButton("⏸️ Suspender")
        self.btn_reanudar = QPushButton("▶️ Reanudar (0)")
        self.btn_cancelar = QPushButton("❌ Cancelar")
        
        button_height = 38
        self.btn_cobrar.setFixedHeight(button_height)
        self.btn_suspender.setFixedHeight(button_height)
        self.btn_reanudar.setFixedHeight(button_height)
        self.btn_cancelar.setFixedHeight(button_height)
        
        self.btn_cobrar.setProperty("class", "venta-button")
        self.btn_cancelar.setProperty("class", "venta-button")
        self.btn_suspender.setProperty("class", "venta-button")
        self.btn_reanudar.setProperty("class", "venta-button")

        acciones_layout.addWidget(self.btn_cobrar, 0, 0, 1, 2)
        acciones_layout.addWidget(self.btn_suspender, 1, 0)
        acciones_layout.addWidget(self.btn_reanudar, 1, 1)
        acciones_layout.addWidget(self.btn_cancelar, 2, 0, 1, 2)
        
        layout_derecho.addWidget(group_acciones)
        
        # Agregar paneles al splitter
        splitter.addWidget(panel_izquierdo)
        splitter.addWidget(panel_derecho)
        splitter.setSizes([600, 500])
        main_layout.addWidget(splitter)
    
    def conectar_eventos(self):
        """Conecta todos los eventos de la interfaz."""
        # Búsqueda
        self.txt_busqueda.returnPressed.connect(self.buscar_productos)
        self.btn_buscar.clicked.connect(self.buscar_productos)
        self.btn_limpiar_busqueda.clicked.connect(self.limpiar_busqueda_productos)
        self.txt_busqueda.textChanged.connect(self.buscar_productos_en_tiempo_real)
        
        # Cliente
        self.txt_cliente.returnPressed.connect(self.buscar_cliente)
        self.btn_buscar_cliente.clicked.connect(self.buscar_cliente)
        self.btn_agregar_cliente.clicked.connect(self.agregar_cliente)
        self.btn_limpiar_cliente.clicked.connect(self.limpiar_cliente)
        
        # Acciones principales
        self.btn_cobrar.clicked.connect(self.procesar_pago)
        self.btn_cancelar.clicked.connect(self.cancelar_venta)
        self.btn_suspender.clicked.connect(self.suspender_venta)
        self.btn_reanudar.clicked.connect(self.mostrar_ventas_espera)
        
        # Báscula
        self.timer_bascula.timeout.connect(self.leer_peso)
        

    def limpiar_busqueda_productos(self):
        """Limpia la búsqueda de productos y muestra todos los productos."""
        self.txt_busqueda.clear()
        self.cargar_productos_interactivos()

    def buscar_productos_en_tiempo_real(self, texto: str):
        """Busca productos en tiempo real mientras se escribe."""
        if len(texto.strip()) >= 2:
            self.cargar_productos_interactivos(texto.strip())

    def cargar_productos_interactivos(self, filtro: str = ""):
        """Carga los productos en el grid interactivo."""
        # Limpiar grid existente
        for i in reversed(range(self.grid_productos.count())):
            widget = self.grid_productos.itemAt(i).widget()
            if widget:
                widget.setParent(None)
        
        try:
            cursor = self.conexion.cursor()
            query = """
                SELECT id, nombre, precio, existencia, unidad, categoria, 
                       stock_minimo, imagen_path, es_compuesto, es_subproducto,
                       codigo_barras
                FROM productos 
                WHERE oculto = 0
            """
            params = []
            
            if filtro:
                query += " AND (nombre LIKE ? OR id = ? OR categoria LIKE ? OR codigo_barras = ?)"
                params = [f'%{filtro}%', filtro, f'%{filtro}%', filtro]
            
            query += " ORDER BY nombre"
            cursor.execute(query, params)
            productos = cursor.fetchall()
            
            # Crear tarjetas de productos
            col_count = 3
            for i, producto in enumerate(productos):
                producto_data = {
                    'id': producto[0],
                    'nombre': producto[1],
                    'precio': float(producto[2]),
                    'existencia': float(producto[3]),
                    'unidad': producto[4],
                    'categoria': producto[5],
                    'stock_minimo': float(producto[6]),
                    'imagen_path': producto[7],
                    'es_compuesto': producto[8],
                    'es_subproducto': producto[9],
                    'codigo_barras': producto[10]
                }
                
                card = ProductCard(producto_data)
                card.product_selected.connect(self.seleccionar_producto)
                
                row = i // col_count
                col = i % col_count
                self.grid_productos.addWidget(card, row, col)
                
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al cargar productos: {str(e)}", QMessageBox.Critical)

    def buscar_productos(self):
        """Busca productos según el filtro ingresado."""
        filtro = self.txt_busqueda.text().strip()
        self.cargar_productos_interactivos(filtro)

    def seleccionar_producto(self, producto: Dict[str, Any]):
        """Maneja la selección de un producto con LECTURA AUTOMÁTICA DE BÁSCULA."""
        if self._selected_card:
            self._selected_card.set_selected(False)
            
        self._selected_card = self.sender()
        if self._selected_card:
            self._selected_card.set_selected(True)
            
        self.producto_seleccionado = producto
        
        unidad = producto['unidad'].lower()
        
        if any(peso_keyword in unidad for peso_keyword in ['kg', 'kilogramo', 'kilo', 'gramo', 'gr']):
            # PRODUCTOS POR PESO - LECTURA AUTOMÁTICA DE BÁSCULA
            self.iniciar_monitoreo_peso(producto)
        else:
            # Productos por unidad - pedir cantidad
            self.agregar_producto_por_unidad(producto)

    # ── SCANNER v9: Keyboard-wedge listener con debounce ─────────────────────

    def _cargar_hardware_config(self) -> None:
        """Carga configuración de hardware desde tabla hardware_config."""
        try:
            rows = self.conexion.execute(
                "SELECT tipo, habilitado, configuracion FROM hardware_config"
            ).fetchall()
            for tipo, hab, cfg_json in rows:
                import json
                cfg = json.loads(cfg_json) if cfg_json else {}
                if tipo == "impresora":
                    self._hw_impresora_habilitada = bool(hab)
                    self._hw_impresora_cfg        = cfg
                elif tipo == "cajon":
                    self._hw_cajon_habilitado = bool(hab)
                    self._hw_cajon_cfg        = cfg
                elif tipo == "scanner":
                    self._scanner_minlen = int(cfg.get("min_len", 3))
                    debounce = int(cfg.get("debounce_ms", 80))
                    self._scanner_timer.setInterval(debounce)
        except Exception:
            pass  # tablas aún no migradas o hardware_config no existe

    def keyPressEvent(self, event) -> None:
        """
        Intercepta teclas para scanner keyboard-wedge.
        Escáneres HID envían caracteres rápidamente seguidos de Enter/Tab.
        Debounce de 80ms: si no llega más carácter antes del timeout,
        procesa como código de barras.
        """
        key  = event.key()
        text = event.text()

        # Enter / Tab = fin de código
        if key in (Qt.Key_Return, Qt.Key_Enter, Qt.Key_Tab):
            if self._scanner_buffer:
                self._scanner_timer.stop()
                self._procesar_buffer_scanner()
            else:
                super().keyPressEvent(event)
            return

        # Carácter imprimible → acumular en buffer
        if text and text.isprintable():
            self._scanner_buffer += text
            self._scanner_timer.start()  # reinicia debounce
            return

        # Resto de teclas → comportamiento normal
        super().keyPressEvent(event)

    def _procesar_buffer_scanner(self) -> None:
        """Llamado cuando debounce expira o llega Enter — busca el producto."""
        codigo = self._scanner_buffer.strip()
        self._scanner_buffer = ""
        if not codigo or len(codigo) < self._scanner_minlen:
            return

        try:
            row = self.conexion.execute(
                """
                SELECT id, nombre, precio_venta, precio_kilo,
                       existencia, unidad, tipo,
                       imagen_path, categoria, descripcion, codigo_barras
                FROM productos
                WHERE (codigo_barras = ? OR id = ?)
                  AND activo = 1
                  AND oculto = 0
                LIMIT 1
                """,
                (codigo, codigo if codigo.isdigit() else -1)
            ).fetchone()

            if row:
                producto = {
                    'id':            row[0], 'nombre':      row[1],
                    'precio_venta':  row[2], 'precio_kilo': row[3],
                    'existencia':    row[4], 'unidad':      row[5],
                    'tipo':          row[6], 'imagen_path': row[7],
                    'categoria':     row[8], 'descripcion': row[9],
                    'codigo_barras': row[10],
                    'precio_unitario': row[2],
                }
                self.agregar_producto_a_carrito(producto)
                # Feedback visual breve
                if hasattr(self, 'txt_busqueda'):
                    self.txt_busqueda.setText(f"✓ {row[1]}")
                    QTimer.singleShot(1200, lambda: self.txt_busqueda.clear())
            else:
                # No encontrado — mostrar en buscador para lookup manual
                if hasattr(self, 'txt_busqueda'):
                    self.txt_busqueda.setText(codigo)
                    self.buscar_productos()
        except Exception as exc:
            import logging
            logging.getLogger('spj.ventas').warning("scanner lookup: %s", exc)

    # ── FLUJO TARJETAS v9 ─────────────────────────────────────────────────────

    def procesar_tarjeta_escaneo(self, numero_o_qr: str) -> None:
        """
        Punto de entrada cuando se escanea una tarjeta en módulo ventas.
        Casos:
          1. Tarjeta asignada → cargar cliente automáticamente
          2. Tarjeta libre   → DialogoAsignarTarjeta
          3. Tarjeta bloqueada → aviso
          4. No encontrada   → aviso
        """
        try:
            from core.services.card_batch_engine import CardBatchEngine
            eng = CardBatchEngine(self.conexion, self.usuario_actual or "cajero")
            tarjeta = eng.buscar_tarjeta(numero_o_qr)

            if not tarjeta:
                QMessageBox.warning(self, "Tarjeta", f"Tarjeta '{numero_o_qr}' no encontrada en el sistema.")
                return

            if tarjeta.estado == "bloqueada":
                QMessageBox.warning(self, "Tarjeta Bloqueada",
                    f"Tarjeta {tarjeta.numero} está bloqueada.\nMotivo: No disponible en este momento.")
                return

            if tarjeta.estado == "asignada" and tarjeta.id_cliente:
                # Cargar cliente asociado
                row = self.conexion.execute(
                    "SELECT id, nombre, telefono, email, direccion, rfc, puntos, codigo_qr, saldo "
                    "FROM clientes WHERE id = ? AND activo = 1",
                    (tarjeta.id_cliente,)
                ).fetchone()
                if row:
                    self.cliente_actual = {
                        'id': row[0], 'nombre': row[1], 'telefono': row[2],
                        'email': row[3], 'direccion': row[4], 'rfc': row[5],
                        'puntos': row[6], 'codigo_qr': row[7], 'saldo': row[8] or 0.0,
                    }
                    self._actualizar_ui_cliente()
                    if hasattr(self, 'lbl_puntos_cliente'):
                        self.lbl_puntos_cliente.setText(f"Puntos: {row[6]} | Nivel: {tarjeta.nivel}")
                    return

            # Tarjeta libre / generada / impresa → dialogo asignación
            dialogo = _DialogoAsignarTarjeta(tarjeta, self.conexion, self)
            if dialogo.exec_() == QDialog.Accepted:
                resultado = dialogo.resultado
                if resultado and resultado.get('cliente_id'):
                    cliente_id = resultado['cliente_id']
                    # Asignar tarjeta
                    eng.asignar_tarjeta(tarjeta.id, cliente_id,
                                        motivo="asignacion_en_venta")
                    # Cargar cliente
                    row = self.conexion.execute(
                        "SELECT id, nombre, telefono, email, direccion, rfc, puntos, codigo_qr, saldo "
                        "FROM clientes WHERE id = ?",
                        (cliente_id,)
                    ).fetchone()
                    if row:
                        self.cliente_actual = {
                            'id': row[0], 'nombre': row[1], 'telefono': row[2],
                            'email': row[3], 'direccion': row[4], 'rfc': row[5],
                            'puntos': row[6], 'codigo_qr': row[7], 'saldo': row[8] or 0.0,
                        }
                        self._actualizar_ui_cliente()
        except ImportError:
            QMessageBox.information(self, "Tarjeta", "Motor de tarjetas no disponible en esta versión.")
        except Exception as exc:
            QMessageBox.critical(self, "Error Tarjeta", str(exc))

    def _actualizar_ui_cliente(self) -> None:
        """Refresca UI con datos del cliente_actual."""
        if not self.cliente_actual:
            return
        nombre = self.cliente_actual.get('nombre', '')
        if hasattr(self, 'txt_cliente'):
            self.txt_cliente.setText(nombre)
        if hasattr(self, 'lbl_nombre_cliente'):
            self.lbl_nombre_cliente.setText(nombre)
        if hasattr(self, 'lbl_puntos_cliente'):
            puntos = self.cliente_actual.get('puntos', 0)
            self.lbl_puntos_cliente.setText(f"Puntos: {puntos}")

    # ── HARDWARE v9: cajón + impresora ESC/POS ────────────────────────────────

    def _abrir_cajon(self) -> None:
        """Envía señal de apertura de cajón si hardware habilitado."""
        if not self._hw_cajon_habilitado:
            return
        try:
            metodo = self._hw_cajon_cfg.get("metodo", "escpos")
            if metodo == "escpos" and HAS_ESC_POS:
                from escpos.printer import Usb, Serial
                # Kick-pulse estándar: DLE + EOT
                pulse = bytes([0x10, 0x14, 0x01, 0x00, 0x05])
                # Intentar conexión USB o serial según config
                puerto = self._hw_cajon_cfg.get("puerto", "USB")
                if puerto == "USB":
                    try:
                        p = Usb(0x04b8, 0x0202)  # VID/PID Epson genérico
                        p._raw(pulse)
                    except Exception:
                        pass  # silencioso si no conectado
            elif metodo == "serial" and HAS_SERIAL:
                import serial as _ser
                puerto_s = self._hw_cajon_cfg.get("puerto_serial", "COM4")
                baud     = int(self._hw_cajon_cfg.get("baud", 9600))
                try:
                    with _ser.Serial(puerto_s, baud, timeout=0.5) as s:
                        s.write(bytes([0x10, 0x14, 0x01, 0x00, 0x05]))
                except Exception:
                    pass
        except Exception as exc:
            import logging
            logging.getLogger('spj.ventas').debug("abrir_cajon: %s", exc)

    def _imprimir_ticket_hardware(self, ticket_data: dict) -> None:
        """Imprime ticket vía ESC/POS si habilitado."""
        if not self._hw_impresora_habilitada:
            return
        try:
            safe_print_ticket(ticket_data)
        except Exception as exc:
            import logging
            logging.getLogger('spj.ventas').debug("imprimir_ticket_hw: %s", exc)

    def inicializar_bascula(self):
        """Inicialización simple - la conexión se prueba en cada lectura"""
        self.bascula_conectada = False
        self.lbl_estado_bascula.setText("Báscula: ⏳ Conectando...")
        self.lbl_peso_bascula.setText("Peso: 0.000 kg")
        
        # El timer intentará conectar automáticamente
        self.timer_bascula.start()
        logger.debug("🔄 Iniciando monitor de báscula...")
        
    def leer_peso(self):
        try:
            if not self.bascula:
                self.bascula = serial.Serial("COM3", 9600, timeout=0.2)
                self.lbl_estado_bascula.setText("Báscula: ✅ Conectada")
                logger.debug("🔌 Báscula conectada")

            self.bascula.write(b'P\r\n')
            datos = self.bascula.readline().decode('utf-8', errors='ignore').strip()

            peso = self.extraer_peso_de_respuesta(datos)
            if peso is not None:
                self.peso_actual = peso
                self.lbl_peso_bascula.setText(f"Peso: {peso:.3f} kg")

                if self.producto_pendiente:
                    self.procesar_peso_para_producto(peso)

        except Exception as e:
            self.bascula = None
            self.lbl_estado_bascula.setText("Báscula: ❌ Desconectada")
                
    def iniciar_monitoreo_peso(self, producto: Dict[str, Any]):
        """Inicia el monitoreo del peso para agregar automáticamente."""
        self.producto_pendiente = producto
        self.lecturas_estables = []
        self.peso_inicial = 0 
        self.lecturas_peso = [self.peso_actual]  # Iniciar con lectura actual
        self.monitoreo_inicio = time.time()
        
        # NO CAMBIAR EL TEXTO DEL DISPLAY - SOLO MOSTRAR PESO
        # El display seguirá mostrando "Peso: X.XXX kg" normalmente
        
        # Asegurar que el timer de la báscula esté corriendo
        if not self.timer_bascula.isActive():
            self.timer_bascula.start()
        
        logger.debug(f"🎯 Iniciando monitoreo para: {producto['nombre']}")
        logger.debug(f"📌 Peso inicial: {self.peso_inicial:.3f} kg")

    def procesar_peso_para_producto(self, peso: float):
        """
        Agregar SOLO cuando el peso esté estable (sin movimiento).
        La báscula debe estar conectada SIEMPRE.
        """

        if not hasattr(self, 'producto_pendiente') or not self.producto_pendiente:
            return

        # Inicializar lista si no existe
        if not hasattr(self, 'lecturas_estables'):
            self.lecturas_estables = []

        # Guardar lecturas recientes
        self.lecturas_estables.append(peso)
        if len(self.lecturas_estables) > 4:
            self.lecturas_estables.pop(0)

        # Checar variación
        variacion = max(self.lecturas_estables) - min(self.lecturas_estables)

        if len(self.lecturas_estables) >= 2 and variacion <= 0.005:
            peso_neto = peso - self.peso_inicial  # 💡 ahora sí será REAL

            logger.debug(f"DEBUG - peso: {peso:.3f} | peso_inicial: {self.peso_inicial:.3f} | neto: {peso_neto:.3f}")

            if peso_neto > 0.010:
                logger.debug(f"✔ PESO ESTABLE → AGREGADO: {peso_neto:.3f} kg")
                self.agregar_producto_directo(self.producto_pendiente, peso_neto)
                self.finalizar_monitoreo_peso()
                self.lecturas_estables = []
        else:
            logger.debug(f"⌛ Esperando peso estable... Variación: {variacion:.4f} kg")

                
    def finalizar_monitoreo_peso(self):
        """Finaliza el monitoreo de peso y limpia el estado."""
        if hasattr(self, 'producto_pendiente'):
            del self.producto_pendiente
        if hasattr(self, 'peso_inicial'):
            del self.peso_inicial
        if hasattr(self, 'monitoreo_inicio'):
            del self.monitoreo_inicio
        
        self.lecturas_peso = []
        # NO RESTAURAR ESTILOS - EL DISPLAY SIGUE MOSTRANDO PESO NORMALMENTE
        
        logger.debug("🔚 Monitoreo de peso finalizado")

    def extraer_peso_de_respuesta(self, respuesta: str) -> Optional[float]:
        """
        Extrae el peso numérico de la respuesta de la báscula Rhino.
        Maneja diferentes formatos de respuesta.
        """
        import re
        
        if not respuesta:
            return None

        try:
            # Patrones comunes para básculas Rhino
            patrones = [
                # Formato: "S T         +0.560 kg"
                r'[STUS\s]*([+-]?\d+\.\d+)\s*[kK][gG]',
                # Formato: "+0.560"
                r'([+-]?\d+\.\d+)',
                # Formato: "0560" (sin decimales)
                r'(\d{3,4})',
            ]
            
            for patron in patrones:
                coincidencia = re.search(patron, respuesta)
                if coincidencia:
                    peso_str = coincidencia.group(1)
                    
                    # Si el peso no tiene punto decimal, asumir que son gramos
                    if '.' not in peso_str:
                        if len(peso_str) >= 3:
                            # Convertir a kg (ej: "0560" -> 0.560 kg)
                            peso = float(peso_str) / 1000.0
                        else:
                            peso = float(peso_str)
                    else:
                        peso = float(peso_str)
                    
                    # Asegurar que el peso sea positivo
                    return abs(peso)
                    
            return None
            
        except (ValueError, AttributeError):
            return None
    
    def preguntar_peso_manual(self):
        """Pregunta al usuario si desea ingresar el peso manualmente."""
        logger.debug("🔔 Llamando a preguntar_peso_manual")  # DEBUG
        if hasattr(self, 'producto_pendiente') and self.producto_pendiente:
            respuesta = QMessageBox.question(
                self, "Peso No Detectado",
                f"No se detectó un peso estable para {self.producto_pendiente['nombre']}.\n\n"
                f"¿Desea ingresar el peso manualmente?",
                QMessageBox.Yes | QMessageBox.No
            )
            
            if respuesta == QMessageBox.Yes:
                cantidad, ok = QInputDialog.getDouble(
                    self, "Peso Manual", 
                    f"Ingrese el peso para {self.producto_pendiente['nombre']} (kg):",
                    value=0.100, min=0.001, max=9999.0, decimals=3
                )
                if ok and cantidad > 0:
                    self.agregar_producto_directo(self.producto_pendiente, cantidad)
            
            self.finalizar_monitoreo_peso()
            
    def agregar_producto_por_unidad(self, producto: Dict[str, Any]):
        """Agrega producto por unidad pidiendo cantidad."""
        cantidad, ok = QInputDialog.getDouble(
            self, "Cantidad", 
            f"Ingrese la cantidad para {producto['nombre']}:",
            value=1.0, min=0.001, max=9999.0, decimals=3
        )
        if ok and cantidad > 0:
            self.agregar_producto_directo(producto, cantidad)
        else:
            self.limpiar_seleccion_producto()

    def agregar_producto_directo(self, producto: Dict[str, Any], cantidad: float):
        """Agrega un producto directamente al carrito - CON DEBUG"""
        logger.debug(f"🎯 Intentando agregar: {producto['nombre']} - {cantidad:.3f} {producto['unidad']}")
        
        if cantidad <= 0:
            logger.error("❌ Cantidad menor o igual a cero")
            QMessageBox.warning(self, "Advertencia", "La cantidad debe ser mayor a cero.")
            self.limpiar_seleccion_producto()
            return
            
        if cantidad > producto['existencia']:
            QMessageBox.warning(self, "Stock Insuficiente",
                f"Stock insuficiente. Disponible: {producto['existencia']:.2f} {producto['unidad']}")
            self.limpiar_seleccion_producto()
            return
            
        for item in self.compra_actual:
            if item['id'] == producto['id']:
                respuesta = QMessageBox.question(
                    self, "Producto Duplicado", 
                    f"El producto '{producto['nombre']}' ya está en el carrito.\n\n"
                    f"¿Desea modificar la cantidad existente?",
                    QMessageBox.Yes | QMessageBox.No
                )
                
                if respuesta == QMessageBox.Yes:
                    for i, item in enumerate(self.compra_actual):
                        if item['id'] == producto['id']:
                            nueva_cantidad = item['cantidad'] + cantidad
                            if nueva_cantidad > producto['existencia']:
                                QMessageBox.warning(
                                    self, "Stock Insuficiente",
                                    f"Stock insuficiente. Disponible: {producto['existencia']:.2f} {producto['unidad']}"
                                )
                                break
                                
                            item['cantidad'] = nueva_cantidad
                            item['total'] = round(nueva_cantidad * item['precio_unitario'], 2)
                            self.actualizar_tabla_compra()
                            self.mostrar_mensaje("Éxito", f"Cantidad actualizada: {nueva_cantidad:.3f} {producto['unidad']}")
                            break
                else:
                    total_item = round(cantidad * producto['precio'], 2)
                    item_compra = {
                        'id': producto['id'],
                        'nombre': f"{producto['nombre']} (adicional)",
                        'cantidad': cantidad,
                        'unidad': producto['unidad'],
                        'precio_unitario': producto['precio'],
                        'total': total_item
                    }
                    self.compra_actual.append(item_compra)
                    self.actualizar_tabla_compra()
                    
                self.limpiar_seleccion_producto()
                return
                
        total_item = round(cantidad * producto['precio'], 2)
        item_compra = {
            'id': producto['id'],
            'nombre': producto['nombre'],
            'cantidad': cantidad,
            'unidad': producto['unidad'],
            'precio_unitario': producto['precio'],
            'total': total_item
        }
        
        self.compra_actual.append(item_compra)
        self.actualizar_tabla_compra()
        self.limpiar_seleccion_producto()

    

    def limpiar_seleccion_producto(self):
        """Limpia la selección actual del producto."""
        if self._selected_card:
            self._selected_card.set_selected(False)
            self._selected_card = None
            
        self.producto_seleccionado = None

    def actualizar_tabla_compra(self):
        """Actualiza la tabla del carrito de compras con información de scroll."""
        self.tabla_compra.setRowCount(len(self.compra_actual))
        
        for row, item in enumerate(self.compra_actual):  # CORREGIDO: era 'compra_compra'
            self.tabla_compra.setItem(row, 0, QTableWidgetItem(item['nombre']))
            
            cantidad_item = QTableWidgetItem(f"{item['cantidad']:.3f}")
            cantidad_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.tabla_compra.setItem(row, 1, cantidad_item)
            
            precio_item = QTableWidgetItem(f"${item['precio_unitario']:.2f}")
            precio_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.tabla_compra.setItem(row, 2, precio_item)
            
            total_item = QTableWidgetItem(f"${item['total']:.2f}")
            total_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
            self.tabla_compra.setItem(row, 3, total_item)
            
            btn_modificar = QPushButton("✏️")
            btn_modificar.setToolTip("Modificar cantidad")
            btn_modificar.setProperty("class", "table-edit-button")
            btn_modificar.clicked.connect(lambda checked, r=row: self.modificar_cantidad_producto(r))
            self.tabla_compra.setCellWidget(row, 4, btn_modificar)
            
            btn_eliminar = QPushButton("❌")
            btn_eliminar.setToolTip("Eliminar producto")
            btn_eliminar.setProperty("class", "table-delete-button")
            btn_eliminar.clicked.connect(lambda checked, r=row: self.eliminar_producto_carrito(r))
            self.tabla_compra.setCellWidget(row, 5, btn_eliminar)
            
        self.calcular_totales()
        
        # MEJORA: Actualizar indicador de scroll para 4 productos
        if len(self.compra_actual) > 4:
            self.lbl_info_carrito.setText(f"⚠️ Use scroll para ver {len(self.compra_actual) - 4} productos más")
            self.lbl_info_carrito.setStyleSheet("color: orange; font-size: 9px;")
        else:
            self.lbl_info_carrito.setText("")

    def modificar_cantidad_producto(self, row: int):
        """Modifica la cantidad de un producto en el carrito."""
        if 0 <= row < len(self.compra_actual):
            producto = self.compra_actual[row]
            cantidad_actual = producto['cantidad']
            
            cantidad, ok = QInputDialog.getDouble(
                self, "Modificar Cantidad", 
                f"Ingrese la nueva cantidad para {producto['nombre']}:",
                value=cantidad_actual, min=0.001, max=9999.0, decimals=3
            )
            
            if ok and cantidad > 0:
                stock_disponible = self.obtener_stock_producto(producto['id'])
                if cantidad > stock_disponible:
                    QMessageBox.warning(self, "Stock Insuficiente",
                        f"Stock insuficiente. Disponible: {stock_disponible:.2f} {producto['unidad']}")
                    return
                    
                producto['cantidad'] = cantidad
                producto['total'] = round(cantidad * producto['precio_unitario'], 2)
                self.actualizar_tabla_compra()

    def obtener_stock_producto(self, producto_id: int) -> float:
        """Obtiene el stock disponible de un producto."""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT existencia FROM productos WHERE id = ?", (producto_id,))
            resultado = cursor.fetchone()
            return resultado[0] if resultado else 0.0
        except sqlite3.Error:
            return 0.0

    def eliminar_producto_carrito(self, row: int):
        """Elimina un producto del carrito."""
        if 0 <= row < len(self.compra_actual):
            producto = self.compra_actual[row]['nombre']
            self.compra_actual.pop(row)
            self.actualizar_tabla_compra()
            self.mostrar_mensaje("Éxito", f"Producto '{producto}' eliminado del carrito.")

    def calcular_totales(self):
        """Calcula los totales de la venta."""
        subtotal = sum(item['total'] for item in self.compra_actual)
        impuestos = subtotal * 0.16
        total_final = subtotal + impuestos
        
        self.totales = {
            'subtotal': subtotal,
            'impuestos': impuestos,
            'total_final': total_final
        }
        
        self.lbl_total.setText(f"TOTAL: ${total_final:.2f}")
        
        puntos_venta = int(total_final)
        self.lbl_puntos_venta.setText(f"Puntos: {puntos_venta}")

    def buscar_cliente(self):
        """Busca un cliente por ID, nombre, teléfono o código."""
        termino = self.txt_cliente.text().strip()
        if not termino:
            self.limpiar_cliente()
            return
            
        try:
            cursor = self.conexion.cursor()
            query = """
                SELECT id, nombre, telefono, email, direccion, rfc, puntos, codigo_qr, saldo
                FROM clientes 
                WHERE (id = ? OR nombre LIKE ? OR telefono LIKE ? OR codigo_qr = ? OR email LIKE ?)
                AND activo = 1
                LIMIT 1
            """
            
            cursor.execute(query, (termino, f'%{termino}%', f'%{termino}%', termino, f'%{termino}%'))
            cliente = cursor.fetchone()
            
            if cliente:
                self.cliente_actual = {
                    'id': cliente[0],
                    'nombre': cliente[1],
                    'telefono': cliente[2],
                    'email': cliente[3],
                    'direccion': cliente[4],
                    'rfc': cliente[5],
                    'puntos': cliente[6],
                    'codigo_qr': cliente[7],
                    'saldo': cliente[8]
                }
                self.actualizar_info_cliente()
                self.txt_cliente.clear()
            else:
                self.limpiar_cliente()
                respuesta = QMessageBox.question(
                    self, "Cliente No Encontrado", 
                    f"¿Desea agregar '{termino}' como nuevo cliente?",
                    QMessageBox.Yes | QMessageBox.No
                )
                if respuesta == QMessageBox.Yes:
                    self.agregar_cliente_con_nombre(termino)
                
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al buscar cliente: {str(e)}", QMessageBox.Critical)

    def actualizar_info_cliente(self):
        """Actualiza la información del cliente en la interfaz."""
        if self.cliente_actual:
            self.lbl_nombre_cliente.setText(f"Nombre: {self.cliente_actual['nombre']}")
            self.lbl_telefono_cliente.setText(f"Teléfono: {self.cliente_actual['telefono'] or '-'}")
            self.lbl_email_cliente.setText(f"Email: {self.cliente_actual['email'] or '-'}")
            self.lbl_puntos_cliente.setText(f"Puntos: {self.cliente_actual['puntos']}")
        else:
            self.limpiar_cliente()

    def agregar_cliente(self):
        """Abre el diálogo para agregar un nuevo cliente."""
        dialogo = DialogoAgregarCliente(self)
        if dialogo.exec_() == QDialog.Accepted:
            cliente_data = dialogo.get_cliente_data()
            self.guardar_nuevo_cliente(cliente_data)

    def agregar_cliente_con_nombre(self, nombre: str):
        """Agrega un cliente con nombre predefinido."""
        dialogo = DialogoAgregarCliente(self)
        dialogo.txt_nombre.setText(nombre)
        if dialogo.exec_() == QDialog.Accepted:
            cliente_data = dialogo.get_cliente_data()
            self.guardar_nuevo_cliente(cliente_data)

    def guardar_nuevo_cliente(self, cliente_data: Dict[str, Any]):
        """Guarda un nuevo cliente en la base de datos."""
        try:
            cursor = self.conexion.cursor()
            
            codigo_qr = None
            if cliente_data['generar_tarjeta']:
                codigo_qr = f"CLI_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            cursor.execute("""
                INSERT INTO clientes (nombre, telefono, email, direccion, puntos, codigo_qr, activo)
                VALUES (?, ?, ?, ?, 0, ?, 1)
            """, (
                cliente_data['nombre'],
                cliente_data['telefono'],
                cliente_data['email'],
                cliente_data['direccion'],
                codigo_qr
            ))
            
            cliente_id = cursor.lastrowid
            self.conexion.commit()
            
            self.cliente_actual = {
                'id': cliente_id,
                'nombre': cliente_data['nombre'],
                'telefono': cliente_data['telefono'],
                'email': cliente_data['email'],
                'direccion': cliente_data['direccion'],
                'puntos': 0,
                'codigo_qr': codigo_qr,
                'saldo': 0.0
            }
            
            self.actualizar_info_cliente()
            self.mostrar_mensaje("Éxito", f"Cliente '{cliente_data['nombre']}' agregado correctamente.")
            
        except sqlite3.Error as e:
            self.conexion.rollback()
            self.mostrar_mensaje("Error", f"Error al guardar cliente: {str(e)}", QMessageBox.Critical)

    def limpiar_cliente(self):
        """Limpia la información del cliente actual."""
        self.cliente_actual = None
        self.lbl_nombre_cliente.setText("Nombre: Público General")
        self.lbl_telefono_cliente.setText("Teléfono: -")
        self.lbl_email_cliente.setText("Email: -")
        self.lbl_puntos_cliente.setText("Puntos: 0")
        self.txt_cliente.clear()

    def suspender_venta(self):
        """Suspende la venta actual y la guarda en espera."""
        if not self.compra_actual:
            QMessageBox.warning(self, "Advertencia", "No hay productos en el carrito para suspender.")
            return
            
        nombre_venta = ""
        if not self.cliente_actual:
            dialogo = DialogoSuspender(self)
            if dialogo.exec_() == QDialog.Accepted:
                nombre_venta = dialogo.get_nombre_venta()
            else:
                return
        else:
            nombre_venta = f"Venta - {self.cliente_actual['nombre']}"
            
        venta_id = f"venta_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.ventas_en_espera[venta_id] = {
            'nombre': nombre_venta,
            'cliente': self.cliente_actual,
            'compra': self.compra_actual.copy(),
            'totales': self.totales.copy(),
            'timestamp': datetime.now()
        }
        
        self.btn_reanudar.setText(f"▶️ Reanudar ({len(self.ventas_en_espera)})")
        
        self.mostrar_mensaje("Éxito", f"Venta '{nombre_venta}' suspendida correctamente.")
        self.cancelar_venta()

    def mostrar_ventas_espera(self):
        """Muestra diálogo para seleccionar venta en espera."""
        if not self.ventas_en_espera:
            QMessageBox.information(self, "Ventas en Espera", "No hay ventas suspendidas.")
            return
            
        ventas_lista = []
        for venta_id, venta_data in self.ventas_en_espera.items():
            ventas_lista.append(f"{venta_data['nombre']} - ${venta_data['totales']['total_final']:.2f}")
            
        item, ok = QInputDialog.getItem(
            self, "Reanudar Venta", "Seleccione la venta a reanudar:", ventas_lista, 0, False
        )
        
        if ok and item:
            for venta_id, venta_data in self.ventas_en_espera.items():
                if f"{venta_data['nombre']} - ${venta_data['totales']['total_final']:.2f}" == item:
                    self.reanudar_venta(venta_id)
                    break

    def reanudar_venta(self, venta_id: str):
        """Reanuda una venta desde la lista de espera."""
        if venta_id in self.ventas_en_espera:
            venta_data = self.ventas_en_espera.pop(venta_id)
            
            self.cancelar_venta(silent=True)
            
            self.compra_actual = venta_data['compra'].copy()
            self.cliente_actual = venta_data['cliente']
            self.totales = venta_data['totales'].copy()
            
            self.actualizar_tabla_compra()
            if self.cliente_actual:
                self.actualizar_info_cliente()
            else:
                self.limpiar_cliente()
                
            self.btn_reanudar.setText(f"▶️ Reanudar ({len(self.ventas_en_espera)})")
            
            self.mostrar_mensaje("Éxito", f"Venta '{venta_data['nombre']}' reanudada.")

    def procesar_pago(self):
        """Inicia el proceso de pago de la venta."""
        if not self.compra_actual:
            QMessageBox.warning(self, "Advertencia", "No hay productos en el carrito.")
            return
            
        dialogo = DialogoPago(self.totales['total_final'], self)
        if dialogo.exec_() == QDialog.Accepted:
            datos_pago = dialogo.get_datos_pago()
            self.finalizar_venta(datos_pago)

    def finalizar_venta(self, datos_pago: Dict[str, Any]):
        """Finaliza la venta de forma ATÓMICA usando SalesEngine (todo o nada)."""
        try:
            # Try enterprise SalesService first, fall back to legacy services.py
            try:
                from core.services.sales_service import SalesService as _SalesEngine, ItemVenta, DatosPago
                from core.services.inventory_service import StockInsuficienteError
                from services import VentaError
            except ImportError:
                from services import SalesEngine as _SalesEngine, ItemVenta, DatosPago, StockInsuficienteError, VentaError

            usuario = self.obtener_usuario_actual()
            cliente_id = self.cliente_actual['id'] if self.cliente_actual else None

            # Convertir carrito a ItemVenta
            items = [
                ItemVenta(
                    producto_id=item['id'],
                    nombre=item.get('nombre', ''),
                    cantidad=item['cantidad'],
                    precio_unitario=item['precio_unitario'],
                    unidad=item.get('unidad', 'pza'),
                )
                for item in self.compra_actual
            ]

            dp = DatosPago(
                forma_pago=datos_pago.get('forma_pago', 'Efectivo'),
                efectivo_recibido=datos_pago.get('efectivo_recibido', 0),
                cambio=datos_pago.get('cambio', 0),
                saldo_credito=datos_pago.get('saldo_credito', 0),
            )

            # Leer IVA desde configuración
            try:
                iva_row = self.conexion.execute(
                    "SELECT valor FROM configuracion WHERE clave='iva'"
                ).fetchone()
                iva_rate = float(iva_row[0]) if iva_row else 0.0
            except Exception:
                iva_rate = 0.0

            # Block 8: Credit validation
            if dp.forma_pago == 'Credito' and cliente_id:
                try:
                    from repositories.ventas import VentaRepository, CreditoInsuficienteError
                    _vrepo = VentaRepository(self.conexion)
                    if _vrepo.is_credit_enabled():
                        _vrepo.validate_credit(cliente_id, float(sum(it.precio_unitario * it.cantidad for it in items)))
                except Exception as _ce:
                    if 'LIMITE_CREDITO' in str(_ce) or 'NO_PERMITE' in str(_ce):
                        from PyQt5.QtWidgets import QMessageBox
                        QMessageBox.critical(self, 'Credito Rechazado', 'No se puede procesar a credito:\n' + str(_ce))
                        return

            engine = _SalesEngine(self.conexion, sucursal_id=self.sucursal_id)
            resultado = engine.procesar_venta(
                items=items,
                datos_pago=dp,
                usuario=usuario,
                cliente_id=cliente_id,
                iva_rate=iva_rate,
            )

            # Ticket usando el venta_id real (no random)
            self.generar_ticket(resultado.venta_id, datos_pago)

            # ── HARDWARE v9: cajón + impresora ────────────────────────────────
            self._abrir_cajon()
            self._imprimir_ticket_hardware({
                'venta_id': resultado.venta_id,
                'fecha':    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'cajero':   usuario,
                'cliente':  self.cliente_actual['nombre'] if self.cliente_actual else 'Público General',
                'items':    self.compra_actual,
                'totales':  self.totales,
                'pago':     datos_pago,
            })

            # ── FIDELIDAD Enterprise (Block 5/8) ──────────────────────────────
            puntos_ganados = 0; nivel_nuevo = ""; subio_nivel = False; _loyalty_msgs = []
            if self.cliente_actual:
                # Try LoyaltyEnterpriseEngine first
                _loyalty_ok = False
                if getattr(self, '_loyalty_engine', None):
                    try:
                        import logging as _llog
                        _total_cost = sum(float(it.get('costo_unitario',0)) * float(it.get('cantidad',1)) for it in self.compra_actual)
                        _total_rev  = float(resultado.total)
                        _margin_r   = (_total_rev - _total_cost) / _total_rev if _total_rev > 0 else 0.0
                        _earn = self._loyalty_engine.earn_points(
                            cliente_id  = self.cliente_actual['id'],
                            sale_total  = _total_rev,
                            margin_real = _margin_r,
                            branch_id   = self.sucursal_id,
                            sale_id     = resultado.venta_id,
                            items       = [{'producto_id':it.get('id'),'cantidad':it.get('cantidad',1),'subtotal':it.get('precio_unitario',0)*it.get('cantidad',1)} for it in self.compra_actual],
                        )
                        puntos_ganados = _earn.points_earned; nivel_nuevo = _earn.level_after
                        subio_nivel = _earn.level_up; _loyalty_msgs = _earn.ticket_messages or []
                        self.cliente_actual['puntos'] = _earn.points_total
                        if hasattr(self,'lbl_puntos_cliente'):
                            self.lbl_puntos_cliente.setText(f"Puntos: {_earn.points_total:,} | Nivel: {_earn.level_after}")
                        _loyalty_ok = True
                    except Exception as _le:
                        _llog.getLogger('spj.ventas').warning('loyalty earn failed: %s', _le)
                # Fallback to legacy engine
                if not _loyalty_ok and getattr(self, '_fidelidad_engine', None):
                    try:
                        fid_result = self._fidelidad_engine.procesar_post_venta(cliente_id=self.cliente_actual['id'],venta_id=resultado.venta_id,total_venta=resultado.total)
                        puntos_ganados = fid_result.puntos_ganados; nivel_nuevo = fid_result.nivel_despues
                        subio_nivel = fid_result.subio_nivel; self.cliente_actual['puntos'] = fid_result.puntos_totales
                    except Exception:
                        puntos_ganados = getattr(resultado, 'puntos_ganados', 0)

            # Engagement messages from loyalty engine
            mensaje = f"¡Venta #{resultado.folio} completada!\n\n"
            if _loyalty_msgs:
                for _lm in _loyalty_msgs[:3]:
                    mensaje += _lm + "\n"
            mensaje += f"Total: ${resultado.total:.2f}\n"
            mensaje += f"Forma de pago: {dp.forma_pago}\n"
            if dp.forma_pago == 'Efectivo':
                mensaje += f"Cambio: ${resultado.cambio:.2f}\n"
            elif dp.forma_pago == 'Crédito':
                mensaje += f"Saldo adeudado: ${dp.saldo_credito:.2f}\n"
            if self.cliente_actual and puntos_ganados > 0:
                mensaje += f"Puntos ganados: {puntos_ganados}\n"
                mensaje += f"Puntos totales: {self.cliente_actual.get('puntos', 0)}"
                if nivel_nuevo:
                    mensaje += f" | Nivel: {nivel_nuevo}"
            if subio_nivel and nivel_nuevo:
                mensaje += f"\n\n🎉 ¡Felicidades! Subiste al nivel {nivel_nuevo}"

            QMessageBox.information(self, "Venta Exitosa", mensaje)
            self.cancelar_venta(silent=True)

            if hasattr(self.main_window, 'notificar_evento'):
                self.main_window.notificar_evento('inventario_actualizado', {
                    'modulo': 'ventas', 'accion': 'venta_realizada'
                })

            # ── Hook PolloOperativoEngine: descontar stock sucursal ───────────
            self._hook_descontar_inventario_pollo(resultado)

        except StockInsuficienteError as e:
            self.mostrar_mensaje(
                "Stock Insuficiente",
                f"❌ {e}\n\nAjuste la cantidad o consulte inventario.",
                QMessageBox.Warning
            )
        except VentaError as e:
            self.mostrar_mensaje("Error en Venta", f"❌ {e}", QMessageBox.Critical)
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error inesperado: {str(e)}", QMessageBox.Critical)


    def _hook_descontar_inventario_pollo(self, resultado) -> None:
        """
        Post-venta: descuenta inventario operativo vía PolloOperativoEngine.
        Silencioso si engine no disponible o receta inexistente.
        Los errores aquí NO anulan la venta (ya procesada y registrada).
        """
        try:
            from core.services.pollo_operativo_engine import PolloOperativoEngine
            conn = self.conexion
            usuario = getattr(self, 'usuario_actual', None) or 'cajero'
            sucursal_id = getattr(self, 'sucursal_id', 1) or 1

            # Construir lista de items desde el resultado de venta
            items = []
            if hasattr(resultado, 'detalles') and resultado.detalles:
                for det in resultado.detalles:
                    pid = getattr(det, 'producto_id', None)
                    qty = getattr(det, 'cantidad', 0)
                    nombre = getattr(det, 'nombre', '')
                    if pid and qty > 0:
                        items.append({
                            'producto_id': pid,
                            'cantidad': float(qty),
                            'nombre': nombre,
                        })
            if not items:
                return

            eng = PolloOperativoEngine(conn, usuario=usuario, sucursal_id=sucursal_id)
            eng.procesar_venta(items)

        except Exception as exc:
            import logging
            logging.getLogger('spj.ventas').warning(
                "Hook inventario pollo (no crítico): %s", exc
            )

    def generar_ticket(self, venta_id: int, datos_pago: Dict[str, Any]):
        """Genera e imprime el ticket de venta."""
        try:
            ticket_data = {
                'venta_id': venta_id,
                'fecha': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'cajero': self.obtener_usuario_actual(),
                'cliente': self.cliente_actual['nombre'] if self.cliente_actual else 'Público General',
                'items': self.compra_actual,
                'totales': self.totales,
                'pago': datos_pago,
                'logo_path': LOGO_TICKET_PATH
            }
            
            if HAS_ESC_POS:
                safe_print_ticket(ticket_data)
                
            self.guardar_ticket_pdf(ticket_data)
            
        except Exception as e:
            logger.error("Error generando ticket: %s", e)

    def guardar_ticket_pdf(self, ticket_data: Dict[str, Any]):
        """Guarda el ticket como PDF en la carpeta TICKETS."""
        try:
            printer = QPrinter(QPrinter.HighResolution)
            printer.setOutputFormat(QPrinter.PdfFormat)
            
            filename = f"ticket_venta_{ticket_data['venta_id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            filepath = os.path.join(TICKETS_FOLDER, filename)
            printer.setOutputFileName(filepath)
            
            doc = QTextDocument()
            html = self.generar_html_ticket(ticket_data)
            doc.setHtml(html)
            doc.print_(printer)
            
        except Exception as e:
            logger.error("Error guardando PDF: %s", e)

    def generar_html_ticket(self, ticket_data: Dict[str, Any]) -> str:
        """Genera el contenido HTML para el ticket PDF."""
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ text-align: center; margin-bottom: 20px; }}
                .logo {{ max-width: 150px; margin-bottom: 10px; }}
                .info {{ margin-bottom: 15px; }}
                .table {{ width: 100%; border-collapse: collapse; margin-bottom: 15px; }}
                .table th, .table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                .table th {{ background-color: #f2f2f2; }}
                .totales {{ text-align: right; margin-bottom: 15px; }}
                .footer {{ text-align: center; margin-top: 20px; font-style: italic; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>TICKET DE VENTA</h2>
                <p>Venta #: {ticket_data['venta_id']}</p>
                <p>Fecha: {ticket_data['fecha']}</p>
            </div>
            
            <div class="info">
                <p><strong>Cajero:</strong> {ticket_data['cajero']}</p>
                <p><strong>Cliente:</strong> {ticket_data['cliente']}</p>
            </div>
            
            <table class="table">
                <thead>
                    <tr>
                        <th>Producto</th>
                        <th>Cantidad</th>
                        <th>Precio</th>
                        <th>Total</th>
                    </tr>
                </thead>
                <tbody>
        """
        
        for item in ticket_data['items']:
            html += f"""
                    <tr>
                        <td>{item['nombre']}</td>
                        <td>{item['cantidad']} {item['unidad']}</td>
                        <td>${item['precio_unitario']:.2f}</td>
                        <td>${item['total']:.2f}</td>
                    </tr>
            """
            
        html += f"""
                </tbody>
            </table>
            
            <div class="totales">
                <p><strong>Subtotal:</strong> ${ticket_data['totales']['subtotal']:.2f}</p>
                <p><strong>IVA (16%):</strong> ${ticket_data['totales']['impuestos']:.2f}</p>
                <p><strong>Total:</strong> ${ticket_data['totales']['total_final']:.2f}</p>
                <p><strong>Forma de pago:</strong> {ticket_data['pago']['forma_pago']}</p>
        """
        
        if ticket_data['pago']['forma_pago'] == 'Efectivo':
            html += f"""
                <p><strong>Recibido:</strong> ${ticket_data['pago']['efectivo_recibido']:.2f}</p>
                <p><strong>Cambio:</strong> ${ticket_data['pago']['cambio']:.2f}</p>
            """
        elif ticket_data['pago']['forma_pago'] == 'Crédito':
            html += f"""
                <p><strong>Saldo adeudado:</strong> ${ticket_data['pago']['saldo_credito']:.2f}</p>
            """
            
        html += """
            </div>
            
            <div class="footer">
                <p>¡Gracias por su compra!</p>
                <p>Vuelva pronto</p>
            </div>
        </body>
        </html>
        """
        
        return html

    def cancelar_venta(self, silent: bool = False):
        """Cancela la venta actual."""
        if not silent and self.compra_actual:
            respuesta = QMessageBox.question(
                self, "Confirmar Cancelación",
                "¿Está seguro de cancelar la venta actual?",
                QMessageBox.Yes | QMessageBox.No
            )
            if respuesta == QMessageBox.No:
                return
                
        self.compra_actual.clear()
        self.limpiar_seleccion_producto()
        self.limpiar_cliente()
        self.actualizar_tabla_compra()
        self.calcular_totales()
        
        if not silent:
            self.mostrar_mensaje("Información", "Venta cancelada.")

    def closeEvent(self, event):
        """Maneja el cierre del módulo."""
        self.timer_bascula.stop()
        self.timer_estabilidad.stop()
        self.desconectar_eventos_sistema()
        
        if hasattr(self, 'bascula') and self.bascula and self.bascula.is_open:
            self.bascula.close()
            
        super().closeEvent(event)

    def changeEvent(self, event):
        """Maneja cambios en el tema del sistema."""
        if event.type() == event.PaletteChange:
            self.aplicar_tema_desde_config()
        super().changeEvent(event)
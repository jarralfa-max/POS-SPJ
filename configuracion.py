# modulos/configuracion.py
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *
import sqlite3
from .base import ModuloBase
import os
import json
import bcrypt
# ── Prefer security.auth for hashing (enterprise) ─────────────────────────────
try:
    from security.auth import hash_password as _hash_password, MIN_PASSWORD_LEN
    _USE_AUTH_MODULE = True
except ImportError:
    _USE_AUTH_MODULE = False

class ModuloConfiguracion(ModuloBase):
    def __init__(self, conexion, parent=None):
        super().__init__(conexion, parent)
        self.verificar_tablas_configuracion()
        self.init_ui()

    def set_sucursal(self, sucursal_id: int, sucursal_nombre: str):
        """Recibe la sucursal activa desde MainWindow."""
        self.sucursal_id     = sucursal_id
        self.sucursal_nombre = sucursal_nombre


    def verificar_tablas_configuracion(self):
        """Verifica y crea las tablas necesarias para el módulo de configuración"""
        try:
            cursor = self.conexion.cursor()
            
            # Crear tabla de configuración de fidelidad si no existe
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS config_programa_fidelidad (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre_programa TEXT,
                    puntos_por_peso DECIMAL(10,2) DEFAULT 1.0,
                    niveles TEXT,
                    requisitos TEXT,
                    descuentos TEXT,
                    activo INTEGER DEFAULT 1,
                    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Insertar configuración por defecto si no existe
            cursor.execute('''
                INSERT OR IGNORE INTO config_programa_fidelidad 
                (id, nombre_programa, puntos_por_peso) 
                VALUES (1, 'Programa de Puntos', 1.0)
            ''')
            
            # Asegurar que existan las configuraciones básicas
            configuraciones_base = [
                ('impuesto_por_defecto', '16.0', 'Impuesto por defecto en porcentaje'),
                ('requerir_admin', 'False', 'Requerir administrador para acciones críticas'),
                ('tema', 'Claro', 'Tema de la aplicación')
            ]
            
            for clave, valor, descripcion in configuraciones_base:
                cursor.execute('''
                    INSERT OR IGNORE INTO configuracion (clave, valor, descripcion)
                    VALUES (?, ?, ?)
                ''', (clave, valor, descripcion))
            
            self.conexion.commit()
            print("✅ Tablas de configuración verificadas y creadas")
            
        except sqlite3.Error as e:
            print(f"❌ Error al verificar tablas de configuración: {e}")

    def init_ui(self):
        """Inicializa la interfaz de usuario"""
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)

        # Encabezado
        header_layout = QHBoxLayout()
        title = QLabel("Configuración del Sistema")
        title.setObjectName("tituloPrincipal")
        title.setAlignment(Qt.AlignCenter)
        font = title.font()
        font.setPointSize(16)
        font.setBold(True)
        title.setFont(font)
        header_layout.addWidget(title)
        layout.addLayout(header_layout)

        # Línea separadora
        separator = QFrame()
        separator.setFrameShape(QFrame.HLine)
        separator.setFrameShadow(QFrame.Sunken)
        layout.addWidget(separator)

        # Pestañas principales
        self.tabs_config = QTabWidget()
        
        # Crear pestañas
        self.tab_general = self.crear_tab_general()
        self.tab_usuarios = self.crear_tab_usuarios()
        self.tab_fidelizacion = self.crear_tab_fidelizacion()
        self.tab_sucursales = self.crear_tab_sucursales()
        
        self.tabs_config.addTab(self.tab_general, "⚙️ General")
        self.tabs_config.addTab(self.tab_usuarios, "👥 Usuarios")
        self.tabs_config.addTab(self.tab_fidelizacion, "🎯 Fidelización")
        self.tabs_config.addTab(self.tab_sucursales, "🏪 Sucursales")

        # ── v9: nuevas pestañas ───────────────────────────────────────────────
        self.tab_hardware = self.crear_tab_hardware()
        self.tab_ticket_designer = self.crear_tab_ticket_designer()
        self.tab_loyalty_weights = self.crear_tab_loyalty_weights()

        self.tabs_config.addTab(self.tab_hardware,         "🖨️ Hardware POS")
        self.tabs_config.addTab(self.tab_ticket_designer,  "🎟️ Diseño Tickets")
        self.tabs_config.addTab(self.tab_loyalty_weights,  "⭐ Pesos Fidelidad")

        layout.addWidget(self.tabs_config)
        self.setLayout(layout)

        # Cargar datos iniciales
        self.cargar_configuracion_general()
        self.cargar_usuarios()
        self.cargar_configuracion_fidelidad()
        self.cargar_sucursales()
        self._cargar_hardware_config()
        self._cargar_loyalty_weights()

    def crear_tab_general(self):
        """Crea la pestaña de configuración general"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(15)

        # Grupo de Apariencia
        grupo_apariencia = QGroupBox("Apariencia")
        grupo_apariencia.setStyleSheet("QGroupBox { font-weight: bold; }")
        layout_apariencia = QFormLayout()
        
        self.combo_temas = QComboBox()
        self.combo_temas.addItems(["Claro", "Oscuro", "Azul", "Verde", "Rojo"])
        self.combo_temas.setToolTip("Seleccione el tema visual de la aplicación")
        
        btn_aplicar_tema = QPushButton("Aplicar Tema")
        btn_aplicar_tema.setIcon(self.obtener_icono("refresh.png"))
        btn_aplicar_tema.clicked.connect(self.aplicar_tema)
        
        layout_apariencia.addRow("Tema de la aplicación:", self.combo_temas)
        layout_apariencia.addRow("", btn_aplicar_tema)
        grupo_apariencia.setLayout(layout_apariencia)

        # Grupo de Impuestos
        grupo_impuestos = QGroupBox("Configuración Fiscal")
        grupo_impuestos.setStyleSheet("QGroupBox { font-weight: bold; }")
        layout_impuestos = QFormLayout()
        
        self.spin_impuesto = QDoubleSpinBox()
        self.spin_impuesto.setRange(0.0, 100.0)
        self.spin_impuesto.setSuffix(" %")
        self.spin_impuesto.setDecimals(2)
        self.spin_impuesto.setToolTip("Impuesto por defecto aplicado a las ventas")
        
        btn_guardar_impuesto = QPushButton("Guardar Impuesto")
        btn_guardar_impuesto.setIcon(self.obtener_icono("save.png"))
        btn_guardar_impuesto.clicked.connect(self.guardar_impuesto)
        
        layout_impuestos.addRow("IVA por defecto:", self.spin_impuesto)
        layout_impuestos.addRow("", btn_guardar_impuesto)
        grupo_impuestos.setLayout(layout_impuestos)

        # Grupo de Seguridad
        grupo_seguridad = QGroupBox("Seguridad")
        grupo_seguridad.setStyleSheet("QGroupBox { font-weight: bold; }")
        layout_seguridad = QVBoxLayout()
        
        self.chk_requerir_admin = QCheckBox("Requerir autorización de administrador para acciones críticas")
        self.chk_requerir_admin.setToolTip("Activar para requerir permisos de administrador en operaciones sensibles")
        
        btn_guardar_seguridad = QPushButton("Guardar Configuración de Seguridad")
        btn_guardar_seguridad.setIcon(self.obtener_icono("security.png"))
        btn_guardar_seguridad.clicked.connect(self.guardar_seguridad)
        
        layout_seguridad.addWidget(self.chk_requerir_admin)
        layout_seguridad.addWidget(btn_guardar_seguridad, 0, Qt.AlignLeft)
        grupo_seguridad.setLayout(layout_seguridad)

        # Agregar grupos al layout principal
        layout.addWidget(grupo_apariencia)
        layout.addWidget(grupo_impuestos)
        layout.addWidget(grupo_seguridad)
        layout.addStretch()

        return tab

    def crear_tab_usuarios(self):
        """Crea la pestaña de gestión de usuarios"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(10)

        # Barra de herramientas
        toolbar = QHBoxLayout()
        
        self.btn_nuevo_usuario = QPushButton("Nuevo Usuario")
        self.btn_nuevo_usuario.setIcon(self.obtener_icono("add.png"))
        self.btn_nuevo_usuario.setToolTip("Crear un nuevo usuario")
        
        self.btn_editar_usuario = QPushButton("Editar Usuario")
        self.btn_editar_usuario.setIcon(self.obtener_icono("edit.png"))
        self.btn_editar_usuario.setToolTip("Editar usuario seleccionado")
        self.btn_editar_usuario.setEnabled(False)
        
        self.btn_eliminar_usuario = QPushButton("Eliminar Usuario")
        self.btn_eliminar_usuario.setIcon(self.obtener_icono("delete.png"))
        self.btn_eliminar_usuario.setToolTip("Eliminar usuario seleccionado")
        self.btn_eliminar_usuario.setEnabled(False)
        
        self.btn_actualizar = QPushButton("Actualizar")
        self.btn_actualizar.setIcon(self.obtener_icono("refresh.png"))
        self.btn_actualizar.setToolTip("Actualizar lista de usuarios")
        
        toolbar.addWidget(self.btn_nuevo_usuario)
        toolbar.addWidget(self.btn_editar_usuario)
        toolbar.addWidget(self.btn_eliminar_usuario)
        toolbar.addStretch()
        toolbar.addWidget(self.btn_actualizar)
        layout.addLayout(toolbar)

        # Tabla de usuarios
        self.tabla_usuarios = QTableWidget()
        self.tabla_usuarios.setColumnCount(7)
        self.tabla_usuarios.setHorizontalHeaderLabels([
            "ID", "Usuario", "Nombre", "Rol", "Fecha Creación", "Estado"
        ])
        
        # Configurar tabla
        self.tabla_usuarios.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_usuarios.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tabla_usuarios.setSelectionMode(QAbstractItemView.SingleSelection)
        self.tabla_usuarios.setAlternatingRowColors(True)
        header = self.tabla_usuarios.horizontalHeader()
        header.setSectionResizeMode(1, QHeaderView.Stretch)
        header.setSectionResizeMode(2, QHeaderView.Stretch)
        
        layout.addWidget(self.tabla_usuarios)

        # Conexiones
        self.btn_nuevo_usuario.clicked.connect(self.nuevo_usuario)
        self.btn_editar_usuario.clicked.connect(self.editar_usuario)
        self.btn_eliminar_usuario.clicked.connect(self.eliminar_usuario)
        self.btn_actualizar.clicked.connect(self.cargar_usuarios)
        self.tabla_usuarios.itemSelectionChanged.connect(self.actualizar_botones_usuarios)

        return tab

    def crear_tab_fidelizacion(self):
        """Crea la pestaña de configuración de fidelización"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(15)

        # Información del programa actual
        grupo_info = QGroupBox("Información del Programa Actual")
        grupo_info.setStyleSheet("QGroupBox { font-weight: bold; }")
        layout_info = QVBoxLayout()
        
        self.lbl_info_programa = QLabel()
        self.lbl_info_programa.setWordWrap(True)
        self.lbl_info_programa.setStyleSheet("""
            QLabel {
                background-color: #f0f0f0;
                border: 1px solid #ccc;
                border-radius: 5px;
                padding: 10px;
                margin: 5px;
            }
        """)
        layout_info.addWidget(self.lbl_info_programa)
        grupo_info.setLayout(layout_info)

        # Configuración del programa
        grupo_config = QGroupBox("Configuración del Programa de Fidelidad")
        grupo_config.setStyleSheet("QGroupBox { font-weight: bold; }")
        layout_config = QFormLayout()
        layout_config.setLabelAlignment(Qt.AlignRight)
        
        self.edit_nombre_programa = QLineEdit()
        self.edit_nombre_programa.setPlaceholderText("Ej: Programa de Puntos MiTienda")
        self.edit_nombre_programa.setToolTip("Nombre del programa de fidelidad")
        
        self.spin_puntos_por_peso = QDoubleSpinBox()
        self.spin_puntos_por_peso.setRange(0.01, 100.0)
        self.spin_puntos_por_peso.setValue(1.0)
        self.spin_puntos_por_peso.setSuffix(" puntos por $")
        self.spin_puntos_por_peso.setToolTip("Puntos ganados por cada peso gastado")
        
        self.edit_niveles = QLineEdit()
        self.edit_niveles.setPlaceholderText("Ej: Bronce,Plata,Oro,Diamante")
        self.edit_niveles.setToolTip("Niveles del programa separados por comas")
        
        self.edit_requisitos = QLineEdit()
        self.edit_requisitos.setPlaceholderText("Ej: 0,1000,5000,10000")
        self.edit_requisitos.setToolTip("Puntos requeridos para cada nivel")
        
        self.edit_descuentos = QLineEdit()
        self.edit_descuentos.setPlaceholderText("Ej: 0,5,10,15")
        self.edit_descuentos.setToolTip("Porcentaje de descuento para cada nivel")
        
        btn_guardar_fidelidad = QPushButton("💾 Guardar Configuración de Fidelidad")
        btn_guardar_fidelidad.setIcon(self.obtener_icono("save.png"))
        btn_guardar_fidelidad.clicked.connect(self.guardar_configuracion_fidelidad)
        
        layout_config.addRow("Nombre del Programa:", self.edit_nombre_programa)
        layout_config.addRow("Puntos por $ gastado:", self.spin_puntos_por_peso)
        layout_config.addRow("Niveles:", self.edit_niveles)
        layout_config.addRow("Requisitos (puntos):", self.edit_requisitos)
        layout_config.addRow("Descuentos (%):", self.edit_descuentos)
        layout_config.addRow("", btn_guardar_fidelidad)
        grupo_config.setLayout(layout_config)

        # Agregar grupos al layout
        layout.addWidget(grupo_info)
        layout.addWidget(grupo_config)
        layout.addStretch()

        return tab
    
    def _actualizar_suma_pesos(self):
        suma = (self.spin_peso_frecuencia.value() + self.spin_peso_volumen.value() +
                self.spin_peso_margen.value() + self.spin_peso_comunidad.value())
        color = "red" if suma != 100 else "green"
        self.lbl_suma_pesos.setText(
            f"<span style='color:{color}'>Suma: {suma}% "
            f"{'✓' if suma == 100 else '⚠ Debe ser 100%'}</span>"
        )

    
    # ── v9: Tab Diseño Tickets ───────────────────────────────────────────────

    def crear_tab_ticket_designer(self):
        """
        Diseñador visual de tickets y etiquetas.
        Muestra lista de elementos con drag-drop de posición.
        Vista previa simulada.
        """
        from PyQt5.QtWidgets import (
            QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
            QPushButton, QListWidget, QTextEdit, QLabel,
            QComboBox, QGroupBox
        )
        from PyQt5.QtCore import Qt
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(10, 10, 10, 10)

        # Tipo de diseño
        top = QHBoxLayout()
        top.addWidget(QLabel("Tipo:"))
        self.combo_design_tipo = QComboBox()
        self.combo_design_tipo.addItems(["ticket", "etiqueta"])
        self.combo_design_tipo.currentTextChanged.connect(self._cargar_diseno_ticket)
        top.addWidget(self.combo_design_tipo)
        top.addStretch()
        btn_guardar_d = QPushButton("💾 Guardar Diseño")
        btn_guardar_d.clicked.connect(self._guardar_diseno_ticket)
        top.addWidget(btn_guardar_d)
        layout.addLayout(top)

        splitter = QSplitter(Qt.Horizontal)

        # Panel izquierdo: elementos disponibles + lista actual
        panel_izq = QWidget()
        lay_izq = QVBoxLayout(panel_izq)

        grp_elem = QGroupBox("Elementos disponibles")
        lay_elem = QVBoxLayout(grp_elem)
        self.lista_elementos_disponibles = QListWidget()
        elementos = [
            "header_empresa", "header_sucursal", "fecha", "folio", "cajero",
            "tabla_items", "totales", "forma_pago", "puntos_cliente",
            "separador", "codigo_qr", "codigo_barras", "logo",
            "texto_personalizado", "footer",
        ]
        self.lista_elementos_disponibles.addItems(elementos)
        lay_elem.addWidget(self.lista_elementos_disponibles)
        btn_agregar_e = QPushButton("➕ Agregar al diseño")
        btn_agregar_e.clicked.connect(self._agregar_elemento_diseno)
        lay_elem.addWidget(btn_agregar_e)
        lay_izq.addWidget(grp_elem)

        grp_actual = QGroupBox("Elementos en diseño (orden = posición)")
        lay_act = QVBoxLayout(grp_actual)
        self.lista_diseno_actual = QListWidget()
        self.lista_diseno_actual.setDragDropMode(QListWidget.InternalMove)
        lay_act.addWidget(self.lista_diseno_actual)
        btn_quitar_e = QPushButton("✖ Quitar seleccionado")
        btn_quitar_e.clicked.connect(self._quitar_elemento_diseno)
        lay_act.addWidget(btn_quitar_e)
        lay_izq.addWidget(grp_actual)

        splitter.addWidget(panel_izq)

        # Panel derecho: vista previa JSON
        panel_der = QWidget()
        lay_der = QVBoxLayout(panel_der)
        lay_der.addWidget(QLabel("Vista previa (JSON elementos):"))
        self.txt_preview_diseno = QTextEdit()
        self.txt_preview_diseno.setReadOnly(True)
        self.txt_preview_diseno.setMaximumHeight(300)
        lay_der.addWidget(self.txt_preview_diseno)

        lay_der.addWidget(QLabel("Variables disponibles:"))
        variables_info = QTextEdit()
        variables_info.setReadOnly(True)
        variables_info.setMaximumHeight(150)
        variables_info.setPlainText(
            "empresa = Nombre del negocio\n"
            "sucursal = Nombre sucursal\n"
            "fecha = Fecha/hora de venta\n"
            "folio = Número de folio\n"
            "cajero = Usuario cajero\n"
            "cliente = Nombre del cliente\n"
            "footer = Mensaje pie de ticket\n"
        )
        lay_der.addWidget(variables_info)
        splitter.addWidget(panel_der)

        layout.addWidget(splitter)

        # Cargar diseño actual
        self._cargar_diseno_ticket()
        return tab

    def _cargar_diseno_ticket(self):
        """Carga elementos del diseño activo para el tipo seleccionado."""
        try:
            import json
            tipo = self.combo_design_tipo.currentText() if hasattr(self, 'combo_design_tipo') else "ticket"
            row = self.conexion.execute(
                "SELECT elementos FROM ticket_design_config WHERE tipo=? AND activo=1 LIMIT 1",
                (tipo,)
            ).fetchone()
            if row:
                elementos = json.loads(row[0])
                self.lista_diseno_actual.clear()
                for elem in elementos:
                    label = elem.get("id", elem.get("tipo", "elemento"))
                    self.lista_diseno_actual.addItem(label)
                self.txt_preview_diseno.setPlainText(
                    json.dumps(elementos, indent=2, ensure_ascii=False)
                )
        except Exception:
            pass
        
    def _guardar_diseno_ticket(self):
        try:
            import json
            from PyQt5.QtWidgets import QMessageBox
            tipo = self.combo_design_tipo.currentText()
            elementos = []
            for i in range(self.lista_diseno_actual.count()):
                eid = self.lista_diseno_actual.item(i).text()
                elementos.append({"id": eid, "tipo": eid, "y_pos": i})
            elementos_json = json.dumps(elementos, ensure_ascii=False)
            self.conexion.execute(
                """
                UPDATE ticket_design_config
                SET elementos=?, activo=1
                WHERE tipo=? AND nombre='Default'
                """,
                (elementos_json, tipo)
            )
            if self.conexion.execute(
                "SELECT changes()"
            ).fetchone()[0] == 0:
                self.conexion.execute(
                    "INSERT OR REPLACE INTO ticket_design_config (tipo, nombre, elementos, activo) "
                    "VALUES (?,?,?,1)",
                    (tipo, "Default", elementos_json)
                )
            self.conexion.commit()
            QMessageBox.information(self, "Diseño", f"Diseño de {tipo} guardado.")
        except Exception as exc:
            from PyQt5.QtWidgets import QMessageBox
            QMessageBox.critical(self, "Error", str(exc))
            
    def _agregar_elemento_diseno(self):
        item = self.lista_elementos_disponibles.currentItem()
        if item:
            self.lista_diseno_actual.addItem(item.text())
            self._actualizar_preview_diseno()

    def _quitar_elemento_diseno(self):
        fila = self.lista_diseno_actual.currentRow()
        if fila >= 0:
            self.lista_diseno_actual.takeItem(fila)
            self._actualizar_preview_diseno()

    def _actualizar_preview_diseno(self):
        import json
        elementos = []
        for i in range(self.lista_diseno_actual.count()):
            eid = self.lista_diseno_actual.item(i).text()
            elementos.append({"id": eid, "tipo": eid, "y_pos": i})
        self.txt_preview_diseno.setPlainText(
            json.dumps(elementos, indent=2, ensure_ascii=False)
        )
        
    # ── v9: Tab Hardware POS ─────────────────────────────────────────────────

    def crear_tab_hardware(self):
        """
        Configuración de hardware:
        - Impresora térmica (tipo, puerto, ancho)
        - Cajón (método, señal)
        - Scanner (debounce, longitud mínima)
        - Báscula (puerto serial, baud)
        """
        from PyQt5.QtWidgets import (
            QWidget, QVBoxLayout, QFormLayout, QGroupBox,
            QComboBox, QLineEdit, QSpinBox, QCheckBox, QHBoxLayout, QPushButton
        )
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(14)
        layout.setContentsMargins(14, 14, 14, 14)

        # Impresora
        grp_imp = QGroupBox("🖨️ Impresora Térmica")
        form_imp = QFormLayout(grp_imp)
        self.chk_imp_habilitada = QCheckBox("Habilitada")
        self.combo_imp_tipo = QComboBox()
        self.combo_imp_tipo.addItems(["escpos_usb", "escpos_serial", "win32print", "simulado"])
        self.txt_imp_puerto = QLineEdit()
        self.txt_imp_puerto.setPlaceholderText("USB / COM3")
        self.spin_imp_ancho = QSpinBox()
        self.spin_imp_ancho.setRange(48, 120)
        self.spin_imp_ancho.setValue(80)
        self.spin_imp_ancho.setSuffix(" mm")
        form_imp.addRow("Estado:", self.chk_imp_habilitada)
        form_imp.addRow("Tipo:", self.combo_imp_tipo)
        form_imp.addRow("Puerto:", self.txt_imp_puerto)
        form_imp.addRow("Ancho papel:", self.spin_imp_ancho)
        layout.addWidget(grp_imp)

        # Cajón
        grp_caj = QGroupBox("🗃️ Cajón de Dinero")
        form_caj = QFormLayout(grp_caj)
        self.chk_caj_habilitado = QCheckBox("Habilitado")
        self.combo_caj_metodo = QComboBox()
        self.combo_caj_metodo.addItems(["escpos", "serial", "parallel"])
        self.combo_caj_pin = QComboBox()
        self.combo_caj_pin.addItems(["kick1", "kick2"])
        form_caj.addRow("Estado:", self.chk_caj_habilitado)
        form_caj.addRow("Método:", self.combo_caj_metodo)
        form_caj.addRow("Pin de activación:", self.combo_caj_pin)
        layout.addWidget(grp_caj)

        # Scanner
        grp_scan = QGroupBox("🔍 Lector de Código de Barras")
        form_scan = QFormLayout(grp_scan)
        self.chk_scan_habilitado = QCheckBox("Habilitado")
        self.spin_scan_debounce = QSpinBox()
        self.spin_scan_debounce.setRange(20, 500)
        self.spin_scan_debounce.setValue(80)
        self.spin_scan_debounce.setSuffix(" ms")
        self.spin_scan_minlen = QSpinBox()
        self.spin_scan_minlen.setRange(1, 20)
        self.spin_scan_minlen.setValue(3)
        form_scan.addRow("Estado:", self.chk_scan_habilitado)
        form_scan.addRow("Debounce:", self.spin_scan_debounce)
        form_scan.addRow("Longitud mínima:", self.spin_scan_minlen)
        layout.addWidget(grp_scan)

        # Báscula
        grp_bas = QGroupBox("⚖️ Báscula Serial")
        form_bas = QFormLayout(grp_bas)
        self.chk_bas_habilitada = QCheckBox("Habilitada")
        self.txt_bas_puerto = QLineEdit()
        self.txt_bas_puerto.setPlaceholderText("COM3")
        self.spin_bas_baud = QSpinBox()
        self.spin_bas_baud.setRange(1200, 115200)
        self.spin_bas_baud.setValue(9600)
        form_bas.addRow("Estado:", self.chk_bas_habilitada)
        form_bas.addRow("Puerto:", self.txt_bas_puerto)
        form_bas.addRow("Baud rate:", self.spin_bas_baud)
        layout.addWidget(grp_bas)

        # Botón guardar
        btn_hw = QPushButton("💾 Guardar Configuración Hardware")
        btn_hw.clicked.connect(self._guardar_hardware_config)
        layout.addWidget(btn_hw)
        layout.addStretch()
        return tab

    def _cargar_hardware_config(self):
        """Carga valores desde hardware_config en la UI."""
        try:
            import json
            rows = self.conexion.execute(
                "SELECT tipo, habilitado, configuracion FROM hardware_config"
            ).fetchall()
            for tipo, hab, cfg_json in rows:
                cfg = json.loads(cfg_json) if cfg_json else {}
                if tipo == "impresora":
                    self.chk_imp_habilitada.setChecked(bool(hab))
                    idx = self.combo_imp_tipo.findText(cfg.get("tipo", "escpos_usb"))
                    if idx >= 0: self.combo_imp_tipo.setCurrentIndex(idx)
                    self.txt_imp_puerto.setText(cfg.get("puerto", "USB"))
                    self.spin_imp_ancho.setValue(int(cfg.get("ancho_mm", 80)))
                elif tipo == "cajon":
                    self.chk_caj_habilitado.setChecked(bool(hab))
                    idx = self.combo_caj_metodo.findText(cfg.get("metodo", "escpos"))
                    if idx >= 0: self.combo_caj_metodo.setCurrentIndex(idx)
                    idx_p = self.combo_caj_pin.findText(cfg.get("pin", "kick1"))
                    if idx_p >= 0: self.combo_caj_pin.setCurrentIndex(idx_p)
                elif tipo == "scanner":
                    self.chk_scan_habilitado.setChecked(bool(hab))
                    self.spin_scan_debounce.setValue(int(cfg.get("debounce_ms", 80)))
                    self.spin_scan_minlen.setValue(int(cfg.get("min_len", 3)))
                elif tipo == "bascula":
                    self.chk_bas_habilitada.setChecked(bool(hab))
                    self.txt_bas_puerto.setText(cfg.get("puerto", "COM3"))
                    self.spin_bas_baud.setValue(int(cfg.get("baud", 9600)))
        except Exception:
            pass  # tabla no migrada aún
        
    def _guardar_hardware_config(self):
        """Guarda configuración de hardware en hardware_config."""
        try:
            import json
            from PyQt5.QtWidgets import QMessageBox
            config_map = {
                "impresora": (
                    1 if self.chk_imp_habilitada.isChecked() else 0,
                    json.dumps({
                        "tipo":     self.combo_imp_tipo.currentText(),
                        "puerto":   self.txt_imp_puerto.text().strip() or "USB",
                        "ancho_mm": self.spin_imp_ancho.value(),
                    })
                ),
                "cajon": (
                    1 if self.chk_caj_habilitado.isChecked() else 0,
                    json.dumps({
                        "metodo": self.combo_caj_metodo.currentText(),
                        "pin":    self.combo_caj_pin.currentText(),
                    })
                ),
                "scanner": (
                    1 if self.chk_scan_habilitado.isChecked() else 0,
                    json.dumps({
                        "debounce_ms": self.spin_scan_debounce.value(),
                        "min_len":     self.spin_scan_minlen.value(),
                    })
                ),
                "bascula": (
                    1 if self.chk_bas_habilitada.isChecked() else 0,
                    json.dumps({
                        "puerto": self.txt_bas_puerto.text().strip() or "COM3",
                        "baud":   self.spin_bas_baud.value(),
                    })
                ),
            }
            for tipo, (hab, cfg) in config_map.items():
                self.conexion.execute(
                    """
                    UPDATE hardware_config
                    SET habilitado=?, configuracion=?, actualizado_en=datetime('now')
                    WHERE tipo=?
                    """,
                    (hab, cfg, tipo)
                )
            self.conexion.commit()
            QMessageBox.information(self, "Hardware",
                "Configuración de hardware guardada correctamente.")
        except Exception as exc:
            from PyQt5.QtWidgets import QMessageBox
            QMessageBox.critical(self, "Error", str(exc))

    
    def crear_tab_loyalty_weights(self):
        """
        Configuración de pesos de scoring multivariable:
        frecuencia, volumen, margen, comunidad.
        Umbrales de nivel: Plata, Oro, Platino.
        """
        from PyQt5.QtWidgets import (
            QWidget, QVBoxLayout, QFormLayout, QGroupBox,
            QSpinBox, QDoubleSpinBox, QPushButton, QLabel
        )
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(12)
        layout.setContentsMargins(14, 14, 14, 14)

        # Pesos dimensiones
        grp_pesos = QGroupBox("Pesos de Scoring (deben sumar 100)")
        form_p = QFormLayout(grp_pesos)

        self.spin_peso_frecuencia = QSpinBox()
        self.spin_peso_frecuencia.setRange(0, 100)
        self.spin_peso_frecuencia.setSuffix(" %")
        self.spin_peso_frecuencia.setValue(30)

        self.spin_peso_volumen = QSpinBox()
        self.spin_peso_volumen.setRange(0, 100)
        self.spin_peso_volumen.setSuffix(" %")
        self.spin_peso_volumen.setValue(30)

        self.spin_peso_margen = QSpinBox()
        self.spin_peso_margen.setRange(0, 100)
        self.spin_peso_margen.setSuffix(" %")
        self.spin_peso_margen.setValue(30)

        self.spin_peso_comunidad = QSpinBox()
        self.spin_peso_comunidad.setRange(0, 100)
        self.spin_peso_comunidad.setSuffix(" %")
        self.spin_peso_comunidad.setValue(10)

        form_p.addRow("Frecuencia de visitas:", self.spin_peso_frecuencia)
        form_p.addRow("Volumen de compra:", self.spin_peso_volumen)
        form_p.addRow("Margen generado:", self.spin_peso_margen)
        form_p.addRow("Comunidad/Referidos:", self.spin_peso_comunidad)

        self.lbl_suma_pesos = QLabel("Suma: 100%")
        form_p.addRow("", self.lbl_suma_pesos)
        for sp in (self.spin_peso_frecuencia, self.spin_peso_volumen,
                   self.spin_peso_margen, self.spin_peso_comunidad):
            sp.valueChanged.connect(self._actualizar_suma_pesos)
        layout.addWidget(grp_pesos)

        # Umbrales de nivel
        grp_umbrales = QGroupBox("Umbrales de Nivel")
        form_u = QFormLayout(grp_umbrales)
        self.spin_umbral_plata   = QDoubleSpinBox()
        self.spin_umbral_oro     = QDoubleSpinBox()
        self.spin_umbral_platino = QDoubleSpinBox()
        for sp in (self.spin_umbral_plata, self.spin_umbral_oro, self.spin_umbral_platino):
            sp.setRange(0, 100)
            sp.setDecimals(1)
        self.spin_umbral_plata.setValue(40)
        self.spin_umbral_oro.setValue(65)
        self.spin_umbral_platino.setValue(85)
        form_u.addRow("Plata (score ≥):", self.spin_umbral_plata)
        form_u.addRow("Oro (score ≥):", self.spin_umbral_oro)
        form_u.addRow("Platino (score ≥):", self.spin_umbral_platino)
        layout.addWidget(grp_umbrales)

        # Parámetros adicionales
        grp_extra = QGroupBox("Parámetros Adicionales")
        form_e = QFormLayout(grp_extra)
        self.spin_periodo_dias = QSpinBox()
        self.spin_periodo_dias.setRange(7, 365)
        self.spin_periodo_dias.setValue(90)
        self.spin_periodo_dias.setSuffix(" días")
        self.spin_puntos_por_peso = QDoubleSpinBox()
        self.spin_puntos_por_peso.setRange(0.01, 100)
        self.spin_puntos_por_peso.setValue(1.0)
        self.spin_puntos_por_peso.setDecimals(2)
        self.spin_bonus_referido = QSpinBox()
        self.spin_bonus_referido.setRange(0, 5000)
        self.spin_bonus_referido.setValue(50)
        form_e.addRow("Período análisis:", self.spin_periodo_dias)
        form_e.addRow("Puntos por $1 gastado:", self.spin_puntos_por_peso)
        form_e.addRow("Bono por referido:", self.spin_bonus_referido)
        layout.addWidget(grp_extra)

        btn_guardar_lw = QPushButton("💾 Guardar Configuración Fidelidad")
        btn_guardar_lw.clicked.connect(self._guardar_loyalty_weights)
        layout.addWidget(btn_guardar_lw)
        layout.addStretch()
        return tab
    
    def _cargar_loyalty_weights(self):
        """Carga valores de loyalty_config en los spinboxes."""
        try:
            rows = self.conexion.execute(
                "SELECT clave, valor FROM loyalty_config"
            ).fetchall()
            cfg = {r[0]: r[1] for r in rows}
            self.spin_peso_frecuencia.setValue(int(cfg.get("peso_frecuencia", 30)))
            self.spin_peso_volumen.setValue(int(cfg.get("peso_volumen", 30)))
            self.spin_peso_margen.setValue(int(cfg.get("peso_margen", 30)))
            self.spin_peso_comunidad.setValue(int(cfg.get("peso_comunidad", 10)))
            self.spin_umbral_plata.setValue(float(cfg.get("umbral_plata", 40)))
            self.spin_umbral_oro.setValue(float(cfg.get("umbral_oro", 65)))
            self.spin_umbral_platino.setValue(float(cfg.get("umbral_platino", 85)))
            self.spin_periodo_dias.setValue(int(cfg.get("periodo_dias", 90)))
            self.spin_puntos_por_peso.setValue(float(cfg.get("puntos_por_peso", 1.0)))
            self.spin_bonus_referido.setValue(int(cfg.get("bonus_referido", 50)))
            self._actualizar_suma_pesos()
        except Exception:
            pass

    def _guardar_loyalty_weights(self):
        """Persiste pesos y umbrales en loyalty_config."""
        try:
            from PyQt5.QtWidgets import QMessageBox
            suma = (self.spin_peso_frecuencia.value() + self.spin_peso_volumen.value() +
                    self.spin_peso_margen.value() + self.spin_peso_comunidad.value())
            if suma != 100:
                QMessageBox.warning(self, "Pesos inválidos",
                    f"Los pesos deben sumar 100%. Suma actual: {suma}%")
                return
            updates = [
                ("peso_frecuencia",  str(self.spin_peso_frecuencia.value())),
                ("peso_volumen",     str(self.spin_peso_volumen.value())),
                ("peso_margen",      str(self.spin_peso_margen.value())),
                ("peso_comunidad",   str(self.spin_peso_comunidad.value())),
                ("umbral_plata",     str(self.spin_umbral_plata.value())),
                ("umbral_oro",       str(self.spin_umbral_oro.value())),
                ("umbral_platino",   str(self.spin_umbral_platino.value())),
                ("periodo_dias",     str(self.spin_periodo_dias.value())),
                ("puntos_por_peso",  str(self.spin_puntos_por_peso.value())),
                ("bonus_referido",   str(self.spin_bonus_referido.value())),
            ]
            for clave, valor in updates:
                self.conexion.execute(
                    "INSERT OR REPLACE INTO loyalty_config (clave, valor) VALUES (?,?)",
                    (clave, valor)
                )
            self.conexion.commit()
            QMessageBox.information(self, "Fidelidad",
                "Configuración de fidelidad guardada correctamente.")
        except Exception as exc:
            from PyQt5.QtWidgets import QMessageBox
            QMessageBox.critical(self, "Error", str(exc))

    # === MÉTODOS DE CONFIGURACIÓN GENERAL ===
    def cargar_configuracion_general(self):
        """Carga la configuración general desde la base de datos"""
        try:
            cursor = self.conexion.cursor()
            
            # Cargar tema
            cursor.execute("SELECT valor FROM configuracion WHERE clave = 'tema'")
            resultado = cursor.fetchone()
            if resultado:
                tema = resultado[0]
                index = self.combo_temas.findText(tema, Qt.MatchFixedString)
                if index >= 0:
                    self.combo_temas.setCurrentIndex(index)
            
            # Cargar impuesto
            cursor.execute("SELECT valor FROM configuracion WHERE clave = 'impuesto_por_defecto'")
            resultado = cursor.fetchone()
            if resultado:
                self.spin_impuesto.setValue(float(resultado[0]))
            else:
                self.spin_impuesto.setValue(16.0)
            
            # Cargar seguridad
            cursor.execute("SELECT valor FROM configuracion WHERE clave = 'requerir_admin'")
            resultado = cursor.fetchone()
            if resultado:
                self.chk_requerir_admin.setChecked(resultado[0].lower() == 'true')
                
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al cargar configuración general: {str(e)}", QMessageBox.Critical)

    def aplicar_tema(self):
        """Aplica el tema seleccionado"""
        tema_seleccionado = self.combo_temas.currentText()
        try:
            cursor = self.conexion.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO configuracion (clave, valor, descripcion) VALUES (?, ?, ?)",
                ('tema', tema_seleccionado, 'Tema de la aplicación')
            )
            self.conexion.commit()
            
            # Notificar a la ventana principal
            if hasattr(self.main_window, 'aplicar_tema'):
                self.main_window.aplicar_tema(tema_seleccionado)
                
            self.mostrar_mensaje("Éxito", f"Tema '{tema_seleccionado}' aplicado correctamente.\nReinicie la aplicación para ver todos los cambios.")
            
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al aplicar tema: {str(e)}", QMessageBox.Critical)

    def guardar_impuesto(self):
        """Guarda la configuración de impuesto"""
        impuesto = self.spin_impuesto.value()
        try:
            cursor = self.conexion.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO configuracion (clave, valor, descripcion) VALUES (?, ?, ?)",
                ('impuesto_por_defecto', str(impuesto), 'Impuesto por defecto en porcentaje')
            )
            self.conexion.commit()
            self.mostrar_mensaje("Éxito", f"Impuesto por defecto guardado: {impuesto}%")
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al guardar impuesto: {str(e)}", QMessageBox.Critical)

    def guardar_seguridad(self):
        """Guarda la configuración de seguridad"""
        requerir_admin = "True" if self.chk_requerir_admin.isChecked() else "False"
        try:
            cursor = self.conexion.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO configuracion (clave, valor, descripcion) VALUES (?, ?, ?)",
                ('requerir_admin', requerir_admin, 'Requerir administrador para acciones críticas')
            )
            self.conexion.commit()
            estado = "activada" if self.chk_requerir_admin.isChecked() else "desactivada"
            self.mostrar_mensaje("Éxito", f"Configuración de seguridad {estado} correctamente.")
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al guardar configuración de seguridad: {str(e)}", QMessageBox.Critical)

    # === MÉTODOS DE GESTIÓN DE USUARIOS ===
    def cargar_usuarios(self):
        """Carga la lista de usuarios en la tabla"""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("""
                SELECT id, usuario, nombre, rol, fecha_creacion, activo
                FROM usuarios
                ORDER BY usuario
            """)
            usuarios = cursor.fetchall()

            self.tabla_usuarios.setRowCount(len(usuarios))
            for fila, usuario in enumerate(usuarios):
                for columna, valor in enumerate(usuario):
                    item = QTableWidgetItem(str(valor) if valor is not None else "")
                    
                    # Marcar estado activo/inactivo
                    if columna == 5:  # Columna de estado
                        item.setText("Activo" if valor == 1 else "Inactivo")
                        item.setForeground(QColor("green") if valor == 1 else QColor("red"))
                    
                    self.tabla_usuarios.setItem(fila, columna, item)

            # Ajustar columnas
            self.tabla_usuarios.resizeColumnsToContents()
            
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al cargar usuarios: {str(e)}", QMessageBox.Critical)

    def nuevo_usuario(self):
        """Abre diálogo para crear nuevo usuario"""
        dialogo = DialogoUsuario(self.conexion, self)
        if dialogo.exec_() == QDialog.Accepted:
            self.cargar_usuarios()
            self.registrar_actualizacion("usuario_creado", {"accion": "nuevo_usuario"})

    def editar_usuario(self):
        """Abre diálogo para editar usuario seleccionado"""
        fila = self.tabla_usuarios.currentRow()
        if fila < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un usuario para editar.")
            return

        try:
            id_usuario = int(self.tabla_usuarios.item(fila, 0).text())
            cursor = self.conexion.cursor()
            cursor.execute("SELECT * FROM usuarios WHERE id = ?", (id_usuario,))
            usuario_data = cursor.fetchone()
            
            if usuario_data:
                columnas = [desc[0] for desc in cursor.description]
                usuario_dict = dict(zip(columnas, usuario_data))
                
                dialogo = DialogoUsuario(self.conexion, self, usuario_dict)
                if dialogo.exec_() == QDialog.Accepted:
                    self.cargar_usuarios()
                    self.registrar_actualizacion("usuario_editado", {"usuario_id": id_usuario})
                    
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error al editar usuario: {str(e)}", QMessageBox.Critical)

    def eliminar_usuario(self):
        """Elimina el usuario seleccionado"""
        fila = self.tabla_usuarios.currentRow()
        if fila < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un usuario para eliminar.")
            return

        try:
            id_usuario = int(self.tabla_usuarios.item(fila, 0).text())
            nombre_usuario = self.tabla_usuarios.item(fila, 1).text()
            
            # Prevenir eliminación del admin
            if nombre_usuario.lower() == 'admin':
                self.mostrar_mensaje("Error", "No se puede eliminar el usuario administrador principal.")
                return
            
            respuesta = QMessageBox.question(
                self,
                "Confirmar Eliminación",
                f"¿Está seguro que desea eliminar al usuario '{nombre_usuario}'?\n\nEsta acción no se puede deshacer.",
                QMessageBox.Yes | QMessageBox.No,
                QMessageBox.No
            )
            
            if respuesta == QMessageBox.Yes:
                cursor = self.conexion.cursor()
                cursor.execute("DELETE FROM usuarios WHERE id = ?", (id_usuario,))
                self.conexion.commit()
                self.mostrar_mensaje("Éxito", f"Usuario '{nombre_usuario}' eliminado correctamente.")
                self.cargar_usuarios()
                self.registrar_actualizacion("usuario_eliminado", {"usuario_id": id_usuario, "nombre": nombre_usuario})
                
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error al eliminar usuario: {str(e)}", QMessageBox.Critical)

    def actualizar_botones_usuarios(self):
        """Actualiza el estado de los botones según la selección"""
        seleccionado = self.tabla_usuarios.currentRow() >= 0
        self.btn_editar_usuario.setEnabled(seleccionado)
        self.btn_eliminar_usuario.setEnabled(seleccionado)

    # === MÉTODOS DE FIDELIZACIÓN ===
    def cargar_configuracion_fidelidad(self):
        """Carga la configuración del programa de fidelidad"""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT * FROM config_programa_fidelidad WHERE id = 1")
            config = cursor.fetchone()
            
            if config:
                nombre = config[1] or "Sin nombre"
                puntos = config[2] or 0
                
                texto_info = f"""
                <b>Programa:</b> {nombre}<br>
                <b>Puntos por $ gastado:</b> {puntos}<br>
                <b>Estado:</b> <span style='color: green'>Activo</span>
                """
                
                if config[3]:  # Niveles
                    niveles = config[3].split(',')
                    texto_info += f"<br><b>Niveles:</b> {', '.join(niveles)}"
                
                self.lbl_info_programa.setText(texto_info)
                self.edit_nombre_programa.setText(nombre)
                self.spin_puntos_por_peso.setValue(float(puntos))
                self.edit_niveles.setText(config[3] or "")
                self.edit_requisitos.setText(config[4] or "")
                self.edit_descuentos.setText(config[5] or "")
            else:
                self.lbl_info_programa.setText("<b>Programa no configurado</b><br>Configure los parámetros del programa de fidelidad.")
                
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al cargar configuración de fidelidad: {str(e)}", QMessageBox.Critical)

    def guardar_configuracion_fidelidad(self):
        """Guarda la configuración del programa de fidelidad"""
        try:
            nombre_programa = self.edit_nombre_programa.text().strip()
            if not nombre_programa:
                self.mostrar_mensaje("Advertencia", "El nombre del programa es obligatorio.")
                return

            cursor = self.conexion.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO config_programa_fidelidad 
                (id, nombre_programa, puntos_por_peso, niveles, requisitos, descuentos, activo)
                VALUES (1, ?, ?, ?, ?, ?, 1)
            """, (
                nombre_programa,
                self.spin_puntos_por_peso.value(),
                self.edit_niveles.text().strip() or None,
                self.edit_requisitos.text().strip() or None,
                self.edit_descuentos.text().strip() or None
            ))
            
            self.conexion.commit()
            self.mostrar_mensaje("Éxito", "Configuración de fidelidad guardada correctamente.")
            self.cargar_configuracion_fidelidad()
            self.registrar_actualizacion("config_fidelidad_actualizada", {"programa": nombre_programa})
            
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al guardar configuración de fidelidad: {str(e)}", QMessageBox.Critical)

    def actualizar_datos(self):
        """Actualiza todos los datos del módulo"""
        self.cargar_configuracion_general()
        self.cargar_usuarios()
        self.cargar_configuracion_fidelidad()
        self.cargar_sucursales()

    # =========================================================================
    # PESTAÑA DE SUCURSALES
    # =========================================================================
    def crear_tab_sucursales(self):
        """Crea la pestaña de gestión de sucursales."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(10)

        # Toolbar
        toolbar = QHBoxLayout()
        btn_nueva = QPushButton("➕ Nueva Sucursal")
        btn_editar = QPushButton("✏️ Editar")
        btn_eliminar = QPushButton("🗑️ Eliminar")
        self.btn_editar_suc   = btn_editar
        self.btn_eliminar_suc = btn_eliminar
        btn_editar.setEnabled(False)
        btn_eliminar.setEnabled(False)

        btn_nueva.clicked.connect(self.nueva_sucursal)
        btn_editar.clicked.connect(self.editar_sucursal)
        btn_eliminar.clicked.connect(self.eliminar_sucursal)

        toolbar.addWidget(btn_nueva)
        toolbar.addWidget(btn_editar)
        toolbar.addWidget(btn_eliminar)
        toolbar.addStretch()
        layout.addLayout(toolbar)

        # Tabla
        self.tabla_sucursales = QTableWidget()
        self.tabla_sucursales.setColumnCount(5)
        self.tabla_sucursales.setHorizontalHeaderLabels(
            ["ID", "Nombre", "Dirección", "Teléfono", "Estado"]
        )
        self.tabla_sucursales.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_sucursales.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tabla_sucursales.setSelectionMode(QAbstractItemView.SingleSelection)
        self.tabla_sucursales.setAlternatingRowColors(True)
        hdr = self.tabla_sucursales.horizontalHeader()
        hdr.setSectionResizeMode(0, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(1, QHeaderView.Stretch)
        hdr.setSectionResizeMode(2, QHeaderView.Stretch)
        hdr.setSectionResizeMode(3, QHeaderView.ResizeToContents)
        hdr.setSectionResizeMode(4, QHeaderView.ResizeToContents)
        self.tabla_sucursales.itemSelectionChanged.connect(self._actualizar_botones_suc)
        layout.addWidget(self.tabla_sucursales)

        # Info
        info = QLabel("💡 Los cajeros solo verán sus ventas. El administrador puede ver todas las sucursales en Reportes.")
        info.setWordWrap(True)
        info.setStyleSheet("color: #666; font-style: italic; padding: 4px;")
        layout.addWidget(info)

        return tab

    def cargar_sucursales(self):
        """Carga la tabla de sucursales."""
        if not hasattr(self, "tabla_sucursales"):
            return
        try:
            rows = self.conexion.execute(
                "SELECT id, nombre, direccion, telefono, activa FROM sucursales ORDER BY id"
            ).fetchall()
            self.tabla_sucursales.setRowCount(len(rows))
            for i, (sid, nombre, direccion, telefono, activa) in enumerate(rows):
                self.tabla_sucursales.setItem(i, 0, QTableWidgetItem(str(sid)))
                self.tabla_sucursales.setItem(i, 1, QTableWidgetItem(nombre or ""))
                self.tabla_sucursales.setItem(i, 2, QTableWidgetItem(direccion or ""))
                self.tabla_sucursales.setItem(i, 3, QTableWidgetItem(telefono or ""))
                estado_item = QTableWidgetItem("✅ Activa" if activa else "❌ Inactiva")
                estado_item.setForeground(QColor("#27ae60") if activa else QColor("#c0392b"))
                self.tabla_sucursales.setItem(i, 4, estado_item)
        except Exception as e:
            print(f"Error cargando sucursales: {e}")

    def _actualizar_botones_suc(self):
        seleccionado = self.tabla_sucursales.currentRow() >= 0
        self.btn_editar_suc.setEnabled(seleccionado)
        self.btn_eliminar_suc.setEnabled(seleccionado)

    def nueva_sucursal(self):
        dlg = DialogoSucursalEdit(self.conexion, parent=self)
        if dlg.exec_() == QDialog.Accepted:
            self.cargar_sucursales()
            QMessageBox.information(self, "Éxito", "Sucursal creada correctamente.")

    def editar_sucursal(self):
        fila = self.tabla_sucursales.currentRow()
        if fila < 0:
            return
        sid = int(self.tabla_sucursales.item(fila, 0).text())
        row = self.conexion.execute(
            "SELECT id, nombre, direccion, telefono, activa FROM sucursales WHERE id=?", (sid,)
        ).fetchone()
        if not row:
            return
        data = dict(zip(["id", "nombre", "direccion", "telefono", "activa"], row))
        dlg = DialogoSucursalEdit(self.conexion, sucursal_data=data, parent=self)
        if dlg.exec_() == QDialog.Accepted:
            self.cargar_sucursales()
            QMessageBox.information(self, "Éxito", "Sucursal actualizada.")

    def eliminar_sucursal(self):
        fila = self.tabla_sucursales.currentRow()
        if fila < 0:
            return
        sid  = int(self.tabla_sucursales.item(fila, 0).text())
        nombre = self.tabla_sucursales.item(fila, 1).text()
        if sid == 1:
            QMessageBox.warning(self, "No permitido", "No se puede eliminar la sucursal Principal.")
            return
        resp = QMessageBox.question(
            self, "Confirmar",
            f"¿Eliminar la sucursal «{nombre}»?\n\nLas ventas y usuarios asociados quedarán sin sucursal.",
            QMessageBox.Yes | QMessageBox.No
        )
        if resp == QMessageBox.Yes:
            try:
                self.conexion.execute("DELETE FROM sucursales WHERE id=?", (sid,))
                self.conexion.commit()
                self.cargar_sucursales()
            except Exception as e:
                QMessageBox.critical(self, "Error", str(e))

    def closeEvent(self, event):
        """Maneja el cierre del módulo"""
        self.registrar_actualizacion("modulo_cerrado", {"modulo": "configuracion"})
        super().closeEvent(event)


class DialogoUsuario(QDialog):
    def __init__(self, conexion, parent=None, usuario_data=None):
        super().__init__(parent)
        self.conexion = conexion
        self.usuario_data = usuario_data
        self.es_edicion = usuario_data is not None
        
        self.setWindowTitle("Editar Usuario" if self.es_edicion else "Nuevo Usuario")
        self.setFixedSize(500, 500)
        self.setModal(True)
        
        self.init_ui()
        if self.es_edicion:
                self.setWindowTitle("Editar Usuario")
                self.cargar_datos()
        else:
            self.setWindowTitle("Crear Nuevo Usuario")

    def init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(15)

        # Título
        titulo = QLabel("Editar Usuario" if self.es_edicion else "Crear Nuevo Usuario")
        titulo.setAlignment(Qt.AlignCenter)
        font = titulo.font()
        font.setPointSize(14)
        font.setBold(True)
        titulo.setFont(font)
        layout.addWidget(titulo)

        # Formulario
        form_layout = QFormLayout()
        form_layout.setLabelAlignment(Qt.AlignRight)
        form_layout.setVerticalSpacing(10)
        
        self.edit_usuario = QLineEdit()
        self.edit_usuario.setPlaceholderText("Nombre de usuario único")
        
        self.edit_nombre = QLineEdit()
        self.edit_nombre.setPlaceholderText("Nombre completo del usuario")
        
        self.edit_contrasena = QLineEdit()
        self.edit_contrasena.setEchoMode(QLineEdit.Password)
        self.edit_contrasena.setPlaceholderText("Mínimo 6 caracteres" if not self.es_edicion else "Dejar en blanco para no cambiar")
        
        self.edit_confirmar = QLineEdit()
        self.edit_confirmar.setEchoMode(QLineEdit.Password)
        self.edit_confirmar.setPlaceholderText("Confirmar contraseña")
        
        self.combo_rol = QComboBox()
        self.combo_rol.addItems(["admin", "cajero", "vendedor", "inventario"])

        # Sucursal asignada
        self.combo_sucursal_usuario = QComboBox()
        self._cargar_sucursales_combo()

        # Módulos permitidos
        grupo_modulos = QGroupBox("Módulos Permitidos")
        layout_modulos = QGridLayout()
        self.modulos_checkboxes = {}
        
        modulos = [
            ("ventas", "Ventas"),
            ("clientes", "Clientes"), 
            ("productos", "Productos"),
            ("inventario", "Inventario"),
            ("compras", "Compras"),
            ("gastos", "Gastos"),
            ("reportes", "Reportes"),
            ("configuracion", "Configuración")
        ]
        
        for i, (clave, nombre) in enumerate(modulos):
            chk = QCheckBox(nombre)
            self.modulos_checkboxes[clave] = chk
            layout_modulos.addWidget(chk, i // 2, i % 2)
        
        grupo_modulos.setLayout(layout_modulos)

        form_layout.addRow("Usuario*:", self.edit_usuario)
        form_layout.addRow("Nombre completo:", self.edit_nombre)
        form_layout.addRow("Contraseña*:", self.edit_contrasena)
        form_layout.addRow("Confirmar*:", self.edit_confirmar)
        form_layout.addRow("Rol*:", self.combo_rol)
        form_layout.addRow("Sucursal:", self.combo_sucursal_usuario)
        form_layout.addRow("Permisos:", grupo_modulos)
        
        layout.addLayout(form_layout)

        # Botones
        btn_layout = QHBoxLayout()
        self.btn_guardar = QPushButton("💾 Guardar")
        self.btn_guardar.setDefault(True)
        self.btn_cancelar = QPushButton("❌ Cancelar")
        
        btn_layout.addWidget(self.btn_guardar)
        btn_layout.addWidget(self.btn_cancelar)
        layout.addLayout(btn_layout)

        # Conexiones
        self.btn_guardar.clicked.connect(self.guardar_usuario)
        self.btn_cancelar.clicked.connect(self.reject)

    def _cargar_sucursales_combo(self):
        """Carga las sucursales en el combo del formulario de usuario."""
        self.combo_sucursal_usuario.clear()
        try:
            sucursales = self.conexion.execute(
                "SELECT id, nombre FROM sucursales WHERE activa=1 ORDER BY id"
            ).fetchall()
            for sid, nombre in sucursales:
                self.combo_sucursal_usuario.addItem(f"🏪 {nombre}", sid)
        except Exception:
            self.combo_sucursal_usuario.addItem("🏪 Principal", 1)

    def cargar_datos_usuario(self):
        """Carga los datos del usuario en el formulario"""
        if not self.usuario_data:
            return
            
        self.edit_usuario.setText(self.usuario_data.get('usuario', ''))
        self.edit_nombre.setText(self.usuario_data.get('nombre', ''))
        self.combo_rol.setCurrentText(self.usuario_data.get('rol', 'vendedor'))

        # Cargar sucursal
        suc_id = self.usuario_data.get('sucursal_id', 1) or 1
        for i in range(self.combo_sucursal_usuario.count()):
            if self.combo_sucursal_usuario.itemData(i) == suc_id:
                self.combo_sucursal_usuario.setCurrentIndex(i)
                break
        
        # Cargar módulos permitidos
        modulos_permitidos = self.usuario_data.get('modulos_permitidos', '')
        if modulos_permitidos:
            for modulo in modulos_permitidos.split(','):
                modulo = modulo.strip()
                if modulo in self.modulos_checkboxes:
                    self.modulos_checkboxes[modulo].setChecked(True)

    def validar_formulario(self):
        """Valida los datos del formulario"""
        usuario = self.edit_usuario.text().strip()
        contrasena = self.edit_contrasena.text()
        confirmar = self.edit_confirmar.text()
        
        if not usuario:
            QMessageBox.warning(self, "Error", "El nombre de usuario es obligatorio.")
            return False
            
        if not self.es_edicion and not contrasena:
            QMessageBox.warning(self, "Error", "La contraseña es obligatoria para nuevos usuarios.")
            return False
            
        if contrasena and len(contrasena) < 6:
            QMessageBox.warning(self, "Error", "La contraseña debe tener al menos 6 caracteres.")
            return False
            
        if contrasena and contrasena != confirmar:
            QMessageBox.warning(self, "Error", "Las contraseñas no coinciden.")
            return False
            
        return True

    def guardar_usuario(self):
            usuario = self.txt_usuario.text().strip()
            nombre = self.txt_nombre.text().strip()
            contrasena = self.txt_contrasena.text() # NO strip() por si el espacio es intencional, pero se hashea
            rol = self.cmb_rol.currentText()
            modulos = [self.lst_modulos.item(i).text() for i in range(self.lst_modulos.count()) if self.lst_modulos.item(i).checkState() == Qt.Checked]
            modulos_str = json.dumps(modulos)
            
            if not usuario or not nombre or not rol:
                QMessageBox.warning(self, "Advertencia", "Todos los campos obligatorios deben ser llenados.")
                return

            if not self.es_edicion and not contrasena:
                QMessageBox.warning(self, "Advertencia", "Se debe ingresar una contraseña para el nuevo usuario.")
                return

            try:
                cursor = self.conexion.cursor()
                
                # --- Lógica de Hashing de Contraseña ---
                hashed_password = None
                if contrasena:
                    # Aplicar bcrypt para generar un hash seguro de la contraseña
                    if _USE_AUTH_MODULE:
                        try:
                            hashed_password = _hash_password(contrasena)
                        except Exception:
                            hashed_password = bcrypt.hashpw(contrasena.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                    else:
                        hashed_password = bcrypt.hashpw(contrasena.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                
                if self.es_edicion:
                    id_usuario = self.usuario_data['id']
                    
                    # Prepara la lista de campos a actualizar
                    sucursal_id_val = self.combo_sucursal_usuario.currentData() or 1
                    update_fields = ['usuario', 'nombre', 'rol', 'modulos_permitidos', 'sucursal_id']
                    update_values = [usuario, nombre, rol, modulos_str, sucursal_id_val]


                    # Si se proporcionó una nueva contraseña, añádela a la actualización
                    if hashed_password:
                        update_fields.append('contrasena')
                        update_values.append(hashed_password)
                    
                    # Construye la consulta de actualización de forma dinámica
                    sets = ', '.join([f"{f} = ?" for f in update_fields])
                    update_values.append(id_usuario)

                    cursor.execute(f"UPDATE usuarios SET {sets} WHERE id=?", update_values)
                    
                else:
                    # Nuevo usuario - verificar que no exista
                    cursor.execute("SELECT id FROM usuarios WHERE usuario = ?", (usuario,))
                    if cursor.fetchone():
                        QMessageBox.warning(self, "Error", "Ya existe un usuario con ese nombre.")
                        return
                    
                    # USAR EL HASH EN LA INSERCIÓN
                    sucursal_id_val = self.combo_sucursal_usuario.currentData() or 1
                    cursor.execute("""
                        INSERT INTO usuarios (usuario, nombre, contrasena, rol, modulos_permitidos, sucursal_id, fecha_creacion, activo)
                        VALUES (?, ?, ?, ?, ?, ?, datetime('now'), 1)
                    """, (usuario, nombre, hashed_password, rol, modulos_str, sucursal_id_val))
                
                self.conexion.commit()
                QMessageBox.information(self, "Éxito", 
                                      "Usuario guardado correctamente." if self.es_edicion 
                                      else "Usuario creado correctamente.")
                self.accept()
                
            except sqlite3.Error as e:
                self.conexion.rollback()
                QMessageBox.critical(self, "Error", f"Error en base de datos: {str(e)}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error inesperado: {str(e)}")

# =============================================================================
# DIÁLOGO PARA CREAR / EDITAR SUCURSAL
# =============================================================================
class DialogoSucursalEdit(QDialog):
    """Formulario para crear o editar una sucursal."""

    def __init__(self, conexion, sucursal_data=None, parent=None):
        super().__init__(parent)
        self.conexion       = conexion
        self.sucursal_data  = sucursal_data
        self.es_edicion     = sucursal_data is not None
        self.setWindowTitle("Editar Sucursal" if self.es_edicion else "Nueva Sucursal")
        self.setModal(True)
        self.setFixedSize(440, 300)
        self._init_ui()
        if self.es_edicion:
            self._cargar_datos()

    def _init_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        layout.setContentsMargins(20, 20, 20, 20)

        form = QFormLayout()
        form.setVerticalSpacing(10)

        self.txt_nombre    = QLineEdit()
        self.txt_nombre.setPlaceholderText("Ej: Sucursal Norte")
        self.txt_direccion = QLineEdit()
        self.txt_direccion.setPlaceholderText("Calle, colonia, ciudad")
        self.txt_telefono  = QLineEdit()
        self.txt_telefono.setPlaceholderText("10 dígitos")
        self.chk_activa    = QCheckBox("Sucursal activa")
        self.chk_activa.setChecked(True)

        form.addRow("Nombre*:",    self.txt_nombre)
        form.addRow("Dirección:", self.txt_direccion)
        form.addRow("Teléfono:",  self.txt_telefono)
        form.addRow("",            self.chk_activa)
        layout.addLayout(form)
        layout.addStretch()

        btns = QHBoxLayout()
        btn_guardar  = QPushButton("💾 Guardar")
        btn_cancelar = QPushButton("❌ Cancelar")
        btn_guardar.setMinimumHeight(34)
        btn_cancelar.setMinimumHeight(34)
        btn_guardar.clicked.connect(self._guardar)
        btn_cancelar.clicked.connect(self.reject)
        btns.addWidget(btn_guardar)
        btns.addWidget(btn_cancelar)
        layout.addLayout(btns)

    def _cargar_datos(self):
        self.txt_nombre.setText(self.sucursal_data.get("nombre", ""))
        self.txt_direccion.setText(self.sucursal_data.get("direccion", "") or "")
        self.txt_telefono.setText(self.sucursal_data.get("telefono", "") or "")
        self.chk_activa.setChecked(bool(self.sucursal_data.get("activa", 1)))

    def _guardar(self):
        nombre = self.txt_nombre.text().strip()
        if not nombre:
            QMessageBox.warning(self, "Error", "El nombre de la sucursal es obligatorio.")
            return
        direccion = self.txt_direccion.text().strip() or None
        telefono  = self.txt_telefono.text().strip() or None
        activa    = 1 if self.chk_activa.isChecked() else 0
        try:
            if self.es_edicion:
                self.conexion.execute(
                    "UPDATE sucursales SET nombre=?, direccion=?, telefono=?, activa=? WHERE id=?",
                    (nombre, direccion, telefono, activa, self.sucursal_data["id"])
                )
            else:
                self.conexion.execute(
                    "INSERT INTO sucursales (nombre, direccion, telefono, activa) VALUES (?,?,?,?)",
                    (nombre, direccion, telefono, activa)
                )
            self.conexion.commit()
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Error", str(e))


    

# modulos/inventario.py - VERSIÓN COMPLETAMENTE CORREGIDA
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *
import sqlite3
from .base import ModuloBase
import os
from datetime import datetime, date

class ModuloInventario(ModuloBase):
    def __init__(self, conexion, parent=None):
        super().__init__(conexion, parent)
        self.main_window     = parent
        self.usuario_actual  = "admin"
        self.sucursal_id     = 1
        self.sucursal_nombre = "Principal"
        self.init_ui()
        self.conectar_eventos()
        self.verificar_y_reparar_tablas()
        
    def set_sucursal(self, sucursal_id: int, sucursal_nombre: str):
        """Recibe la sucursal activa desde MainWindow."""
        self.sucursal_id     = sucursal_id
        self.sucursal_nombre = sucursal_nombre

    def set_usuario_actual(self, usuario, rol):
        """Establece el usuario actual para el módulo"""
        self.usuario_actual = usuario
        self.rol_usuario = rol
        
    def obtener_usuario_actual(self):
        """Obtiene el usuario actual para registrar en movimientos"""
        return self.usuario_actual if self.usuario_actual else "Sistema"

    def verificar_y_reparar_tablas(self):
        """Verifica y repara todas las tablas necesarias"""
        try:
            cursor = self.conexion.cursor()
            
            # Verificar y crear tabla movimientos_inventario con estructura completa
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS movimientos_inventario (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    producto_id INTEGER NOT NULL,
                    tipo_movimiento TEXT NOT NULL,
                    cantidad REAL NOT NULL,
                    costo_unitario REAL DEFAULT 0,
                    costo_total REAL DEFAULT 0,
                    fecha TEXT NOT NULL,
                    usuario TEXT NOT NULL DEFAULT 'Sistema',
                    descripcion TEXT,
                    referencia TEXT,
                    existencia_anterior REAL,
                    existencia_nueva REAL
                )
            """)
            
            # Verificar y agregar columnas faltantes en movimientos_inventario
            cursor.execute("PRAGMA table_info(movimientos_inventario)")
            columnas_existentes = [col[1] for col in cursor.fetchall()]
            
            columnas_por_agregar = {
                'usuario': 'TEXT NOT NULL DEFAULT "Sistema"',
                'costo_unitario': 'REAL DEFAULT 0',
                'costo_total': 'REAL DEFAULT 0',
                'descripcion': 'TEXT',
                'referencia': 'TEXT',
                'existencia_anterior': 'REAL',
                'existencia_nueva': 'REAL'
            }
            
            for columna, tipo in columnas_por_agregar.items():
                if columna not in columnas_existentes:
                    try:
                        cursor.execute(f"ALTER TABLE movimientos_inventario ADD COLUMN {columna} {tipo}")
                        print(f"✅ Columna {columna} agregada a movimientos_inventario")
                    except sqlite3.Error as e:
                        print(f"❌ Error al agregar columna {columna}: {e}")
            
            # Verificar tabla compras_pollo
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS compras_pollo (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fecha TEXT NOT NULL,
                    numero_pollos INTEGER NOT NULL,
                    kilos_totales REAL NOT NULL,
                    costo_total REAL NOT NULL,
                    costo_kilo REAL NOT NULL,
                    proveedor TEXT,
                    estado TEXT DEFAULT 'PENDIENTE',
                    metodo_pago TEXT,
                    descripcion TEXT,
                    usuario TEXT,
                    fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Verificar tabla transferencias_inventario
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transferencias_inventario (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    producto_origen_id INTEGER NOT NULL,
                    producto_destino_id INTEGER,
                    cantidad REAL NOT NULL,
                    usuario_origen TEXT NOT NULL,
                    usuario_destino TEXT NOT NULL,
                    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
                    observaciones TEXT,
                    usuario_registro TEXT
                )
            """)
            
            # Verificar tabla compras_generales
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS compras_generales (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    producto_id INTEGER NOT NULL,
                    fecha DATE NOT NULL,
                    cantidad REAL NOT NULL,
                    costo_unitario REAL NOT NULL,
                    costo_total REAL NOT NULL,
                    proveedor TEXT,
                    estado TEXT DEFAULT 'PENDIENTE',
                    metodo_pago TEXT,
                    descripcion TEXT,
                    usuario TEXT,
                    fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            self.conexion.commit()
            print("✅ Todas las tablas de inventario verificadas y corregidas")
            
        except sqlite3.Error as e:
            print(f"❌ Error al verificar tablas: {e}")
            self.conexion.rollback()

    def init_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)

        # --- Pestañas ---
        self.tab_widget = QTabWidget()
        
        # Pestaña de Inventario General
        self.tab_general = QWidget()
        self.init_tab_general()
        self.tab_widget.addTab(self.tab_general, "Inventario General")
        
        # Pestaña de Inventario de Pollo
        self.tab_pollo = QWidget()
        self.init_tab_pollo()
        self.tab_widget.addTab(self.tab_pollo, "Inventario de Pollo")
        
        # Pestaña de Transferencias
        self.tab_transferencias = QWidget()
        self.init_tab_transferencias()
        self.tab_widget.addTab(self.tab_transferencias, "Transferencias")

        layout.addWidget(self.tab_widget)
        self.setLayout(layout)

    def init_tab_general(self):
        """Inicializa la pestaña de inventario general"""
        layout = QVBoxLayout(self.tab_general)

        # --- Barra de herramientas ---
        toolbar = QHBoxLayout()
        
        self.busqueda_general = QLineEdit()
        self.busqueda_general.setPlaceholderText("Buscar por nombre, categoría...")
        self.btn_buscar_general = QPushButton()
        self.btn_buscar_general.setIcon(self.obtener_icono("search.png"))
        self.btn_buscar_general.setToolTip("Buscar Producto")
        
        self.combo_categoria = QComboBox()
        self.combo_categoria.addItem("Todas las categorías")
        self.cargar_categorias()
        
        self.btn_actualizar = QPushButton("Actualizar")
        self.btn_actualizar.setIcon(self.obtener_icono("refresh.png"))
        
        toolbar.addWidget(QLabel("Buscar:"))
        toolbar.addWidget(self.busqueda_general)
        toolbar.addWidget(self.btn_buscar_general)
        toolbar.addSpacing(20)
        toolbar.addWidget(QLabel("Categoría:"))
        toolbar.addWidget(self.combo_categoria)
        toolbar.addStretch()
        toolbar.addWidget(self.btn_actualizar)
        layout.addLayout(toolbar)

        # --- Tabla de Inventario General ---
        self.tabla_general = QTableWidget()
        self.tabla_general.setColumnCount(7)
        self.tabla_general.setHorizontalHeaderLabels([
            "ID", "Nombre", "Precio", "Existencia", "Stock Mín", "Unidad", "Categoría"
        ])
        self.tabla_general.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_general.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tabla_general.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.tabla_general)

        # --- Botones de acción ---
        acciones_layout = QHBoxLayout()
        self.btn_ajustar_existencia = QPushButton("Ajustar Existencia")
        self.btn_ajustar_existencia.setIcon(self.obtener_icono("edit.png"))
        self.btn_ajustar_existencia.setEnabled(False)
        
        self.btn_ver_movimientos = QPushButton("Ver Movimientos")
        self.btn_ver_movimientos.setIcon(self.obtener_icono("list.png"))
        self.btn_ver_movimientos.setEnabled(False)
        
        self.btn_agregar_compra = QPushButton("Agregar Compra")
        self.btn_agregar_compra.setIcon(self.obtener_icono("add.png"))
        self.btn_agregar_compra.setEnabled(False)
        
        acciones_layout.addWidget(self.btn_ajustar_existencia)
        acciones_layout.addWidget(self.btn_ver_movimientos)
        acciones_layout.addWidget(self.btn_agregar_compra)
        acciones_layout.addStretch()
        layout.addLayout(acciones_layout)

        # --- Conexiones ---
        self.btn_buscar_general.clicked.connect(self.buscar_productos_general)
        self.btn_actualizar.clicked.connect(self.cargar_inventario_general)
        self.combo_categoria.currentTextChanged.connect(self.cargar_inventario_general)
        self.btn_ajustar_existencia.clicked.connect(self.ajustar_existencia)
        self.btn_ver_movimientos.clicked.connect(self.ver_movimientos)
        self.btn_agregar_compra.clicked.connect(self.agregar_compra_general)
        self.tabla_general.itemSelectionChanged.connect(self.actualizar_botones_general)

        # Cargar datos iniciales
        self.cargar_inventario_general()

    def init_tab_pollo(self):
        """Inicializa la pestaña de inventario de pollo"""
        layout = QVBoxLayout(self.tab_pollo)

        # --- Sección de nueva compra de pollo ---
        group_compra = QGroupBox("Nueva Compra de Pollo")
        form_layout = QFormLayout()

        self.date_compra_pollo = QDateEdit()
        self.date_compra_pollo.setDate(QDate.currentDate())
        self.date_compra_pollo.setDisplayFormat("dd/MM/yyyy")
        self.date_compra_pollo.setCalendarPopup(True)

        self.spin_numero_pollos = QSpinBox()
        self.spin_numero_pollos.setRange(1, 1000)
        self.spin_numero_pollos.setValue(1)

        self.spin_kilos_totales = QDoubleSpinBox()
        self.spin_kilos_totales.setRange(0.1, 10000.0)
        self.spin_kilos_totales.setDecimals(2)
        self.spin_kilos_totales.setSuffix(" kg")

        self.spin_costo_total = QDoubleSpinBox()
        self.spin_costo_total.setRange(0.01, 1000000.0)
        self.spin_costo_total.setPrefix("$ ")
        self.spin_costo_total.setDecimals(2)

        self.combo_proveedor_pollo = QComboBox()
        self.combo_proveedor_pollo.setEditable(True)
        self.cargar_proveedores_pollo()

        self.combo_estado_pollo = QComboBox()
        self.combo_estado_pollo.addItems(["PENDIENTE", "PAGADO", "PARCIAL"])

        self.combo_metodo_pago_pollo = QComboBox()
        self.combo_metodo_pago_pollo.addItems(["Efectivo", "Tarjeta", "Transferencia", "Crédito"])

        self.edit_descripcion_pollo = QTextEdit()
        self.edit_descripcion_pollo.setMaximumHeight(60)
        self.edit_descripcion_pollo.setPlaceholderText("Descripción de la compra...")

        self.lbl_costo_kilo = QLabel("$0.00 por kg")
        self.lbl_costo_kilo.setStyleSheet("font-weight: bold; color: #2E86AB;")

        # Estado de configuración
        self.lbl_estado_configuracion = QLabel("")
        self.lbl_estado_configuracion.setStyleSheet("font-weight: bold; padding: 5px;")
        form_layout.addRow("Estado:", self.lbl_estado_configuracion)

        form_layout.addRow("Fecha:", self.date_compra_pollo)
        form_layout.addRow("Número de Pollos:", self.spin_numero_pollos)
        form_layout.addRow("Kilos Totales:", self.spin_kilos_totales)
        form_layout.addRow("Costo Total:", self.spin_costo_total)
        form_layout.addRow("Costo por Kilo:", self.lbl_costo_kilo)
        form_layout.addRow("Proveedor:", self.combo_proveedor_pollo)
        form_layout.addRow("Estado:", self.combo_estado_pollo)
        form_layout.addRow("Método de Pago:", self.combo_metodo_pago_pollo)
        form_layout.addRow("Descripción:", self.edit_descripcion_pollo)

        # Botones de acción
        buttons_layout = QHBoxLayout()
        self.btn_ver_subproductos = QPushButton("Ver Subproductos Calculados")
        self.btn_ver_subproductos.setIcon(self.obtener_icono("list.png"))
        self.btn_ver_subproductos.clicked.connect(self.mostrar_subproductos_calculados)
        
        btn_registrar = QPushButton("Registrar Compra")
        btn_registrar.setIcon(self.obtener_icono("add.png"))
        btn_registrar.clicked.connect(self.registrar_compra_pollo)

        buttons_layout.addWidget(self.btn_ver_subproductos)
        buttons_layout.addStretch()
        buttons_layout.addWidget(btn_registrar)

        form_layout.addRow(buttons_layout)
        group_compra.setLayout(form_layout)
        layout.addWidget(group_compra)

        # --- Botón para ver historial ---
        btn_historial_layout = QHBoxLayout()
        self.btn_ver_historial = QPushButton("Ver Historial de Compras de Pollo")
        self.btn_ver_historial.setIcon(self.obtener_icono("history.png"))
        self.btn_ver_historial.clicked.connect(self.mostrar_historial_pollo)
        btn_historial_layout.addWidget(self.btn_ver_historial)
        btn_historial_layout.addStretch()
        layout.addLayout(btn_historial_layout)

        # --- Conexiones ---
        self.spin_kilos_totales.valueChanged.connect(self.calcular_costo_kilo)
        self.spin_costo_total.valueChanged.connect(self.calcular_costo_kilo)

        # Verificar configuración inicial
        self.verificar_configuracion_rendimiento()

    def init_tab_transferencias(self):
        """Inicializa la pestaña de transferencias"""
        layout = QVBoxLayout(self.tab_transferencias)

        # --- Formulario de transferencia ---
        group_transferencia = QGroupBox("Nueva Transferencia")
        form_layout = QFormLayout()

        self.combo_producto_origen = QComboBox()
        self.cargar_productos_con_existencia()

        self.spin_cantidad_transferencia = QDoubleSpinBox()
        self.spin_cantidad_transferencia.setRange(0.01, 10000.0)
        self.spin_cantidad_transferencia.setDecimals(2)

        self.edit_usuario_origen = QLineEdit()
        self.edit_usuario_origen.setText(self.usuario_actual)
        self.edit_usuario_origen.setReadOnly(True)

        self.edit_usuario_destino = QLineEdit()
        self.edit_usuario_destino.setPlaceholderText("Ingrese el usuario destino")

        self.edit_observaciones_transferencia = QTextEdit()
        self.edit_observaciones_transferencia.setMaximumHeight(80)
        self.edit_observaciones_transferencia.setPlaceholderText("Observaciones opcionales...")

        form_layout.addRow("Producto Origen:", self.combo_producto_origen)
        form_layout.addRow("Cantidad:", self.spin_cantidad_transferencia)
        form_layout.addRow("Usuario Origen:", self.edit_usuario_origen)
        form_layout.addRow("Usuario Destino:", self.edit_usuario_destino)
        form_layout.addRow("Observaciones:", self.edit_observaciones_transferencia)

        btn_transferir = QPushButton("Realizar Transferencia")
        btn_transferir.setIcon(self.obtener_icono("transfer.png"))
        btn_transferir.clicked.connect(self.realizar_transferencia)

        form_layout.addRow(btn_transferir)
        group_transferencia.setLayout(form_layout)
        layout.addWidget(group_transferencia)

        # --- Historial de transferencias ---
        group_historial_transferencias = QGroupBox("Historial de Transferencias")
        historial_layout = QVBoxLayout()

        self.tabla_transferencias = QTableWidget()
        self.tabla_transferencias.setColumnCount(7)
        self.tabla_transferencias.setHorizontalHeaderLabels([
            "ID", "Fecha", "Producto", "Cantidad", "Origen", "Destino", "Observaciones"
        ])
        self.tabla_transferencias.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_transferencias.horizontalHeader().setStretchLastSection(True)

        historial_layout.addWidget(self.tabla_transferencias)
        group_historial_transferencias.setLayout(historial_layout)
        layout.addWidget(group_historial_transferencias)

        # Cargar datos iniciales
        self.cargar_historial_transferencias()

    # === FUNCIONES PARA RENDIMIENTOS ===
    def verificar_configuracion_rendimiento(self):
        """Verifica si existe configuración de rendimiento y actualiza el estado"""
        try:
            rendimientos = self.obtener_configuracion_rendimiento_pollo()
            
            if rendimientos:
                total_porcentaje = sum(comp['porcentaje'] for comp in rendimientos.values())
                self.lbl_estado_configuracion.setText(
                    f"✅ Configuración encontrada: {len(rendimientos)} subproductos "
                    f"(Total: {total_porcentaje:.1f}%)"
                )
                self.lbl_estado_configuracion.setStyleSheet(
                    "color: green; font-weight: bold; background-color: #e8f5e8; padding: 5px; border: 1px solid green;"
                )
                return True
            else:
                self.lbl_estado_configuracion.setText(
                    "❌ No hay configuración de rendimiento. Configure los productos compuestos primero."
                )
                self.lbl_estado_configuracion.setStyleSheet(
                    "color: red; font-weight: bold; background-color: #ffe8e8; padding: 5px; border: 1px solid red;"
                )
                return False
                
        except Exception as e:
            self.lbl_estado_configuracion.setText(f"❌ Error al verificar configuración: {str(e)}")
            self.lbl_estado_configuracion.setStyleSheet(
                "color: red; font-weight: bold; background-color: #ffe8e8; padding: 5px; border: 1px solid red;"
            )
            return False

    def obtener_configuracion_rendimiento_pollo(self):
        """Obtiene la configuración REAL de rendimiento desde productos compuestos"""
        try:
            cursor = self.conexion.cursor()
            
            # Buscar TODOS los productos compuestos que podrían ser pollo
            cursor.execute("""
                SELECT id, nombre FROM productos 
                WHERE es_compuesto = 1 AND oculto = 0
                ORDER BY nombre
            """)
            
            productos_compuestos = cursor.fetchall()
            
            if not productos_compuestos:
                return None
            
            # Buscar en todos los productos compuestos
            for producto_id, producto_nombre in productos_compuestos:
                # Obtener los componentes del producto compuesto desde composicion_productos
                cursor.execute("""
                    SELECT p.id, p.nombre, p.unidad, cp.porcentaje
                    FROM composicion_productos cp
                    JOIN productos p ON cp.producto_componente_id = p.id
                    WHERE cp.producto_compuesto_id = ?
                    ORDER BY cp.porcentaje DESC
                """, (producto_id,))
                
                componentes = cursor.fetchall()
                
                if componentes:
                    # Construir diccionario de rendimientos
                    rendimientos = {}
                    total_porcentaje = 0
                    
                    for componente_id, nombre, unidad, porcentaje in componentes:
                        if porcentaje and porcentaje > 0:
                            rendimientos[nombre] = {
                                'id': componente_id,
                                'porcentaje': float(porcentaje),
                                'unidad': unidad,
                                'producto_compuesto': producto_nombre,
                                'producto_compuesto_id': producto_id
                            }
                            total_porcentaje += float(porcentaje)
                    
                    # Solo retornar si hay componentes válidos
                    if rendimientos and total_porcentaje > 0:
                        print(f"Configuración encontrada en: {producto_nombre} "
                              f"({len(rendimientos)} subproductos, {total_porcentaje:.1f}%)")
                        return rendimientos
            
            # Si llegamos aquí, no hay configuración válida
            return None
            
        except sqlite3.Error as e:
            print(f"Error al obtener configuración de rendimiento: {e}")
            return None

    def mostrar_subproductos_calculados(self):
        """Muestra diálogo con los subproductos calculados"""
        try:
            # Verificar configuración primero
            rendimientos = self.obtener_configuracion_rendimiento_pollo()
            if not rendimientos:
                self.mostrar_mensaje(
                    "Error", 
                    "No hay configuración de rendimiento disponible.\n\n"
                    "Configure los productos compuestos en el módulo de Productos primero.",
                    QMessageBox.Critical
                )
                return

            kilos_totales = self.spin_kilos_totales.value()
            costo_total = self.spin_costo_total.value()
            
            if kilos_totales <= 0:
                self.mostrar_mensaje(
                    "Advertencia", 
                    "Ingrese los kilos totales para calcular los subproductos.",
                    QMessageBox.Warning
                )
                return

            # Crear diálogo
            dialogo = QDialog(self)
            dialogo.setWindowTitle("Subproductos Calculados")
            dialogo.setMinimumSize(700, 500)
            
            layout = QVBoxLayout(dialogo)
            
            # Información de la compra
            info_layout = QHBoxLayout()
            info_layout.addWidget(QLabel(f"<b>Kilos Totales:</b> {kilos_totales:.2f} kg"))
            info_layout.addWidget(QLabel(f"<b>Costo Total:</b> ${costo_total:.2f}"))
            info_layout.addWidget(QLabel(f"<b>Costo por Kilo:</b> ${costo_total/kilos_totales:.2f}"))
            info_layout.addStretch()
            layout.addLayout(info_layout)
            
            # Tabla de subproductos
            tabla = QTableWidget()
            tabla.setColumnCount(5)
            tabla.setHorizontalHeaderLabels([
                "Subproducto", "Rendimiento %", "Cantidad (kg)", "Costo Unitario", "Costo Total"
            ])
            tabla.setEditTriggers(QAbstractItemView.NoEditTriggers)
            
            # Calcular y llenar la tabla
            costo_kilo = costo_total / kilos_totales
            tabla.setRowCount(len(rendimientos))
            
            row = 0
            total_porcentaje = 0
            total_costo = 0
            
            for subproducto_nombre, datos in rendimientos.items():
                porcentaje = datos['porcentaje']
                cantidad = kilos_totales * (porcentaje / 100)
                costo_subproducto = cantidad * costo_kilo
                
                total_porcentaje += porcentaje
                total_costo += costo_subproducto

                # Subproducto
                tabla.setItem(row, 0, QTableWidgetItem(subproducto_nombre))
                
                # Porcentaje
                porcentaje_item = QTableWidgetItem(f"{porcentaje:.1f}%")
                porcentaje_item.setTextAlignment(Qt.AlignCenter)
                tabla.setItem(row, 1, porcentaje_item)
                
                # Cantidad
                cantidad_item = QTableWidgetItem(f"{cantidad:.2f} kg")
                cantidad_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                tabla.setItem(row, 2, cantidad_item)
                
                # Costo Unitario
                costo_unitario_item = QTableWidgetItem(f"${costo_kilo:.2f}")
                costo_unitario_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                tabla.setItem(row, 3, costo_unitario_item)
                
                # Costo Total
                costo_total_item = QTableWidgetItem(f"${costo_subproducto:.2f}")
                costo_total_item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                tabla.setItem(row, 4, costo_total_item)
                
                row += 1

            # Ajustar columnas
            header = tabla.horizontalHeader()
            header.setSectionResizeMode(0, QHeaderView.Stretch)
            for i in range(1, 5):
                header.setSectionResizeMode(i, QHeaderView.ResizeToContents)
            
            layout.addWidget(tabla)
            
            # Totales
            totales_layout = QHBoxLayout()
            totales_layout.addWidget(QLabel(f"<b>Total Porcentaje:</b> {total_porcentaje:.1f}%"))
            totales_layout.addWidget(QLabel(f"<b>Total Costo Subproductos:</b> ${total_costo:.2f}"))
            totales_layout.addStretch()
            
            if abs(total_porcentaje - 100.0) > 0.01:
                lbl_advertencia = QLabel("⚠️ La suma de porcentajes no es 100%")
                lbl_advertencia.setStyleSheet("color: orange; font-weight: bold;")
                totales_layout.addWidget(lbl_advertencia)
            
            layout.addLayout(totales_layout)
            
            # Botón cerrar
            btn_cerrar = QPushButton("Cerrar")
            btn_cerrar.clicked.connect(dialogo.accept)
            layout.addWidget(btn_cerrar)
            
            dialogo.exec_()
            
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error al calcular subproductos: {str(e)}", QMessageBox.Critical)

    def mostrar_historial_pollo(self):
        """Muestra diálogo con el historial de compras de pollo"""
        try:
            dialogo = QDialog(self)
            dialogo.setWindowTitle("Historial de Compras de Pollo")
            dialogo.setMinimumSize(900, 600)
            
            layout = QVBoxLayout(dialogo)
            
            # Barra de herramientas
            toolbar = QHBoxLayout()
            btn_actualizar = QPushButton("Actualizar")
            btn_actualizar.setIcon(self.obtener_icono("refresh.png"))
            btn_actualizar.clicked.connect(lambda: self.cargar_historial_pollo(tabla))
            
            toolbar.addStretch()
            toolbar.addWidget(btn_actualizar)
            layout.addLayout(toolbar)
            
            # Tabla de historial
            tabla = QTableWidget()
            tabla.setColumnCount(8)
            tabla.setHorizontalHeaderLabels([
                "ID", "Fecha", "Pollos", "Kilos", "Costo Total", "Costo/Kg", "Estado", "Usuario"
            ])
            tabla.setEditTriggers(QAbstractItemView.NoEditTriggers)
            tabla.horizontalHeader().setStretchLastSection(True)
            
            # Ajustar columnas
            header = tabla.horizontalHeader()
            for i in [0, 2]:  # ID, Pollos
                header.setSectionResizeMode(i, QHeaderView.ResizeToContents)
            for i in [3, 4, 5]:  # Kilos, Costo Total, Costo/Kg
                header.setSectionResizeMode(i, QHeaderView.ResizeToContents)
            
            layout.addWidget(tabla)
            
            # Botón cerrar
            btn_cerrar = QPushButton("Cerrar")
            btn_cerrar.clicked.connect(dialogo.accept)
            layout.addWidget(btn_cerrar)
            
            # Cargar datos
            self.cargar_historial_pollo(tabla)
            dialogo.exec_()
            
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error al mostrar historial: {str(e)}", QMessageBox.Critical)

    def registrar_compra_pollo(self):
        """Registra una compra de pollo y genera subproductos - VERSIÓN CORREGIDA"""
        try:
            # Verificar configuración primero
            rendimientos = self.obtener_configuracion_rendimiento_pollo()
            if not rendimientos:
                self.mostrar_mensaje(
                    "Error", 
                    "No se puede registrar la compra porque no hay configuración de rendimiento.\n\n"
                    "Configure los productos compuestos en el módulo de Productos primero.",
                    QMessageBox.Critical
                )
                return

            fecha = self.date_compra_pollo.date().toString("yyyy-MM-dd")
            numero_pollos = self.spin_numero_pollos.value()
            kilos_totales = self.spin_kilos_totales.value()
            costo_total = self.spin_costo_total.value()
            proveedor = self.combo_proveedor_pollo.currentText()
            estado = self.combo_estado_pollo.currentText()
            metodo_pago = self.combo_metodo_pago_pollo.currentText()
            descripcion = self.edit_descripcion_pollo.toPlainText()
            usuario = self.obtener_usuario_actual()

            if kilos_totales <= 0:
                self.mostrar_mensaje("Error", "Los kilos totales deben ser mayores a cero.", QMessageBox.Critical)
                return

            if costo_total <= 0:
                self.mostrar_mensaje("Error", "El costo total debe ser mayor a cero.", QMessageBox.Critical)
                return

            # Verificar suma de porcentajes
            total_porcentaje = sum(comp['porcentaje'] for comp in rendimientos.values())
            if abs(total_porcentaje - 100.0) > 1.0:  # Permitir 1% de tolerancia
                respuesta = QMessageBox.question(
                    self, 
                    "Confirmar Compra", 
                    f"La suma de porcentajes ({total_porcentaje:.1f}%) no es 100%.\n\n"
                    f"¿Desea continuar con el registro de la compra?",
                    QMessageBox.Yes | QMessageBox.No
                )
                if respuesta != QMessageBox.Yes:
                    return

            cursor = self.conexion.cursor()

            # Registrar compra principal
            costo_kilo = costo_total / kilos_totales
            cursor.execute("""
                INSERT INTO compras_pollo 
                (fecha, numero_pollos, kilos_totales, costo_total, costo_kilo, 
                 proveedor, estado, metodo_pago, descripcion, usuario)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (fecha, numero_pollos, kilos_totales, costo_total, 
                  costo_kilo, proveedor, estado, metodo_pago, 
                  descripcion, usuario))

            compra_id = cursor.lastrowid

            # Procesar cada subproducto
            subproductos_procesados = 0
            for subproducto_nombre, datos in rendimientos.items():
                porcentaje = datos['porcentaje']
                producto_id = datos['id']
                
                if porcentaje > 0:
                    cantidad = kilos_totales * (porcentaje / 100)
                    costo_subproducto = cantidad * costo_kilo

                    # Obtener existencia anterior
                    cursor.execute("SELECT existencia FROM productos WHERE id = ?", (producto_id,))
                    resultado = cursor.fetchone()
                    existencia_anterior = resultado[0] if resultado and resultado[0] is not None else 0
                    existencia_nueva = existencia_anterior + cantidad

                    # Registrar movimiento de inventario - CORREGIDO
                    cursor.execute("""
                        INSERT INTO movimientos_inventario 
                        (producto_id, tipo_movimiento, cantidad, costo_unitario, 
                         costo_total, fecha, usuario, referencia, descripcion,
                         existencia_anterior, existencia_nueva)
                        VALUES (?, 'ENTRADA', ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (producto_id, cantidad, costo_kilo, costo_subproducto, 
                          fecha, usuario, f"COMPRA_POLLO_{compra_id}", 
                          f"Subproducto '{subproducto_nombre}' de compra de pollo #{compra_id}",
                          existencia_anterior, existencia_nueva))

                    # Actualizar existencia VÍA InventoryService (ÚNICA vía permitida)
                    from core.services.inventory_service import InventoryService as _InventoryService
                    _inv_svc = _InventoryService(self.conexion, usuario, getattr(self, 'sucursal_id', 1))
                    _inv_svc.registrar_entrada(
                        producto_id=producto_id,
                        cantidad=cantidad,
                        descripcion=f"Subproducto '{subproducto_nombre}' de compra pollo #{compra_id}",
                        costo_unitario=costo_kilo,
                    )

                    subproductos_procesados += 1

            self.conexion.commit()

            # Registrar en finanzas como gasto
            self.registrar_gasto_finanzas({
                'fecha': fecha,
                'descripcion': f"Compra de pollo - {numero_pollos} pollos, {kilos_totales} kg",
                'monto': costo_total,
                'categoria': 'COMPRAS_POLLO',
                'metodo_pago': metodo_pago,
                'estado': estado,
                'referencia': f"POLLO_{compra_id}"
            })

            self.mostrar_mensaje("Éxito", 
                f"✅ Compra de pollo registrada correctamente.\n\n"
                f"ID: {compra_id}\n"
                f"Subproductos generados: {subproductos_procesados}\n"
                f"Kilos totales: {kilos_totales} kg\n"
                f"Costo total: ${costo_total:.2f}")
            
            self.limpiar_formulario_pollo()

            # Notificar actualización a todos los módulos
            if hasattr(self.main_window, 'notificar_evento'):
                self.main_window.notificar_evento('inventario_actualizado', {
                    'modulo': 'inventario_pollo',
                    'compra_id': compra_id,
                    'kilos_totales': kilos_totales
                })

        except sqlite3.Error as e:
            self.conexion.rollback()
            self.mostrar_mensaje("Error", f"Error al registrar compra: {str(e)}", QMessageBox.Critical)

    def calcular_costo_kilo(self):
        """Calcula el costo por kilo basado en kilos totales y costo total"""
        try:
            kilos = self.spin_kilos_totales.value()
            costo_total = self.spin_costo_total.value()
            
            if kilos > 0:
                costo_kilo = costo_total / kilos
                self.lbl_costo_kilo.setText(f"${costo_kilo:.2f} por kg")
            else:
                self.lbl_costo_kilo.setText("$0.00 por kg")
                
        except Exception as e:
            self.lbl_costo_kilo.setText("Error en cálculo")

    # === FUNCIONES DE INVENTARIO GENERAL ===
    def cargar_inventario_general(self):
        """Carga el inventario general"""
        try:
            cursor = self.conexion.cursor()
            
            categoria = self.combo_categoria.currentText()
            condicion_categoria = ""
            parametros = []
            
            if categoria != "Todas las categorías":
                condicion_categoria = "AND p.categoria = ?"
                parametros = [categoria]
            
            consulta = f"""
                SELECT p.id, p.nombre, p.precio, p.existencia, p.stock_minimo, 
                       p.unidad, p.categoria
                FROM productos p
                WHERE 1=1
                {condicion_categoria}
                ORDER BY p.nombre
            """
            
            cursor.execute(consulta, parametros)
            productos = cursor.fetchall()

            self.tabla_general.setRowCount(len(productos))
            for row, producto in enumerate(productos):
                for col, valor in enumerate(producto):
                    if col == 2:  # Precio
                        item = QTableWidgetItem(f"${valor:,.2f}" if valor is not None else "$0.00")
                        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        self.tabla_general.setItem(row, col, item)
                    elif col in [3, 4]:  # Existencia y Stock Mínimo
                        item = QTableWidgetItem(f"{valor:,.2f}" if valor is not None else "0.00")
                        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        
                        # Resaltar existencia baja
                        if col == 3 and valor is not None and producto[4] is not None and valor <= producto[4]:
                            item.setForeground(QColor('red'))
                            item.setToolTip(f"Existencia por debajo del mínimo ({producto[4]})")
                            
                        self.tabla_general.setItem(row, col, item)
                    else:
                        self.tabla_general.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))

        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al cargar inventario: {str(e)}", QMessageBox.Critical)

    def buscar_productos_general(self):
        """Busca productos en el inventario general"""
        texto = self.busqueda_general.text().strip()
        if not texto:
            self.cargar_inventario_general()
            return

        try:
            cursor = self.conexion.cursor()
            consulta = """
                SELECT p.id, p.nombre, p.precio, p.existencia, p.stock_minimo, 
                       p.unidad, p.categoria
                FROM productos p
                WHERE (p.nombre LIKE ? OR p.categoria LIKE ?)
                ORDER BY p.nombre
            """
            parametros = [f"%{texto}%", f"%{texto}%"]
            
            cursor.execute(consulta, parametros)
            productos = cursor.fetchall()

            self.tabla_general.setRowCount(len(productos))
            for row, producto in enumerate(productos):
                for col, valor in enumerate(producto):
                    if col == 2:  # Precio
                        item = QTableWidgetItem(f"${valor:,.2f}" if valor is not None else "$0.00")
                        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        self.tabla_general.setItem(row, col, item)
                    elif col in [3, 4]:  # Existencia y Stock Mínimo
                        item = QTableWidgetItem(f"{valor:,.2f}" if valor is not None else "0.00")
                        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        self.tabla_general.setItem(row, col, item)
                    else:
                        self.tabla_general.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))

        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error en búsqueda: {str(e)}", QMessageBox.Critical)

    def ajustar_existencia(self):
        """Abre diálogo para ajustar existencia del producto seleccionado"""
        fila = self.tabla_general.currentRow()
        if fila < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un producto para ajustar existencia.")
            return

        try:
            producto_id = int(self.tabla_general.item(fila, 0).text())
            producto_nombre = self.tabla_general.item(fila, 1).text()
            existencia_actual = float(self.tabla_general.item(fila, 3).text().replace(',', ''))

            dialogo = DialogoAjusteExistencia(self.conexion, producto_id, producto_nombre, existencia_actual, self.usuario_actual, self)
            if dialogo.exec_() == QDialog.Accepted:
                self.cargar_inventario_general()
                # Notificar actualización
                if hasattr(self.main_window, 'notificar_evento'):
                    self.main_window.notificar_evento('inventario_actualizado', {
                        'producto_id': producto_id,
                        'modulo': 'inventario'
                    })

        except Exception as e:
            self.mostrar_mensaje("Error", f"Error al ajustar existencia: {str(e)}", QMessageBox.Critical)

    def agregar_compra_general(self):
        """Abre diálogo para agregar compra de producto general"""
        fila = self.tabla_general.currentRow()
        if fila < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un producto para agregar compra.")
            return

        try:
            producto_id = int(self.tabla_general.item(fila, 0).text())
            producto_nombre = self.tabla_general.item(fila, 1).text()

            dialogo = DialogoCompraGeneral(self.conexion, producto_id, producto_nombre, self.usuario_actual, self)
            if dialogo.exec_() == QDialog.Accepted:
                self.cargar_inventario_general()
                # Notificar actualización
                if hasattr(self.main_window, 'notificar_evento'):
                    self.main_window.notificar_evento('inventario_actualizado', {
                        'producto_id': producto_id,
                        'modulo': 'inventario'
                    })

        except Exception as e:
            self.mostrar_mensaje("Error", f"Error al agregar compra: {str(e)}", QMessageBox.Critical)

    def ver_movimientos(self):
        """Muestra los movimientos del producto seleccionado"""
        fila = self.tabla_general.currentRow()
        if fila < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un producto para ver movimientos.")
            return

        try:
            producto_id = int(self.tabla_general.item(fila, 0).text())
            producto_nombre = self.tabla_general.item(fila, 1).text()

            dialogo = DialogoMovimientos(self.conexion, producto_id, producto_nombre, self)
            dialogo.exec_()

        except Exception as e:
            self.mostrar_mensaje("Error", f"Error al cargar movimientos: {str(e)}", QMessageBox.Critical)

    def actualizar_botones_general(self):
        """Habilita/deshabilita botones según selección"""
        hay_seleccion = self.tabla_general.currentRow() >= 0
        self.btn_ajustar_existencia.setEnabled(hay_seleccion)
        self.btn_ver_movimientos.setEnabled(hay_seleccion)
        self.btn_agregar_compra.setEnabled(hay_seleccion)

    def cargar_historial_pollo(self, tabla=None):
        """Carga el historial de compras de pollo en la tabla especificada"""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("""
                SELECT id, fecha, numero_pollos, kilos_totales, costo_total, 
                       costo_kilo, estado, usuario
                FROM compras_pollo 
                ORDER BY fecha DESC, id DESC
                LIMIT 100
            """)
            
            compras = cursor.fetchall()
            
            target_tabla = tabla if tabla else self.tabla_historial_pollo
            
            if target_tabla:
                target_tabla.setRowCount(len(compras))
                
                for row, compra in enumerate(compras):
                    for col, valor in enumerate(compra):
                        if col == 4:  # Costo total
                            item = QTableWidgetItem(f"${valor:,.2f}" if valor is not None else "$0.00")
                            item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                            target_tabla.setItem(row, col, item)
                        elif col == 5:  # Costo por kilo
                            item = QTableWidgetItem(f"${valor:.2f}" if valor is not None else "$0.00")
                            item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                            target_tabla.setItem(row, col, item)
                        elif col == 6:  # Estado
                            item = QTableWidgetItem(str(valor) if valor is not None else "")
                            if valor == "PENDIENTE":
                                item.setForeground(QColor('orange'))
                            elif valor == "PAGADO":
                                item.setForeground(QColor('green'))
                            target_tabla.setItem(row, col, item)
                        else:
                            target_tabla.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))

        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al cargar historial: {str(e)}", QMessageBox.Critical)

    def limpiar_formulario_pollo(self):
        """Limpia el formulario de compra de pollo"""
        self.date_compra_pollo.setDate(QDate.currentDate())
        self.spin_numero_pollos.setValue(1)
        self.spin_kilos_totales.setValue(0.0)
        self.spin_costo_total.setValue(0.0)
        self.edit_descripcion_pollo.clear()
        self.lbl_costo_kilo.setText("$0.00 por kg")
        # Re-verificar configuración
        self.verificar_configuracion_rendimiento()

    # === FUNCIONES DE TRANSFERENCIAS ===
    def cargar_productos_con_existencia(self):
        """Carga los productos disponibles para transferencia"""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("""
                SELECT id, nombre, unidad, existencia 
                FROM productos 
                WHERE existencia > 0
                ORDER BY nombre
            """)
            
            productos = cursor.fetchall()
            self.combo_producto_origen.clear()
            
            for id_prod, nombre, unidad, existencia in productos:
                self.combo_producto_origen.addItem(f"{nombre} ({existencia:.2f} {unidad})", id_prod)
                
        except sqlite3.Error as e:
            print(f"Error al cargar productos para transferencia: {e}")

    def realizar_transferencia(self):
        """Realiza una transferencia de inventario - VERSIÓN CORREGIDA"""
        try:
            if self.combo_producto_origen.currentIndex() < 0:
                self.mostrar_mensaje("Error", "Seleccione un producto para transferir.", QMessageBox.Critical)
                return

            producto_id = self.combo_producto_origen.currentData()
            cantidad = self.spin_cantidad_transferencia.value()
            usuario_origen = self.edit_usuario_origen.text().strip()
            usuario_destino = self.edit_usuario_destino.text().strip()
            observaciones = self.edit_observaciones_transferencia.toPlainText().strip()

            if not usuario_destino:
                self.mostrar_mensaje("Error", "Ingrese el usuario destino.", QMessageBox.Critical)
                return

            if cantidad <= 0:
                self.mostrar_mensaje("Error", "La cantidad debe ser mayor a cero.", QMessageBox.Critical)
                return

            cursor = self.conexion.cursor()

            # Verificar existencia suficiente
            cursor.execute("SELECT nombre, existencia FROM productos WHERE id = ?", (producto_id,))
            producto = cursor.fetchone()
            
            if not producto:
                self.mostrar_mensaje("Error", "Producto no encontrado.", QMessageBox.Critical)
                return

            nombre_producto, existencia_actual = producto
            if existencia_actual < cantidad:
                self.mostrar_mensaje("Error", 
                    f"Existencia insuficiente. Disponible: {existencia_actual}", 
                    QMessageBox.Critical)
                return

            # Registrar transferencia - CORREGIDO
            fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            cursor.execute("""
                INSERT INTO transferencias_inventario 
                (producto_origen_id, cantidad, usuario_origen, usuario_destino, 
                 fecha, observaciones, usuario_registro)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (producto_id, cantidad, usuario_origen, usuario_destino, 
                  fecha, observaciones, self.usuario_actual))

            # Descontar existencia VÍA InventoryService (ÚNICA vía permitida)
            from core.services.inventory_service import InventoryService as _InventoryService
            _inv_svc = _InventoryService(self.conexion, self.usuario_actual, getattr(self, 'sucursal_id', 1))
            _inv_svc.registrar_salida_manual(
                producto_id=producto_id,
                cantidad=cantidad,
                motivo=f"Transferencia a {usuario_destino}: {observaciones}",
                referencia=f"TRANSFER_{producto_id}_{fecha[:10]}",
            )

            self.conexion.commit()

            self.mostrar_mensaje("Éxito", f"Transferencia realizada correctamente.\nProducto: {nombre_producto}\nCantidad: {cantidad}")
            self.limpiar_formulario_transferencia()
            self.cargar_historial_transferencias()
            self.cargar_productos_con_existencia()

            # Notificar actualización
            if hasattr(self.main_window, 'notificar_evento'):
                self.main_window.notificar_evento('inventario_actualizado', {
                    'producto_id': producto_id,
                    'modulo': 'transferencias'
                })

        except sqlite3.Error as e:
            self.conexion.rollback()
            self.mostrar_mensaje("Error", f"Error al realizar transferencia: {str(e)}", QMessageBox.Critical)

    def cargar_historial_transferencias(self):
        """Carga el historial de transferencias"""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("""
                SELECT t.id, t.fecha, p.nombre, t.cantidad, t.usuario_origen, 
                       t.usuario_destino, t.observaciones
                FROM transferencias_inventario t
                JOIN productos p ON t.producto_origen_id = p.id
                ORDER BY t.fecha DESC
                LIMIT 100
            """)
            
            transferencias = cursor.fetchall()
            self.tabla_transferencias.setRowCount(len(transferencias))
            
            for row, transferencia in enumerate(transferencias):
                for col, valor in enumerate(transferencia):
                    if col == 3:  # Cantidad
                        item = QTableWidgetItem(f"{valor:.2f}" if valor is not None else "0.00")
                        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        self.tabla_transferencias.setItem(row, col, item)
                    else:
                        self.tabla_transferencias.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))

        except sqlite3.Error as e:
            print(f"Error al cargar historial de transferencias: {e}")

    def limpiar_formulario_transferencia(self):
        """Limpia el formulario de transferencia"""
        self.spin_cantidad_transferencia.setValue(0.0)
        self.edit_usuario_destino.clear()
        self.edit_observaciones_transferencia.clear()
        self.cargar_productos_con_existencia()

    # === FUNCIONES AUXILIARES ===
    def cargar_categorias(self):
        """Carga las categorías de productos"""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT DISTINCT categoria FROM productos WHERE categoria IS NOT NULL AND categoria != '' ORDER BY categoria")
            categorias = cursor.fetchall()
            
            self.combo_categoria.clear()
            self.combo_categoria.addItem("Todas las categorías")
            
            for categoria in categorias:
                self.combo_categoria.addItem(categoria[0])
                
        except sqlite3.Error as e:
            print(f"Error al cargar categorías: {e}")

    def cargar_proveedores_pollo(self):
        """Carga los proveedores de pollo de la base de datos"""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT DISTINCT proveedor FROM compras_pollo WHERE proveedor IS NOT NULL ORDER BY proveedor")
            proveedores = cursor.fetchall()
            
            self.combo_proveedor_pollo.clear()
            for proveedor in proveedores:
                self.combo_proveedor_pollo.addItem(proveedor[0])
                
        except sqlite3.Error as e:
            print(f"Error al cargar proveedores: {e}")

    def conectar_eventos(self):
        """Conecta eventos generales del módulo"""
        pass

    def obtener_icono(self, nombre_icono):
        """Obtiene un icono por nombre"""
        # Implementación básica usando iconos del sistema
        if nombre_icono == "search.png":
            return QApplication.style().standardIcon(QStyle.SP_FileDialogContentsView)
        elif nombre_icono == "refresh.png":
            return QApplication.style().standardIcon(QStyle.SP_BrowserReload)
        elif nombre_icono == "add.png":
            return QApplication.style().standardIcon(QStyle.SP_FileDialogNewFolder)
        elif nombre_icono == "edit.png":
            return QApplication.style().standardIcon(QStyle.SP_FileDialogDetailedView)
        elif nombre_icono == "list.png":
            return QApplication.style().standardIcon(QStyle.SP_FileDialogListView)
        elif nombre_icono == "transfer.png":
            return QApplication.style().standardIcon(QStyle.SP_FileLinkIcon)
        elif nombre_icono == "history.png":
            return QApplication.style().standardIcon(QStyle.SP_DirOpenIcon)
        else:
            return QApplication.style().standardIcon(QStyle.SP_FileIcon)

    def mostrar_mensaje(self, titulo, mensaje, tipo=QMessageBox.Information):
        """Muestra un mensaje al usuario"""
        msg = QMessageBox(self)
        msg.setWindowTitle(titulo)
        msg.setText(mensaje)
        msg.setIcon(tipo)
        msg.exec_()

    def registrar_gasto_finanzas(self, datos_gasto):
        """Registra un gasto en el módulo de finanzas"""
        try:
            if hasattr(self.main_window, 'registrar_gasto_finanzas'):
                self.main_window.registrar_gasto_finanzas(datos_gasto)
            else:
                # Si no existe el método, registrar directamente en la base de datos
                cursor = self.conexion.cursor()
                
                # Verificar si existe la tabla gastos
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS gastos (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fecha TEXT NOT NULL,
                        descripcion TEXT NOT NULL,
                        monto REAL NOT NULL,
                        categoria TEXT NOT NULL,
                        metodo_pago TEXT NOT NULL,
                        estado TEXT NOT NULL,
                        referencia TEXT,
                        usuario TEXT
                    )
                """)
                
                cursor.execute("""
                    INSERT INTO gastos 
                    (fecha, descripcion, monto, categoria, metodo_pago, estado, referencia, usuario)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    datos_gasto['fecha'],
                    datos_gasto['descripcion'],
                    datos_gasto['monto'],
                    datos_gasto['categoria'],
                    datos_gasto['metodo_pago'],
                    datos_gasto['estado'],
                    datos_gasto['referencia'],
                    self.usuario_actual
                ))
                self.conexion.commit()
        except Exception as e:
            print(f"Error al registrar gasto en finanzas: {e}")

# ===== DIÁLOGOS AUXILIARES =====

class DialogoAjusteExistencia(QDialog):
    def __init__(self, conexion, producto_id, producto_nombre, existencia_actual, usuario, parent=None):
        super().__init__(parent)
        self.conexion = conexion
        self.producto_id = producto_id
        self.producto_nombre = producto_nombre
        self.existencia_actual = existencia_actual
        self.usuario = usuario
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle(f"Ajustar Existencia - {self.producto_nombre}")
        self.setModal(True)
        self.resize(400, 300)

        layout = QVBoxLayout()

        # Información del producto
        info_layout = QFormLayout()
        info_layout.addRow("Producto:", QLabel(self.producto_nombre))
        info_layout.addRow("Existencia Actual:", QLabel(f"{self.existencia_actual:.2f}"))

        # Tipo de ajuste
        self.combo_tipo_ajuste = QComboBox()
        self.combo_tipo_ajuste.addItems(["ENTRADA", "SALIDA", "AJUSTE"])
        info_layout.addRow("Tipo de Ajuste:", self.combo_tipo_ajuste)

        # Cantidad
        self.spin_cantidad = QDoubleSpinBox()
        self.spin_cantidad.setRange(0.01, 10000.0)
        self.spin_cantidad.setDecimals(2)
        self.spin_cantidad.setValue(0.0)
        info_layout.addRow("Cantidad:", self.spin_cantidad)

        # Observaciones
        self.edit_observaciones = QTextEdit()
        self.edit_observaciones.setMaximumHeight(80)
        self.edit_observaciones.setPlaceholderText("Motivo del ajuste...")
        info_layout.addRow("Observaciones:", self.edit_observaciones)

        layout.addLayout(info_layout)

        # Botones
        btn_layout = QHBoxLayout()
        btn_aceptar = QPushButton("Aceptar")
        btn_cancelar = QPushButton("Cancelar")
        
        btn_aceptar.clicked.connect(self.aceptar_ajuste)
        btn_cancelar.clicked.connect(self.reject)
        
        btn_layout.addWidget(btn_aceptar)
        btn_layout.addWidget(btn_cancelar)
        layout.addLayout(btn_layout)

        self.setLayout(layout)

    def aceptar_ajuste(self):
        try:
            tipo_ajuste = self.combo_tipo_ajuste.currentText()
            cantidad = self.spin_cantidad.value()
            observaciones = self.edit_observaciones.toPlainText().strip()

            if cantidad <= 0:
                QMessageBox.warning(self, "Error", "La cantidad debe ser mayor a cero.")
                return

            cursor = self.conexion.cursor()
            fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Calcular nueva existencia
            if tipo_ajuste == "ENTRADA":
                nueva_existencia = self.existencia_actual + cantidad
            elif tipo_ajuste == "SALIDA":
                if cantidad > self.existencia_actual:
                    QMessageBox.warning(self, "Error", "Cantidad mayor a la existencia actual.")
                    return
                nueva_existencia = self.existencia_actual - cantidad
            else:  # AJUSTE
                nueva_existencia = cantidad

            # Ajustar existencia VÍA InventoryService (ÚNICA vía permitida)
            from core.services.inventory_service import InventoryService as _InventoryService
            _inv_svc = _InventoryService(self.conexion, self.usuario)

            if tipo_ajuste == "ENTRADA":
                _inv_svc.registrar_entrada(
                    producto_id=self.producto_id,
                    cantidad=cantidad,
                    descripcion=observaciones or "Ajuste de entrada manual",
                )
            elif tipo_ajuste == "SALIDA":
                _inv_svc.registrar_salida_manual(
                    producto_id=self.producto_id,
                    cantidad=cantidad,
                    motivo=observaciones or "Ajuste de salida manual",
                )
            else:  # AJUSTE directo a valor absoluto
                _inv_svc.ajustar_stock(
                    producto_id=self.producto_id,
                    cantidad_nueva=nueva_existencia,
                    motivo=observaciones or "Ajuste de inventario físico",
                )

            self.conexion.commit()
            QMessageBox.information(self, "Éxito", f"Existencia ajustada correctamente.\nNueva existencia: {nueva_existencia:.2f}")
            self.accept()

        except sqlite3.Error as e:
            self.conexion.rollback()
            QMessageBox.critical(self, "Error", f"Error al ajustar existencia: {str(e)}")

class DialogoCompraGeneral(QDialog):
    def __init__(self, conexion, producto_id, producto_nombre, usuario, parent=None):
        super().__init__(parent)
        self.conexion = conexion
        self.producto_id = producto_id
        self.producto_nombre = producto_nombre
        self.usuario = usuario
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle(f"Compra - {self.producto_nombre}")
        self.setModal(True)
        self.resize(400, 300)

        layout = QVBoxLayout()

        # Formulario de compra
        form_layout = QFormLayout()

        self.date_compra = QDateEdit()
        self.date_compra.setDate(QDate.currentDate())
        self.date_compra.setDisplayFormat("dd/MM/yyyy")

        self.spin_cantidad = QDoubleSpinBox()
        self.spin_cantidad.setRange(0.01, 10000.0)
        self.spin_cantidad.setDecimals(2)
        self.spin_cantidad.setValue(1.0)

        self.spin_costo_unitario = QDoubleSpinBox()
        self.spin_costo_unitario.setRange(0.01, 10000.0)
        self.spin_costo_unitario.setPrefix("$ ")
        self.spin_costo_unitario.setDecimals(2)

        self.combo_proveedor = QComboBox()
        self.combo_proveedor.setEditable(True)
        self.cargar_proveedores()

        self.edit_observaciones = QTextEdit()
        self.edit_observaciones.setMaximumHeight(80)
        self.edit_observaciones.setPlaceholderText("Observaciones de la compra...")

        form_layout.addRow("Fecha:", self.date_compra)
        form_layout.addRow("Cantidad:", self.spin_cantidad)
        form_layout.addRow("Costo Unitario:", self.spin_costo_unitario)
        form_layout.addRow("Proveedor:", self.combo_proveedor)
        form_layout.addRow("Observaciones:", self.edit_observaciones)

        layout.addLayout(form_layout)

        # Botones
        btn_layout = QHBoxLayout()
        btn_aceptar = QPushButton("Registrar Compra")
        btn_cancelar = QPushButton("Cancelar")
        
        btn_aceptar.clicked.connect(self.registrar_compra)
        btn_cancelar.clicked.connect(self.reject)
        
        btn_layout.addWidget(btn_aceptar)
        btn_layout.addWidget(btn_cancelar)
        layout.addLayout(btn_layout)

        self.setLayout(layout)

    def cargar_proveedores(self):
        try:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT DISTINCT proveedor FROM compras_generales WHERE proveedor IS NOT NULL ORDER BY proveedor")
            proveedores = cursor.fetchall()
            
            for proveedor in proveedores:
                self.combo_proveedor.addItem(proveedor[0])
                
        except sqlite3.Error:
            pass

    def registrar_compra(self):
        try:
            fecha = self.date_compra.date().toString("yyyy-MM-dd")
            cantidad = self.spin_cantidad.value()
            costo_unitario = self.spin_costo_unitario.value()
            proveedor = self.combo_proveedor.currentText()
            observaciones = self.edit_observaciones.toPlainText().strip()
            costo_total = cantidad * costo_unitario

            cursor = self.conexion.cursor()

            # Obtener existencia anterior
            cursor.execute("SELECT existencia FROM productos WHERE id = ?", (self.producto_id,))
            resultado = cursor.fetchone()
            existencia_anterior = resultado[0] if resultado and resultado[0] is not None else 0
            existencia_nueva = existencia_anterior + cantidad

            # Registrar entrada VÍA InventoryService (ÚNICA vía permitida)
            from core.services.inventory_service import InventoryService as _InventoryService
            _inv_svc = _InventoryService(self.conexion, self.usuario)
            _inv_svc.registrar_entrada(
                producto_id=self.producto_id,
                cantidad=cantidad,
                descripcion=observaciones or f"Compra — {proveedor}",
                referencia="COMPRA",
                costo_unitario=costo_unitario,
            )

            # Registrar en tabla de compras_generales - CORREGIDO
            cursor.execute("""
                INSERT INTO compras_generales 
                (producto_id, fecha, cantidad, costo_unitario, costo_total, 
                 proveedor, descripcion, usuario)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (self.producto_id, fecha, cantidad, costo_unitario, costo_total, 
                  proveedor, observaciones, self.usuario))

            self.conexion.commit()
            QMessageBox.information(self, "Éxito", "Compra registrada correctamente.")
            self.accept()

        except sqlite3.Error as e:
            self.conexion.rollback()
            QMessageBox.critical(self, "Error", f"Error al registrar compra: {str(e)}")

class DialogoMovimientos(QDialog):
    def __init__(self, conexion, producto_id, producto_nombre, parent=None):
        super().__init__(parent)
        self.conexion = conexion
        self.producto_id = producto_id
        self.producto_nombre = producto_nombre
        self.init_ui()

    def init_ui(self):
        self.setWindowTitle(f"Movimientos - {self.producto_nombre}")
        self.setModal(True)
        self.resize(800, 500)

        layout = QVBoxLayout()

        # Tabla de movimientos
        self.tabla_movimientos = QTableWidget()
        self.tabla_movimientos.setColumnCount(9)
        self.tabla_movimientos.setHorizontalHeaderLabels([
            "Fecha", "Tipo", "Cantidad", "Costo Unitario", "Costo Total", 
            "Usuario", "Descripción", "Exist. Ant.", "Exist. Nueva"
        ])
        self.tabla_movimientos.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_movimientos.horizontalHeader().setStretchLastSection(True)

        layout.addWidget(self.tabla_movimientos)

        # Botón cerrar
        btn_cerrar = QPushButton("Cerrar")
        btn_cerrar.clicked.connect(self.accept)
        layout.addWidget(btn_cerrar)

        self.setLayout(layout)
        self.cargar_movimientos()

    def cargar_movimientos(self):
        try:
            cursor = self.conexion.cursor()
            cursor.execute("""
                SELECT fecha, tipo_movimiento, cantidad, costo_unitario, 
                       costo_total, usuario, descripcion, existencia_anterior, existencia_nueva
                FROM movimientos_inventario
                WHERE producto_id = ?
                ORDER BY fecha DESC
            """, (self.producto_id,))
            
            movimientos = cursor.fetchall()
            self.tabla_movimientos.setRowCount(len(movimientos))
            
            for row, movimiento in enumerate(movimientos):
                for col, valor in enumerate(movimiento):
                    if col in [2, 3, 4, 7, 8]:  # Cantidad, costos, existencias
                        if valor is not None:
                            if col in [3, 4]:  # Costos
                                item_text = f"${valor:.2f}" if valor != 0 else ""
                            else:  # Cantidades y existencias
                                item_text = f"{valor:.2f}"
                        else:
                            item_text = ""
                            
                        item = QTableWidgetItem(item_text)
                        item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                        self.tabla_movimientos.setItem(row, col, item)
                    else:
                        self.tabla_movimientos.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))

        except sqlite3.Error as e:
            QMessageBox.critical(self, "Error", f"Error al cargar movimientos: {str(e)}")
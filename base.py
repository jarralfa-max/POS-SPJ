# modulos/base.py
from PyQt5.QtWidgets import QWidget, QMessageBox, QStyle
from PyQt5.QtGui import QIcon
import os
import sqlite3
import config
from typing import Optional, Dict, Callable, List, Any, Union
from datetime import datetime

class ModuloBase(QWidget):
    """Clase base para todos los módulos de la aplicación que maneja funcionalidades comunes y conexión a BD."""

    def __init__(self, conexion: sqlite3.Connection, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self.conexion = conexion
        self.main_window = parent  # Referencia a la ventana principal
        self.usuario_actual: Optional[str] = None
        self.rol_usuario: Optional[str] = None
        self.sesion_iniciada: bool = False
        self._callbacks_actualizacion: Dict[str, Callable] = {}
        
        # Si no se proporciona conexión, intentar obtenerla del parent
        if self.conexion is None and hasattr(parent, 'conexion'):
            self.conexion = parent.conexion

    def inicializar_bd(self) -> bool:
        """Inicializa la base de datos con todas las tablas necesarias."""
        try:
            cursor = self.conexion.cursor()
            
            print("🔧 Inicializando estructura completa de la base de datos...")
            
            # ==============================================
            # 1. TABLA DE USUARIOS Y ROLES
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS usuarios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    usuario TEXT UNIQUE NOT NULL,
                    contrasena TEXT NOT NULL,
                    nombre TEXT NOT NULL,
                    rol TEXT NOT NULL,
                    modulos_permitidos TEXT,
                    sucursal_id INTEGER DEFAULT 1,
                    activo INTEGER DEFAULT 1,
                    fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    ultimo_acceso TIMESTAMP,
                    email TEXT,
                    telefono TEXT
                )
            ''')
            
            # ==============================================
            # 2. TABLA DE PRODUCTOS (COMPLETA)
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS productos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT NOT NULL,
                    descripcion TEXT,
                    precio DECIMAL(10,2) NOT NULL,
                    precio_compra DECIMAL(10,2) DEFAULT 0,
                    existencia DECIMAL(10,3) DEFAULT 0,
                    stock_minimo DECIMAL(10,3) DEFAULT 0,
                    unidad TEXT DEFAULT 'pza',
                    categoria TEXT,
                    codigo_barras TEXT UNIQUE,
                    codigo TEXT,
                    costo DECIMAL(10,2) DEFAULT 0,
                    unidad_medida TEXT DEFAULT 'pza',
                    oculto BOOLEAN DEFAULT 0,
                    es_compuesto BOOLEAN DEFAULT 0,
                    es_subproducto BOOLEAN DEFAULT 0,
                    producto_padre_id INTEGER DEFAULT NULL,
                    activo BOOLEAN DEFAULT 1,
                    imagen_path TEXT,
                    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    proveedor_id INTEGER,
                    ubicacion TEXT,
                    notas TEXT,
                    FOREIGN KEY (producto_padre_id) REFERENCES productos(id),
                    FOREIGN KEY (proveedor_id) REFERENCES proveedores(id)
                )
            ''')
            
            # ==============================================
            # 3. TABLA DE COMPONENTES DE PRODUCTOS COMPUESTOS
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS componentes_producto (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    producto_compuesto_id INTEGER NOT NULL,
                    producto_componente_id INTEGER NOT NULL,
                    cantidad DECIMAL(10,3) NOT NULL,
                    unidad TEXT DEFAULT 'pza',
                    costo_adicional DECIMAL(10,2) DEFAULT 0,
                    instrucciones TEXT,
                    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (producto_compuesto_id) REFERENCES productos(id) ON DELETE CASCADE,
                    FOREIGN KEY (producto_componente_id) REFERENCES productos(id) ON DELETE CASCADE,
                    UNIQUE(producto_compuesto_id, producto_componente_id)
                )
            ''')
            
            # ==============================================
            # 4. TABLA DE CATEGORÍAS
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS categorias (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT UNIQUE NOT NULL,
                    descripcion TEXT,
                    activo INTEGER DEFAULT 1,
                    fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    color TEXT,
                    icono TEXT
                )
            ''')
            
            # ==============================================
            # 5. TABLA DE CLIENTES (COMPLETA)
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS clientes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT NOT NULL,
                    apellido_paterno TEXT,
                    apellido_materno TEXT,
                    nombre_completo TEXT GENERATED ALWAYS AS (
                        COALESCE(nombre || ' ' || COALESCE(apellido_paterno, '') || ' ' || COALESCE(apellido_materno, '') , nombre)
                    ) VIRTUAL,
                    telefono TEXT,
                    email TEXT,
                    direccion TEXT,
                    rfc TEXT,
                    puntos INTEGER DEFAULT 0,
                    nivel_fidelidad TEXT DEFAULT 'BASICO',
                    descuento REAL DEFAULT 0,
                    saldo REAL DEFAULT 0,
                    limite_credito REAL DEFAULT 0,
                    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    fecha_ultima_compra TIMESTAMP,
                    fecha_nacimiento DATE,
                    activo INTEGER DEFAULT 1,
                    codigo_qr TEXT,
                    observaciones TEXT,
                    referencia TEXT,
                    tipo_cliente TEXT DEFAULT 'NORMAL'
                )
            ''')
            
            # ==============================================
            # 6. TABLA DE PROVEEDORES
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS proveedores (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT NOT NULL,
                    contacto TEXT,
                    telefono TEXT,
                    email TEXT,
                    direccion TEXT,
                    rfc TEXT,
                    productos TEXT,
                    activo INTEGER DEFAULT 1,
                    fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP,
                    condiciones_pago TEXT,
                    dias_credito INTEGER DEFAULT 0,
                    cuenta_bancaria TEXT,
                    observaciones TEXT
                )
            ''')
            
            # ==============================================
            # 7. TABLA DE VENTAS (COMPLETA)
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ventas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    folio TEXT UNIQUE,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    usuario TEXT NOT NULL,
                    cliente_id INTEGER,
                    subtotal DECIMAL(10,2) NOT NULL,
                    descuento DECIMAL(10,2) DEFAULT 0,
                    iva DECIMAL(10,2) NOT NULL,
                    total DECIMAL(10,2) NOT NULL,
                    estado TEXT DEFAULT 'completada',
                    forma_pago TEXT,
                    efectivo_recibido DECIMAL(10,2) DEFAULT 0,
                    cambio DECIMAL(10,2) DEFAULT 0,
                    puntos_ganados INTEGER DEFAULT 0,
                    puntos_usados INTEGER DEFAULT 0,
                    descuento_puntos DECIMAL(10,2) DEFAULT 0,
                    observaciones TEXT,
                    caja_id INTEGER,
                    impreso BOOLEAN DEFAULT 0,
                    fecha_impresion TIMESTAMP,
                    FOREIGN KEY (cliente_id) REFERENCES clientes(id),
                    FOREIGN KEY (caja_id) REFERENCES cajas(id)
                )
            ''')
            
            # ==============================================
            # 8. TABLA DE DETALLES DE VENTA
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS detalles_venta (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    venta_id INTEGER NOT NULL,
                    producto_id INTEGER NOT NULL,
                    cantidad DECIMAL(10,3) NOT NULL,
                    precio_unitario DECIMAL(10,2) NOT NULL,
                    descuento DECIMAL(10,2) DEFAULT 0,
                    subtotal DECIMAL(10,2) NOT NULL,
                    unidad TEXT,
                    comentarios TEXT,
                    FOREIGN KEY (venta_id) REFERENCES ventas(id) ON DELETE CASCADE,
                    FOREIGN KEY (producto_id) REFERENCES productos(id)
                )
            ''')
            
            # ==============================================
            # 9. TABLA DE MOVIMIENTOS DE INVENTARIO
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS movimientos_inventario (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    producto_id INTEGER NOT NULL,
                    tipo TEXT NOT NULL,
                    cantidad DECIMAL(10,3) NOT NULL,
                    existencia_anterior DECIMAL(10,3) NOT NULL,
                    existencia_nueva DECIMAL(10,3) NOT NULL,
                    descripcion TEXT,
                    referencia TEXT,
                    referencia_id INTEGER,
                    usuario TEXT,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    costo DECIMAL(10,2) DEFAULT 0,
                    lote TEXT,
                    fecha_caducidad DATE,
                    ubicacion TEXT,
                    FOREIGN KEY (producto_id) REFERENCES productos(id)
                )
            ''')
            
            # ==============================================
            # 10. TABLA DE TRANSFERENCIAS DE INVENTARIO
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transferencias_inventario (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    producto_id INTEGER NOT NULL,
                    cantidad DECIMAL(10,3) NOT NULL,
                    tipo TEXT NOT NULL,
                    origen TEXT,
                    destino TEXT,
                    motivo TEXT,
                    usuario TEXT NOT NULL,
                    fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
                    observaciones TEXT,
                    estado TEXT DEFAULT 'COMPLETADA',
                    fecha_completada DATETIME,
                    FOREIGN KEY (producto_id) REFERENCES productos(id)
                )
            ''')
            
            # ==============================================
            # 11. TABLA DE COMPRAS
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS compras (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    folio TEXT UNIQUE,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    proveedor_id INTEGER,
                    usuario TEXT NOT NULL,
                    subtotal DECIMAL(10,2) NOT NULL,
                    iva DECIMAL(10,2) NOT NULL,
                    total DECIMAL(10,2) NOT NULL,
                    estado TEXT DEFAULT 'completada',
                    forma_pago TEXT,
                    observaciones TEXT,
                    fecha_entrega DATE,
                    factura TEXT,
                    FOREIGN KEY (proveedor_id) REFERENCES proveedores(id)
                )
            ''')
            
            # ==============================================
            # 12. TABLA DE DETALLES DE COMPRA
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS detalles_compra (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    compra_id INTEGER NOT NULL,
                    producto_id INTEGER NOT NULL,
                    cantidad DECIMAL(10,3) NOT NULL,
                    precio_unitario DECIMAL(10,2) NOT NULL,
                    subtotal DECIMAL(10,2) NOT NULL,
                    lote TEXT,
                    fecha_caducidad DATE,
                    FOREIGN KEY (compra_id) REFERENCES compras(id) ON DELETE CASCADE,
                    FOREIGN KEY (producto_id) REFERENCES productos(id)
                )
            ''')
            
            # ==============================================
            # 13. TABLA DE COMPRAS DE POLLO
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS compras_pollo (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fecha DATE NOT NULL,
                    numero_pollos INTEGER NOT NULL,
                    kilos_totales REAL NOT NULL,
                    costo_total REAL NOT NULL,
                    costo_kilo REAL NOT NULL,
                    proveedor TEXT,
                    estado TEXT DEFAULT 'PENDIENTE',
                    metodo_pago TEXT,
                    descripcion TEXT,
                    usuario TEXT,
                    fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP,
                    lote TEXT
                )
            ''')
            
            # ==============================================
            # 14. TABLA DE GASTOS
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS gastos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fecha DATE NOT NULL,
                    categoria TEXT NOT NULL,
                    concepto TEXT NOT NULL,
                    descripcion TEXT,
                    monto DECIMAL(10,2) NOT NULL,
                    monto_pagado DECIMAL(10,2) DEFAULT 0,
                    metodo_pago TEXT,
                    estado TEXT DEFAULT 'PAGADO',
                    referencia TEXT,
                    comprobante TEXT,
                    observaciones TEXT,
                    usuario TEXT,
                    fecha_registro DATETIME DEFAULT CURRENT_TIMESTAMP,
                    proveedor_id INTEGER,
                    recurrente BOOLEAN DEFAULT 0,
                    frecuencia TEXT,
                    fecha_proximo TIMESTAMP,
                    FOREIGN KEY (proveedor_id) REFERENCES proveedores(id)
                )
            ''')
            
            # ==============================================
            # 15. TABLA DE PUNTOS DE FIDELIDAD
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS puntos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cliente_id INTEGER NOT NULL,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    puntos INTEGER NOT NULL,
                    tipo TEXT NOT NULL,
                    venta_id INTEGER,
                    concepto TEXT,
                    saldo_anterior INTEGER,
                    saldo_actual INTEGER,
                    expiracion DATE,
                    FOREIGN KEY (cliente_id) REFERENCES clientes(id),
                    FOREIGN KEY (venta_id) REFERENCES ventas(id)
                )
            ''')
            
            # ==============================================
            # 16. TABLA DE HISTORIAL DE PUNTOS
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS historico_puntos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cliente_id INTEGER NOT NULL,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    tipo TEXT NOT NULL,
                    puntos INTEGER NOT NULL,
                    descripcion TEXT,
                    saldo_actual INTEGER,
                    usuario TEXT,
                    venta_id INTEGER,
                    FOREIGN KEY (cliente_id) REFERENCES clientes(id),
                    FOREIGN KEY (venta_id) REFERENCES ventas(id)
                )
            ''')
            
            # ==============================================
            # 17. TABLA DE MOVIMIENTOS DE CAJA
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS movimientos_caja (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    tipo TEXT NOT NULL,
                    monto REAL NOT NULL,
                    descripcion TEXT,
                    usuario TEXT,
                    venta_id INTEGER,
                    forma_pago TEXT,
                    referencia TEXT,
                    caja_id INTEGER,
                    FOREIGN KEY (venta_id) REFERENCES ventas(id),
                    FOREIGN KEY (caja_id) REFERENCES cajas(id)
                )
            ''')
            
            # ==============================================
            # 18. TABLA DE CAJAS
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS cajas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT NOT NULL,
                    ubicacion TEXT,
                    fondo_inicial DECIMAL(10,2) DEFAULT 0,
                    saldo_actual DECIMAL(10,2) DEFAULT 0,
                    estado TEXT DEFAULT 'CERRADA',
                    fecha_apertura TIMESTAMP,
                    fecha_cierre TIMESTAMP,
                    usuario_apertura TEXT,
                    usuario_cierre TEXT,
                    observaciones TEXT
                )
            ''')
            
            # ==============================================
            # 19. TABLA DE PERSONAL
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS personal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nombre TEXT NOT NULL,
                    apellidos TEXT,
                    puesto TEXT,
                    salario REAL DEFAULT 0,
                    fecha_ingreso TEXT,
                    activo INTEGER DEFAULT 1,
                    telefono TEXT,
                    email TEXT,
                    direccion TEXT,
                    fecha_nacimiento DATE,
                    curp TEXT,
                    rfc TEXT,
                    nss TEXT,
                    contacto_emergencia TEXT,
                    telefono_emergencia TEXT,
                    observaciones TEXT
                )
            ''')
            
            # ==============================================
            # 20. TABLA DE ASISTENCIAS
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS asistencias (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    personal_id INTEGER NOT NULL,
                    fecha DATE NOT NULL,
                    hora_entrada TIME,
                    hora_salida TIME,
                    horas_trabajadas DECIMAL(4,2),
                    estado TEXT DEFAULT 'PRESENTE',
                    observaciones TEXT,
                    FOREIGN KEY (personal_id) REFERENCES personal(id)
                )
            ''')
            
            # ==============================================
            # 21. TABLA DE TARJETAS DE FIDELIDAD
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tarjetas_fidelidad (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cliente_id INTEGER NOT NULL,
                    codigo_tarjeta TEXT UNIQUE NOT NULL,
                    fecha_emision DATE NOT NULL,
                    fecha_expiracion DATE,
                    estado TEXT DEFAULT 'ACTIVA',
                    puntos_acumulados INTEGER DEFAULT 0,
                    nivel TEXT DEFAULT 'BASICO',
                    fecha_ultimo_uso DATE,
                    usuario_emision TEXT,
                    observaciones TEXT,
                    FOREIGN KEY (cliente_id) REFERENCES clientes(id)
                )
            ''')
            
            # ==============================================
            # 22. TABLA DE CONFIGURACIÓN DEL SISTEMA
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS configuracion (
                    clave TEXT PRIMARY KEY,
                    valor TEXT,
                    descripcion TEXT,
                    editable INTEGER DEFAULT 1,
                    categoria TEXT DEFAULT 'General',
                    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # ==============================================
            # 23. TABLA DE AUDITORÍA/LOGS
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    usuario TEXT,
                    modulo TEXT NOT NULL,
                    accion TEXT NOT NULL,
                    detalles TEXT,
                    ip TEXT,
                    user_agent TEXT
                )
            ''')
            
            # ==============================================
            # 24. TABLA DE RENDIMIENTO DE POLLO
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS rendimiento_pollo (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    producto_pollo_id INTEGER NOT NULL UNIQUE,
                    precio_kg DECIMAL(10,2) NOT NULL,
                    kg_totales DECIMAL(10,2) NOT NULL,
                    kg_por_pollo DECIMAL(10,2) NOT NULL,
                    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    usuario TEXT,
                    FOREIGN KEY (producto_pollo_id) REFERENCES productos(id)
                )
            ''')
            
            # ==============================================
            # 25. TABLA DE RENDIMIENTO DERIVADOS
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS rendimiento_derivados (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    producto_pollo_id INTEGER NOT NULL,
                    producto_derivado_id INTEGER NOT NULL,
                    porcentaje_rendimiento DECIMAL(5,2) NOT NULL,
                    es_subproducto BOOLEAN DEFAULT 0,
                    producto_padre_id INTEGER,
                    fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (producto_pollo_id) REFERENCES productos(id),
                    FOREIGN KEY (producto_derivado_id) REFERENCES productos(id),
                    FOREIGN KEY (producto_padre_id) REFERENCES productos(id),
                    UNIQUE(producto_pollo_id, producto_derivado_id)
                )
            ''')
            
            # ==============================================
            # 26. TABLA DE INVENTARIO SUBPRODUCTOS
            # ==============================================
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS inventario_subproductos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    compra_pollo_id INTEGER,
                    producto_id INTEGER NOT NULL,
                    cantidad REAL NOT NULL,
                    costo_unitario REAL NOT NULL,
                    fecha_creacion TEXT NOT NULL,
                    usuario TEXT NOT NULL,
                    lote TEXT,
                    FOREIGN KEY (compra_pollo_id) REFERENCES compras_pollo(id),
                    FOREIGN KEY (producto_id) REFERENCES productos(id)
                )
            ''')
            
            # ==============================================
            # INSERTAR DATOS INICIALES
            # ==============================================
            
            # Configuración básica del sistema
            configuraciones = [
                ('iva', '0.16', 'Porcentaje de IVA', 'Sistema'),
                ('tema', 'Oscuro', 'Tema de la interfaz', 'Interfaz'),
                ('empresa_nombre', 'Surtidora de Pollo Juanis', 'Nombre de la empresa', 'Empresa'),
                ('empresa_telefono', '', 'Teléfono de la empresa', 'Empresa'),
                ('empresa_direccion', '', 'Dirección de la empresa', 'Empresa'),
                ('puntos_por_dolar', '1', 'Puntos ganados por cada dólar gastado', 'Fidelidad'),
                ('tasa_puntos', '100', 'Valor en pesos de 100 puntos', 'Fidelidad'),
                ('requerir_admin', 'True', 'Requerir usuario admin para funciones críticas', 'Seguridad'),
                ('whatsapp_numero', '+525659274265', 'Número de WhatsApp para notificaciones', 'Comunicación'),
                ('impuesto_por_defecto', '16', 'Porcentaje de impuesto por defecto', 'Sistema'),
                ('moneda', 'MXN', 'Símbolo de moneda', 'Sistema'),
                ('logo_empresa', 'logo.png', 'Ruta del logo de la empresa', 'Empresa'),
                ('backup_automatico', 'True', 'Realizar backup automático', 'Sistema')
            ]
            
            for clave, valor, descripcion, categoria in configuraciones:
                cursor.execute('''
                    INSERT OR IGNORE INTO configuracion (clave, valor, descripcion, categoria)
                    VALUES (?, ?, ?, ?)
                ''', (clave, valor, descripcion, categoria))
            
            # Usuarios por defecto
            usuarios = [
                ('admin', 'admin123', 'Administrador Principal', 'admin', 'ventas,clientes,productos,caja,reportes,gastos,personal,configuracion,tarjetas'),
                ('cajero', 'cajero123', 'Cajero Principal', 'cajero', 'ventas,caja'),
                ('vendedor', 'vendedor123', 'Vendedor General', 'vendedor', 'ventas,clientes')
            ]
            
            for usuario, contrasena, nombre, rol, modulos in usuarios:
                cursor.execute('''
                    INSERT OR IGNORE INTO usuarios (usuario, contrasena, nombre, rol, modulos_permitidos)
                    VALUES (?, ?, ?, ?, ?)
                ''', (usuario, contrasena, nombre, rol, modulos))
            
            # Categorías básicas
            categorias = [
                ('Pollo y Derivados', 'Productos relacionados con pollo'),
                ('Carnes', 'Diferentes tipos de carnes'),
                ('Abarrotes', 'Productos de abarrotes básicos'),
                ('Lácteos', 'Leche, queso, crema, etc.'),
                ('Frutas y Verduras', 'Frutas y verduras frescas'),
                ('Limpieza', 'Productos de limpieza del hogar'),
                ('Bebidas', 'Refrescos, jugos, agua, etc.')
            ]
            
            for nombre, descripcion in categorias:
                cursor.execute('''
                    INSERT OR IGNORE INTO categorias (nombre, descripcion)
                    VALUES (?, ?)
                ''', (nombre, descripcion))
            
            # Productos básicos de pollo
            productos_pollo = [
                ('Pollo Entero', 'Pollo completo para cortar', 85.00, 70.00, 'kg', 'Pollo y Derivados'),
                ('Pechuga de Pollo', 'Pechuga de pollo sin hueso', 120.00, 95.00, 'kg', 'Pollo y Derivados'),
                ('Muslo de Pollo', 'Muslo de pollo con hueso', 90.00, 75.00, 'kg', 'Pollo y Derivados'),
                ('Ala de Pollo', 'Alas de pollo', 70.00, 55.00, 'kg', 'Pollo y Derivados'),
                ('Milanesa de Pollo', 'Milanesa de pollo empanizada', 110.00, 85.00, 'kg', 'Pollo y Derivados'),
                ('Huevo Blanco', 'Huevo blanco grade', 45.00, 35.00, 'pza', 'Pollo y Derivados')
            ]
            
            for nombre, descripcion, precio, costo, unidad, categoria in productos_pollo:
                cursor.execute('''
                    INSERT OR IGNORE INTO productos (nombre, descripcion, precio, costo, unidad, categoria, existencia)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (nombre, descripcion, precio, costo, unidad, categoria, 100.0))
            
            # Caja por defecto
            cursor.execute('''
                INSERT OR IGNORE INTO cajas (nombre, ubicacion, fondo_inicial, saldo_actual, estado)
                VALUES (?, ?, ?, ?, ?)
            ''', ('Caja Principal', 'Mostrador principal', 1000.00, 1000.00, 'CERRADA'))

            # Sucursal por defecto (se agregan más desde Configuración)
            cursor.execute('''
                INSERT OR IGNORE INTO sucursales (id, nombre, direccion, activa, es_matriz)
                VALUES (1, 'Principal', 'Dirección principal', 1, 1)
            ''')
            
            self.conexion.commit()
            print("✅ Base de datos inicializada correctamente con todas las tablas")
            
            # Verificar estructura final
            self.verificar_estructura_completa()
            
            return True
            
        except sqlite3.Error as e:
            print(f"❌ Error al inicializar la BD: {e}")
            self.mostrar_mensaje("Error", f"No se pudo inicializar la base de datos: {str(e)}")
            return False

    def verificar_estructura_completa(self):
        """Verifica que todas las tablas se hayan creado correctamente"""
        try:
            cursor = self.conexion.cursor()
            
            # Lista de todas las tablas que deben existir
            tablas_requeridas = [
                'usuarios', 'productos', 'componentes_producto', 'categorias',
                'clientes', 'proveedores', 'ventas', 'detalles_venta',
                'movimientos_inventario', 'transferencias_inventario', 'compras',
                'detalles_compra', 'compras_pollo', 'gastos', 'puntos',
                'historico_puntos', 'movimientos_caja', 'cajas', 'personal',
                'asistencias', 'tarjetas_fidelidad', 'configuracion', 'logs',
                'rendimiento_pollo', 'rendimiento_derivados', 'inventario_subproductos'
            ]
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tablas_existentes = [tabla[0] for tabla in cursor.fetchall()]
            
            print("\n=== VERIFICACIÓN DE ESTRUCTURA DE BD ===")
            for tabla in tablas_requeridas:
                if tabla in tablas_existentes:
                    print(f"✅ {tabla}")
                else:
                    print(f"❌ {tabla} - FALTANTE")
            
            print(f"Total tablas requeridas: {len(tablas_requeridas)}")
            print(f"Total tablas existentes: {len(tablas_existentes)}")
            print("=== FIN VERIFICACIÓN ===\n")
            
        except Exception as e:
            print(f"Error en verificación: {e}")

    def obtener_icono(self, nombre_icono: str) -> QIcon:
        """Obtiene un icono desde la carpeta de recursos o usa iconos del sistema como fallback."""
        # Primero intentar cargar desde archivo en el directorio actual
        if os.path.exists(nombre_icono):
            return QIcon(nombre_icono)
        
        # Intentar en subcarpeta 'icons' si existe
        ruta_icono = os.path.join('icons', nombre_icono)
        if os.path.exists(ruta_icono):
            return QIcon(ruta_icono)
        
        # Si no existe el archivo, usar iconos del sistema como fallback
        iconos_sistema = {
            "search.png": QStyle.SP_FileDialogContentsView,
            "filter.png": QStyle.SP_FileDialogDetailedView,
            "add.png": QStyle.SP_FileDialogNewFolder,
            "edit.png": QStyle.SP_FileDialogContentsView,
            "delete.png": QStyle.SP_TrashIcon,
            "payment.png": QStyle.SP_DialogApplyButton,
            "refresh.png": QStyle.SP_BrowserReload,
            "list.png": QStyle.SP_FileDialogListView,
            "transfer.png": QStyle.SP_FileDialogBack,
            "save.png": QStyle.SP_DialogSaveButton,
            "cancel.png": QStyle.SP_DialogCancelButton,
            "print.png": QStyle.SP_ComputerIcon,
            "config.png": QStyle.SP_ComputerIcon
        }
        
        if nombre_icono in iconos_sistema:
            return self.style().standardIcon(iconos_sistema[nombre_icono])
        
        # Icono por defecto si no se encuentra ninguno
        print(f"Advertencia: Icono '{nombre_icono}' no encontrado, usando icono por defecto")
        return QIcon()

    def mostrar_mensaje(self, titulo: str, mensaje: str, 
                       icono=QMessageBox.Information, 
                       botones=QMessageBox.Ok) -> int:
        """Muestra un mensaje al usuario de forma segura."""
        try:
            msg = QMessageBox(self)
            msg.setWindowTitle(titulo)
            msg.setText(mensaje)
            msg.setIcon(icono)
            msg.setStandardButtons(botones)
            return msg.exec_()
        except Exception as e:
            print(f"Error al mostrar mensaje: {e}")
            # Fallback: mostrar en consola
            print(f"[{titulo}] {mensaje}")
            return QMessageBox.Ok

    def ejecutar_consulta(self, consulta: str, parametros: tuple = None) -> Optional[sqlite3.Cursor]:
        """Ejecuta una consulta SQL de forma segura."""
        try:
            cursor = self.conexion.cursor()
            if parametros:
                cursor.execute(consulta, parametros)
            else:
                cursor.execute(consulta)
            return cursor
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error BD", f"Error en consulta: {str(e)}")
            return None

    def insertar_registro(self, tabla: str, datos: Dict[str, Any]) -> Optional[int]:
        """Inserta un registro en una tabla."""
        try:
            columnas = ', '.join(datos.keys())
            placeholders = ', '.join(['?' for _ in datos])
            valores = list(datos.values())
            
            consulta = f"INSERT INTO {tabla} ({columnas}) VALUES ({placeholders})"
            cursor = self.ejecutar_consulta(consulta, valores)
            
            if cursor:
                self.conexion.commit()
                return cursor.lastrowid
            return None
        except sqlite3.Error as e:
            self.conexion.rollback()
            self.mostrar_mensaje("Error BD", f"Error al insertar: {str(e)}")
            return None

    def actualizar_registro(self, tabla: str, datos: Dict[str, Any], where: str, where_params: tuple = None) -> bool:
        """Actualiza un registro en una tabla."""
        try:
            sets = ', '.join([f"{k} = ?" for k in datos.keys()])
            valores = list(datos.values())
            
            if where_params:
                valores.extend(where_params)
            
            consulta = f"UPDATE {tabla} SET {sets} WHERE {where}"
            cursor = self.ejecutar_consulta(consulta, valores)
            
            if cursor:
                self.conexion.commit()
                return cursor.rowcount > 0
            return False
        except sqlite3.Error as e:
            self.conexion.rollback()
            self.mostrar_mensaje("Error BD", f"Error al actualizar: {str(e)}")
            return False

    def set_usuario_actual(self, usuario: str, rol: str):
        """Establece el usuario actual para el módulo"""
        self.usuario_actual = usuario
        self.rol_usuario = rol
        self.sesion_iniciada = True

    def obtener_usuario_actual(self) -> str:
        """Obtiene el usuario actual para registrar en movimientos"""
        return self.usuario_actual if self.usuario_actual else "Sistema"

    def registrar_actualizacion(self, tipo_evento: str, detalles=None, usuario=None):
        """Registra actualizaciones del módulo."""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            usuario_info = usuario if usuario else self.usuario_actual or "Sistema"
            modulo = self.__class__.__name__
            
            mensaje = f"[{timestamp}] [{usuario_info}] [{modulo}] Evento: {tipo_evento}"
            
            if detalles:
                if isinstance(detalles, dict):
                    detalles_str = ", ".join([f"{k}:{v}" for k, v in detalles.items()])
                    mensaje += f" - {detalles_str}"
                else:
                    mensaje += f" - {detalles}"
            
            print(mensaje)
            
            # Guardar en archivo de log
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
                
            with open(f"{log_dir}/sistema_actualizaciones.log", "a", encoding="utf-8") as f:
                f.write(mensaje + "\n")
                
        except Exception as e:
            print(f"Error al registrar actualización: {e}")

    def limpiar(self):
        """Limpia recursos del módulo"""
        self._callbacks_actualizacion.clear()

    def closeEvent(self, event):
        """Maneja el cierre del módulo"""
        self.limpiar()
        event.accept()
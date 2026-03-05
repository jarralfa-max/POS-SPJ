# modulos/clientes.py
import os
import re
from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, 
                            QPushButton, QTableWidget, QTableWidgetItem, QMessageBox,
                            QDialog, QFormLayout, QSpinBox, QDoubleSpinBox, QCheckBox,
                            QTabWidget, QAbstractItemView, QComboBox)
from PyQt5.QtCore import Qt, QRandomGenerator
from PyQt5.QtGui import QPixmap, QColor, QIcon
import sqlite3
from .base import ModuloBase


class ModuloClientes(ModuloBase): 
    def __init__(self, conexion, main_window=None):
        super().__init__(conexion, parent=main_window)
        self.conexion = conexion
        self.main_window = main_window
        self.cliente_actual = None
        self.filtro_activo = True
        self.init_ui()
        self.conectar_eventos()

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

        title = QLabel("Gestión de Clientes")
        title.setObjectName("tituloPrincipal")
        header_layout.addWidget(title)
        header_layout.addStretch()
        layout.addLayout(header_layout)

        # --- Barra de herramientas ---
        toolbar = QHBoxLayout()
        self.busqueda_cliente = QLineEdit()
        self.busqueda_cliente.setPlaceholderText("Buscar por nombre, teléfono, ID o código QR...")
        self.btn_buscar_cliente = QPushButton()
        self.btn_buscar_cliente.setIcon(self.obtener_icono("search.png"))
        self.btn_buscar_cliente.setToolTip("Buscar Cliente")
        
        self.combo_filtro = QComboBox()
        self.combo_filtro.addItems(["Activos", "Todos", "Inactivos"])
        self.combo_filtro.setCurrentText("Activos")
        
        self.btn_nuevo_cliente = QPushButton("Nuevo Cliente")
        self.btn_nuevo_cliente.setIcon(self.obtener_icono("add.png"))
        
        toolbar.addWidget(QLabel("Buscar:"))
        toolbar.addWidget(self.busqueda_cliente)
        toolbar.addWidget(self.btn_buscar_cliente)
        toolbar.addSpacing(20)
        toolbar.addWidget(QLabel("Filtro:"))
        toolbar.addWidget(self.combo_filtro)
        toolbar.addStretch()
        toolbar.addWidget(self.btn_nuevo_cliente)
        layout.addLayout(toolbar)

        # --- Tabla de Clientes ---
        self.tabla_clientes = QTableWidget()
        self.tabla_clientes.setColumnCount(9)
        self.tabla_clientes.setHorizontalHeaderLabels([
            "ID", "Nombre", "Apellido", "Teléfono", "Puntos", "Nivel", "Saldo", "Límite Crédito", "Estado"
        ])
        self.tabla_clientes.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_clientes.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tabla_clientes.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.tabla_clientes)

        # --- Barra de estado/botones de acción ---
        acciones_layout = QHBoxLayout()
        self.btn_editar_cliente = QPushButton("Editar")
        self.btn_editar_cliente.setIcon(self.obtener_icono("edit.png"))
        self.btn_editar_cliente.setEnabled(False)
        
        self.btn_eliminar_cliente = QPushButton("Eliminar")
        self.btn_eliminar_cliente.setIcon(self.obtener_icono("delete.png"))
        self.btn_eliminar_cliente.setEnabled(False)
        
        self.btn_ver_historial = QPushButton("Historial")
        self.btn_ver_historial.setIcon(self.obtener_icono("history.png"))
        self.btn_ver_historial.setEnabled(False)
        
        self.btn_asignar_tarjeta = QPushButton("Asignar Tarjeta")
        self.btn_asignar_tarjeta.setIcon(self.obtener_icono("card.png"))
        self.btn_asignar_tarjeta.setEnabled(False)

        # v9: Botón ver tarjetas y gestión completa
        self.btn_ver_tarjetas = QPushButton("💳 Tarjetas")
        self.btn_ver_tarjetas.setEnabled(False)
        
        acciones_layout.addWidget(self.btn_editar_cliente)
        acciones_layout.addWidget(self.btn_eliminar_cliente)
        acciones_layout.addWidget(self.btn_ver_historial)
        acciones_layout.addWidget(self.btn_asignar_tarjeta)
        acciones_layout.addWidget(self.btn_ver_tarjetas)
        acciones_layout.addStretch()
        layout.addLayout(acciones_layout)

        self.setLayout(layout)

        # --- Conexiones ---
        self.busqueda_cliente.returnPressed.connect(self.buscar_clientes)
        self.btn_buscar_cliente.clicked.connect(self.buscar_clientes)
        self.combo_filtro.currentIndexChanged.connect(self.cargar_clientes)
        self.btn_nuevo_cliente.clicked.connect(self.nuevo_cliente)
        self.btn_editar_cliente.clicked.connect(self.editar_cliente)
        self.btn_eliminar_cliente.clicked.connect(self.eliminar_cliente)
        self.btn_ver_historial.clicked.connect(self.ver_historial_cliente)
        self.btn_asignar_tarjeta.clicked.connect(self.asignar_tarjeta_cliente)
        self.btn_ver_tarjetas.clicked.connect(self.ver_tarjetas_cliente)
        self.tabla_clientes.itemSelectionChanged.connect(self.actualizar_botones)

        # --- Inicialización ---
        self.cargar_clientes()
        
    def conectar_eventos(self):
        """Conectar a los eventos del sistema"""
        if hasattr(self.main_window, 'registrar_evento'):
            # Registrar handlers para eventos de otros módulos
            self.main_window.registrar_evento('venta_realizada', self.on_venta_realizada)
            self.main_window.registrar_evento('producto_actualizado', self.on_datos_actualizados)
            self.main_window.registrar_evento('gasto_creado', self.on_datos_actualizados)

    def desconectar_eventos(self):
        """Desconectar eventos al cerrar el módulo"""
        if hasattr(self.main_window, 'desregistrar_evento'):
            self.main_window.desregistrar_evento('venta_realizada', self.on_venta_realizada)
            self.main_window.desregistrar_evento('producto_actualizado', self.on_datos_actualizados)
            self.main_window.desregistrar_evento('gasto_creado', self.on_datos_actualizados)

    def on_venta_realizada(self, datos):
        """Actualizar cuando se realiza una venta (para puntos del cliente)"""
        if datos and 'cliente_id' in datos and datos['cliente_id']:
            print(f"Cliente {datos['cliente_id']} realizó una compra")
            # Si estamos viendo el historial de este cliente, actualizarlo
            if (hasattr(self, 'dialogo_historial') and 
                self.dialogo_historial and 
                self.dialogo_historial.id_cliente == datos['cliente_id']):
                self.dialogo_historial.cargar_historial_compras()

    def on_datos_actualizados(self, datos):
        """Actualizar datos generales cuando otros módulos cambian información"""
        print("Datos actualizados en módulo clientes")
        # Podrías actualizar información específica si es necesario

    def obtener_icono(self, nombre_icono):
        """Obtiene un icono de la carpeta de recursos o usa uno por defecto."""
        if os.path.exists(f"iconos/{nombre_icono}"):
            return QIcon(f"iconos/{nombre_icono}")
        return QIcon.fromTheme("document")

    def mostrar_mensaje(self, titulo, mensaje, icono=QMessageBox.Information, botones=QMessageBox.Ok):
        """Muestra un mensaje al usuario."""
        msg = QMessageBox(self)
        msg.setWindowTitle(titulo)
        msg.setText(mensaje)
        msg.setIcon(icono)
        msg.setStandardButtons(botones)
        return msg.exec_()

    def cargar_clientes(self):
        """Carga los clientes en la tabla según el filtro seleccionado."""
        try:
            cursor = self.conexion.cursor()
            
            filtro = self.combo_filtro.currentText()
            if filtro == "Activos":
                condicion = "WHERE activo = 1"
                params = ()
            elif filtro == "Inactivos":
                condicion = "WHERE activo = 0"
                params = ()
            else:
                condicion = ""
                params = ()

            query = f"""
                SELECT id, nombre, apellido, telefono, puntos, nivel_fidelidad, 
                       saldo, limite_credito, activo
                FROM clientes
                {condicion}
                ORDER BY nombre
            """
            
            cursor.execute(query, params)
            clientes = cursor.fetchall()

            self.tabla_clientes.setRowCount(len(clientes))
            for row, cliente in enumerate(clientes):
                for col, valor in enumerate(cliente):
                    if col == 8:  # Columna de estado
                        estado_texto = "Activo" if valor == 1 else "Inactivo"
                        item = QTableWidgetItem(estado_texto)
                        if valor != 1:
                            item.setForeground(QColor('red'))
                        self.tabla_clientes.setItem(row, col, item)
                    elif col in [6, 7]:  # Saldo y Límite de crédito
                        item = QTableWidgetItem(f"${valor:,.2f}" if valor is not None else "$0.00")
                        self.tabla_clientes.setItem(row, col, item)
                    else:
                        self.tabla_clientes.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))

        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al cargar clientes: {str(e)}", QMessageBox.Critical)

    def buscar_clientes(self):
        """Busca clientes según el texto ingresado."""
        texto = self.busqueda_cliente.text().strip()
        if not texto:
            self.cargar_clientes()
            return

        try:
            cursor = self.conexion.cursor()
            
            filtro = self.combo_filtro.currentText()
            condicion_activo = ""
            if filtro == "Activos":
                condicion_activo = "AND c.activo = 1"
            elif filtro == "Inactivos":
                condicion_activo = "AND c.activo = 0"

            # Determinar el tipo de búsqueda
            if texto.startswith("CLI-") or texto.startswith("QR-"):
                # Búsqueda por código QR o ID
                consulta = f"""
                    SELECT c.id, c.nombre, c.apellido, c.telefono, 
                           c.puntos, c.nivel_fidelidad, c.saldo, c.limite_credito, c.activo
                    FROM clientes c
                    WHERE (c.codigo_qr = ? OR c.id = ?)
                    {condicion_activo}
                """
                params = (texto, texto.split('-')[-1] if '-' in texto else texto)
            else:
                # Búsqueda por nombre, apellido, teléfono o ID (parcial)
                consulta = f"""
                    SELECT c.id, c.nombre, c.apellido, c.telefono, 
                           c.puntos, c.nivel_fidelidad, c.saldo, c.limite_credito, c.activo
                    FROM clientes c
                    WHERE (c.nombre LIKE ? OR c.apellido LIKE ? OR c.telefono LIKE ? OR c.id = ?)
                    {condicion_activo}
                """
                params = (f"%{texto}%", f"%{texto}%", f"%{texto}%", texto)

            cursor.execute(consulta, params)
            clientes = cursor.fetchall()

            self.tabla_clientes.setRowCount(len(clientes))
            for row, cliente in enumerate(clientes):
                for col, valor in enumerate(cliente):
                    if col == 8:  # Columna de estado
                        estado_texto = "Activo" if valor == 1 else "Inactivo"
                        item = QTableWidgetItem(estado_texto)
                        if valor != 1:
                            item.setForeground(QColor('red'))
                        self.tabla_clientes.setItem(row, col, item)
                    elif col in [6, 7]:  # Saldo y Límite de crédito
                        item = QTableWidgetItem(f"${valor:,.2f}" if valor is not None else "$0.00")
                        self.tabla_clientes.setItem(row, col, item)
                    else:
                        self.tabla_clientes.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))

        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error en búsqueda: {str(e)}", QMessageBox.Critical)

    def nuevo_cliente(self):
        """Abre el diálogo para crear un nuevo cliente."""
        dialogo = DialogoCliente(self.conexion, self)
        if dialogo.exec_() == QDialog.Accepted:
            self.cargar_clientes()
            # NOTIFICAR EVENTO
            if hasattr(self.main_window, 'notificar_evento'):
                self.main_window.notificar_evento('cliente_creado', {
                    'modulo': 'clientes',
                    'accion': 'crear'
                })
                
    def editar_cliente(self):
        """Abre el diálogo para editar un cliente seleccionado."""
        fila_seleccionada = self.tabla_clientes.currentRow()
        if fila_seleccionada < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un cliente para editar.")
            return

        try:
            id_cliente = int(self.tabla_clientes.item(fila_seleccionada, 0).text())
            cursor = self.conexion.cursor()
            cursor.execute("SELECT * FROM clientes WHERE id = ?", (id_cliente,))
            cliente_data = cursor.fetchone()
            
            if cliente_data:
                columnas = [description[0] for description in cursor.description]
                cliente_dict = dict(zip(columnas, cliente_data))
                
                dialogo = DialogoCliente(self.conexion, self, cliente_dict)
                if dialogo.exec_() == QDialog.Accepted:
                    self.cargar_clientes()
                    # NOTIFICAR EVENTO
                    if hasattr(self.main_window, 'notificar_evento'):
                        self.main_window.notificar_evento('cliente_actualizado', {
                            'id': id_cliente,
                            'modulo': 'clientes'
                        })
            else:
                self.mostrar_mensaje("Error", "Cliente no encontrado.")

        except ValueError:
            self.mostrar_mensaje("Error", "ID de cliente inválido.")
        except sqlite3.Error as e:
            self.mostrar_mensaje("Error", f"Error al cargar datos del cliente: {str(e)}", QMessageBox.Critical)

    def eliminar_cliente(self):
        """Elimina un cliente (lógicamente, cambiando su estado a inactivo)."""
        fila_seleccionada = self.tabla_clientes.currentRow()
        if fila_seleccionada < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un cliente para eliminar.")
            return

        try:
            id_cliente = int(self.tabla_clientes.item(fila_seleccionada, 0).text())
            nombre_cliente = self.tabla_clientes.item(fila_seleccionada, 1).text()
            
            respuesta = self.mostrar_mensaje(
                "Confirmar Eliminación",
                f"¿Está seguro que desea desactivar al cliente '{nombre_cliente}'?\n\n"
                f"Esto lo marcará como inactivo, no se eliminarán los datos permanentemente.",
                QMessageBox.Question,
                QMessageBox.Yes | QMessageBox.No
            )
            
            if respuesta == QMessageBox.Yes:
                cursor = self.conexion.cursor()
                cursor.execute("UPDATE clientes SET activo = 0, fecha_inactivacion = date('now') WHERE id = ?", (id_cliente,))
                self.conexion.commit()
                self.mostrar_mensaje("Éxito", "Cliente desactivado correctamente.")
                self.cargar_clientes()
                # NOTIFICAR EVENTO
                if hasattr(self.main_window, 'notificar_evento'):
                    self.main_window.notificar_evento('cliente_eliminado', {
                        'id': id_cliente,
                        'modulo': 'clientes'
                    })
                    
        except ValueError:
            self.mostrar_mensaje("Error", "ID de cliente inválido.")
        except sqlite3.Error as e:
            self.conexion.rollback()
            self.mostrar_mensaje("Error", f"Error al desactivar cliente: {str(e)}", QMessageBox.Critical)

    def ver_historial_cliente(self):
        """Muestra el historial de un cliente (compras, puntos, créditos, etc.)."""
        fila_seleccionada = self.tabla_clientes.currentRow()
        if fila_seleccionada < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un cliente para ver su historial.")
            return

        try:
            id_cliente = int(self.tabla_clientes.item(fila_seleccionada, 0).text())
            nombre_cliente = self.tabla_clientes.item(fila_seleccionada, 1).text()
            apellido_cliente = self.tabla_clientes.item(fila_seleccionada, 2).text() if self.tabla_clientes.item(fila_seleccionada, 2) else ""
            nombre_completo = f"{nombre_cliente} {apellido_cliente}".strip()

            dialogo = DialogoHistorialCliente(self.conexion, id_cliente, nombre_completo, self)
            dialogo.exec_()

        except ValueError:
            self.mostrar_mensaje("Error", "ID de cliente inválido.")
        except Exception as e:
            self.mostrar_mensaje("Error", f"Error al abrir historial: {str(e)}", QMessageBox.Critical)

    def asignar_tarjeta_cliente(self):
        """Asigna una tarjeta libre al cliente seleccionado (v9)."""
        fila = self.tabla_clientes.currentRow()
        if fila < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un cliente para asignar una tarjeta.")
            return
        try:
            id_cliente     = int(self.tabla_clientes.item(fila, 0).text())
            nombre_cliente = self.tabla_clientes.item(fila, 1).text()

            from core.services.card_batch_engine import CardBatchEngine
            eng = CardBatchEngine(self.conexion, self.usuario_actual or "admin")

            dlg = _DialogoAsignarTarjetaCliente(
                id_cliente, nombre_cliente, self.conexion, self
            )
            if dlg.exec_() == QDialog.Accepted and dlg.tarjeta_id:
                res = eng.asignar_tarjeta(dlg.tarjeta_id, id_cliente,
                                           motivo="asignacion_desde_clientes")
                if res.exito:
                    self.mostrar_mensaje("Tarjeta Asignada",
                        f"Tarjeta asignada correctamente a {nombre_cliente}.")
                    self.cargar_clientes()
                else:
                    self.mostrar_mensaje("Error", res.mensaje, QMessageBox.Warning)
        except ImportError:
            self.mostrar_mensaje("Módulo no disponible",
                "CardBatchEngine no disponible. Actualice la base de datos a v14.")
        except Exception as exc:
            self.mostrar_mensaje("Error", str(exc), QMessageBox.Critical)

    def ver_tarjetas_cliente(self):
        """Muestra tarjetas asignadas, historial de asignaciones y opciones de bloqueo (v9)."""
        fila = self.tabla_clientes.currentRow()
        if fila < 0:
            self.mostrar_mensaje("Advertencia", "Seleccione un cliente.")
            return
        try:
            id_cliente     = int(self.tabla_clientes.item(fila, 0).text())
            nombre_cliente = self.tabla_clientes.item(fila, 1).text()
            dlg = _DialogoTarjetasCliente(id_cliente, nombre_cliente, self.conexion, self)
            dlg.exec_()
            self.cargar_clientes()
        except Exception as exc:
            self.mostrar_mensaje("Error", str(exc), QMessageBox.Critical)

    def actualizar_botones(self):
        """Habilita/deshabilita botones según la selección en la tabla."""
        seleccionado = len(self.tabla_clientes.selectedItems()) > 0
        self.btn_editar_cliente.setEnabled(seleccionado)
        self.btn_eliminar_cliente.setEnabled(seleccionado)
        self.btn_ver_historial.setEnabled(seleccionado)
        self.btn_asignar_tarjeta.setEnabled(seleccionado)
        if hasattr(self, 'btn_ver_tarjetas'):
            self.btn_ver_tarjetas.setEnabled(seleccionado)

    def actualizar_datos(self):
        """Actualiza los datos del módulo."""
        self.cargar_clientes()

   
    def registrar_actualizacion(self, tipo_evento, detalles=None, usuario=None):
        """
        Registra actualizaciones del módulo de clientes.
        
        Args:
            tipo_evento (str): Tipo de evento ('cliente_creado', 'cliente_actualizado', etc.)
            detalles (str/dict): Detalles específicos del evento
            usuario (str): Usuario que realizó la acción
        """
        try:
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            usuario_info = usuario if usuario else "Sistema"
            
            # Construir mensaje
            mensaje = f"[{timestamp}] [{usuario_info}] [CLIENTES] Evento: {tipo_evento}"
            
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
                
            with open(f"{log_dir}/clientes_actualizaciones.log", "a", encoding="utf-8") as f:
                f.write(mensaje + "\n")
                
        except Exception as e:
            print(f"Error al registrar actualización en clientes: {e}")

class DialogoCliente(QDialog):
    def __init__(self, conexion, parent=None, cliente_data=None):
        super().__init__(parent)
        self.conexion = conexion
        self.cliente_data = cliente_data
        self.setWindowTitle("Nuevo Cliente" if not cliente_data else "Editar Cliente")
        self.setFixedSize(400, 500)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()

        form_layout = QFormLayout()
        
        self.edit_nombre = QLineEdit()
        self.edit_apellido = QLineEdit()
        self.edit_telefono = QLineEdit()
        self.edit_telefono.setInputMask("9999999999;_")
        
        self.edit_puntos = QSpinBox()
        self.edit_puntos.setRange(0, 999999)
        self.edit_nivel = QLineEdit()
        self.edit_descuento = QDoubleSpinBox()
        self.edit_descuento.setRange(0.0, 100.0)
        self.edit_descuento.setSuffix(" %")
        
        self.edit_saldo = QDoubleSpinBox()
        self.edit_saldo.setRange(-999999.99, 999999.99)
        self.edit_saldo.setPrefix("$ ")
        self.edit_limite_credito = QDoubleSpinBox()
        self.edit_limite_credito.setRange(0.0, 999999.99)
        self.edit_limite_credito.setPrefix("$ ")
        
        self.chk_activo = QCheckBox("Activo")

        if self.cliente_data:
            self.edit_nombre.setText(self.cliente_data.get('nombre', ''))
            self.edit_apellido.setText(self.cliente_data.get('apellido', ''))
            self.edit_telefono.setText(self.cliente_data.get('telefono', ''))
            self.edit_puntos.setValue(self.cliente_data.get('puntos', 0))
            self.edit_nivel.setText(self.cliente_data.get('nivel_fidelidad', ''))
            self.edit_descuento.setValue(self.cliente_data.get('descuento', 0.0))
            self.edit_saldo.setValue(self.cliente_data.get('saldo', 0.0))
            self.edit_limite_credito.setValue(self.cliente_data.get('limite_credito', 0.0))
            self.chk_activo.setChecked(self.cliente_data.get('activo', 1) == 1)

        form_layout.addRow("Nombre*:", self.edit_nombre)
        form_layout.addRow("Apellido:", self.edit_apellido)
        form_layout.addRow("Teléfono:", self.edit_telefono)
        form_layout.addRow("Puntos:", self.edit_puntos)
        form_layout.addRow("Nivel Fidelidad:", self.edit_nivel)
        form_layout.addRow("Descuento (%):", self.edit_descuento)
        form_layout.addRow("Saldo Crédito:", self.edit_saldo)
        form_layout.addRow("Límite Crédito:", self.edit_limite_credito)
        form_layout.addRow(self.chk_activo)

        btn_layout = QHBoxLayout()
        self.btn_guardar = QPushButton("Guardar")
        self.btn_cancelar = QPushButton("Cancelar")
        btn_layout.addWidget(self.btn_guardar)
        btn_layout.addWidget(self.btn_cancelar)

        layout.addLayout(form_layout)
        layout.addLayout(btn_layout)
        self.setLayout(layout)

        self.btn_guardar.clicked.connect(self.guardar)
        self.btn_cancelar.clicked.connect(self.reject)

    def validar_formulario(self):
        """Valida los datos del formulario."""
        if not self.edit_nombre.text().strip():
            QMessageBox.warning(self, "Error", "El nombre es obligatorio.")
            return False
        
        telefono = self.edit_telefono.text().strip()
        if telefono and not re.fullmatch(r'\d{10}', telefono):
            QMessageBox.warning(self, "Error", "El teléfono debe tener 10 dígitos.")
            return False
            
        return True

    def generar_id_cliente(self):
        """Genera un ID de cliente único de 4 dígitos."""
        cursor = self.conexion.cursor()
        while True:
            nuevo_id = f"{QRandomGenerator.global_().bounded(1000, 10000)}"
            cursor.execute("SELECT COUNT(*) FROM clientes WHERE id = ?", (nuevo_id,))
            if cursor.fetchone()[0] == 0:
                return nuevo_id

    def guardar(self):
        """Guarda el cliente en la base de datos."""
        if not self.validar_formulario():
            return

        try:
            cursor = self.conexion.cursor()
            
            nombre = self.edit_nombre.text().strip()
            apellido = self.edit_apellido.text().strip() or None
            telefono = self.edit_telefono.text().strip() or None
            puntos = self.edit_puntos.value()
            nivel = self.edit_nivel.text().strip() or None
            descuento = self.edit_descuento.value()
            saldo = self.edit_saldo.value()
            limite_credito = self.edit_limite_credito.value()
            activo = 1 if self.chk_activo.isChecked() else 0

            if self.cliente_data:  # Editar
                id_cliente = self.cliente_data['id']
                cursor.execute("""
                    UPDATE clientes 
                    SET nombre = ?, apellido = ?, telefono = ?, puntos = ?, nivel_fidelidad = ?,
                        descuento = ?, saldo = ?, limite_credito = ?, activo = ?
                    WHERE id = ?
                """, (nombre, apellido, telefono, puntos, nivel, descuento, saldo, limite_credito, activo, id_cliente))
                
                self.conexion.commit()
                QMessageBox.information(self, "Éxito", "Cliente actualizado correctamente.")
                self.accept()
            else:  # Nuevo
                cursor.execute("""
                    SELECT COUNT(*) FROM clientes
                    WHERE nombre = ? AND apellido = ? AND telefono = ?
                """, (nombre, apellido, telefono))
                if cursor.fetchone()[0] > 0:
                    respuesta = QMessageBox.question(
                        self, "Cliente Existente",
                        "Ya existe un cliente con estos datos. ¿Desea crearlo de todos modos?",
                        QMessageBox.Yes | QMessageBox.No
                    )
                    if respuesta == QMessageBox.No:
                        return
                
                id_cliente = self.generar_id_cliente()
                cursor.execute("""
                    INSERT INTO clientes (id, nombre, apellido, telefono, puntos, nivel_fidelidad, 
                                        descuento, saldo, limite_credito, activo)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (id_cliente, nombre, apellido, telefono, puntos, nivel, descuento, saldo, limite_credito, activo))
                
                self.conexion.commit()
                QMessageBox.information(self, "Éxito", f"Cliente creado correctamente con ID: {id_cliente}")
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


class DialogoHistorialCliente(QDialog):
    def __init__(self, conexion, id_cliente, nombre_cliente, parent=None):
        super().__init__(parent)
        self.conexion = conexion
        self.id_cliente = id_cliente
        self.nombre_cliente = nombre_cliente
        self.setWindowTitle(f"Historial de {nombre_cliente}")
        self.resize(800, 600)
        self.init_ui()

    def init_ui(self):
        layout = QVBoxLayout()
        
        lbl_titulo = QLabel(f"Historial del Cliente: {self.nombre_cliente} (ID: {self.id_cliente})")
        lbl_titulo.setObjectName("tituloPrincipal")
        layout.addWidget(lbl_titulo)
        
        tabs = QTabWidget()
        
        # Pestaña de Compras
        self.tab_compras = QWidget()
        layout_compras = QVBoxLayout()
        self.tabla_compras = QTableWidget()
        self.tabla_compras.setColumnCount(5)
        self.tabla_compras.setHorizontalHeaderLabels(["Fecha", "Total", "Método Pago", "Puntos Ganados", "Detalles"])
        layout_compras.addWidget(self.tabla_compras)
        self.tab_compras.setLayout(layout_compras)
        tabs.addTab(self.tab_compras, "Compras")

        # Pestaña de Puntos
        self.tab_puntos = QWidget()
        layout_puntos = QVBoxLayout()
        self.tabla_puntos = QTableWidget()
        self.tabla_puntos.setColumnCount(5)
        self.tabla_puntos.setHorizontalHeaderLabels(["Fecha", "Tipo", "Puntos", "Saldo Actual", "Descripción"])
        layout_puntos.addWidget(self.tabla_puntos)
        self.tab_puntos.setLayout(layout_puntos)
        tabs.addTab(self.tab_puntos, "Historial de Puntos")

        # Pestaña de Créditos
        self.tab_creditos = QWidget()
        layout_creditos = QVBoxLayout()
        self.tabla_creditos = QTableWidget()
        self.tabla_creditos.setColumnCount(5)
        self.tabla_creditos.setHorizontalHeaderLabels(["Fecha", "Tipo", "Monto", "Descripción", "Usuario"])
        layout_creditos.addWidget(self.tabla_creditos)
        self.tab_creditos.setLayout(layout_creditos)
        tabs.addTab(self.tab_creditos, "Movimientos de Crédito")

        layout.addWidget(tabs)
        
        btn_cerrar = QPushButton("Cerrar")
        btn_cerrar.clicked.connect(self.close)
        layout.addWidget(btn_cerrar)
        
        self.setLayout(layout)
        
        self.cargar_historial_compras()
        self.cargar_historial_puntos()
        self.cargar_historial_creditos()

    def cargar_historial_compras(self):
        """Carga el historial de compras del cliente."""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("""
                SELECT fecha, total, metodo_pago, puntos_ganados 
                FROM ventas 
                WHERE cliente_id = ? 
                ORDER BY fecha DESC
            """, (self.id_cliente,))
            ventas = cursor.fetchall()
            
            self.tabla_compras.setRowCount(len(ventas))
            for row, venta in enumerate(ventas):
                for col, valor in enumerate(venta):
                    if col == 1:  # Total
                        self.tabla_compras.setItem(row, col, QTableWidgetItem(f"${valor:.2f}"))
                    elif col == 3:  # Puntos
                        self.tabla_compras.setItem(row, col, QTableWidgetItem(str(valor) if valor else "0"))
                    else:
                        self.tabla_compras.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))
                        
        except sqlite3.Error as e:
            QMessageBox.critical(self, "Error", f"Error al cargar historial de compras: {str(e)}")

    def cargar_historial_puntos(self):
        """Carga el historial de puntos del cliente."""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("""
                SELECT fecha, tipo, puntos, saldo_actual, descripcion 
                FROM historico_puntos 
                WHERE id_cliente = ? 
                ORDER BY fecha DESC
            """, (self.id_cliente,))
            puntos = cursor.fetchall()
            
            self.tabla_puntos.setRowCount(len(puntos))
            for row, punto in enumerate(puntos):
                for col, valor in enumerate(punto):
                    self.tabla_puntos.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))
                        
        except sqlite3.Error as e:
            QMessageBox.critical(self, "Error", f"Error al cargar historial de puntos: {str(e)}")

    def cargar_historial_creditos(self):
        """Carga el historial de movimientos de crédito del cliente."""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("""
                SELECT fecha, tipo, monto, descripcion, usuario 
                FROM movimientos_credito 
                WHERE cliente_id = ? 
                ORDER BY fecha DESC
            """, (self.id_cliente,))
            creditos = cursor.fetchall()
            
            self.tabla_creditos.setRowCount(len(creditos))
            for row, credito in enumerate(creditos):
                for col, valor in enumerate(credito):
                    if col == 2:  # Monto
                        self.tabla_creditos.setItem(row, col, QTableWidgetItem(f"${valor:.2f}"))
                    else:
                        self.tabla_creditos.setItem(row, col, QTableWidgetItem(str(valor) if valor is not None else ""))
                        
        except sqlite3.Error as e:
            QMessageBox.critical(self, "Error", f"Error al cargar historial de créditos: {str(e)}")

# ── v9: Diálogos Tarjetas desde Clientes ─────────────────────────────────────

class _DialogoAsignarTarjetaCliente(QDialog):
    """Selecciona una tarjeta libre para asignar al cliente."""

    def __init__(self, cliente_id, cliente_nombre, conexion, parent=None):
        super().__init__(parent)
        self.cliente_id   = cliente_id
        self.cliente_nombre = cliente_nombre
        self.conexion     = conexion
        self.tarjeta_id   = None
        self.setWindowTitle(f"Asignar Tarjeta — {cliente_nombre}")
        self.setMinimumWidth(440)
        self.setModal(True)
        self._build_ui()

    def _build_ui(self):
        from PyQt5.QtWidgets import (
            QVBoxLayout, QLabel, QComboBox, QHBoxLayout,
            QPushButton, QGroupBox, QLineEdit, QFormLayout
        )
        layout = QVBoxLayout(self)
        layout.setSpacing(12)
        layout.setContentsMargins(16, 16, 16, 16)

        layout.addWidget(QLabel(f"Asignar tarjeta libre a: <b>{self.cliente_nombre}</b>"))

        # Tarjetas libres
        self.combo_tarjeta = QComboBox()
        self._cargar_tarjetas_libres()
        layout.addWidget(QLabel("Tarjeta disponible:"))
        layout.addWidget(self.combo_tarjeta)

        # O ingresar número manualmente
        grp = QGroupBox("O buscar por número")
        lay_g = QHBoxLayout(grp)
        self.txt_numero = QLineEdit()
        self.txt_numero.setPlaceholderText("Número de tarjeta…")
        btn_buscar = QPushButton("Buscar")
        btn_buscar.clicked.connect(self._buscar_numero)
        lay_g.addWidget(self.txt_numero)
        lay_g.addWidget(btn_buscar)
        layout.addWidget(grp)

        self.lbl_estado_busqueda = QLabel("")
        layout.addWidget(self.lbl_estado_busqueda)

        # Botones
        btns = QHBoxLayout()
        btn_ok  = QPushButton("✅ Asignar")
        btn_ok.clicked.connect(self._confirmar)
        btn_cancel = QPushButton("Cancelar")
        btn_cancel.clicked.connect(self.reject)
        btns.addStretch()
        btns.addWidget(btn_ok)
        btns.addWidget(btn_cancel)
        layout.addLayout(btns)

    def _cargar_tarjetas_libres(self):
        try:
            rows = self.conexion.execute(
                "SELECT id, numero, COALESCE(nivel,'Bronce') FROM tarjetas_fidelidad "
                "WHERE estado IN ('libre','impresa','generada') ORDER BY id LIMIT 100"
            ).fetchall()
            self.combo_tarjeta.clear()
            for tid, num, nivel in rows:
                self.combo_tarjeta.addItem(f"{num} [{nivel}]", tid)
        except Exception:
            pass

    def _buscar_numero(self):
        numero = self.txt_numero.text().strip()
        if not numero:
            return
        row = self.conexion.execute(
            "SELECT id, numero, estado FROM tarjetas_fidelidad WHERE numero=? OR codigo_qr=?",
            (numero, numero)
        ).fetchone()
        if not row:
            self.lbl_estado_busqueda.setText("❌ No encontrada")
        elif row[2] == "asignada":
            self.lbl_estado_busqueda.setText(f"⚠ Tarjeta {row[1]} ya está asignada")
        elif row[2] == "bloqueada":
            self.lbl_estado_busqueda.setText("🔒 Tarjeta bloqueada")
        else:
            # Preseleccionar en combo si existe, si no agregar
            found = False
            for i in range(self.combo_tarjeta.count()):
                if self.combo_tarjeta.itemData(i) == row[0]:
                    self.combo_tarjeta.setCurrentIndex(i)
                    found = True
                    break
            if not found:
                self.combo_tarjeta.addItem(f"{row[1]} [búsqueda]", row[0])
                self.combo_tarjeta.setCurrentIndex(self.combo_tarjeta.count() - 1)
            self.lbl_estado_busqueda.setText(f"✓ {row[1]} — {row[2]}")

    def _confirmar(self):
        self.tarjeta_id = self.combo_tarjeta.currentData()
        if not self.tarjeta_id:
            from PyQt5.QtWidgets import QMessageBox
            QMessageBox.warning(self, "Error", "Seleccione una tarjeta")
            return
        self.accept()


class _DialogoTarjetasCliente(QDialog):
    """Vista de tarjetas asignadas a un cliente con opciones de gestión."""

    def __init__(self, cliente_id, cliente_nombre, conexion, parent=None):
        super().__init__(parent)
        self.cliente_id     = cliente_id
        self.cliente_nombre = cliente_nombre
        self.conexion       = conexion
        self.setWindowTitle(f"Tarjetas — {cliente_nombre}")
        self.setMinimumWidth(600)
        self.setMinimumHeight(480)
        self.setModal(True)
        self._build_ui()
        self._cargar_datos()

    def _build_ui(self):
        from PyQt5.QtWidgets import (
            QVBoxLayout, QLabel, QTableWidget, QTableWidgetItem,
            QTabWidget, QWidget, QHBoxLayout, QPushButton, QHeaderView
        )
        from PyQt5.QtCore import Qt

        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 12, 12, 12)

        layout.addWidget(QLabel(f"<b>Cliente:</b> {self.cliente_nombre} (ID: {self.cliente_id})"))

        tabs = QTabWidget()

        # Tab 1: Tarjetas actuales
        tab_tarjetas = QWidget()
        lay_t = QVBoxLayout(tab_tarjetas)
        cols_t = ["ID Tarjeta", "Número", "Estado", "Nivel", "Puntos", "Fecha Asignación"]
        self.tabla_tarjetas = QTableWidget(0, len(cols_t))
        self.tabla_tarjetas.setHorizontalHeaderLabels(cols_t)
        self.tabla_tarjetas.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tabla_tarjetas.setEditTriggers(0)  # NoEditTriggers
        lay_t.addWidget(self.tabla_tarjetas)

        btn_row = QHBoxLayout()
        self.btn_bloquear  = QPushButton("🔒 Bloquear")
        self.btn_liberar   = QPushButton("🔓 Liberar")
        self.btn_bloquear.clicked.connect(self._bloquear_tarjeta)
        self.btn_liberar.clicked.connect(self._liberar_tarjeta)
        btn_row.addStretch()
        btn_row.addWidget(self.btn_bloquear)
        btn_row.addWidget(self.btn_liberar)
        lay_t.addLayout(btn_row)
        tabs.addTab(tab_tarjetas, "Tarjetas Actuales")

        # Tab 2: Historial de asignaciones
        tab_hist = QWidget()
        lay_h = QVBoxLayout(tab_hist)
        cols_h = ["Acción", "Fecha", "Tarjeta", "Motivo", "Usuario"]
        self.tabla_historial = QTableWidget(0, len(cols_h))
        self.tabla_historial.setHorizontalHeaderLabels(cols_h)
        self.tabla_historial.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tabla_historial.setEditTriggers(0)
        lay_h.addWidget(self.tabla_historial)
        tabs.addTab(tab_hist, "Historial de Asignaciones")

        # Tab 3: Score de fidelidad
        tab_score = QWidget()
        lay_s = QVBoxLayout(tab_score)
        self.lbl_score = QLabel("Cargando score de fidelidad…")
        self.lbl_score.setWordWrap(True)
        lay_s.addWidget(self.lbl_score)
        tabs.addTab(tab_score, "Score Fidelidad")

        layout.addWidget(tabs)

        btn_cerrar = QPushButton("Cerrar")
        btn_cerrar.clicked.connect(self.accept)
        layout.addWidget(btn_cerrar)

    def _cargar_datos(self):
        from PyQt5.QtWidgets import QTableWidgetItem
        # Tarjetas asignadas
        try:
            rows = self.conexion.execute(
                "SELECT id, numero, estado, COALESCE(nivel,'Bronce'), puntos_actuales, fecha_asignacion "
                "FROM tarjetas_fidelidad WHERE id_cliente = ? ORDER BY fecha_asignacion DESC",
                (self.cliente_id,)
            ).fetchall()
            self.tabla_tarjetas.setRowCount(len(rows))
            for i, r in enumerate(rows):
                for j, v in enumerate(r):
                    self.tabla_tarjetas.setItem(i, j, QTableWidgetItem(str(v or "")))
        except Exception:
            pass

        # Historial
        try:
            rows_h = self.conexion.execute(
                """
                SELECT h.accion, h.fecha, tf.numero, h.motivo, h.usuario
                FROM card_assignment_history h
                LEFT JOIN tarjetas_fidelidad tf ON tf.id = h.tarjeta_id
                WHERE h.cliente_id_nuevo = ? OR h.cliente_id_prev = ?
                ORDER BY h.fecha DESC LIMIT 100
                """,
                (self.cliente_id, self.cliente_id)
            ).fetchall()
            self.tabla_historial.setRowCount(len(rows_h))
            for i, r in enumerate(rows_h):
                for j, v in enumerate(r):
                    self.tabla_historial.setItem(i, j, QTableWidgetItem(str(v or "")))
        except Exception:
            pass

        # Score fidelidad
        try:
            score_row = self.conexion.execute(
                "SELECT score_total, nivel, visitas_periodo, importe_total, "
                "margen_generado, referidos, fecha_calculo "
                "FROM loyalty_scores WHERE cliente_id = ?",
                (self.cliente_id,)
            ).fetchone()
            if score_row:
                txt = (
                    f"<b>Score Total:</b> {score_row[0]:.1f}/100  |  "
                    f"<b>Nivel:</b> {score_row[1]}<br>"
                    f"<b>Visitas período:</b> {score_row[2]}<br>"
                    f"<b>Importe total:</b> ${score_row[3]:,.2f}<br>"
                    f"<b>Margen generado:</b> ${score_row[4]:,.2f}<br>"
                    f"<b>Referidos:</b> {score_row[5]}<br>"
                    f"<small>Calculado: {score_row[6]}</small>"
                )
                self.lbl_score.setText(txt)
            else:
                self.lbl_score.setText("Sin datos de score. Se calculará en la próxima venta.")
        except Exception:
            self.lbl_score.setText("Módulo de fidelidad no disponible.")

    def _tarjeta_seleccionada(self):
        fila = self.tabla_tarjetas.currentRow()
        if fila < 0:
            return None, None
        try:
            tid = int(self.tabla_tarjetas.item(fila, 0).text())
            num = self.tabla_tarjetas.item(fila, 1).text()
            return tid, num
        except Exception:
            return None, None

    def _bloquear_tarjeta(self):
        tid, num = self._tarjeta_seleccionada()
        if not tid:
            from PyQt5.QtWidgets import QMessageBox
            QMessageBox.warning(self, "Selección", "Seleccione una tarjeta")
            return
        from PyQt5.QtWidgets import QInputDialog, QMessageBox
        motivo, ok = QInputDialog.getText(self, "Motivo", f"Motivo de bloqueo para {num}:")
        if not ok or not motivo.strip():
            return
        try:
            from core.services.card_batch_engine import CardBatchEngine
            eng = CardBatchEngine(self.conexion, "admin")
            res = eng.bloquear_tarjeta(tid, motivo.strip())
            if res.exito:
                self._cargar_datos()
                QMessageBox.information(self, "Bloqueada", f"Tarjeta {num} bloqueada.")
            else:
                QMessageBox.warning(self, "Error", res.mensaje)
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))

    def _liberar_tarjeta(self):
        tid, num = self._tarjeta_seleccionada()
        if not tid:
            from PyQt5.QtWidgets import QMessageBox
            QMessageBox.warning(self, "Selección", "Seleccione una tarjeta")
            return
        from PyQt5.QtWidgets import QMessageBox
        res = QMessageBox.question(self, "Liberar",
            f"¿Liberar tarjeta {num}?\nSe desvinculará del cliente.")
        if res != QMessageBox.Yes:
            return
        try:
            from core.services.card_batch_engine import CardBatchEngine
            eng = CardBatchEngine(self.conexion, "admin")
            result = eng.liberar_tarjeta(tid, motivo="liberacion_manual")
            if result.exito:
                self._cargar_datos()
                QMessageBox.information(self, "Liberada", f"Tarjeta {num} liberada.")
            else:
                QMessageBox.warning(self, "Error", result.mensaje)
        except Exception as exc:
            QMessageBox.critical(self, "Error", str(exc))

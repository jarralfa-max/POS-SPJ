# config.py
import os
import sqlite3

# --- Rutas ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ICONS_DIR = os.path.join(BASE_DIR, "recursos", "icons")
DATABASE_NAME = "punto_venta.db"

# --- TEMAS ELEGANTES Y MODERNOS ---
TEMAS = {
    "Oscuro Moderno": """
        /* ===== PALETA DE COLORES OSCURO MODERNO ===== */
        QMainWindow, QDialog, QWidget {
            background-color: #1E1E1E;
            color: #E8E8E8;
            font-family: 'Segoe UI', 'Roboto', sans-serif;
            font-size: 11px;
        }
        
        /* ===== BOTONES PRINCIPALES ===== */
        QPushButton {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #4A5568, stop:1 #2D3748);
            color: #FFFFFF;
            border: 1px solid #4A5568;
            border-radius: 8px;
            padding: 10px 15px;
            font-weight: 600;
            font-size: 11px;
            min-height: 35px;
        }
        QPushButton:hover {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #5A6578, stop:1 #3D4758);
            border: 1px solid #63B3ED;
        }
        QPushButton:pressed {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #2D3748, stop:1 #1A202C);
        }
        QPushButton:disabled {
            background: #2D3748;
            color: #718096;
            border: 1px solid #4A5568;
        }
        
        /* ===== BOTONES DE MÓDULOS ===== */
        QPushButton[class="botonModuloVentas"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #48BB78, stop:1 #38A169);
            border: 1px solid #48BB78;
        }
        QPushButton[class="botonModuloClientes"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #4299E1, stop:1 #3182CE);
            border: 1px solid #4299E1;
        }
        QPushButton[class="botonModuloProductos"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #ED8936, stop:1 #DD6B20);
            border: 1px solid #ED8936;
        }
        QPushButton[class="botonModuloCaja"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #9F7AEA, stop:1 #805AD5);
            border: 1px solid #9F7AEA;
        }
        QPushButton[class="botonModuloReportes"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #718096, stop:1 #4A5568);
            border: 1px solid #718096;
        }
        QPushButton[class="botonModuloGastos"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #F56565, stop:1 #E53E3E);
            border: 1px solid #F56565;
        }
        QPushButton[class="botonModuloConfiguracion"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #A0AEC0, stop:1 #718096);
            border: 1px solid #A0AEC0;
        }
        
        /* ===== PANEL LATERAL ===== */
        QFrame[class="panelLateral"] {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #2D3748, stop:1 #1A202C);
            border-right: 2px solid #4A5568;
        }
        
        QLabel[class="tituloModulos"] {
            color: #63B3ED;
            font-size: 16px;
            font-weight: bold;
            padding: 15px;
            background: transparent;
        }
        
        QFrame[class="panelUsuario"] {
            background: rgba(74, 85, 104, 0.7);
            border: 1px solid #4A5568;
            border-radius: 10px;
            padding: 10px;
        }
        
        QLabel[class="labelUsuario"] {
            color: #E2E8F0;
            font-weight: bold;
            font-size: 12px;
        }
        
        QLabel[class="labelRol"] {
            color: #CBD5E0;
            font-size: 10px;
        }
        
        QPushButton[class="botonLogin"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #48BB78, stop:1 #38A169);
            color: white;
            font-weight: bold;
            border: none;
            border-radius: 8px;
        }
        
        QPushButton[class="botonLogout"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #F56565, stop:1 #E53E3E);
            color: white;
            font-weight: bold;
            border: none;
            border-radius: 8px;
        }
        
        /* ===== PÁGINA INICIAL ===== */
        QWidget[class="paginaInicial"] {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                stop:0 #1A202C, stop:0.5 #2D3748, stop:1 #1A202C);
        }
        
        QWidget[class="contenedorCentral"] {
            background: transparent;
        }
        
        QLabel[class="logoCentral"] {
            background: transparent;
        }
        
        QLabel[class="logoPlaceholder"] {
            color: #63B3ED;
            font-size: 32px;
            font-weight: bold;
            background: transparent;
        }
        
        QLabel[class="labelNegocio"] {
            color: #E2E8F0;
            font-size: 28px;
            font-weight: bold;
            background: transparent;
        }
        
        QLabel[class="labelBienvenida"] {
            color: #68D391;
            font-size: 24px;
            font-weight: bold;
            background: transparent;
        }
        
        QLabel[class="labelInstruccion"] {
            color: #CBD5E0;
            font-size: 16px;
            font-style: italic;
            background: transparent;
        }
        
        /* ===== BARRA DE MENÚ ===== */
        QMenuBar {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #2D3748, stop:1 #1A202C);
            color: #E2E8F0;
            border-bottom: 1px solid #4A5568;
        }
        
        QMenuBar::item {
            background: transparent;
            padding: 8px 12px;
            border-radius: 4px;
        }
        
        QMenuBar::item:selected {
            background: #4A5568;
        }
        
        QMenu {
            background: #2D3748;
            color: #E2E8F0;
            border: 1px solid #4A5568;
            border-radius: 8px;
            padding: 5px;
        }
        
        QMenu::item {
            padding: 8px 25px;
            border-radius: 4px;
        }
        
        QMenu::item:selected {
            background: #4A5568;
        }
        
        /* ===== CAMPOS DE ENTRADA ===== */
        QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox, QTextEdit {
            background: #2D3748;
            color: #E2E8F0;
            border: 2px solid #4A5568;
            border-radius: 6px;
            padding: 8px 12px;
            font-size: 11px;
            selection-background-color: #63B3ED;
        }
        
        QLineEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus, 
        QComboBox:focus, QTextEdit:focus {
            border-color: #63B3ED;
            background: #2D3748;
        }
        
        QComboBox::drop-down {
            border: none;
            background: #4A5568;
            border-radius: 0 4px 4px 0;
        }
        
        QComboBox::down-arrow {
            image: none;
            border-left: 5px solid transparent;
            border-right: 5px solid transparent;
            border-top: 5px solid #CBD5E0;
        }
        
        /* ===== TABLAS GENERALES ===== */
        QTableWidget {
            background: #2D3748;
            alternate-background-color: #1A202C;
            color: #E2E8F0;
            gridline-color: #4A5568;
            border: 1px solid #4A5568;
            border-radius: 6px;
            font-size: 11px;
        }
        
        QTableWidget::item {
            padding: 8px;
            border-bottom: 1px solid #4A5568;
        }
        
        QTableWidget::item:selected {
            background: #63B3ED;
            color: #1A202C;
        }
        
        QHeaderView::section {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #4A5568, stop:1 #2D3748);
            color: #E2E8F0;
            padding: 10px;
            border: none;
            font-weight: bold;
            font-size: 11px;
        }
        
        /* ===== TABLA DE CARRITO ESPECÍFICA - COMPACTA ===== */
        QTableWidget[class="tabla-carrito"] {
            min-height: 140px;
            max-height: 280px;
            font-size: 10px;
            background: #252a36;
            alternate-background-color: #1f2430;
        }
        
        QTableWidget[class="tabla-carrito"]::item {
            height: 28px;
            padding: 3px 5px;
            border-bottom: 1px solid #3b4354;
        }
        
        QTableWidget[class="tabla-carrito"] QHeaderView::section {
            padding: 5px 6px;
            font-size: 9px;
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #3b4354, stop:1 #2a3040);
        }
        
        /* Botones de tabla más compactos */
        QTableWidget[class="tabla-carrito"] QPushButton[class="table-edit-button"],
        QTableWidget[class="tabla-carrito"] QPushButton[class="table-delete-button"] {
            padding: 1px 3px;
            font-size: 8px;
            min-width: 22px;
            max-width: 22px;
            min-height: 18px;
            max-height: 18px;
            margin: 1px;
        }
        
        /* ===== GROUP BOXES ===== */
        QGroupBox {
            font-weight: bold;
            font-size: 12px;
            color: #63B3ED;
            border: 2px solid #4A5568;
            border-radius: 10px;
            margin-top: 15px;
            padding-top: 15px;
            background: #2D3748;
        }
        
        QGroupBox::title {
            subcontrol-origin: margin;
            subcontrol-position: top center;
            padding: 5px 15px;
            background: #2D3748;
            border-radius: 5px;
        }
        
        /* ===== GRUPOS DE VENTA ===== */
        QGroupBox[class="venta-group"] {
            background: #1f2430;
            border: 2px solid #3b4354;
            border-radius: 12px;
            padding: 12px;
            margin-top: 8px;
        }

        QGroupBox[class="venta-group"]::title {
            background: #1f2430;
            color: #8fa2c2;
            padding: 4px 10px;
            border-radius: 6px;
            font-weight: bold;
            font-size: 11px;
        }

        /* ===== GRUPO RESUMEN MÁS GRANDE ===== */
        QGroupBox[class="resumen-group"] {
            background: #252a36;
            border: 3px solid #4A5568;
            border-radius: 12px;
            padding: 15px;
            margin-top: 8px;
        }

        QGroupBox[class="resumen-group"]::title {
            background: #252a36;
            color: #63B3ED;
            padding: 6px 15px;
            border-radius: 8px;
            font-weight: bold;
            font-size: 13px;
        }

        /* Display de total más prominente */
        QLabel[class="total-display-prominente"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #2c5282, stop:1 #2a4365);
            border: 3px solid #4299e1;
            border-radius: 12px;
            padding: 18px;
            font-size: 22px;
            font-weight: bold;
            color: #ffffff;
            text-align: center;
            margin: 5px;
            min-height: 30px;
        }

        /* Displays de información más grandes */
        QLabel[class="info-display-grande"] {
            background: #2D3748;
            border: 2px solid #4A5568;
            border-radius: 10px;
            padding: 12px;
            font-size: 14px;
            font-weight: bold;
            color: #E2E8F0;
            text-align: center;
            margin: 3px;
            min-height: 20px;
        }

        /* Tarjeta especial total */
        QLabel[class="total-box"] {
            background: #1e3a52;
            border: 3px solid #3675b9;
            border-radius: 12px;
            padding: 15px;
            font-size: 20px;
            font-weight: bold;
            min-height: 25px;
        }

        /* Tarjeta interna de información */
        QLabel[class="info-box"] {
            background: #252a36;
            border-radius: 10px;
            border: 2px solid #3b4354;
            font-size: 14px;
            padding: 12px;
            font-weight: 600;
            margin: 4px;
            min-height: 20px;
        }

        /* Botones grandes estilo POS */
        QPushButton[class="venta-button"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #566179, stop:1 #444b61);
            border-radius: 12px;
            padding: 12px 20px;
            font-size: 14px;
            min-height: 45px;
        }

        QPushButton[class="venta-button"]:hover {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #667189, stop:1 #545b71);
        }
        
        /* ===== SCROLLBARS ===== */
        QScrollBar:vertical {
            background: #2D3748;
            width: 15px;
            border-radius: 7px;
        }
        
        QScrollBar::handle:vertical {
            background: #4A5568;
            border-radius: 7px;
            min-height: 20px;
        }
        
        QScrollBar::handle:vertical:hover {
            background: #63B3ED;
        }
        
        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
            border: none;
            background: none;
        }
        
        /* ===== CHECKBOXES Y RADIO BUTTONS ===== */
        QCheckBox, QRadioButton {
            color: #E2E8F0;
            spacing: 8px;
        }
        
        QCheckBox::indicator, QRadioButton::indicator {
            width: 16px;
            height: 16px;
            border: 2px solid #4A5568;
            border-radius: 3px;
            background: #2D3748;
        }
        
        QCheckBox::indicator:checked, QRadioButton::indicator:checked {
            background: #63B3ED;
            border: 2px solid #63B3ED;
        }
        
        QRadioButton::indicator {
            border-radius: 8px;
        }
        
        QRadioButton::indicator:checked {
            background: #63B3ED;
            border: 2px solid #63B3ED;
        }
        
        /* ===== SPLITTER ===== */
        QSplitter::handle {
            background: #4A5568;
        }
        
        QSplitter::handle:hover {
            background: #63B3ED;
        }
        
        /* ===== BARRA DE ESTADO ===== */
        QStatusBar {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #2D3748, stop:1 #1A202C);
            color: #CBD5E0;
            border-top: 1px solid #4A5568;
        }
        
        /* ===== DIÁLOGOS ===== */
        QDialog {
            background: #2D3748;
            color: #E2E8F0;
            border: 2px solid #4A5568;
            border-radius: 10px;
        }
        
        QLabel[class="dialog-title"] {
            color: #63B3ED;
            font-size: 16px;
            font-weight: bold;
        }
        
        QLineEdit[class="dialog-input"] {
            background: #1A202C;
            border: 2px solid #4A5568;
            border-radius: 6px;
            padding: 10px;
        }
        
        QPushButton[class="cancel-button"] {
            background: #718096;
            color: white;
            border: none;
            border-radius: 6px;
            padding: 10px 20px;
        }
        
        QPushButton[class="accept-button"] {
            background: #48BB78;
            color: white;
            border: none;
            border-radius: 6px;
            padding: 10px 20px;
        }
        
        /* ===== TARJETAS DE PRODUCTOS (PARA MÓDULO VENTAS) ===== */
        ProductCard {
            border: 2px solid #4A5568;
            border-radius: 12px;
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #2D3748, stop:1 #1A202C);
            margin: 8px;
            padding: 0px;
        }
        
        ProductCard:hover {
            border: 2px solid #63B3ED;
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #3D4758, stop:1 #2A3040);
        }
        
        QLabel[class="product-image"] {
            border: 2px solid #4A5568;
            border-radius: 8px;
            background: #1A202C;
        }
        
        QLabel[class="product-name"] {
            color: #E2E8F0;
            font-weight: bold;
            font-size: 12px;
        }
        
        QLabel[class="product-price"] {
            color: #68D391;
            font-size: 11px;
            font-weight: bold;
        }
        
        QLabel[class="product-stock"] {
            color: #CBD5E0;
            font-size: 10px;
        }
        
        /* ===== BOTONES DE TABLA ===== */
        QPushButton[class="table-edit-button"] {
            background: #4299E1;
            color: white;
            border: none;
            border-radius: 4px;
            padding: 6px 10px;
            font-size: 10px;
        }
        
        QPushButton[class="table-delete-button"] {
            background: #F56565;
            color: white;
            border: none;
            border-radius: 4px;
            padding: 6px 10px;
            font-size: 10px;
        }
        
        /* ===== DISPLAYS INFORMATIVOS ===== */
        QLabel[class="weight-display"] {
            color: #68D391;
            font-size: 13px;
            font-weight: bold;
            background: #1A202C;
            border: 2px solid #68D391;
            border-radius: 8px;
            padding: 10px;
        }
        
        QLabel[class="total-display"] {
            color: #63B3ED;
            font-size: 16px;
            font-weight: bold;
            background: #1A202C;
            border: 2px solid #63B3ED;
            border-radius: 8px;
            padding: 12px;
        }
        
        QLabel[class="points-display"] {
            color: #D69E2E;
            font-size: 13px;
            font-weight: bold;
            background: #1A202C;
            border: 2px solid #D69E2E;
            border-radius: 8px;
            padding: 10px;
        }
        
        /* ===== ETIQUETAS INFORMATIVAS ===== */
        QLabel[class="info-label"] {
            color: #CBD5E0;
            font-size: 9px;
            font-style: italic;
            background: transparent;
            padding: 2px;
        }
        
        /* ===== CLIENTE GROUP ===== */
        QGroupBox[class="client-group"] {
            background: #1f2430;
            border: 2px solid #3b4354;
            border-radius: 10px;
            padding: 10px;
            margin-top: 5px;
            max-height: 90px;
        }
        
        QGroupBox[class="client-group"]::title {
            background: #1f2430;
            color: #8fa2c2;
            padding: 3px 8px;
            border-radius: 5px;
            font-size: 10px;
        }
        
        QLineEdit[class="client-input"] {
            font-size: 10px;
            padding: 6px 8px;
        }
        
        QLabel[class="client-info-highlight"] {
            color: #63B3ED;
            font-weight: bold;
            font-size: 11px;
        }
        
        QLabel[class="client-info"] {
            color: #CBD5E0;
            font-size: 9px;
        }
        
        /* ===== ESTILOS_PANEL_LATERAL ===== */

        .panelLateral {
            background-color: #2d3748;
            border-right: 1px solid #4a5568;
        }

        /* Botones de módulos - estado normal */
        .botonModulo {
            background-color: #4a5568;
            border: 1px solid #718096;
            border-radius: 8px;
            color: #e2e8f0;
            font-weight: 500;
            font-size: 13px;
            padding: 8px 12px;
            text-align: left;
        }

        .botonModulo:hover {
            background-color: #5a6778;
            border-color: #90cdf4;
        }

        .botonModulo:pressed {
            background-color: #3c4a5c;
        }

        .botonModulo:disabled {
            background-color: #2d3748;
            color: #718096;
            border-color: #4a5568;
        }

        /* Botón de módulo activo */
        .botonModuloActivo {
            background-color: #63b3ed;
            border: 2px solid #90cdf4;
            border-radius: 8px;
            color: #1a202c;
            font-weight: 600;
            font-size: 13px;
            padding: 8px 12px;
            text-align: left;
        }

        .botonModuloActivo:hover {
            background-color: #4299e1;
        }

        /* Botones de login/logout */
        .botonLogin {
            background-color: #48bb78;
            border: 1px solid #68d391;
            border-radius: 8px;
            color: white;
            font-weight: 600;
            font-size: 12px;
        }

        .botonLogin:hover {
            background-color: #38a169;
        }

        .botonLogout {
            background-color: #e53e3e;
            border: 1px solid #fc8181;
            border-radius: 8px;
            color: white;
            font-weight: 600;
            font-size: 12px;
        }

        .botonLogout:hover {
            background-color: #c53030;
        }
        
    """,
    
    "Claro Elegante": """
        /* ===== PALETA DE COLORES CLARO ELEGANTE ===== */
        QMainWindow, QDialog, QWidget {
            background-color: #F7FAFC;
            color: #2D3748;
            font-family: 'Segoe UI', 'Roboto', sans-serif;
            font-size: 11px;
        }
        
        /* ===== BOTONES PRINCIPALES ===== */
        QPushButton {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #FFFFFF, stop:1 #EDF2F7);
            color: #2D3748;
            border: 2px solid #E2E8F0;
            border-radius: 8px;
            padding: 10px 15px;
            font-weight: 600;
            font-size: 11px;
            min-height: 35px;
        }
        QPushButton:hover {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #FFFFFF, stop:1 #E2E8F0);
            border: 2px solid #4299E1;
        }
        QPushButton:pressed {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #EDF2F7, stop:1 #CBD5E0);
        }
        QPushButton:disabled {
            background: #EDF2F7;
            color: #A0AEC0;
            border: 2px solid #E2E8F0;
        }
        
        /* ===== BOTONES DE MÓDULOS ===== */
        QPushButton[class="botonModuloVentas"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #48BB78, stop:1 #38A169);
            color: white;
            border: 2px solid #48BB78;
        }
        QPushButton[class="botonModuloClientes"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #4299E1, stop:1 #3182CE);
            color: white;
            border: 2px solid #4299E1;
        }
        QPushButton[class="botonModuloProductos"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #ED8936, stop:1 #DD6B20);
            color: white;
            border: 2px solid #ED8936;
        }
        QPushButton[class="botonModuloCaja"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #9F7AEA, stop:1 #805AD5);
            color: white;
            border: 2px solid #9F7AEA;
        }
        QPushButton[class="botonModuloReportes"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #718096, stop:1 #4A5568);
            color: white;
            border: 2px solid #718096;
        }
        QPushButton[class="botonModuloGastos"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #F56565, stop:1 #E53E3E);
            color: white;
            border: 2px solid #F56565;
        }
        QPushButton[class="botonModuloConfiguracion"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #A0AEC0, stop:1 #718096);
            color: white;
            border: 2px solid #A0AEC0;
        }
        
        /* ===== PANEL LATERAL ===== */
        QFrame[class="panelLateral"] {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #EDF2F7, stop:1 #E2E8F0);
            border-right: 2px solid #CBD5E0;
        }
        
        QLabel[class="tituloModulos"] {
            color: #2D3748;
            font-size: 16px;
            font-weight: bold;
            padding: 15px;
            background: transparent;
        }
        
        QFrame[class="panelUsuario"] {
            background: rgba(255, 255, 255, 0.8);
            border: 2px solid #CBD5E0;
            border-radius: 10px;
            padding: 10px;
        }
        
        QLabel[class="labelUsuario"] {
            color: #2D3748;
            font-weight: bold;
            font-size: 12px;
        }
        
        QLabel[class="labelRol"] {
            color: #4A5568;
            font-size: 10px;
        }
        
        QPushButton[class="botonLogin"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #48BB78, stop:1 #38A169);
            color: white;
            font-weight: bold;
            border: none;
            border-radius: 8px;
        }
        
        QPushButton[class="botonLogout"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #F56565, stop:1 #E53E3E);
            color: white;
            font-weight: bold;
            border: none;
            border-radius: 8px;
        }
        
        /* ===== PÁGINA INICIAL ===== */
        QWidget[class="paginaInicial"] {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                stop:0 #EDF2F7, stop:0.5 #E2E8F0, stop:1 #EDF2F7);
        }
        
        QWidget[class="contenedorCentral"] {
            background: transparent;
        }
        
        QLabel[class="logoCentral"] {
            background: transparent;
        }
        
        QLabel[class="logoPlaceholder"] {
            color: #4299E1;
            font-size: 32px;
            font-weight: bold;
            background: transparent;
        }
        
        QLabel[class="labelNegocio"] {
            color: #2D3748;
            font-size: 28px;
            font-weight: bold;
            background: transparent;
        }
        
        QLabel[class="labelBienvenida"] {
            color: #38A169;
            font-size: 24px;
            font-weight: bold;
            background: transparent;
        }
        
        QLabel[class="labelInstruccion"] {
            color: #4A5568;
            font-size: 16px;
            font-style: italic;
            background: transparent;
        }
        
        /* ===== BARRA DE MENÚ ===== */
        QMenuBar {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #FFFFFF, stop:1 #EDF2F7);
            color: #2D3748;
            border-bottom: 2px solid #E2E8F0;
        }
        
        QMenuBar::item {
            background: transparent;
            padding: 8px 12px;
            border-radius: 4px;
        }
        
        QMenuBar::item:selected {
            background: #4299E1;
            color: white;
        }
        
        QMenu {
            background: #FFFFFF;
            color: #2D3748;
            border: 2px solid #E2E8F0;
            border-radius: 8px;
            padding: 5px;
        }
        
        QMenu::item {
            padding: 8px 25px;
            border-radius: 4px;
        }
        
        QMenu::item:selected {
            background: #4299E1;
            color: white;
        }
        
        /* ===== CAMPOS DE ENTRADA ===== */
        QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox, QTextEdit {
            background: #FFFFFF;
            color: #2D3748;
            border: 2px solid #E2E8F0;
            border-radius: 6px;
            padding: 8px 12px;
            font-size: 11px;
            selection-background-color: #4299E1;
        }
        
        QLineEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus, 
        QComboBox:focus, QTextEdit:focus {
            border-color: #4299E1;
            background: #FFFFFF;
        }
        
        /* ===== TABLAS GENERALES ===== */
        QTableWidget {
            background: #FFFFFF;
            alternate-background-color: #F7FAFC;
            color: #2D3748;
            gridline-color: #E2E8F0;
            border: 2px solid #E2E8F0;
            border-radius: 6px;
            font-size: 11px;
        }
        
        QTableWidget::item {
            padding: 8px;
            border-bottom: 1px solid #E2E8F0;
        }
        
        QTableWidget::item:selected {
            background: #4299E1;
            color: white;
        }
        
        QHeaderView::section {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #EDF2F7, stop:1 #E2E8F0);
            color: #2D3748;
            padding: 10px;
            border: none;
            font-weight: bold;
            font-size: 11px;
        }
        
        /* ===== TABLA DE CARRITO ESPECÍFICA - COMPACTA ===== */
        QTableWidget[class="tabla-carrito"] {
            min-height: 140px;
            max-height: 280px;
            font-size: 10px;
            background: #FFFFFF;
            alternate-background-color: #F7FAFC;
        }
        
        QTableWidget[class="tabla-carrito"]::item {
            height: 28px;
            padding: 3px 5px;
            border-bottom: 1px solid #E2E8F0;
        }
        
        QTableWidget[class="tabla-carrito"] QHeaderView::section {
            padding: 5px 6px;
            font-size: 9px;
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #EDF2F7, stop:1 #E2E8F0);
        }
        
        /* Botones de tabla más compactos */
        QTableWidget[class="tabla-carrito"] QPushButton[class="table-edit-button"],
        QTableWidget[class="tabla-carrito"] QPushButton[class="table-delete-button"] {
            padding: 1px 3px;
            font-size: 8px;
            min-width: 22px;
            max-width: 22px;
            min-height: 18px;
            max-height: 18px;
            margin: 1px;
        }
        
        /* ===== GROUP BOXES ===== */
        QGroupBox {
            font-weight: bold;
            font-size: 12px;
            color: #4299E1;
            border: 2px solid #E2E8F0;
            border-radius: 10px;
            margin-top: 15px;
            padding-top: 15px;
            background: #FFFFFF;
        }
        
        QGroupBox::title {
            subcontrol-origin: margin;
            subcontrol-position: top center;
            padding: 5px 15px;
            background: #FFFFFF;
            border-radius: 5px;
        }
        
        /* ===== GRUPOS DE VENTA ===== */
        QGroupBox[class="venta-group"] {
            background: #FFFFFF;
            border: 2px solid #E2E8F0;
            border-radius: 12px;
            padding: 12px;
            margin-top: 8px;
        }

        QGroupBox[class="venta-group"]::title {
            background: #FFFFFF;
            color: #4A5568;
            padding: 4px 10px;
            border-radius: 6px;
            font-weight: bold;
            font-size: 11px;
        }

        /* ===== GRUPO RESUMEN MÁS GRANDE ===== */
        QGroupBox[class="resumen-group"] {
            background: #FFFFFF;
            border: 3px solid #4299E1;
            border-radius: 12px;
            padding: 15px;
            margin-top: 8px;
        }

        QGroupBox[class="resumen-group"]::title {
            background: #FFFFFF;
            color: #4299E1;
            padding: 6px 15px;
            border-radius: 8px;
            font-weight: bold;
            font-size: 13px;
        }

        /* Display de total más prominente */
        QLabel[class="total-display-prominente"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #4299E1, stop:1 #3182CE);
            border: 3px solid #2B6CB0;
            border-radius: 12px;
            padding: 18px;
            font-size: 22px;
            font-weight: bold;
            color: #ffffff;
            text-align: center;
            margin: 5px;
            min-height: 30px;
        }

        /* Displays de información más grandes */
        QLabel[class="info-display-grande"] {
            background: #EDF2F7;
            border: 2px solid #CBD5E0;
            border-radius: 10px;
            padding: 12px;
            font-size: 14px;
            font-weight: bold;
            color: #2D3748;
            text-align: center;
            margin: 3px;
            min-height: 20px;
        }

        /* Tarjeta especial total */
        QLabel[class="total-box"] {
            background: #4299E1;
            border: 3px solid #2B6CB0;
            border-radius: 12px;
            padding: 15px;
            font-size: 20px;
            font-weight: bold;
            color: white;
            min-height: 25px;
        }

        /* Tarjeta interna de información */
        QLabel[class="info-box"] {
            background: #EDF2F7;
            border-radius: 10px;
            border: 2px solid #CBD5E0;
            font-size: 14px;
            padding: 12px;
            font-weight: 600;
            margin: 4px;
            min-height: 20px;
        }

        /* Botones grandes estilo POS */
        QPushButton[class="venta-button"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #A0AEC0, stop:1 #718096);
            color: white;
            border-radius: 12px;
            padding: 12px 20px;
            font-size: 14px;
            min-height: 45px;
        }

        QPushButton[class="venta-button"]:hover {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #B0BEC0, stop:1 #819096);
        }
        
        /* ===== SCROLLBARS ===== */
        QScrollBar:vertical {
            background: #EDF2F7;
            width: 15px;
            border-radius: 7px;
        }
        
        QScrollBar::handle:vertical {
            background: #CBD5E0;
            border-radius: 7px;
            min-height: 20px;
        }
        
        QScrollBar::handle:vertical:hover {
            background: #4299E1;
        }
        
        /* ===== TARJETAS DE PRODUCTOS (PARA MÓDULO VENTAS) ===== */
        ProductCard {
            border: 2px solid #E2E8F0;
            border-radius: 12px;
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #FFFFFF, stop:1 #F7FAFC);
            margin: 8px;
            padding: 0px;
        }
        
        ProductCard:hover {
            border: 2px solid #4299E1;
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #FFFFFF, stop:1 #EDF2F7);
        }
        
        QLabel[class="product-image"] {
            border: 2px solid #E2E8F0;
            border-radius: 8px;
            background: #F7FAFC;
        }
        
        QLabel[class="product-name"] {
            color: #2D3748;
            font-weight: bold;
            font-size: 12px;
        }
        
        QLabel[class="product-price"] {
            color: #38A169;
            font-size: 11px;
            font-weight: bold;
        }
        
        QLabel[class="product-stock"] {
            color: #718096;
            font-size: 10px;
        }
        
        /* ===== DISPLAYS INFORMATIVOS ===== */
        QLabel[class="weight-display"] {
            color: #38A169;
            font-size: 13px;
            font-weight: bold;
            background: #F7FAFC;
            border: 2px solid #38A169;
            border-radius: 8px;
            padding: 10px;
        }
        
        QLabel[class="total-display"] {
            color: #4299E1;
            font-size: 16px;
            font-weight: bold;
            background: #F7FAFC;
            border: 2px solid #4299E1;
            border-radius: 8px;
            padding: 12px;
        }
        
        QLabel[class="points-display"] {
            color: #D69E2E;
            font-size: 13px;
            font-weight: bold;
            background: #F7FAFC;
            border: 2px solid #D69E2E;
            border-radius: 8px;
            padding: 10px;
        }
        
        /* ===== ETIQUETAS INFORMATIVAS ===== */
        QLabel[class="info-label"] {
            color: #718096;
            font-size: 9px;
            font-style: italic;
            background: transparent;
            padding: 2px;
        }
        
        /* ===== CLIENTE GROUP ===== */
        QGroupBox[class="client-group"] {
            background: #FFFFFF;
            border: 2px solid #E2E8F0;
            border-radius: 10px;
            padding: 10px;
            margin-top: 5px;
            max-height: 90px;
        }
        
        QGroupBox[class="client-group"]::title {
            background: #FFFFFF;
            color: #4A5568;
            padding: 3px 8px;
            border-radius: 5px;
            font-size: 10px;
        }
        
        QLineEdit[class="client-input"] {
            font-size: 10px;
            padding: 6px 8px;
        }
        
        QLabel[class="client-info-highlight"] {
            color: #4299E1;
            font-weight: bold;
            font-size: 11px;
        }
        
        QLabel[class="client-info"] {
            color: #718096;
            font-size: 9px;
        }
    """,
    
    "Azul Profesional": """
        /* ===== PALETA AZUL PROFESIONAL ===== */
        QMainWindow, QDialog, QWidget {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                stop:0 #1E3A8A, stop:0.5 #1E40AF, stop:1 #1D4ED8);
            color: #EFF6FF;
            font-family: 'Segoe UI', 'Roboto', sans-serif;
            font-size: 11px;
        }
        
        /* ===== BOTONES PRINCIPALES ===== */
        QPushButton {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #3B82F6, stop:1 #2563EB);
            color: white;
            border: 2px solid #60A5FA;
            border-radius: 8px;
            padding: 10px 15px;
            font-weight: 600;
            font-size: 11px;
            min-height: 35px;
        }
        QPushButton:hover {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #60A5FA, stop:1 #3B82F6);
            border: 2px solid #93C5FD;
        }
        QPushButton:pressed {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #2563EB, stop:1 #1D4ED8);
        }
        
        /* ===== PANEL LATERAL ===== */
        QFrame[class="panelLateral"] {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #1E40AF, stop:1 #1D4ED8);
            border-right: 2px solid #3B82F6;
        }
        
        /* ===== GROUP BOXES ===== */
        QGroupBox {
            background: rgba(30, 64, 175, 0.7);
            border: 2px solid #3B82F6;
            border-radius: 10px;
            color: #EFF6FF;
        }
        
        /* ===== TABLA DE CARRITO ESPECÍFICA - COMPACTA ===== */
        QTableWidget[class="tabla-carrito"] {
            min-height: 140px;
            max-height: 280px;
            font-size: 10px;
            background: rgba(255, 255, 255, 0.1);
            alternate-background-color: rgba(255, 255, 255, 0.05);
        }
        
        QTableWidget[class="tabla-carrito"]::item {
            height: 28px;
            padding: 3px 5px;
            border-bottom: 1px solid rgba(59, 130, 246, 0.3);
        }
        
        QTableWidget[class="tabla-carrito"] QHeaderView::section {
            padding: 5px 6px;
            font-size: 9px;
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 rgba(59, 130, 246, 0.7), stop:1 rgba(37, 99, 235, 0.7));
        }
        
        /* ===== GRUPO RESUMEN MÁS GRANDE ===== */
        QGroupBox[class="resumen-group"] {
            background: rgba(30, 64, 175, 0.8);
            border: 3px solid #3B82F6;
            border-radius: 12px;
            padding: 15px;
            margin-top: 8px;
        }

        QGroupBox[class="resumen-group"]::title {
            background: rgba(30, 64, 175, 0.8);
            color: #EFF6FF;
            padding: 6px 15px;
            border-radius: 8px;
            font-weight: bold;
            font-size: 13px;
        }

        /* Display de total más prominente */
        QLabel[class="total-display-prominente"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #1E40AF, stop:1 #1D4ED8);
            border: 3px solid #60A5FA;
            border-radius: 12px;
            padding: 18px;
            font-size: 22px;
            font-weight: bold;
            color: #ffffff;
            text-align: center;
            margin: 5px;
            min-height: 30px;
        }

        /* Tarjeta especial total */
        QLabel[class="total-box"] {
            background: #1E40AF;
            border: 3px solid #60A5FA;
            border-radius: 12px;
            padding: 15px;
            font-size: 20px;
            font-weight: bold;
            color: white;
            min-height: 25px;
        }
    """,
    
    "Verde Naturaleza": """
        /* ===== PALETA VERDE NATURALEZA ===== */
        QMainWindow, QDialog, QWidget {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                stop:0 #065F46, stop:0.5 #047857, stop:1 #059669);
            color: #ECFDF5;
            font-family: 'Segoe UI', 'Roboto', sans-serif;
            font-size: 11px;
        }
        
        /* ===== BOTONES PRINCIPALES ===== */
        QPushButton {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #10B981, stop:1 #059669);
            color: white;
            border: 2px solid #34D399;
            border-radius: 8px;
            padding: 10px 15px;
            font-weight: 600;
            font-size: 11px;
            min-height: 35px;
        }
        QPushButton:hover {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #34D399, stop:1 #10B981);
            border: 2px solid #6EE7B7;
        }
        
        /* ===== PANEL LATERAL ===== */
        QFrame[class="panelLateral"] {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #047857, stop:1 #059669);
            border-right: 2px solid #10B981;
        }
        
        /* ===== TABLA DE CARRITO ESPECÍFICA - COMPACTA ===== */
        QTableWidget[class="tabla-carrito"] {
            min-height: 140px;
            max-height: 280px;
            font-size: 10px;
            background: rgba(255, 255, 255, 0.1);
            alternate-background-color: rgba(255, 255, 255, 0.05);
        }
        
        QTableWidget[class="tabla-carrito"]::item {
            height: 28px;
            padding: 3px 5px;
            border-bottom: 1px solid rgba(16, 185, 129, 0.3);
        }
        
        QTableWidget[class="tabla-carrito"] QHeaderView::section {
            padding: 5px 6px;
            font-size: 9px;
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 rgba(16, 185, 129, 0.7), stop:1 rgba(5, 150, 105, 0.7));
        }
        
        /* ===== GRUPO RESUMEN MÁS GRANDE ===== */
        QGroupBox[class="resumen-group"] {
            background: rgba(6, 95, 70, 0.8);
            border: 3px solid #10B981;
            border-radius: 12px;
            padding: 15px;
            margin-top: 8px;
        }

        QGroupBox[class="resumen-group"]::title {
            background: rgba(6, 95, 70, 0.8);
            color: #ECFDF5;
            padding: 6px 15px;
            border-radius: 8px;
            font-weight: bold;
            font-size: 13px;
        }

        /* Display de total más prominente */
        QLabel[class="total-display-prominente"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #047857, stop:1 #059669);
            border: 3px solid #34D399;
            border-radius: 12px;
            padding: 18px;
            font-size: 22px;
            font-weight: bold;
            color: #ffffff;
            text-align: center;
            margin: 5px;
            min-height: 30px;
        }

        /* Tarjeta especial total */
        QLabel[class="total-box"] {
            background: #047857;
            border: 3px solid #34D399;
            border-radius: 12px;
            padding: 15px;
            font-size: 20px;
            font-weight: bold;
            color: white;
            min-height: 25px;
        }
    """,
    
    "Púrpura Creativo": """
        /* ===== PALETA PÚRPURA CREATIVO ===== */
        QMainWindow, QDialog, QWidget {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:1,
                stop:0 #5B21B6, stop:0.5 #6D28D9, stop:1 #7C3AED);
            color: #FAF5FF;
            font-family: 'Segoe UI', 'Roboto', sans-serif;
            font-size: 11px;
        }
        
        /* ===== BOTONES PRINCIPALES ===== */
        QPushButton {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #8B5CF6, stop:1 #7C3AED);
            color: white;
            border: 2px solid #A78BFA;
            border-radius: 8px;
            padding: 10px 15px;
            font-weight: 600;
            font-size: 11px;
            min-height: 35px;
        }
        QPushButton:hover {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #A78BFA, stop:1 #8B5CF6);
            border: 2px solid #C4B5FD;
        }
        
        /* ===== PANEL LATERAL ===== */
        QFrame[class="panelLateral"] {
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                stop:0 #6D28D9, stop:1 #7C3AED);
            border-right: 2px solid #8B5CF6;
        }
        
        /* ===== TABLA DE CARRITO ESPECÍFICA - COMPACTA ===== */
        QTableWidget[class="tabla-carrito"] {
            min-height: 140px;
            max-height: 280px;
            font-size: 10px;
            background: rgba(255, 255, 255, 0.1);
            alternate-background-color: rgba(255, 255, 255, 0.05);
        }
        
        QTableWidget[class="tabla-carrito"]::item {
            height: 28px;
            padding: 3px 5px;
            border-bottom: 1px solid rgba(139, 92, 246, 0.3);
        }
        
        QTableWidget[class="tabla-carrito"] QHeaderView::section {
            padding: 5px 6px;
            font-size: 9px;
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 rgba(139, 92, 246, 0.7), stop:1 rgba(124, 58, 237, 0.7));
        }
        
        /* ===== GRUPO RESUMEN MÁS GRANDE ===== */
        QGroupBox[class="resumen-group"] {
            background: rgba(91, 33, 182, 0.8);
            border: 3px solid #8B5CF6;
            border-radius: 12px;
            padding: 15px;
            margin-top: 8px;
        }

        QGroupBox[class="resumen-group"]::title {
            background: rgba(91, 33, 182, 0.8);
            color: #FAF5FF;
            padding: 6px 15px;
            border-radius: 8px;
            font-weight: bold;
            font-size: 13px;
        }

        /* Display de total más prominente */
        QLabel[class="total-display-prominente"] {
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                stop:0 #6D28D9, stop:1 #7C3AED);
            border: 3px solid #A78BFA;
            border-radius: 12px;
            padding: 18px;
            font-size: 22px;
            font-weight: bold;
            color: #ffffff;
            text-align: center;
            margin: 5px;
            min-height: 30px;
        }

        /* Tarjeta especial total */
        QLabel[class="total-box"] {
            background: #6D28D9;
            border: 3px solid #A78BFA;
            border-radius: 12px;
            padding: 15px;
            font-size: 20px;
            font-weight: bold;
            color: white;
            min-height: 25px;
        }
    """
}

# --- Configuración por defecto ---
CONFIGURACION_POR_DEFECTO = {
    'tema': 'Oscuro Moderno',
    'requerir_admin': 'True',
    'whatsapp_numero': '+525659274265',
    'impuesto_por_defecto': '16'
}

# --- Usuarios por defecto ---
USUARIOS_POR_DEFECTO = [
    ('admin', 'admin123', 'admin', 'ventas,clientes,productos,caja,reportes,gastos,personal,configuracion,tarjetas'),
    ('cajero', 'cajero123', 'cajero', 'ventas,caja')
]

class GestorTemas:
    def __init__(self, conexion):
        self.conexion = conexion
        self.temas = TEMAS
    
    def obtener_temas_disponibles(self):
        """Devuelve una lista con los nombres de los temas disponibles"""
        return list(self.temas.keys())
    
    def obtener_estilo_tema(self, nombre_tema):
        """Devuelve el estilo CSS para un tema específico"""
        return self.temas.get(nombre_tema, "")
    
    def aplicar_tema(self, widget, nombre_tema):
        if nombre_tema in self.temas:
            estilo = self.temas[nombre_tema]
            widget.setStyleSheet(estilo)
            
            # Guardar en BD
            try:
                cursor = self.conexion.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO configuracion (clave, valor, descripcion) VALUES (?, ?, ?)",
                    ('tema', nombre_tema, 'Tema de la interfaz')
                )
                self.conexion.commit()
            except sqlite3.Error:
                pass
                
            return True
        return False

    def obtener_tema_actual(self):
        """Obtiene el nombre del tema actualmente aplicado"""
        try:
            cursor = self.conexion.cursor()
            cursor.execute("SELECT valor FROM configuracion WHERE clave = 'tema'")
            resultado = cursor.fetchone()
            return resultado[0] if resultado else "Oscuro Moderno"
        except sqlite3.Error:
            return "Oscuro Moderno"
    
    def cargar_tema_guardado(self, widget):
        """
        Carga el último tema usado desde la BD y lo aplica
        Args:
            widget: Widget al que aplicar el tema
        Returns:
            str: Nombre del tema cargado o None si hubo error
        """
        try:
            tema = self.obtener_tema_actual()
            if self.aplicar_tema(widget, tema):
                return tema
            return None
        except Exception as e:
            print(f"Error al cargar tema desde BD: {str(e)}")
            return None
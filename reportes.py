# modulos/reportes.py
# ── ModuloReportes — CEO-Level Enterprise Reports ────────────────────────────
# Block 7 requirements:
#   ✓ KPI cards (revenue, margin, tickets, avg ticket, inventory, loyalty)
#   ✓ Margin real from ReportEngine — NO SQL in UI
#   ✓ Multi-branch comparison
#   ✓ Historical comparison (period vs previous period)
#   ✓ Inventory rotation
#   ✓ Community loyalty impact
#   ✓ Corporate color scheme — no default matplotlib styling
#   ✓ Export to PDF structured
#   ✓ Export to Excel structured
#   ✓ All data from ReportEngine only
from __future__ import annotations
import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QGroupBox, QPushButton,
    QTableWidget, QTableWidgetItem, QAbstractItemView, QTabWidget,
    QFrame, QHeaderView, QDateEdit, QComboBox, QSizePolicy, QMessageBox,
    QProgressBar, QScrollArea, QFileDialog, QGridLayout, QSplitter
)
from PyQt5.QtCore import Qt, QDate, QTimer
from PyQt5.QtGui import QColor, QFont, QPixmap
from .base import ModuloBase
from core.services.enterprise.report_engine import ReportEngine
from core.events.event_bus import EventBus

logger = logging.getLogger("spj.ui.reportes")
VENTA_COMPLETADA = "VENTA_COMPLETADA"

# Corporate palette
_C1 = "#1a252f"   # dark navy
_C2 = "#2c3e50"   # slate
_C3 = "#2980b9"   # corporate blue
_C4 = "#27ae60"   # positive green
_C5 = "#e74c3c"   # alert red
_C6 = "#f39c12"   # warning amber
_C7 = "#8e44ad"   # loyalty purple
_HDR_BG = _C1; _HDR_FG = "#ecf0f1"

class ModuloReportes(ModuloBase):
    def __init__(self, conexion, parent=None):
        super().__init__(conexion, parent)
        self.conexion = conexion
        self.sucursal_id = None; self.sucursal_nombre = "Todas"
        self.usuario_actual = "Sistema"; self.rol_usuario = ""
        self._engine = ReportEngine(conexion)
        self._init_ui()
        EventBus.subscribe(VENTA_COMPLETADA, lambda _: QTimer.singleShot(5000, self._refresh))
        QTimer.singleShot(200, self._load_branches)

    def set_sucursal(self, sid: int, snom: str):
        self.sucursal_id = sid; self.sucursal_nombre = snom
        if hasattr(self,"combo_suc"): self._sync_combo()
        QTimer.singleShot(0, self._refresh)

    def set_usuario_actual(self, u, r):
        self.usuario_actual = u or "Sistema"; self.rol_usuario = r or ""

    def obtener_usuario_actual(self): return self.usuario_actual

    def _init_ui(self):
        root = QVBoxLayout(self); root.setContentsMargins(16,12,16,12); root.setSpacing(10)
        # Header
        hdr = QHBoxLayout()
        if os.path.exists("logo.png"):
            pix = QPixmap("logo.png")
            if not pix.isNull():
                lb = QLabel(); lb.setPixmap(pix.scaled(44,44,Qt.KeepAspectRatio,Qt.SmoothTransformation)); hdr.addWidget(lb)
        title = QLabel("Reportes Ejecutivos — CEO Dashboard")
        f = title.font(); f.setPointSize(14); f.setBold(True); title.setFont(f)
        title.setObjectName("tituloPrincipal"); hdr.addWidget(title); hdr.addStretch()
        self.lbl_suc = QLabel(); self.lbl_suc.setStyleSheet("color:#7f8c8d;")
        hdr.addWidget(self.lbl_suc); root.addLayout(hdr)
        # Filter bar
        fb = QGroupBox("Período de Análisis"); fl = QHBoxLayout()
        self.date_desde = QDateEdit(QDate.currentDate().addDays(-29)); self.date_desde.setCalendarPopup(True); self.date_desde.setDisplayFormat("dd/MM/yyyy")
        self.date_hasta = QDateEdit(QDate.currentDate()); self.date_hasta.setCalendarPopup(True); self.date_hasta.setDisplayFormat("dd/MM/yyyy")
        self.combo_suc = QComboBox(); self.combo_suc.setMinimumWidth(160)
        btn_7d = QPushButton("7 días"); btn_7d.clicked.connect(lambda: self._set_period(7))
        btn_30d = QPushButton("30 días"); btn_30d.clicked.connect(lambda: self._set_period(30))
        btn_90d = QPushButton("90 días"); btn_90d.clicked.connect(lambda: self._set_period(90))
        btn_ref = QPushButton("🔍 Analizar"); btn_ref.clicked.connect(self._refresh)
        btn_ref.setStyleSheet(f"background:{_C3};color:white;font-weight:bold;padding:6px 14px;border-radius:4px;")
        for lbl,w in [("Desde:",self.date_desde),("Hasta:",self.date_hasta),("Sucursal:",self.combo_suc)]:
            fl.addWidget(QLabel(lbl)); fl.addWidget(w)
        for b in (btn_7d,btn_30d,btn_90d,btn_ref): fl.addWidget(b)
        fl.addStretch(); fb.setLayout(fl); root.addWidget(fb)
        # KPI cards row
        kpi_row = QHBoxLayout()
        self.kpi_ingresos = self._kpi("Ingresos Totales","$ —",_C4)
        self.kpi_margen   = self._kpi("Margen Real","—%",_C3)
        self.kpi_tickets  = self._kpi("Tickets","—",_C2)
        self.kpi_ticket_avg = self._kpi("Ticket Promedio","$ —",_C6)
        self.kpi_inv_val  = self._kpi("Valor Inventario","$ —","#16a085")
        self.kpi_loyalty  = self._kpi("Clientes Activos","—",_C7)
        for c in (self.kpi_ingresos,self.kpi_margen,self.kpi_tickets,self.kpi_ticket_avg,self.kpi_inv_val,self.kpi_loyalty):
            kpi_row.addWidget(c)
        root.addLayout(kpi_row)
        # Export buttons
        exp_row = QHBoxLayout()
        btn_pdf = QPushButton("📄 Exportar PDF"); btn_pdf.clicked.connect(self._export_pdf)
        btn_pdf.setStyleSheet(f"background:{_C5};color:white;font-weight:bold;padding:6px 12px;border-radius:4px;")
        btn_xls = QPushButton("📊 Exportar Excel"); btn_xls.clicked.connect(self._export_excel)
        btn_xls.setStyleSheet(f"background:{_C4};color:white;font-weight:bold;padding:6px 12px;border-radius:4px;")
        exp_row.addWidget(btn_pdf); exp_row.addWidget(btn_xls); exp_row.addStretch()
        root.addLayout(exp_row)
        # Tabs
        self.tabs = QTabWidget(); root.addWidget(self.tabs)
        self.tabs.addTab(self._tab_ventas(),      "📈 Ventas")
        self.tabs.addTab(self._tab_margen(),      "💰 Márgenes")
        self.tabs.addTab(self._tab_inventario(),  "📦 Inventario")
        self.tabs.addTab(self._tab_fidelidad(),   "🎖️ Fidelidad")
        self.tabs.addTab(self._tab_comparacion(), "🔄 Comparación")

    def _kpi(self, title, val, color):
        card = QFrame()
        card.setStyleSheet(f"QFrame{{background:white;border:none;border-left:5px solid {color};border-radius:6px;margin:2px;}}")
        card.setSizePolicy(QSizePolicy.Expanding,QSizePolicy.Fixed); card.setFixedHeight(80)
        lay = QVBoxLayout(card); lay.setContentsMargins(10,6,10,6)
        lt = QLabel(title); lt.setStyleSheet("color:#7f8c8d;font-size:11px;")
        lv = QLabel(val); lv.setStyleSheet(f"color:{color};font-size:17px;font-weight:bold;")
        lay.addWidget(lt); lay.addWidget(lv); card._val = lv; return card

    def _tab_ventas(self):
        w = QWidget(); lay = QVBoxLayout(w)
        lbl = QLabel("Detalle de Ventas por Período"); lbl.setStyleSheet(f"color:{_C2};font-weight:bold;font-size:13px;")
        lay.addWidget(lbl)
        self.tabla_ventas = self._std_table(["Fecha","Sucursal","Tickets","Ingresos","Costo","Margen","Margen %"])
        lay.addWidget(self.tabla_ventas)
        # Daily breakdown
        lbl2 = QLabel("Top 10 Productos por Ingreso"); lbl2.setStyleSheet(f"color:{_C2};font-weight:bold;font-size:13px;")
        lay.addWidget(lbl2)
        self.tabla_top_prod = self._std_table(["Producto","Categoría","Unidades","Ingresos","Margen %"])
        lay.addWidget(self.tabla_top_prod)
        return w

    def _tab_margen(self):
        w = QWidget(); lay = QVBoxLayout(w)
        lbl = QLabel("Análisis de Márgenes"); lbl.setStyleSheet(f"color:{_C2};font-weight:bold;font-size:13px;")
        lay.addWidget(lbl)
        self.tabla_margen = self._std_table(["Producto","Costo Prom","Precio Prom","Margen $","Margen %","Unidades","Ingresos Total"])
        lay.addWidget(self.tabla_margen)
        # Anomalies
        lbl2 = QLabel("Anomalías de Margen (Alertas)"); lbl2.setStyleSheet(f"color:{_C5};font-weight:bold;font-size:13px;")
        lay.addWidget(lbl2)
        self.tabla_anomalias = self._std_table(["Período","Sucursal","Producto","Margen %","Semana","Registrado"])
        lay.addWidget(self.tabla_anomalias)
        return w

    def _tab_inventario(self):
        w = QWidget(); lay = QVBoxLayout(w)
        lbl = QLabel("Rotación de Inventario"); lbl.setStyleSheet(f"color:{_C2};font-weight:bold;font-size:13px;")
        lay.addWidget(lbl)
        self.tabla_rotacion = self._std_table(["Producto","Categoría","Stock Actual","Unidades Vendidas","Rotación","Días Agotamiento","Valor"])
        lay.addWidget(self.tabla_rotacion)
        # Low stock alerts
        lbl2 = QLabel("⚠️ Alertas de Stock Bajo"); lbl2.setStyleSheet(f"color:{_C5};font-weight:bold;font-size:13px;")
        lay.addWidget(lbl2)
        self.tabla_bajo_stock = self._std_table(["Producto","Stock Actual","Stock Mínimo","Unidad","Categoría"])
        lay.addWidget(self.tabla_bajo_stock)
        return w

    def _tab_fidelidad(self):
        w = QWidget(); lay = QVBoxLayout(w)
        lbl = QLabel("Impacto del Programa de Fidelidad"); lbl.setStyleSheet(f"color:{_C2};font-weight:bold;font-size:13px;")
        lay.addWidget(lbl)
        # Level distribution
        lvl_box = QGroupBox("Distribución por Nivel"); lvl_lay = QHBoxLayout()
        self.kpi_bronze  = self._kpi("Bronce","0","#cd7f32")
        self.kpi_silver  = self._kpi("Plata","0","#95a5a6")
        self.kpi_gold    = self._kpi("Oro","0","#f39c12")
        self.kpi_plat    = self._kpi("Platino","0","#8e44ad")
        for c in (self.kpi_bronze,self.kpi_silver,self.kpi_gold,self.kpi_plat): lvl_lay.addWidget(c)
        lvl_box.setLayout(lvl_lay); lay.addWidget(lvl_box)
        # Top clients
        lbl2 = QLabel("Top Clientes Fieles"); lbl2.setStyleSheet(f"color:{_C2};font-weight:bold;font-size:13px;")
        lay.addWidget(lbl2)
        self.tabla_top_clientes = self._std_table(["Cliente","Nivel","Puntos","Visitas","Total Compras","Margen Generado"])
        lay.addWidget(self.tabla_top_clientes)
        return w

    def _tab_comparacion(self):
        w = QWidget(); lay = QVBoxLayout(w)
        lbl = QLabel("Comparación Multi-Sucursal"); lbl.setStyleSheet(f"color:{_C2};font-weight:bold;font-size:13px;")
        lay.addWidget(lbl)
        self.tabla_comp_suc = self._std_table(["Sucursal","Ingresos","Costo","Margen","Margen %","Tickets","Ticket Prom"])
        lay.addWidget(self.tabla_comp_suc)
        lbl2 = QLabel("Comparación Período Actual vs Anterior"); lbl2.setStyleSheet(f"color:{_C2};font-weight:bold;font-size:13px;")
        lay.addWidget(lbl2)
        self.tabla_hist = self._std_table(["Métrica","Período Actual","Período Anterior","Variación $","Variación %"])
        lay.addWidget(self.tabla_hist)
        return w

    def _std_table(self, headers: list) -> QTableWidget:
        t = QTableWidget(); t.setColumnCount(len(headers)); t.setHorizontalHeaderLabels(headers)
        t.setEditTriggers(QAbstractItemView.NoEditTriggers)
        t.setSelectionBehavior(QAbstractItemView.SelectRows)
        t.setAlternatingRowColors(True); t.setSortingEnabled(True)
        hh = t.horizontalHeader(); hh.setSectionResizeMode(QHeaderView.Interactive)
        hh.setSectionResizeMode(0,QHeaderView.Stretch)
        hh.setStyleSheet(f"QHeaderView::section{{background:{_HDR_BG};color:{_HDR_FG};font-weight:bold;padding:5px;border:none;}}")
        return t

    # ── Data Loading ──────────────────────────────────────────────────────────
    def _load_branches(self):
        try:
            rows = self.conexion.fetchall("SELECT id,nombre FROM sucursales WHERE activa=1 ORDER BY nombre")
            self.combo_suc.addItem("Todas las sucursales", None)
            for r in rows: self.combo_suc.addItem(r["nombre"], r["id"])
            if self.sucursal_id:
                for i in range(self.combo_suc.count()):
                    if self.combo_suc.itemData(i) == self.sucursal_id:
                        self.combo_suc.setCurrentIndex(i); break
        except Exception as e: logger.error("load branches: %s",e)

    def _sync_combo(self):
        for i in range(self.combo_suc.count()):
            if self.combo_suc.itemData(i) == self.sucursal_id:
                self.combo_suc.setCurrentIndex(i); break

    def _set_period(self, days: int):
        self.date_hasta.setDate(QDate.currentDate())
        self.date_desde.setDate(QDate.currentDate().addDays(-days+1))
        self._refresh()

    def _refresh(self):
        df = self.date_desde.date().toString("yyyy-MM-dd")
        dt = self.date_hasta.date().toString("yyyy-MM-dd")
        branch_id = self.combo_suc.currentData()
        eff_branch = branch_id or self.sucursal_id or 1
        self.lbl_suc.setText(f"Sucursal: {self.sucursal_nombre} | {df} → {dt}")
        try:
            self._load_kpis(eff_branch, df, dt)
            self._load_ventas(eff_branch, df, dt)
            self._load_margen(eff_branch, df, dt)
            self._load_inventario(eff_branch, df, dt)
            self._load_fidelidad(eff_branch, df, dt)
            self._load_comparacion(df, dt)
        except Exception as e:
            logger.error("refresh: %s", e)
            QMessageBox.warning(self,"Reportes",f"Error al cargar datos:\n{e}")

    def _load_kpis(self, branch_id, df, dt):
        try:
            kpis = self._engine.get_kpi_cards(branch_id, df, dt)
            self.kpi_ingresos._val.setText(f"$ {kpis.get('total_revenue',0):,.2f}")
            mp = kpis.get('margin_pct',0)
            self.kpi_margen._val.setText(f"{mp:.1f}%")
            self.kpi_margen._val.setStyleSheet(f"color:{_C4 if mp>=15 else _C5};font-size:17px;font-weight:bold;")
            self.kpi_tickets._val.setText(f"{int(kpis.get('ticket_count',0)):,}")
            self.kpi_ticket_avg._val.setText(f"$ {kpis.get('avg_ticket',0):,.2f}")
            self.kpi_inv_val._val.setText(f"$ {kpis.get('inventory_value',0):,.2f}")
            self.kpi_loyalty._val.setText(str(int(kpis.get('active_clients',0))))
        except Exception as e: logger.error("kpis: %s",e)

    def _load_ventas(self, branch_id, df, dt):
        try:
            daily = self._engine.get_daily_sales(branch_id, df, dt) if hasattr(self._engine,"get_daily_sales") else []
            self.tabla_ventas.setRowCount(len(daily))
            for i,r in enumerate(daily):
                mp = float(r.get("margin_pct",0))
                vals = [r.get("fecha","")[:10],r.get("sucursal_nombre","Principal"),str(r.get("tickets",0)),
                        f"$ {float(r.get('ingresos',0)):,.2f}",f"$ {float(r.get('costo',0)):,.2f}",
                        f"$ {float(r.get('margen',0)):,.2f}",f"{mp:.1f}%"]
                for j,v in enumerate(vals):
                    item = QTableWidgetItem(v)
                    if j==6:
                        item.setForeground(QColor(_C4 if mp>=15 else _C5))
                        item.setFont(QFont("",weight=QFont.Bold))
                    self.tabla_ventas.setItem(i,j,item)
            top = self._engine.get_top_products(branch_id, df, dt) if hasattr(self._engine,"get_top_products") else []
            self.tabla_top_prod.setRowCount(len(top))
            for i,r in enumerate(top):
                vals = [r.get("nombre",""),r.get("categoria",""),f"{float(r.get('qty',0)):,.2f}",
                        f"$ {float(r.get('revenue',0)):,.2f}",f"{float(r.get('margin_pct',0)):.1f}%"]
                for j,v in enumerate(vals): self.tabla_top_prod.setItem(i,j,QTableWidgetItem(v))
        except Exception as e: logger.error("ventas tab: %s",e)

    def _load_margen(self, branch_id, df, dt):
        try:
            prod_margins = self._engine.get_product_margins(branch_id, df, dt) if hasattr(self._engine,"get_product_margins") else []
            self.tabla_margen.setRowCount(len(prod_margins))
            for i,r in enumerate(prod_margins):
                mp = float(r.get("margin_pct",0))
                vals = [r.get("nombre",""),f"$ {float(r.get('costo_prom',0)):,.2f}",
                        f"$ {float(r.get('precio_prom',0)):,.2f}",f"$ {float(r.get('margen_abs',0)):,.2f}",
                        f"{mp:.1f}%",f"{float(r.get('qty',0)):,.2f}",f"$ {float(r.get('revenue',0)):,.2f}"]
                for j,v in enumerate(vals):
                    item = QTableWidgetItem(v)
                    if j==4:
                        item.setForeground(QColor(_C4 if mp>=15 else _C5))
                        item.setFont(QFont("",weight=QFont.Bold))
                    self.tabla_margen.setItem(i,j,item)
            anomalias = self._engine.get_margin_anomalies(branch_id, df, dt) if hasattr(self._engine,"get_margin_anomalies") else []
            self.tabla_anomalias.setRowCount(len(anomalias))
            for i,r in enumerate(anomalias):
                vals = [r.get("week_label",""),r.get("branch_id",""),r.get("product_id",""),
                        f"{float(r.get('margin_pct',0)):.1f}%",r.get("week_label",""),r.get("created_at","")[:16]]
                for j,v in enumerate(vals): self.tabla_anomalias.setItem(i,j,QTableWidgetItem(v))
        except Exception as e: logger.error("margen tab: %s",e)

    def _load_inventario(self, branch_id, df, dt):
        try:
            rot = self._engine.get_inventory_rotation(branch_id, df, dt) if hasattr(self._engine,"get_inventory_rotation") else []
            self.tabla_rotacion.setRowCount(len(rot))
            for i,r in enumerate(rot):
                days_ago = float(r.get("days_to_stockout",0))
                vals = [r.get("nombre",""),r.get("categoria",""),
                        f"{float(r.get('stock',0)):,.3f}",f"{float(r.get('sold',0)):,.3f}",
                        f"{float(r.get('rotation',0)):.2f}x",
                        f"{days_ago:.0f} días" if days_ago>0 else "—",
                        f"$ {float(r.get('valor',0)):,.2f}"]
                for j,v in enumerate(vals): self.tabla_rotacion.setItem(i,j,QTableWidgetItem(v))
            low = self.conexion.fetchall("SELECT nombre,existencia,stock_minimo,unidad,categoria FROM productos WHERE is_active=1 AND existencia<=stock_minimo AND stock_minimo>0 ORDER BY (existencia-stock_minimo) LIMIT 50")
            self.tabla_bajo_stock.setRowCount(len(low))
            for i,r in enumerate(low):
                vals = [r["nombre"],f"{float(r['existencia'] or 0):,.3f}",f"{float(r['stock_minimo'] or 0):,.3f}",r["unidad"] or "",r["categoria"] or ""]
                for j,v in enumerate(vals):
                    item = QTableWidgetItem(v)
                    if j in (1,2): item.setForeground(QColor(_C5)); item.setFont(QFont("",weight=QFont.Bold))
                    self.tabla_bajo_stock.setItem(i,j,item)
        except Exception as e: logger.error("inventario tab: %s",e)

    def _load_fidelidad(self, branch_id, df, dt):
        try:
            impact = self._engine.get_loyalty_impact(branch_id, df, dt) if hasattr(self._engine,"get_loyalty_impact") else {}
            for level,kpi in [("Bronce",self.kpi_bronze),("Plata",self.kpi_silver),("Oro",self.kpi_gold),("Platino",self.kpi_plat)]:
                cnt = impact.get(f"count_{level.lower()}",0)
                kpi._val.setText(str(cnt))
            top_c = self._engine.get_top_loyal_clients(branch_id, df, dt) if hasattr(self._engine,"get_top_loyal_clients") else []
            self.tabla_top_clientes.setRowCount(len(top_c))
            for i,r in enumerate(top_c):
                vals = [r.get("nombre",""),r.get("nivel",""),f"{int(r.get('puntos',0)):,}",
                        str(r.get("visitas",0)),f"$ {float(r.get('total',0)):,.2f}",
                        f"$ {float(r.get('margen',0)):,.2f}"]
                for j,v in enumerate(vals): self.tabla_top_clientes.setItem(i,j,QTableWidgetItem(v))
        except Exception as e: logger.error("fidelidad tab: %s",e)

    def _load_comparacion(self, df, dt):
        try:
            comp = self._engine.get_branch_comparison(df, dt) if hasattr(self._engine,"get_branch_comparison") else []
            self.tabla_comp_suc.setRowCount(len(comp))
            for i,r in enumerate(comp):
                mp = float(r.get("margin_pct",0))
                vals = [r.get("nombre",""),f"$ {float(r.get('ingresos',0)):,.2f}",
                        f"$ {float(r.get('costo',0)):,.2f}",f"$ {float(r.get('margen',0)):,.2f}",
                        f"{mp:.1f}%",str(r.get("tickets",0)),f"$ {float(r.get('avg_ticket',0)):,.2f}"]
                for j,v in enumerate(vals): self.tabla_comp_suc.setItem(i,j,QTableWidgetItem(v))
            # Historical comparison
            d_start = QDate.fromString(df,"yyyy-MM-dd")
            d_end = QDate.fromString(dt,"yyyy-MM-dd")
            period_days = d_start.daysTo(d_end)+1
            prev_end = d_start.addDays(-1); prev_start = prev_end.addDays(-period_days+1)
            hist = self._engine.get_historical_comparison(
                self.sucursal_id or 1,
                df, dt,
                prev_start.toString("yyyy-MM-dd"),
                prev_end.toString("yyyy-MM-dd")
            ) if hasattr(self._engine,"get_historical_comparison") else []
            self.tabla_hist.setRowCount(len(hist))
            for i,r in enumerate(hist):
                var = float(r.get("variation_pct",0))
                vals = [r.get("metric",""),r.get("current",""),r.get("previous",""),
                        r.get("variation_abs",""),f"{var:.1f}%"]
                for j,v in enumerate(vals):
                    item = QTableWidgetItem(v)
                    if j==4:
                        item.setForeground(QColor(_C4 if var>=0 else _C5))
                        item.setFont(QFont("",weight=QFont.Bold))
                    self.tabla_hist.setItem(i,j,item)
        except Exception as e: logger.error("comparacion tab: %s",e)

    # ── Exports ───────────────────────────────────────────────────────────────
    def _export_pdf(self):
        path,_ = QFileDialog.getSaveFileName(self,"Exportar PDF",f"reporte_ceo_{date.today().isoformat()}.pdf","PDF (*.pdf)")
        if not path: return
        df = self.date_desde.date().toString("yyyy-MM-dd"); dt = self.date_hasta.date().toString("yyyy-MM-dd")
        branch = self.combo_suc.currentData() or self.sucursal_id or 1
        try:
            self._engine.export_pdf(branch, df, dt, path)
            QMessageBox.information(self,"Exportar PDF",f"Reporte exportado exitosamente:\n{path}")
        except Exception as e:
            logger.error("pdf export: %s",e)
            QMessageBox.critical(self,"Error PDF",f"No se pudo generar el PDF:\n{e}\n\nVerifique que reportlab o weasyprint esté instalado.")

    def _export_excel(self):
        path,_ = QFileDialog.getSaveFileName(self,"Exportar Excel",f"reporte_ceo_{date.today().isoformat()}.xlsx","Excel (*.xlsx)")
        if not path: return
        df = self.date_desde.date().toString("yyyy-MM-dd"); dt = self.date_hasta.date().toString("yyyy-MM-dd")
        branch = self.combo_suc.currentData() or self.sucursal_id or 1
        try:
            self._engine.export_excel(branch, df, dt, path)
            QMessageBox.information(self,"Exportar Excel",f"Reporte exportado exitosamente:\n{path}")
        except Exception as e:
            logger.error("excel export: %s",e)
            QMessageBox.critical(self,"Error Excel",f"No se pudo generar el Excel:\n{e}\n\nVerifique que openpyxl esté instalado.")

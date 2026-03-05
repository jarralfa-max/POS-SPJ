# modulos/caja.py
# Block 8: Enterprise Cash Drawer Module - No raw SQL, full repository usage
from __future__ import annotations
import logging, os, csv
from datetime import date
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QGroupBox, QDateEdit,
    QComboBox, QPushButton, QTableWidget, QTableWidgetItem, QAbstractItemView,
    QMessageBox, QDialog, QFormLayout, QDoubleSpinBox, QTextEdit, QFrame,
    QHeaderView, QSizePolicy, QLineEdit, QFileDialog
)
from PyQt5.QtCore import Qt, QDate, QTimer
from PyQt5.QtGui import QPixmap, QColor, QFont
from .base import ModuloBase
from repositories.caja import CajaRepository, CajaError
from core.events.event_bus import EventBus

logger = logging.getLogger("spj.ui.caja")
VENTA_COMPLETADA = "VENTA_COMPLETADA"
CAJA_MOVIMIENTO  = "CAJA_MOVIMIENTO"
_CLR_POS = "#27ae60"; _CLR_NEG = "#e74c3c"; _CLR_NEUTRAL = "#2980b9"
_CLR_HDR_BG = "#1a252f"; _CLR_HDR_FG = "#ecf0f1"

class ModuloCaja(ModuloBase):
    def __init__(self, conexion, usuario_actual: str, main_window=None, parent=None):
        super().__init__(conexion, parent)
        self.conexion = conexion
        self.usuario_actual = usuario_actual or "Sistema"
        self.rol_usuario = ""; self.main_window = main_window
        self.sucursal_id = 1; self.sucursal_nombre = "Principal"
        self._repo = CajaRepository(conexion)
        self._init_ui(); self._subscribe_events()
        QTimer.singleShot(0, self._refresh)

    def set_sucursal(self, sucursal_id: int, sucursal_nombre: str) -> None:
        self.sucursal_id = sucursal_id; self.sucursal_nombre = sucursal_nombre
        QTimer.singleShot(0, self._refresh)

    def set_usuario_actual(self, usuario: str, rol: str) -> None:
        self.usuario_actual = usuario or "Sistema"; self.rol_usuario = rol or ""

    def obtener_usuario_actual(self) -> str:
        return self.usuario_actual

    def _subscribe_events(self) -> None:
        EventBus.subscribe(VENTA_COMPLETADA, lambda _: QTimer.singleShot(0, self._refresh_resumen))
        EventBus.subscribe(CAJA_MOVIMIENTO,  lambda _: QTimer.singleShot(0, self._refresh))

    def _init_ui(self) -> None:
        root = QVBoxLayout(self); root.setContentsMargins(16,16,16,16); root.setSpacing(12)
        # Header
        hdr = QHBoxLayout()
        if os.path.exists("logo.png"):
            pix = QPixmap("logo.png")
            if not pix.isNull():
                lbl = QLabel(); lbl.setPixmap(pix.scaled(48,48,Qt.KeepAspectRatio,Qt.SmoothTransformation))
                hdr.addWidget(lbl)
        title = QLabel("Gestión de Caja"); title.setObjectName("tituloPrincipal")
        f = title.font(); f.setPointSize(16); f.setBold(True); title.setFont(f)
        hdr.addWidget(title); hdr.addStretch()
        self.lbl_sucursal = QLabel(); self.lbl_sucursal.setStyleSheet("color:#7f8c8d;font-size:12px;")
        hdr.addWidget(self.lbl_sucursal); root.addLayout(hdr)
        # KPI cards
        kpi_lay = QHBoxLayout()
        self.kpi_ingresos = self._kpi("Ingresos del Día","$ 0.00",_CLR_POS)
        self.kpi_egresos  = self._kpi("Egresos del Día","$ 0.00",_CLR_NEG)
        self.kpi_balance  = self._kpi("Balance Neto","$ 0.00",_CLR_NEUTRAL)
        self.kpi_tickets  = self._kpi("Tickets","0","#8e44ad")
        for c in (self.kpi_ingresos,self.kpi_egresos,self.kpi_balance,self.kpi_tickets):
            kpi_lay.addWidget(c)
        root.addLayout(kpi_lay)
        # Filters
        fb = QGroupBox("Filtros"); fl = QHBoxLayout()
        self.date_ini = QDateEdit(QDate.currentDate()); self.date_ini.setCalendarPopup(True); self.date_ini.setDisplayFormat("dd/MM/yyyy")
        self.date_fin = QDateEdit(QDate.currentDate()); self.date_fin.setCalendarPopup(True); self.date_fin.setDisplayFormat("dd/MM/yyyy")
        self.combo_tipo = QComboBox(); self.combo_tipo.addItems(["Todos","INGRESO","EGRESO","APERTURA","CIERRE","AJUSTE"])
        btn_f = QPushButton("Filtrar"); btn_f.clicked.connect(self._refresh)
        btn_h = QPushButton("Hoy"); btn_h.clicked.connect(self._set_today)
        for lbl,w in [("Desde:",self.date_ini),("Hasta:",self.date_fin),("Tipo:",self.combo_tipo)]:
            fl.addWidget(QLabel(lbl)); fl.addWidget(w)
        fl.addWidget(btn_f); fl.addWidget(btn_h); fl.addStretch(); fb.setLayout(fl); root.addWidget(fb)
        # Action buttons
        br = QHBoxLayout()
        self.btn_ap = QPushButton("🔓 Apertura"); self.btn_ap.clicked.connect(self._apertura)
        self.btn_ap.setStyleSheet(f"background:{_CLR_POS};color:white;font-weight:bold;padding:8px 14px;border-radius:4px;")
        self.btn_eg = QPushButton("➖ Egreso"); self.btn_eg.clicked.connect(self._egreso)
        self.btn_eg.setStyleSheet(f"background:{_CLR_NEG};color:white;font-weight:bold;padding:8px 14px;border-radius:4px;")
        self.btn_ci = QPushButton("🔒 Cierre"); self.btn_ci.clicked.connect(self._cierre)
        self.btn_ci.setStyleSheet("background:#e67e22;color:white;font-weight:bold;padding:8px 14px;border-radius:4px;")
        self.btn_ex = QPushButton("📊 Exportar CSV"); self.btn_ex.clicked.connect(self._exportar)
        for b in (self.btn_ap,self.btn_eg,self.btn_ci,self.btn_ex): br.addWidget(b)
        br.addStretch(); root.addLayout(br)
        # Table
        self.tabla = QTableWidget()
        cols = ["ID","Tipo","Monto","Usuario","Referencia","Forma Pago","Fecha/Hora"]
        self.tabla.setColumnCount(len(cols)); self.tabla.setHorizontalHeaderLabels(cols)
        self.tabla.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tabla.setAlternatingRowColors(True); self.tabla.setSortingEnabled(True)
        hh = self.tabla.horizontalHeader(); hh.setSectionResizeMode(4,QHeaderView.Stretch)
        hh.setStyleSheet(f"QHeaderView::section{{background:{_CLR_HDR_BG};color:{_CLR_HDR_FG};font-weight:bold;padding:6px;}}")
        root.addWidget(self.tabla)
        # Forma pago breakdown
        fpb = QGroupBox("Desglose por Forma de Pago"); fpl = QVBoxLayout()
        self.tabla_fp = QTableWidget(0,3); self.tabla_fp.setHorizontalHeaderLabels(["Forma de Pago","Total","Transacciones"])
        self.tabla_fp.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.tabla_fp.setMaximumHeight(150); self.tabla_fp.setEditTriggers(QAbstractItemView.NoEditTriggers)
        fpl.addWidget(self.tabla_fp); fpb.setLayout(fpl); root.addWidget(fpb)

    def _kpi(self, title: str, value: str, color: str) -> QFrame:
        card = QFrame()
        card.setStyleSheet(f"QFrame{{background:white;border:none;border-left:4px solid {color};border-radius:6px;padding:8px;}}")
        card.setSizePolicy(QSizePolicy.Expanding,QSizePolicy.Fixed); card.setFixedHeight(80)
        lay = QVBoxLayout(card); lay.setContentsMargins(8,4,8,4)
        lt = QLabel(title); lt.setStyleSheet("color:#7f8c8d;font-size:11px;")
        lv = QLabel(value); lv.setStyleSheet(f"color:{color};font-size:20px;font-weight:bold;")
        lay.addWidget(lt); lay.addWidget(lv); card._val = lv; return card

    def _refresh(self) -> None:
        self.lbl_sucursal.setText(f"Sucursal: {self.sucursal_nombre}")
        fd = self.date_ini.date().toString("yyyy-MM-dd"); fh = self.date_fin.date().toString("yyyy-MM-dd")
        tipo = self.combo_tipo.currentText()
        try:
            movs = self._repo.get_movimientos(self.sucursal_id,fecha_desde=f"{fd} 00:00:00",fecha_hasta=f"{fh} 23:59:59",tipo=tipo if tipo!="Todos" else None)
            self._fill_table(movs); self._refresh_resumen(); self._refresh_fp(fd,fh)
        except Exception as e: logger.error("caja refresh: %s", e)

    def _refresh_resumen(self) -> None:
        today = date.today().isoformat()
        try:
            r = self._repo.get_resumen_dia(self.sucursal_id, today)
            self.kpi_ingresos._val.setText(f"$ {r['INGRESO']['total']:,.2f}")
            self.kpi_egresos._val.setText(f"$ {r['EGRESO']['total']:,.2f}")
            bal = r["balance_neto"]
            self.kpi_balance._val.setText(f"$ {bal:,.2f}")
            self.kpi_balance._val.setStyleSheet(f"color:{_CLR_POS if bal>=0 else _CLR_NEG};font-size:20px;font-weight:bold;")
            self.kpi_tickets._val.setText(str(r["INGRESO"]["count"]))
        except Exception as e: logger.error("resumen: %s", e)

    def _refresh_fp(self, fd: str, fh: str) -> None:
        try:
            rows = self._repo.get_forma_pago_breakdown(self.sucursal_id, fd, fh)
            self.tabla_fp.setRowCount(len(rows))
            for i,r in enumerate(rows):
                self.tabla_fp.setItem(i,0,QTableWidgetItem(r["forma_pago"]))
                self.tabla_fp.setItem(i,1,QTableWidgetItem(f"$ {r['total']:,.2f}"))
                self.tabla_fp.setItem(i,2,QTableWidgetItem(str(r["count"])))
        except Exception as e: logger.error("fp: %s", e)

    def _fill_table(self, movs: list) -> None:
        colors = {"INGRESO":_CLR_POS,"EGRESO":_CLR_NEG,"APERTURA":"#27ae60","CIERRE":"#e67e22","AJUSTE":"#8e44ad"}
        self.tabla.setRowCount(len(movs))
        for i,m in enumerate(movs):
            clr = QColor(colors.get(m.get("operation_type",""),"#2c3e50"))
            vals = [str(m.get("id","")),m.get("operation_type",""),f"$ {float(m.get('amount',0)):,.2f}",
                    m.get("usuario",""),m.get("reference","") or m.get("notes",""),
                    m.get("forma_pago","") or "—",(m.get("created_at","") or "")[:19]]
            for j,v in enumerate(vals):
                item = QTableWidgetItem(v)
                if j==1: item.setForeground(clr); item.setFont(QFont("",weight=QFont.Bold))
                self.tabla.setItem(i,j,item)

    def _set_today(self) -> None:
        t = QDate.currentDate(); self.date_ini.setDate(t); self.date_fin.setDate(t); self._refresh()

    def _apertura(self) -> None:
        try:
            if self._repo.has_apertura_today(self.sucursal_id):
                QMessageBox.warning(self,"Caja","Ya existe apertura registrada hoy."); return
        except Exception: pass
        dlg = _MontoDialog("Apertura de Caja","Monto inicial:",self)
        if dlg.exec_() != QDialog.Accepted: return
        try:
            op = self._repo.apertura_caja(self.sucursal_id,self.usuario_actual,dlg.monto(),dlg.notas())
            QMessageBox.information(self,"Caja",f"Apertura registrada.\nOp: {op[:8]}...")
            self._refresh()
        except Exception as e: QMessageBox.critical(self,"Error",str(e))

    def _egreso(self) -> None:
        dlg = _EgresoDialog(self)
        if dlg.exec_() != QDialog.Accepted: return
        m = dlg.monto()
        if m <= 0: QMessageBox.warning(self,"Caja","El monto debe ser mayor a 0."); return
        try:
            op = self._repo.egreso(self.sucursal_id,self.usuario_actual,m,dlg.concepto())
            QMessageBox.information(self,"Caja",f"Egreso registrado.\nOp: {op[:8]}...")
            self._refresh()
        except Exception as e: QMessageBox.critical(self,"Error",str(e))

    def _cierre(self) -> None:
        try:
            r = self._repo.get_resumen_dia(self.sucursal_id,date.today().isoformat())
            msg = f"Balance del día: $ {r['balance_neto']:,.2f}\n\n¿Confirmar cierre?"
        except Exception: msg = "¿Confirmar cierre de caja?"
        if QMessageBox.question(self,"Cierre",msg,QMessageBox.Yes|QMessageBox.No) != QMessageBox.Yes: return
        dlg = _MontoDialog("Cierre de Caja","Monto final contado:",self)
        if dlg.exec_() != QDialog.Accepted: return
        try:
            op = self._repo.cierre_caja(self.sucursal_id,self.usuario_actual,dlg.monto(),dlg.notas())
            QMessageBox.information(self,"Caja",f"Cierre registrado.\nOp: {op[:8]}...")
            self._refresh()
        except Exception as e: QMessageBox.critical(self,"Error",str(e))

    def _exportar(self) -> None:
        path,_ = QFileDialog.getSaveFileName(self,"Exportar",f"caja_{date.today().isoformat()}.csv","CSV (*.csv)")
        if not path: return
        fd = self.date_ini.date().toString("yyyy-MM-dd"); fh = self.date_fin.date().toString("yyyy-MM-dd")
        try:
            movs = self._repo.get_movimientos(self.sucursal_id,fecha_desde=f"{fd} 00:00:00",fecha_hasta=f"{fh} 23:59:59")
            with open(path,"w",newline="",encoding="utf-8") as f:
                w = csv.DictWriter(f,fieldnames=["id","operation_type","amount","usuario","reference","forma_pago","created_at"])
                w.writeheader()
                for m in movs: w.writerow({k:m.get(k) for k in w.fieldnames})
            QMessageBox.information(self,"Exportar",f"Exportado: {path}")
        except Exception as e: QMessageBox.critical(self,"Error",str(e))


class _MontoDialog(QDialog):
    def __init__(self,title,label,parent=None):
        super().__init__(parent); self.setWindowTitle(title); self.setMinimumWidth(320)
        lay = QFormLayout(self)
        self._spin = QDoubleSpinBox(); self._spin.setRange(0,9999999); self._spin.setDecimals(2); self._spin.setPrefix("$ ")
        self._obs = QTextEdit(); self._obs.setPlaceholderText("Observaciones"); self._obs.setMaximumHeight(60)
        lay.addRow(label,self._spin); lay.addRow("Observaciones:",self._obs)
        bh = QHBoxLayout(); ok = QPushButton("Confirmar"); ok.clicked.connect(self.accept)
        cn = QPushButton("Cancelar"); cn.clicked.connect(self.reject); bh.addWidget(ok); bh.addWidget(cn); lay.addRow(bh)
    def monto(self): return self._spin.value()
    def notas(self): return self._obs.toPlainText().strip()

class _EgresoDialog(QDialog):
    def __init__(self,parent=None):
        super().__init__(parent); self.setWindowTitle("Registrar Egreso"); self.setMinimumWidth(340)
        lay = QFormLayout(self)
        self._con = QLineEdit(); self._con.setPlaceholderText("Concepto del egreso")
        self._spin = QDoubleSpinBox(); self._spin.setRange(0.01,9999999); self._spin.setDecimals(2); self._spin.setPrefix("$ ")
        lay.addRow("Concepto:",self._con); lay.addRow("Monto:",self._spin)
        bh = QHBoxLayout(); ok = QPushButton("Registrar"); ok.clicked.connect(self.accept)
        cn = QPushButton("Cancelar"); cn.clicked.connect(self.reject); bh.addWidget(ok); bh.addWidget(cn); lay.addRow(bh)
    def monto(self): return self._spin.value()
    def concepto(self): return self._con.text().strip()

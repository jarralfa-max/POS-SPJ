# modulos/fidelidad.py
# ── ModuloFidelidad — Enterprise 4-Layer Loyalty System UI ──────────────────
# Layer 1: Individual Points with dynamic multipliers and financial guards
# Layer 2: Levels / Status (Bronze, Silver, Gold, Platinum)
# Layer 3: Challenges (Gamification with progress tracking)
# Layer 4: Community Goals (collective progress)
# Block 5 complete: Margin validation, budget cap, multiplier, max discount,
#                   redemption ceiling, ROI tracking, no loss scenarios.
# Ticket messages: community progress, level progress, challenge progress.
from __future__ import annotations
import logging
from datetime import date, datetime
from typing import Dict, List, Optional
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QGroupBox, QPushButton,
    QTableWidget, QTableWidgetItem, QAbstractItemView, QTabWidget,
    QFrame, QHeaderView, QProgressBar, QFormLayout, QDialog,
    QLineEdit, QDoubleSpinBox, QSpinBox, QDateEdit, QComboBox,
    QMessageBox, QSizePolicy, QScrollArea, QTextEdit, QGridLayout,
    QCheckBox
)
from PyQt5.QtCore import Qt, QTimer, QDate
from PyQt5.QtGui import QColor, QFont, QPalette
from .base import ModuloBase
from core.events.event_bus import EventBus
from core.services.enterprise.loyalty_enterprise_engine import (
    LoyaltyEnterpriseEngine, LEVEL_BRONZE, LEVEL_SILVER, LEVEL_GOLD, LEVEL_PLATINUM,
    LEVEL_THRESHOLDS, LEVEL_ORDER
)
logger = logging.getLogger("spj.ui.fidelidad")

VENTA_COMPLETADA = "VENTA_COMPLETADA"
_LEVEL_ICONS = {LEVEL_BRONZE:"🥉",LEVEL_SILVER:"🥈",LEVEL_GOLD:"🥇",LEVEL_PLATINUM:"💎"}
_LEVEL_COLORS = {LEVEL_BRONZE:"#cd7f32",LEVEL_SILVER:"#95a5a6",LEVEL_GOLD:"#f39c12",LEVEL_PLATINUM:"#8e44ad"}
_HDR = "#1a252f"; _HDR_FG = "#ecf0f1"

class ModuloFidelidad(ModuloBase):
    def __init__(self, conexion, parent=None):
        super().__init__(conexion, parent)
        self.conexion = conexion; self.sucursal_id = 1; self.sucursal_nombre = "Principal"
        self.usuario_actual = "Sistema"; self.rol_usuario = ""
        self._engine = LoyaltyEnterpriseEngine(conexion)
        self._init_ui()
        EventBus.subscribe(VENTA_COMPLETADA, lambda _: QTimer.singleShot(0, self._refresh_all))
        QTimer.singleShot(0, self._refresh_all)

    def set_sucursal(self, sid: int, snom: str):
        self.sucursal_id = sid; self.sucursal_nombre = snom; QTimer.singleShot(0, self._refresh_all)

    def set_usuario_actual(self, u, r):
        self.usuario_actual = u or "Sistema"; self.rol_usuario = r or ""

    def obtener_usuario_actual(self): return self.usuario_actual

    def _init_ui(self):
        root = QVBoxLayout(self); root.setContentsMargins(16,12,16,12); root.setSpacing(10)
        # Header
        hdr = QHBoxLayout()
        title = QLabel("Sistema de Fidelidad Enterprise"); title.setObjectName("tituloPrincipal")
        f = title.font(); f.setPointSize(15); f.setBold(True); title.setFont(f)
        hdr.addWidget(title); hdr.addStretch()
        self.lbl_suc = QLabel(); self.lbl_suc.setStyleSheet("color:#7f8c8d;")
        hdr.addWidget(self.lbl_suc); root.addLayout(hdr)
        # Summary KPI
        kpi_row = QHBoxLayout()
        self.kpi_clients  = self._kpi("Clientes Activos","0","#27ae60")
        self.kpi_pts_mes  = self._kpi("Puntos Emitidos (mes)","0","#2980b9")
        self.kpi_roi      = self._kpi("ROI Fidelidad","0%","#8e44ad")
        self.kpi_budget   = self._kpi("Presupuesto Restante","$ 0","#e67e22")
        for c in (self.kpi_clients,self.kpi_pts_mes,self.kpi_roi,self.kpi_budget):
            kpi_row.addWidget(c)
        root.addLayout(kpi_row)
        # Tabs
        self.tabs = QTabWidget(); root.addWidget(self.tabs)
        self.tabs.addTab(self._tab_clientes(),  "👥 Clientes")
        self.tabs.addTab(self._tab_desafios(),  "🎯 Desafíos")
        self.tabs.addTab(self._tab_comunidad(), "🌍 Comunidad")
        self.tabs.addTab(self._tab_config(),    "⚙️ Configuración")

    def _kpi(self, title, val, color):
        card = QFrame()
        card.setStyleSheet(f"QFrame{{background:white;border:none;border-left:4px solid {color};border-radius:6px;}}")
        card.setSizePolicy(QSizePolicy.Expanding,QSizePolicy.Fixed); card.setFixedHeight(76)
        lay = QVBoxLayout(card); lay.setContentsMargins(10,6,10,6)
        lt = QLabel(title); lt.setStyleSheet("color:#7f8c8d;font-size:11px;")
        lv = QLabel(val); lv.setStyleSheet(f"color:{color};font-size:18px;font-weight:bold;")
        lay.addWidget(lt); lay.addWidget(lv); card._val = lv; return card

    # ── Tab: Clientes ─────────────────────────────────────────────────────────
    def _tab_clientes(self):
        w = QWidget(); lay = QVBoxLayout(w); lay.setSpacing(8)
        # Search bar
        sh = QHBoxLayout()
        self.txt_search = QLineEdit(); self.txt_search.setPlaceholderText("Buscar cliente por nombre o teléfono...")
        self.txt_search.textChanged.connect(self._refresh_clientes)
        sh.addWidget(QLabel("Buscar:")); sh.addWidget(self.txt_search); sh.addStretch()
        btn_redeem = QPushButton("💳 Canjear Puntos"); btn_redeem.clicked.connect(self._canjear_puntos)
        btn_redeem.setStyleSheet("background:#2980b9;color:white;font-weight:bold;padding:6px 12px;border-radius:4px;")
        sh.addWidget(btn_redeem)
        lay.addLayout(sh)
        # Clients table
        self.tabla_clientes = QTableWidget()
        cols = ["ID","Cliente","Teléfono","Nivel","Puntos","Score","Visitas","Último Acceso"]
        self.tabla_clientes.setColumnCount(len(cols)); self.tabla_clientes.setHorizontalHeaderLabels(cols)
        self.tabla_clientes.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_clientes.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tabla_clientes.setAlternatingRowColors(True); self.tabla_clientes.setSortingEnabled(True)
        hh = self.tabla_clientes.horizontalHeader(); hh.setSectionResizeMode(1,QHeaderView.Stretch)
        hh.setStyleSheet(f"QHeaderView::section{{background:{_HDR};color:{_HDR_FG};font-weight:bold;padding:5px;}}")
        self.tabla_clientes.doubleClicked.connect(self._ver_detalle_cliente)
        lay.addWidget(self.tabla_clientes)
        return w

    # ── Tab: Desafíos ─────────────────────────────────────────────────────────
    def _tab_desafios(self):
        w = QWidget(); lay = QVBoxLayout(w); lay.setSpacing(8)
        btns = QHBoxLayout()
        btn_new = QPushButton("➕ Nuevo Desafío"); btn_new.clicked.connect(self._nuevo_desafio)
        btn_new.setStyleSheet("background:#27ae60;color:white;font-weight:bold;padding:6px 12px;border-radius:4px;")
        btn_ref = QPushButton("🔄 Actualizar"); btn_ref.clicked.connect(self._refresh_desafios)
        btns.addWidget(btn_new); btns.addWidget(btn_ref); btns.addStretch()
        lay.addLayout(btns)
        self.tabla_desafios = QTableWidget()
        cols = ["ID","Nombre","Tipo","Meta","Recompensa (pts)","Inicio","Fin","Estado","Participantes"]
        self.tabla_desafios.setColumnCount(len(cols)); self.tabla_desafios.setHorizontalHeaderLabels(cols)
        self.tabla_desafios.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_desafios.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tabla_desafios.setAlternatingRowColors(True)
        hh = self.tabla_desafios.horizontalHeader(); hh.setSectionResizeMode(1,QHeaderView.Stretch)
        hh.setStyleSheet(f"QHeaderView::section{{background:{_HDR};color:{_HDR_FG};font-weight:bold;padding:5px;}}")
        lay.addWidget(self.tabla_desafios)
        # Progress section
        prog_box = QGroupBox("Progreso de Desafíos Activos"); prog_lay = QVBoxLayout()
        self.scroll_prog = QScrollArea(); self.scroll_prog.setWidgetResizable(True)
        self.widget_prog = QWidget(); self.lay_prog = QVBoxLayout(self.widget_prog)
        self.lay_prog.addStretch(); self.scroll_prog.setWidget(self.widget_prog)
        self.scroll_prog.setMaximumHeight(200)
        prog_lay.addWidget(self.scroll_prog); prog_box.setLayout(prog_lay); lay.addWidget(prog_box)
        return w

    # ── Tab: Comunidad ────────────────────────────────────────────────────────
    def _tab_comunidad(self):
        w = QWidget(); lay = QVBoxLayout(w); lay.setSpacing(8)
        btns = QHBoxLayout()
        btn_new = QPushButton("➕ Nueva Meta Comunitaria"); btn_new.clicked.connect(self._nueva_meta)
        btn_new.setStyleSheet("background:#8e44ad;color:white;font-weight:bold;padding:6px 12px;border-radius:4px;")
        btn_ref = QPushButton("🔄 Actualizar"); btn_ref.clicked.connect(self._refresh_comunidad)
        btns.addWidget(btn_new); btns.addWidget(btn_ref); btns.addStretch()
        lay.addLayout(btns)
        self.tabla_comunidad = QTableWidget()
        cols = ["ID","Nombre","Tipo Meta","Meta","Progreso","Recompensa","Inicio","Fin","Estado"]
        self.tabla_comunidad.setColumnCount(len(cols)); self.tabla_comunidad.setHorizontalHeaderLabels(cols)
        self.tabla_comunidad.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.tabla_comunidad.setAlternatingRowColors(True)
        hh = self.tabla_comunidad.horizontalHeader(); hh.setSectionResizeMode(1,QHeaderView.Stretch)
        hh.setStyleSheet(f"QHeaderView::section{{background:{_HDR};color:{_HDR_FG};font-weight:bold;padding:5px;}}")
        lay.addWidget(self.tabla_comunidad)
        # Community progress cards
        comm_box = QGroupBox("Metas Comunitarias Activas"); comm_lay = QVBoxLayout()
        self.scroll_comm = QScrollArea(); self.scroll_comm.setWidgetResizable(True)
        self.widget_comm = QWidget(); self.lay_comm = QVBoxLayout(self.widget_comm)
        self.lay_comm.addStretch(); self.scroll_comm.setWidget(self.widget_comm)
        self.scroll_comm.setMaximumHeight(200)
        comm_lay.addWidget(self.scroll_comm); comm_box.setLayout(comm_lay); lay.addWidget(comm_box)
        return w

    # ── Tab: Configuración ────────────────────────────────────────────────────
    def _tab_config(self):
        w = QWidget(); lay = QVBoxLayout(w); lay.setSpacing(8)
        scroll = QScrollArea(); scroll.setWidgetResizable(True)
        content = QWidget(); form = QFormLayout(content); form.setSpacing(10)

        def _spin(lo,hi,dec,val):
            s = QDoubleSpinBox(); s.setRange(lo,hi); s.setDecimals(dec); s.setValue(val); return s

        self.cfg_pts_peso    = _spin(0,100,2,1.0); form.addRow("Puntos por $ de compra:", self.cfg_pts_peso)
        self.cfg_margen_min  = _spin(0,1,3,0.15);  form.addRow("Margen mínimo para otorgar puntos (0-1):", self.cfg_margen_min)
        self.cfg_max_desc    = _spin(0,100,1,30.0); form.addRow("Descuento máximo por canje (%):", self.cfg_max_desc)
        self.cfg_budget_cap  = _spin(0,9999999,2,5000.0); form.addRow("Presupuesto mensual máximo ($):", self.cfg_budget_cap)
        self.cfg_roi_min     = _spin(0,10,2,1.2); form.addRow("ROI mínimo aceptable:", self.cfg_roi_min)
        self.cfg_min_compra  = _spin(0,9999,2,50.0); form.addRow("Compra mínima para acumular puntos ($):", self.cfg_min_compra)

        form.addRow(QLabel("─── Niveles / Status ───"))
        self.cfg_plata_pts  = _spin(0,9999,0,200); form.addRow("Puntos para nivel Plata:", self.cfg_plata_pts)
        self.cfg_oro_pts    = _spin(0,9999,0,500); form.addRow("Puntos para nivel Oro:", self.cfg_oro_pts)
        self.cfg_plat_pts   = _spin(0,9999,0,1000); form.addRow("Puntos para nivel Platino:", self.cfg_plat_pts)

        btn_save = QPushButton("💾 Guardar Configuración")
        btn_save.setStyleSheet("background:#2980b9;color:white;font-weight:bold;padding:8px;border-radius:4px;")
        btn_save.clicked.connect(self._save_config)
        form.addRow(btn_save)
        scroll.setWidget(content); lay.addWidget(scroll)
        QTimer.singleShot(100, self._load_config)
        return w

    # ── Data Loading ──────────────────────────────────────────────────────────
    def _refresh_all(self):
        self.lbl_suc.setText(f"Sucursal: {self.sucursal_nombre}")
        self._refresh_kpis(); self._refresh_clientes(); self._refresh_desafios(); self._refresh_comunidad()

    def _refresh_kpis(self):
        ym = datetime.utcnow().strftime("%Y-%m")
        try:
            cap_row = self.conexion.fetchone("SELECT budget_limit,value_issued FROM loyalty_budget_caps WHERE branch_id=? AND year_month=?",(self.sucursal_id,ym))
            if cap_row:
                remaining = float(cap_row["budget_limit"]) - float(cap_row["value_issued"] or 0)
                self.kpi_budget._val.setText(f"$ {remaining:,.2f}")
            pts_row = self.conexion.fetchone("SELECT SUM(points_delta) AS pts FROM loyalty_points_log WHERE branch_id=? AND operation_type='EARN' AND created_at>=date('now','start of month')",(self.sucursal_id,))
            if pts_row: self.kpi_pts_mes._val.setText(f"{int(pts_row['pts'] or 0):,}")
            roi_row = self.conexion.fetchone("SELECT roi_pct FROM loyalty_roi_tracking WHERE branch_id=? AND year_month=?",(self.sucursal_id,ym))
            if roi_row: self.kpi_roi._val.setText(f"{float(roi_row['roi_pct'] or 0):.1f}%")
            cnt_row = self.conexion.fetchone("SELECT COUNT(DISTINCT cliente_id) AS c FROM loyalty_points_log WHERE branch_id=? AND created_at>=date('now','start of month')",(self.sucursal_id,))
            if cnt_row: self.kpi_clients._val.setText(str(cnt_row["c"] or 0))
        except Exception as e: logger.error("kpis: %s",e)

    def _refresh_clientes(self):
        search = getattr(self,"txt_search",None)
        q = search.text().strip() if search else ""
        try:
            params = []; where = "WHERE c.activo=1"
            if q: where += " AND (c.nombre LIKE ? OR c.telefono LIKE ?)"; params.extend([f"%{q}%",f"%{q}%"])
            rows = self.conexion.fetchall(f"""
                SELECT c.id, c.nombre, c.telefono,
                       COALESCE(c.nivel_fidelidad,'Bronce') AS nivel,
                       COALESCE(c.puntos,0) AS puntos,
                       COALESCE(ls.score_total,0) AS score,
                       COUNT(DISTINCT v.id) AS visitas,
                       MAX(v.fecha) AS ultimo
                FROM clientes c
                LEFT JOIN loyalty_scores ls ON ls.cliente_id=c.id
                LEFT JOIN ventas v ON v.cliente_id=c.id AND v.estado='completada'
                {where}
                GROUP BY c.id ORDER BY c.puntos DESC
            """, params)
            self.tabla_clientes.setRowCount(len(rows))
            for i,r in enumerate(rows):
                nivel = r["nivel"] or LEVEL_BRONZE
                clr = QColor(_LEVEL_COLORS.get(nivel,"#2c3e50"))
                vals = [str(r["id"]),r["nombre"] or "",r["telefono"] or "",
                        f"{_LEVEL_ICONS.get(nivel,'')} {nivel}",
                        f"{int(r['puntos'] or 0):,}",f"{float(r['score'] or 0):.1f}",
                        str(r["visitas"] or 0),(r["ultimo"] or "")[:10]]
                for j,v in enumerate(vals):
                    item = QTableWidgetItem(v)
                    if j==3: item.setForeground(clr); item.setFont(QFont("",weight=QFont.Bold))
                    self.tabla_clientes.setItem(i,j,item)
        except Exception as e: logger.error("clientes: %s",e)

    def _refresh_desafios(self):
        try:
            rows = self.conexion.fetchall("""
                SELECT lc.id,lc.name,lc.challenge_type,lc.target_value,lc.points_reward,
                       lc.start_date,lc.end_date,lc.is_active,
                       COUNT(lcp.id) AS participantes
                FROM loyalty_challenges lc
                LEFT JOIN loyalty_challenge_progress lcp ON lcp.challenge_id=lc.id
                GROUP BY lc.id ORDER BY lc.is_active DESC,lc.end_date
            """)
            self.tabla_desafios.setRowCount(len(rows))
            today = date.today().isoformat()
            for i,r in enumerate(rows):
                estado = "⚡ Activo" if r["is_active"] and r["end_date"]>=today else ("✅ Completado" if not r["is_active"] else "⏰ Vencido")
                vals = [str(r["id"]),r["name"],r["challenge_type"],f"{r['target_value']:,.0f}",
                        f"{r['points_reward']:,}",r["start_date"],r["end_date"],estado,str(r["participantes"])]
                for j,v in enumerate(vals): self.tabla_desafios.setItem(i,j,QTableWidgetItem(v))
            # Progress cards
            while self.lay_prog.count()>1: self.lay_prog.takeAt(0).widget().deleteLater() if self.lay_prog.itemAt(0).widget() else None
            activos = [r for r in rows if r["is_active"] and r["end_date"]>=today]
            for ch in activos[:5]:
                avg_row = self.conexion.fetchone("SELECT AVG(current_value/NULLIF(?,0)*100) AS avg_pct FROM loyalty_challenge_progress WHERE challenge_id=?",(ch["target_value"],ch["id"]))
                pct = min(100,float(avg_row["avg_pct"] or 0)) if avg_row else 0
                card = self._progress_card(ch["name"],pct,_LEVEL_COLORS[LEVEL_GOLD])
                self.lay_prog.insertWidget(self.lay_prog.count()-1,card)
        except Exception as e: logger.error("desafios: %s",e)

    def _refresh_comunidad(self):
        try:
            rows = self.conexion.fetchall("""
                SELECT id,name,goal_type,target_value,current_value,
                       reward_type,reward_value,start_date,end_date,is_active,achieved
                FROM loyalty_community_goals ORDER BY is_active DESC,end_date
            """)
            self.tabla_comunidad.setRowCount(len(rows))
            today = date.today().isoformat()
            for i,r in enumerate(rows):
                pct = min(100,float(r["current_value"] or 0)/max(1,float(r["target_value"]))*100)
                estado = "🏆 Logrado" if r["achieved"] else ("⚡ Activo" if r["is_active"] and r["end_date"]>=today else "⏰ Vencido")
                vals = [str(r["id"]),r["name"],r["goal_type"],f"{r['target_value']:,.0f}",
                        f"{pct:.1f}%",f"{r['reward_type']} x{r['reward_value']}",
                        r["start_date"],r["end_date"],estado]
                for j,v in enumerate(vals): self.tabla_comunidad.setItem(i,j,QTableWidgetItem(v))
            # Community cards
            while self.lay_comm.count()>1:
                item = self.lay_comm.itemAt(0)
                if item and item.widget(): item.widget().deleteLater()
                else: break
            activos = [r for r in rows if r["is_active"] and not r["achieved"] and r["end_date"]>=today]
            for g in activos[:3]:
                pct = min(100,float(g["current_value"] or 0)/max(1,float(g["target_value"]))*100)
                card = self._progress_card(g["name"],pct,"#8e44ad",f"Meta: {g['target_value']:,.0f} | Recompensa: {g['reward_type']} x{g['reward_value']}")
                self.lay_comm.insertWidget(self.lay_comm.count()-1,card)
        except Exception as e: logger.error("comunidad: %s",e)

    def _progress_card(self, name: str, pct: float, color: str, subtitle: str = "") -> QFrame:
        card = QFrame()
        card.setStyleSheet(f"QFrame{{background:white;border:none;border-left:4px solid {color};border-radius:6px;padding:4px;}}")
        lay = QVBoxLayout(card); lay.setContentsMargins(10,6,10,6); lay.setSpacing(4)
        lbl_name = QLabel(name); lbl_name.setStyleSheet(f"color:{color};font-weight:bold;")
        pb = QProgressBar(); pb.setRange(0,100); pb.setValue(int(pct))
        pb.setStyleSheet(f"QProgressBar{{border:1px solid #ddd;border-radius:4px;height:16px;}} QProgressBar::chunk{{background:{color};border-radius:3px;}}")
        pb.setFormat(f"{pct:.1f}%")
        lay.addWidget(lbl_name); lay.addWidget(pb)
        if subtitle: lbl_sub = QLabel(subtitle); lbl_sub.setStyleSheet("color:#7f8c8d;font-size:10px;"); lay.addWidget(lbl_sub)
        return card

    # ── Config ────────────────────────────────────────────────────────────────
    def _load_config(self):
        keys = {"LOYALTY_POINTS_PER_PESO":("cfg_pts_peso",1.0),"LOYALTY_MARGIN_FLOOR":("cfg_margen_min",0.15),
                "LOYALTY_MAX_DISCOUNT_PCT":("cfg_max_desc",30.0),"LOYALTY_MONTHLY_BUDGET_CAP":("cfg_budget_cap",5000.0),
                "LOYALTY_ROI_FLOOR":("cfg_roi_min",1.2),"LOYALTY_MIN_PURCHASE":("cfg_min_compra",50.0)}
        for k,(attr,default) in keys.items():
            try:
                row = self.conexion.fetchone("SELECT value FROM system_constants WHERE key=?",(k,))
                val = float(row["value"]) if row else default
                getattr(self,attr).setValue(val)
            except Exception: pass

    def _save_config(self):
        data = [("LOYALTY_POINTS_PER_PESO",self.cfg_pts_peso.value()),
                ("LOYALTY_MARGIN_FLOOR",self.cfg_margen_min.value()),
                ("LOYALTY_MAX_DISCOUNT_PCT",self.cfg_max_desc.value()),
                ("LOYALTY_MONTHLY_BUDGET_CAP",self.cfg_budget_cap.value()),
                ("LOYALTY_ROI_FLOOR",self.cfg_roi_min.value()),
                ("LOYALTY_MIN_PURCHASE",self.cfg_min_compra.value())]
        try:
            with self.conexion.transaction("LOYALTY_CONFIG"):
                for k,v in data:
                    self.conexion.execute("INSERT INTO system_constants(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=datetime('now')",(k,str(v)))
            QMessageBox.information(self,"Configuración","Configuración guardada exitosamente.")
        except Exception as e:
            QMessageBox.critical(self,"Error",f"No se pudo guardar:\n{e}")

    # ── Actions ───────────────────────────────────────────────────────────────
    def _canjear_puntos(self):
        sel = self.tabla_clientes.selectedItems()
        if not sel: QMessageBox.information(self,"Fidelidad","Selecciona un cliente."); return
        row = self.tabla_clientes.currentRow()
        cid = int(self.tabla_clientes.item(row,0).text())
        pts = int(self.tabla_clientes.item(row,4).text().replace(",","") or 0)
        nombre = self.tabla_clientes.item(row,1).text()
        dlg = _CanjearDialog(nombre,pts,self)
        if dlg.exec_() != QDialog.Accepted: return
        pts_canjear = dlg.get_puntos()
        try:
            result = self._engine.redeem_points(cid,pts_canjear,self.sucursal_id)
            if result.allowed:
                QMessageBox.information(self,"Canje Exitoso",
                    f"✅ Se canjearon {result.points_redeemed:,} puntos\n"
                    f"Descuento aplicado: $ {result.discount_value:.2f}\n"
                    f"Puntos restantes: {result.points_after:,}")
                self._refresh_clientes()
            else:
                QMessageBox.warning(self,"Canje Rechazado",f"No se pudo canjear: {result.reason}")
        except Exception as e: QMessageBox.critical(self,"Error",str(e))

    def _ver_detalle_cliente(self, index):
        row = index.row()
        cid = int(self.tabla_clientes.item(row,0).text())
        nombre = self.tabla_clientes.item(row,1).text()
        try:
            logs = self.conexion.fetchall("SELECT points_delta,operation_type,multiplier,balance_after,created_at,notes FROM loyalty_points_log WHERE cliente_id=? ORDER BY created_at DESC LIMIT 50",(cid,))
            dlg = _DetalleClienteDialog(nombre,logs,self)
            dlg.exec_()
        except Exception as e: QMessageBox.critical(self,"Error",str(e))

    def _nuevo_desafio(self):
        dlg = _DesafioDialog(self)
        if dlg.exec_() != QDialog.Accepted: return
        data = dlg.get_data()
        try:
            with self.conexion.transaction("NEW_CHALLENGE"):
                self.conexion.execute("""
                    INSERT INTO loyalty_challenges(name,description,challenge_type,target_value,points_reward,start_date,end_date,is_active,branch_id)
                    VALUES(?,?,?,?,?,?,?,1,?)
                """,(data["name"],data["description"],data["type"],data["target"],data["reward"],data["start"],data["end"],self.sucursal_id))
            self._refresh_desafios()
        except Exception as e: QMessageBox.critical(self,"Error",str(e))

    def _nueva_meta(self):
        dlg = _MetaComunidadDialog(self)
        if dlg.exec_() != QDialog.Accepted: return
        data = dlg.get_data()
        try:
            with self.conexion.transaction("NEW_COMMUNITY_GOAL"):
                self.conexion.execute("""
                    INSERT INTO loyalty_community_goals(name,description,target_value,current_value,goal_type,reward_type,reward_value,start_date,end_date,is_active,branch_id)
                    VALUES(?,?,?,0,?,?,?,?,?,1,?)
                """,(data["name"],data["description"],data["target"],data["goal_type"],data["reward_type"],data["reward_value"],data["start"],data["end"],self.sucursal_id))
            self._refresh_comunidad()
        except Exception as e: QMessageBox.critical(self,"Error",str(e))


# ── Dialogs ───────────────────────────────────────────────────────────────────

class _CanjearDialog(QDialog):
    def __init__(self,nombre,pts_disponibles,parent=None):
        super().__init__(parent); self.setWindowTitle("Canjear Puntos"); self.setMinimumWidth(340)
        lay = QFormLayout(self)
        lay.addRow("Cliente:", QLabel(f"<b>{nombre}</b>"))
        lay.addRow("Puntos disponibles:", QLabel(f"<b>{pts_disponibles:,}</b>"))
        self._spin = QSpinBox(); self._spin.setRange(1,pts_disponibles); self._spin.setValue(min(100,pts_disponibles))
        lay.addRow("Puntos a canjear:", self._spin)
        bh = QHBoxLayout(); ok = QPushButton("Canjear"); ok.clicked.connect(self.accept)
        cn = QPushButton("Cancelar"); cn.clicked.connect(self.reject); bh.addWidget(ok); bh.addWidget(cn); lay.addRow(bh)
    def get_puntos(self): return self._spin.value()

class _DetalleClienteDialog(QDialog):
    def __init__(self,nombre,logs,parent=None):
        super().__init__(parent); self.setWindowTitle(f"Historial — {nombre}"); self.resize(640,400)
        lay = QVBoxLayout(self); lay.addWidget(QLabel(f"<b>Historial de puntos: {nombre}</b>"))
        t = QTableWidget(len(logs),6); t.setHorizontalHeaderLabels(["Delta","Tipo","Multiplicador","Saldo","Fecha","Notas"])
        t.horizontalHeader().setSectionResizeMode(5,QHeaderView.Stretch)
        t.setEditTriggers(QAbstractItemView.NoEditTriggers)
        for i,r in enumerate(logs):
            delta = int(r["points_delta"] or 0)
            clr = QColor("#27ae60") if delta>=0 else QColor("#e74c3c")
            item = QTableWidgetItem(f"{'+' if delta>=0 else ''}{delta:,}"); item.setForeground(clr); item.setFont(QFont("",weight=QFont.Bold))
            t.setItem(i,0,item)
            t.setItem(i,1,QTableWidgetItem(r["operation_type"] or ""))
            t.setItem(i,2,QTableWidgetItem(f"x{float(r['multiplier'] or 1):.2f}"))
            t.setItem(i,3,QTableWidgetItem(f"{int(r['balance_after'] or 0):,}"))
            t.setItem(i,4,QTableWidgetItem((r["created_at"] or "")[:16]))
            t.setItem(i,5,QTableWidgetItem(r["notes"] or ""))
        lay.addWidget(t)
        btn_close = QPushButton("Cerrar"); btn_close.clicked.connect(self.accept); lay.addWidget(btn_close)

class _DesafioDialog(QDialog):
    def __init__(self,parent=None):
        super().__init__(parent); self.setWindowTitle("Nuevo Desafío"); self.setMinimumWidth(400)
        lay = QFormLayout(self)
        self._name = QLineEdit(); lay.addRow("Nombre:", self._name)
        self._desc = QTextEdit(); self._desc.setMaximumHeight(60); lay.addRow("Descripción:", self._desc)
        self._type = QComboBox(); self._type.addItems(["PURCHASES","AMOUNT_SPENT","VISITS","REFERRALS"]); lay.addRow("Tipo:", self._type)
        self._target = QDoubleSpinBox(); self._target.setRange(1,999999); self._target.setDecimals(0); lay.addRow("Meta:", self._target)
        self._reward = QSpinBox(); self._reward.setRange(1,99999); self._reward.setValue(100); lay.addRow("Recompensa (pts):", self._reward)
        self._start = QDateEdit(QDate.currentDate()); self._start.setCalendarPopup(True); lay.addRow("Inicio:", self._start)
        self._end = QDateEdit(QDate.currentDate().addDays(30)); self._end.setCalendarPopup(True); lay.addRow("Fin:", self._end)
        bh = QHBoxLayout(); ok = QPushButton("Crear"); ok.clicked.connect(self.accept)
        cn = QPushButton("Cancelar"); cn.clicked.connect(self.reject); bh.addWidget(ok); bh.addWidget(cn); lay.addRow(bh)
    def get_data(self): return {"name":self._name.text().strip(),"description":self._desc.toPlainText().strip(),"type":self._type.currentText(),"target":self._target.value(),"reward":self._reward.value(),"start":self._start.date().toString("yyyy-MM-dd"),"end":self._end.date().toString("yyyy-MM-dd")}

class _MetaComunidadDialog(QDialog):
    def __init__(self,parent=None):
        super().__init__(parent); self.setWindowTitle("Nueva Meta Comunitaria"); self.setMinimumWidth(420)
        lay = QFormLayout(self)
        self._name = QLineEdit(); lay.addRow("Nombre:", self._name)
        self._desc = QTextEdit(); self._desc.setMaximumHeight(60); lay.addRow("Descripción:", self._desc)
        self._goal_type = QComboBox(); self._goal_type.addItems(["TOTAL_PURCHASES","TOTAL_AMOUNT","NEW_CLIENTS"]); lay.addRow("Tipo de Meta:", self._goal_type)
        self._target = QDoubleSpinBox(); self._target.setRange(1,9999999); self._target.setDecimals(0); lay.addRow("Meta:", self._target)
        self._reward_type = QComboBox(); self._reward_type.addItems(["MULTIPLIER","POINTS_BONUS","DISCOUNT_PCT"]); lay.addRow("Tipo Recompensa:", self._reward_type)
        self._reward_val = QDoubleSpinBox(); self._reward_val.setRange(0.1,10); self._reward_val.setDecimals(2); self._reward_val.setValue(1.5); lay.addRow("Valor Recompensa:", self._reward_val)
        self._start = QDateEdit(QDate.currentDate()); self._start.setCalendarPopup(True); lay.addRow("Inicio:", self._start)
        self._end = QDateEdit(QDate.currentDate().addDays(30)); self._end.setCalendarPopup(True); lay.addRow("Fin:", self._end)
        bh = QHBoxLayout(); ok = QPushButton("Crear"); ok.clicked.connect(self.accept)
        cn = QPushButton("Cancelar"); cn.clicked.connect(self.reject); bh.addWidget(ok); bh.addWidget(cn); lay.addRow(bh)
    def get_data(self): return {"name":self._name.text().strip(),"description":self._desc.toPlainText().strip(),"goal_type":self._goal_type.currentText(),"target":self._target.value(),"reward_type":self._reward_type.currentText(),"reward_value":self._reward_val.value(),"start":self._start.date().toString("yyyy-MM-dd"),"end":self._end.date().toString("yyyy-MM-dd")}

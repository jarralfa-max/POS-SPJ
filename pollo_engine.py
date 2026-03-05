"""
PolloEngine — Motor enterprise de inventario cárnico con FIFO por lote.

REGLA: es el ÚNICO lugar autorizado para hacer UPDATE productos SET existencia.
Todos los módulos UI deben pasar por aquí.
"""
from __future__ import annotations
import sqlite3, uuid, logging
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import List, Optional

log = logging.getLogger("spj.pollo_engine")


# ── Excepciones ───────────────────────────────────────────────────────────────
class PolloEngineError(Exception):          pass
class LoteNoEncontradoError(PolloEngineError): pass
class StockInsuficienteError(PolloEngineError): pass
class RendimientoInvalidoError(PolloEngineError): pass


# ── Value objects ─────────────────────────────────────────────────────────────
@dataclass
class PiezaTransformacion:
    producto_id:    int
    nombre:         str
    kg_obtenidos:   float
    costo_unitario: float = 0.0

@dataclass
class ResultadoLote:
    lote_id:       int
    folio_lote:    str
    numero_pollos: int
    kg_totales:    float
    costo_total:   float
    costo_kilo:    float

@dataclass
class ResultadoTransformacion:
    transformacion_id: int
    lote_id:           int
    kg_entrada:        float
    kg_piezas:         float
    kg_merma:          float
    pct_rendimiento:   float
    pct_merma:         float
    movimientos_ids:   List[int] = field(default_factory=list)

@dataclass
class ItemFIFO:
    lote_id:       int
    folio_lote:    str
    disponible_kg: float
    costo_kilo:    float


# ── Engine ────────────────────────────────────────────────────────────────────
class PolloEngine:
    """
    Motor FIFO para inventario cárnico.
    
    Uso:
        engine = PolloEngine(conn, usuario="admin", sucursal_id=1)
        res = engine.registrar_lote(producto_id=5, numero_pollos=20,
                                    kg_totales=40.5, costo_total=1800)
    """

    def __init__(self, conn: sqlite3.Connection,
                 usuario: str = "Sistema", sucursal_id: int = 1):
        self.conn         = conn
        self.usuario      = usuario
        self.sucursal_id  = sucursal_id

    # ── 1. Registrar Lote ────────────────────────────────────────────────────
    def registrar_lote(
        self,
        producto_pollo_id:    int,
        numero_pollos:        int,
        kg_totales:           float,
        costo_total:          float,
        proveedor:            str           = "",
        proveedor_id:         Optional[int] = None,
        fecha:                Optional[date] = None,
        metodo_pago:          str           = "EFECTIVO",
        estado:               str           = "PAGADO",
        descripcion:          str           = "",
        registrar_en_gastos:  bool          = True,
    ) -> ResultadoLote:
        if numero_pollos <= 0: raise PolloEngineError("numero_pollos debe ser > 0")
        if kg_totales    <= 0: raise PolloEngineError("kg_totales debe ser > 0")
        if costo_total   <  0: raise PolloEngineError("costo_total no puede ser negativo")

        costo_kilo = round(costo_total / kg_totales, 4) if kg_totales else 0
        fecha_str  = (fecha or date.today()).isoformat()
        folio      = f"LOTE-{self.sucursal_id}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        with _TX(self.conn):
            cur = self.conn.execute("""
                INSERT INTO compras_pollo
                    (fecha, numero_pollos, kilos_totales, costo_total, costo_kilo,
                     proveedor, proveedor_id, estado, metodo_pago, descripcion,
                     usuario, lote, sucursal_id, fecha_registro)
                VALUES (?,?,?,?,?,  ?,?,?,?,?,  ?,?,?,datetime('now'))
            """, (fecha_str, numero_pollos, kg_totales, costo_total, costo_kilo,
                  proveedor, proveedor_id, estado, metodo_pago, descripcion,
                  self.usuario, folio, self.sucursal_id))
            lote_id = cur.lastrowid

            # Actualizar el campo lote con el folio generado (por si el INSERT no lo tenía)
            self.conn.execute("UPDATE compras_pollo SET lote=? WHERE id=?", (folio, lote_id))

            # Acreditar stock pollo entero
            self._ledger(producto_pollo_id, +kg_totales, "ENTRADA_LOTE",
                         f"Compra lote {folio} — {numero_pollos} pollos",
                         folio, costo_kilo, lote_id=lote_id)

            # Track subproducto inicial (pollo entero en el lote)
            self._track_subproducto(lote_id, producto_pollo_id, kg_totales, costo_kilo, folio)

            # Gasto automático
            if registrar_en_gastos and costo_total > 0:
                self._registrar_gasto(folio, costo_total,
                                      costo_total if estado == "PAGADO" else 0.0,
                                      estado, proveedor_id, fecha_str,
                                      metodo_pago)

            # Evento sync
            self._evento("LOTE_COMPRA", "compras_pollo", lote_id,
                         {"folio": folio, "pollos": numero_pollos,
                          "kg": kg_totales, "costo": costo_total})

        return ResultadoLote(lote_id, folio, numero_pollos, kg_totales,
                             costo_total, costo_kilo)

    # ── 2. Transformación Lote → Piezas ──────────────────────────────────────
    def transformar_lote(
        self,
        lote_id:          int,
        producto_base_id: int,
        kg_entrada:       float,
        piezas:           List[PiezaTransformacion],
        merma_kg:         float = 0.0,
        notas:            str   = "",
    ) -> ResultadoTransformacion:
        if kg_entrada <= 0:
            raise PolloEngineError("kg_entrada debe ser > 0")

        kg_piezas = sum(p.kg_obtenidos for p in piezas)
        if (kg_piezas + merma_kg) > kg_entrada * 1.05:
            raise RendimientoInvalidoError(
                f"Piezas ({kg_piezas:.3f}) + merma ({merma_kg:.3f}) = "
                f"{kg_piezas+merma_kg:.3f} kg excede entrada {kg_entrada:.3f} kg (±5%)")

        stock_disponible = self._stock_lote(producto_base_id, lote_id)
        if stock_disponible < kg_entrada - 0.001:
            raise StockInsuficienteError(
                f"Lote {lote_id}: disponible {stock_disponible:.3f} kg, "
                f"necesario {kg_entrada:.3f} kg")

        info = self.conn.execute(
            "SELECT costo_kilo, COALESCE(lote,'LOTE-'||id) FROM compras_pollo WHERE id=?",
            (lote_id,)).fetchone()
        if not info:
            raise LoteNoEncontradoError(f"Lote {lote_id} no encontrado")
        costo_kilo_base, folio = info

        pct_rendimiento = round((kg_piezas / kg_entrada) * 100, 2) if kg_entrada else 0
        pct_merma       = round((merma_kg  / kg_entrada) * 100, 2) if kg_entrada else 0
        mids            = []

        with _TX(self.conn):
            cur = self.conn.execute("""
                INSERT INTO rendimiento_pollo
                    (compra_id, peso_entrada, peso_piezas, merma, porcentaje_merma,
                     usuario, fecha)
                VALUES (?,?,?,?,?, ?,datetime('now'))
            """, (lote_id, kg_entrada, kg_piezas, merma_kg, pct_merma, self.usuario))
            transf_id = cur.lastrowid
            ref = f"TRANSF-{lote_id}-{transf_id}"

            # Descontar entrada
            mid = self._ledger(producto_base_id, -kg_entrada, "TRANSFORMACION_SALIDA",
                               f"Transformación {ref}", ref, costo_kilo_base, lote_id=lote_id)
            mids.append(mid)

            # Acreditar piezas
            for p in piezas:
                if p.kg_obtenidos <= 0: continue
                ckg = (p.costo_unitario if p.costo_unitario > 0
                       else round(costo_kilo_base * kg_entrada / kg_piezas, 4)
                       if kg_piezas > 0 else costo_kilo_base)
                mid = self._ledger(p.producto_id, +p.kg_obtenidos, "TRANSFORMACION_ENTRADA",
                                   f"Corte '{p.nombre}' de {ref}", ref, ckg, lote_id=lote_id)
                mids.append(mid)
                self._track_subproducto(lote_id, p.producto_id, p.kg_obtenidos, ckg, folio)

            # Merma
            if merma_kg > 0:
                mid = self._ledger(producto_base_id, -merma_kg, "MERMA",
                                   f"Merma {ref}", ref, costo_kilo_base, lote_id=lote_id)
                mids.append(mid)

            self._evento("TRANSFORMACION", "rendimiento_pollo", transf_id,
                         {"lote_id": lote_id, "kg_entrada": kg_entrada,
                          "kg_piezas": kg_piezas, "merma": merma_kg})

        return ResultadoTransformacion(transf_id, lote_id, kg_entrada, kg_piezas,
                                       merma_kg, pct_rendimiento, pct_merma, mids)

    # ── 3. Consumo FIFO en venta ─────────────────────────────────────────────
    def consumir_fifo(self, producto_id: int, kg: float,
                      venta_id: int, descripcion: str = "") -> List[int]:
        if kg <= 0: return []
        cola = self._cola_fifo(producto_id)
        total_disp = sum(i.disponible_kg for i in cola)
        if total_disp < kg - 0.001:
            raise StockInsuficienteError(
                f"Stock FIFO {producto_id}: {total_disp:.3f} kg < {kg:.3f} kg")

        mids = []
        restante = kg
        with _TX(self.conn):
            for item in cola:
                if restante <= 0: break
                consumir = min(item.disponible_kg, restante)
                mid = self._ledger(
                    producto_id, -consumir, "SALIDA_VENTA",
                    descripcion or f"Venta #{venta_id}",
                    f"VENTA-{venta_id}", item.costo_kilo,
                    lote_id=item.lote_id, venta_id=venta_id)
                mids.append(mid)
                restante -= consumir
        return mids

    # ── 4. Consultas públicas ────────────────────────────────────────────────
    def lotes_activos(self) -> List[dict]:
        rows = self.conn.execute("""
            SELECT cp.id, cp.fecha, cp.numero_pollos, cp.kilos_totales,
                   cp.costo_kilo, cp.proveedor, cp.estado,
                   COALESCE(cp.lote, 'LOTE-'||cp.id) as folio,
                   COALESCE((
                       SELECT SUM(mi.cantidad)
                       FROM movimientos_inventario mi
                       WHERE mi.lote_id = cp.id
                   ), 0) AS mov_neto
            FROM compras_pollo cp
            WHERE COALESCE(cp.sucursal_id, 1) = ?
            ORDER BY cp.fecha ASC, cp.id ASC
        """, (self.sucursal_id,)).fetchall()
        result = []
        for r in rows:
            kg_disp = max(0.0, r[3] + r[8])
            result.append({
                "id": r[0], "fecha": r[1], "pollos": r[2],
                "kg_originales": r[3], "costo_kilo": r[4] or 0,
                "proveedor": r[5] or "", "estado": r[6],
                "folio": r[7], "kg_disponibles": round(kg_disp, 3),
            })
        return result

    def cola_fifo(self, producto_id: int) -> List[ItemFIFO]:
        return self._cola_fifo(producto_id)

    def rendimiento_vs_receta(self, lote_id: int) -> dict:
        receta = {}
        for nombre, pct in self.conn.execute("""
            SELECT p.nombre, rd.porcentaje_rendimiento
            FROM rendimiento_derivados rd
            JOIN productos p ON rd.producto_derivado_id = p.id
        """).fetchall():
            receta[nombre] = pct or 0

        lote_info = self.conn.execute(
            "SELECT kilos_totales FROM compras_pollo WHERE id=?", (lote_id,)).fetchone()
        kg_base = lote_info[0] if lote_info else 1

        real = {}
        for nombre, kg in self.conn.execute("""
            SELECT p.nombre, SUM(mi.cantidad)
            FROM movimientos_inventario mi
            JOIN productos p ON mi.producto_id = p.id
            WHERE mi.lote_id=? AND mi.tipo='TRANSFORMACION_ENTRADA'
            GROUP BY p.nombre
        """, (lote_id,)).fetchall():
            real[nombre] = round((kg / kg_base) * 100, 2) if kg_base else 0

        result = {}
        for nombre in set(receta) | set(real):
            result[nombre] = {
                "teorico_pct": receta.get(nombre, 0),
                "real_pct":    real.get(nombre, 0),
                "diferencia":  round(real.get(nombre, 0) - receta.get(nombre, 0), 2),
            }
        return result

    # ── Privados ─────────────────────────────────────────────────────────────
    def _ledger(self, producto_id: int, delta: float, tipo: str,
                descripcion: str = "", referencia: str = None,
                costo_unitario: float = 0.0,
                lote_id: Optional[int] = None,
                venta_id: Optional[int] = None) -> int:
        row = self.conn.execute(
            "SELECT existencia FROM productos WHERE id=?", (producto_id,)).fetchone()
        if row is None:
            raise PolloEngineError(f"Producto {producto_id} no existe")
        stock_antes = float(row[0] or 0)
        stock_nuevo = round(stock_antes + delta, 4)
        self.conn.execute(
            "UPDATE productos SET existencia=?, fecha_actualizacion=datetime('now') WHERE id=?",
            (stock_nuevo, producto_id))
        cur = self.conn.execute("""
            INSERT INTO movimientos_inventario
                (uuid, producto_id, tipo, tipo_movimiento, cantidad,
                 existencia_anterior, existencia_nueva,
                 costo_unitario, costo_total, descripcion, referencia,
                 usuario, sucursal_id, lote_id, venta_id,
                 fecha, _synced)
            VALUES (?,?,?,?,?,  ?,?,  ?,?,?,?,  ?,?,?,?,  datetime('now'),0)
        """, (str(uuid.uuid4()), producto_id, tipo, tipo,
              round(abs(delta), 4), stock_antes, stock_nuevo,
              costo_unitario, round(abs(delta)*costo_unitario, 4),
              descripcion, referencia, self.usuario,
              self.sucursal_id, lote_id, venta_id))
        return cur.lastrowid

    def _cola_fifo(self, producto_id: int) -> List[ItemFIFO]:
        rows = self.conn.execute("""
            SELECT cp.id, COALESCE(cp.lote,'LOTE-'||cp.id),
                   COALESCE(SUM(mi.cantidad),0) AS stock_lote,
                   cp.costo_kilo
            FROM compras_pollo cp
            LEFT JOIN movimientos_inventario mi
                   ON mi.lote_id=cp.id AND mi.producto_id=?
            WHERE COALESCE(cp.sucursal_id,1)=?
            GROUP BY cp.id
            HAVING stock_lote > 0.001
            ORDER BY cp.fecha ASC, cp.id ASC
        """, (producto_id, self.sucursal_id)).fetchall()
        return [ItemFIFO(r[0], r[1], float(r[2]), float(r[3] or 0)) for r in rows]

    def _stock_lote(self, producto_id: int, lote_id: int) -> float:
        row = self.conn.execute("""
            SELECT COALESCE(SUM(cantidad),0) FROM movimientos_inventario
            WHERE producto_id=? AND lote_id=?
        """, (producto_id, lote_id)).fetchone()
        return float(row[0]) if row else 0.0

    def _track_subproducto(self, lote_id, producto_id, cantidad, costo, folio):
        try:
            self.conn.execute("""
                INSERT OR IGNORE INTO inventario_subproductos
                    (compra_pollo_id, producto_id, cantidad, costo_unitario,
                     fecha_creacion, usuario, lote)
                VALUES (?,?,?,?,datetime('now'),?,?)
            """, (lote_id, producto_id, cantidad, costo, self.usuario, folio))
        except Exception: pass

    def _registrar_gasto(self, folio, monto, monto_pagado, estado,
                         proveedor_id, fecha, metodo_pago):
        try:
            self.conn.execute("""
                INSERT INTO gastos
                    (fecha, categoria, concepto, descripcion, monto,
                     monto_pagado, metodo_pago, estado, referencia,
                     proveedor_id, usuario, fecha_registro)
                VALUES (?,?,?,?,?, ?,?,?,?, ?,?,datetime('now'))
            """, (fecha, "COMPRAS_POLLO",
                  f"Compra pollo {folio}", f"Lote {folio}",
                  monto, monto_pagado, metodo_pago, estado, folio,
                  proveedor_id, self.usuario))
        except Exception as e:
            log.warning("Gasto automático falló: %s", e)

    def _evento(self, tipo, entidad, entidad_id, payload):
        try:
            import json
            self.conn.execute("""
                INSERT INTO sync_eventos
                    (uuid, tabla, operacion, registro_id, payload,
                     sucursal_id, usuario, enviado, creado_en)
                VALUES (?,?,?,?,?, ?,?,0,datetime('now'))
            """, (str(uuid.uuid4()), entidad, tipo, entidad_id,
                  json.dumps(payload, default=str),
                  self.sucursal_id, self.usuario))
        except Exception: pass


class _TX:
    def __init__(self, c): self.c = c
    def __enter__(self):
        self.c.execute("SAVEPOINT peng")
        return self
    def __exit__(self, et, ev, tb):
        if et: self.c.execute("ROLLBACK TO SAVEPOINT peng")
        else:  self.c.execute("RELEASE SAVEPOINT peng")
        return False

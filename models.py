# core/domain/models.py — Domain Models SPJ Enterprise v9.1
# Modelos de dominio: entidades de negocio sin dependencias de BD ni UI.
# Usados como tipos en engines, services y eventos.
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


# ── Purchase ──────────────────────────────────────────────────────────────────

@dataclass
class Purchase:
    """Compra registrada — genera inventario global."""
    id:              Optional[int]
    tipo:            str                # 'pollo' | 'abarrotes'
    producto_id:     int
    producto_nombre: str
    proveedor:       str
    volumen:         float
    unidad:          str
    costo_unitario:  float
    costo_total:     float
    forma_pago:      str
    es_credito:      bool
    saldo_pendiente: float
    estado:          str                # pagado | credito | parcial
    sucursal_id:     int
    usuario:         str
    fecha:           datetime           = field(default_factory=datetime.now)
    notas:           str               = ""
    batch_id:        Optional[int]      = None  # chicken_batch generado
    gasto_id:        Optional[int]      = None  # gasto contable asociado

    @property
    def es_pollo(self) -> bool:
        return self.tipo == "pollo"


# ── Transfer ──────────────────────────────────────────────────────────────────

@dataclass
class Transfer:
    """Traspaso de inventario entre sucursales."""
    id:                Optional[int]
    producto_id:       int
    producto_nombre:   str
    cantidad:          float
    unidad:            str
    sucursal_origen:   int
    sucursal_destino:  int
    estado:            str              # pendiente | recibido | cancelado
    usuario_origen:    str
    usuario_destino:   str             = ""
    observaciones:     str             = ""
    fecha_solicitud:   datetime        = field(default_factory=datetime.now)
    fecha_recepcion:   Optional[datetime] = None
    op_uuid:           str             = ""


# ── Receipt ───────────────────────────────────────────────────────────────────

@dataclass
class Receipt:
    """Recepción confirmada en sucursal destino."""
    traspaso_id:      int
    producto_id:      int
    producto_nombre:  str
    cantidad:         float
    sucursal_destino: int
    costo_unitario:   float
    usuario:          str
    fecha:            datetime = field(default_factory=datetime.now)
    bib_ids:          List[int] = field(default_factory=list)


# ── Batch ─────────────────────────────────────────────────────────────────────

@dataclass
class Batch:
    """Lote de inventario (chicken_batch o inventariable)."""
    id:                Optional[int]
    producto_id:       int
    producto_nombre:   str
    sucursal_id:       int
    cantidad_original: float
    cantidad_disponible: float
    costo_unitario:    float
    costo_total:       float
    proveedor:         str             = ""
    estado:            str             = "disponible"  # disponible | agotado | reservado
    root_batch_id:     Optional[int]   = None
    parent_batch_id:   Optional[int]   = None
    fecha_recepcion:   datetime        = field(default_factory=datetime.now)
    usuario:           str             = ""
    notas:             str             = ""

    @property
    def es_raiz(self) -> bool:
        return self.parent_batch_id is None

    @property
    def pct_disponible(self) -> float:
        if self.cantidad_original <= 0:
            return 0.0
        return round(self.cantidad_disponible / self.cantidad_original * 100, 1)


# ── LoyaltyEvent ─────────────────────────────────────────────────────────────

@dataclass
class LoyaltyEvent:
    """Evento de fidelidad — cambio de puntos o nivel."""
    cliente_id:     int
    tipo:           str         # COMPRA | CANJE | AJUSTE | REFERIDO | NIVEL_UP
    puntos:         int         # positivo = ganancia, negativo = canje/penalización
    saldo_antes:    int
    saldo_despues:  int
    descripcion:    str
    venta_id:       Optional[int]   = None
    nivel_antes:    Optional[str]   = None
    nivel_despues:  Optional[str]   = None
    usuario:        str             = "Sistema"
    fecha:          datetime        = field(default_factory=datetime.now)

    @property
    def es_nivel_up(self) -> bool:
        return (
            self.nivel_antes is not None
            and self.nivel_despues is not None
            and self.nivel_antes != self.nivel_despues
        )


# ── SaleItem / Sale ───────────────────────────────────────────────────────────

@dataclass
class SaleItem:
    producto_id:     int
    nombre:          str
    cantidad:        float
    precio_unitario: float
    descuento:       float      = 0.0
    unidad:          str        = "pza"
    comentarios:     str        = ""
    costo_real:      float      = 0.0
    margen_real:     float      = 0.0

    @property
    def subtotal(self) -> float:
        return round(self.cantidad * self.precio_unitario - self.descuento, 2)


@dataclass
class Sale:
    """Venta completada."""
    id:             Optional[int]
    folio:          str
    sucursal_id:    int
    usuario:        str
    cliente_id:     Optional[int]
    items:          List[SaleItem]
    forma_pago:     str
    subtotal:       float
    descuento:      float
    iva:            float
    total:          float
    efectivo_recibido: float
    cambio:         float
    puntos_ganados: int
    estado:         str             = "completada"
    fecha:          datetime        = field(default_factory=datetime.now)

    @property
    def margen_total(self) -> float:
        return sum(i.margen_real * i.cantidad for i in self.items)


# ── CardPool ──────────────────────────────────────────────────────────────────

@dataclass
class CardPool:
    """Entidad de pool de tarjetas de fidelidad."""
    batch_id:         int
    batch_nombre:     str
    total:            int
    generadas:        int    = 0
    libres:           int    = 0
    asignadas:        int    = 0
    activas:          int    = 0
    canceladas:       int    = 0
    estado:           str    = "activo"  # activo | cerrado | anulado

    @property
    def pct_asignadas(self) -> float:
        if self.total <= 0:
            return 0.0
        return round(self.asignadas / self.total * 100, 1)

    ESTADOS_VALIDOS = frozenset({"generada", "impresa", "libre", "asignada", "bloqueada"})


# ── TicketLayout ─────────────────────────────────────────────────────────────

@dataclass
class TicketLayout:
    """Layout de ticket o etiqueta con versionado."""
    id:          Optional[int]
    tipo:        str            # ticket | etiqueta
    nombre:      str
    version:     int            = 1
    activo:      bool           = False
    ancho_mm:    int            = 80
    alto_mm:     int            = 0
    elementos:   List[Dict]     = field(default_factory=list)
    creado_en:   datetime       = field(default_factory=datetime.now)
    modificado_en: Optional[datetime] = None

    def siguiente_version(self) -> "TicketLayout":
        """Clona el layout incrementando la versión."""
        import copy
        nuevo = copy.deepcopy(self)
        nuevo.id       = None
        nuevo.version  = self.version + 1
        nuevo.activo   = False
        nuevo.modificado_en = datetime.now()
        return nuevo


# ── Enums de dominio (añadidos v9.1) ─────────────────────────────────────────

from enum import Enum

class EstadoCompra(str, Enum):
    PAGADO  = "pagado"
    CREDITO = "credito"
    PARCIAL = "parcial"
    ANULADO = "anulado"

class EstadoTraspaso(str, Enum):
    PENDIENTE  = "pendiente"
    EN_CAMINO  = "en_camino"
    RECIBIDO   = "recibido"
    CANCELADO  = "cancelado"

class EstadoLote(str, Enum):
    DISPONIBLE = "disponible"
    PARCIAL    = "parcial"
    AGOTADO    = "agotado"
    ANULADO    = "anulado"

class TipoLoyaltyEvent(str, Enum):
    GANANCIA    = "ganancia"
    CANJE       = "canje"
    AJUSTE      = "ajuste"
    VENCIMIENTO = "vencimiento"
    BONO        = "bono"

class NivelLoyalty(str, Enum):
    BRONCE  = "Bronce"
    PLATA   = "Plata"
    ORO     = "Oro"
    PLATINO = "Platino"


# ── LoyaltySnapshot (fix #10 — snapshot acumulado) ────────────────────────────

@dataclass
class LoyaltySnapshot:
    """
    Snapshot acumulado del estado de fidelidad.
    Actualización incremental desde ultimo_evento_id — evita recalcular toda la historia.
    Guardado en tabla loyalty_snapshots.
    """
    cliente_id:       int
    puntos_actuales:  int
    nivel:            str          # NivelLoyalty value
    visitas:          int
    importe_total:    float
    fecha_snapshot:   datetime
    ultimo_evento_id: Optional[int] = None   # checkpoint para recalculo incremental

    @classmethod
    def calcular_nivel(cls, puntos: int) -> str:
        """Regla de negocio centralizada — punto único de verdad para niveles."""
        if puntos >= 10_000:
            return NivelLoyalty.PLATINO.value
        elif puntos >= 5_000:
            return NivelLoyalty.ORO.value
        elif puntos >= 1_000:
            return NivelLoyalty.PLATA.value
        return NivelLoyalty.BRONCE.value

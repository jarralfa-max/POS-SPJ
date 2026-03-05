# hardware_utils.py — SPJ Enterprise v9.1
# Hardware desacoplado de UI: cola de impresión asíncrona, timeouts, errores tipados.
from __future__ import annotations

import logging
import queue
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("spj.hardware")

# ── Detección de librerías opcionales (sin prints) ────────────────────────────

try:
    import win32print, win32api
    HAS_WIN32 = True
except ImportError:
    HAS_WIN32 = False
    logger.debug("win32 no disponible — impresión Win32 deshabilitada")

try:
    from escpos.printer import Usb, Serial as EscSerial
    HAS_ESC_POS = True
except ImportError:
    HAS_ESC_POS = False
    Usb = EscSerial = None
    logger.debug("escpos no disponible — impresión térmica deshabilitada")

try:
    import serial
    import serial.tools.list_ports
    HAS_SERIAL = True
except ImportError:
    HAS_SERIAL = False
    serial = None
    logger.debug("pyserial no disponible — puertos seriales deshabilitados")

try:
    import qrcode
    HAS_QRCODE = True
except ImportError:
    HAS_QRCODE = False
    qrcode = None

try:
    from PIL import Image
    HAS_PIL = True
except ImportError:
    HAS_PIL = False
    Image = None


# ── Excepciones de hardware tipadas ──────────────────────────────────────────

class HardwareError(Exception):
    pass

class ImpresionError(HardwareError):
    pass

class ImpresionNoDisponibleError(ImpresionError):
    """No hay driver de impresión disponible en este sistema."""

class ImpresionTimeoutError(ImpresionError):
    """La impresora no respondió dentro del timeout."""

class SerialError(HardwareError):
    pass

class SerialNoDisponibleError(SerialError):
    pass

class SerialTimeoutError(SerialError):
    pass


# ── Configuración de hardware (cargada desde tabla hardware_config) ───────────

@dataclass
class HardwareConfig:
    printer_vid:    int   = 0x04b8      # USB VID impresora térmica
    printer_pid:    int   = 0x0e15      # USB PID impresora térmica
    printer_serial: str   = ""          # Puerto COM (si no es USB)
    printer_baud:   int   = 9600
    printer_timeout: float = 5.0        # segundos
    drawer_kick:    int   = 1           # 1 o 2
    scanner_debounce_ms: int = 80
    bascula_port:   str   = "COM3"
    bascula_baud:   int   = 9600
    bascula_timeout: float = 2.0
    ticket_width:   int   = 32          # caracteres por línea (58mm)


# Instancia global — se actualiza desde ModuloConfiguracion
_hw_config = HardwareConfig()


def set_hardware_config(cfg: HardwareConfig) -> None:
    global _hw_config
    _hw_config = cfg
    logger.info("HardwareConfig actualizada: printer_vid=0x%04x printer_pid=0x%04x",
                cfg.printer_vid, cfg.printer_pid)


# ── Cola de impresión asíncrona ───────────────────────────────────────────────
# Los trabajos se encolan desde la UI y se procesan en un hilo background.
# La UI nunca espera a la impresora — elimina bloqueo.

@dataclass
class _PrintJob:
    texto:      str
    on_success: Optional[Callable[[], None]] = None
    on_error:   Optional[Callable[[Exception], None]] = None
    intentos:   int = 0
    max_intentos: int = 3


_print_queue: queue.Queue = queue.Queue(maxsize=50)
_printer_thread: Optional[threading.Thread] = None
_printer_stop   = threading.Event()


def _printer_worker() -> None:
    """Hilo background que consume la cola de impresión."""
    logger.info("PrintWorker iniciado")
    while not _printer_stop.is_set():
        try:
            job: _PrintJob = _print_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        success = False
        last_exc: Optional[Exception] = None

        for attempt in range(1, job.max_intentos + 1):
            try:
                _imprimir_directo(job.texto)
                success = True
                logger.info("PrintWorker: trabajo completado (intento %d)", attempt)
                break
            except ImpresionNoDisponibleError:
                # Sin driver — loguear y no reintentar
                logger.warning("PrintWorker: no hay impresora disponible (modo consola)")
                _log_ticket_consola(job.texto)
                success = True
                break
            except ImpresionTimeoutError as exc:
                last_exc = exc
                logger.warning("PrintWorker: timeout intento %d/%d", attempt, job.max_intentos)
                if attempt < job.max_intentos:
                    time.sleep(0.5 * attempt)
            except ImpresionError as exc:
                last_exc = exc
                logger.error("PrintWorker: error impresión intento %d: %s", attempt, exc)
                if attempt < job.max_intentos:
                    time.sleep(1.0 * attempt)

        if success and job.on_success:
            try:
                job.on_success()
            except Exception as exc:
                logger.debug("PrintWorker on_success callback error: %s", exc)
        elif not success and job.on_error and last_exc:
            try:
                job.on_error(last_exc)
            except Exception as exc:
                logger.debug("PrintWorker on_error callback error: %s", exc)

        _print_queue.task_done()

    logger.info("PrintWorker detenido")


def start_printer_worker() -> None:
    """Arranca el hilo de impresión. Llamar una vez al iniciar la app."""
    global _printer_thread
    if _printer_thread and _printer_thread.is_alive():
        return
    _printer_stop.clear()
    _printer_thread = threading.Thread(
        target=_printer_worker, name="PrintWorker", daemon=True
    )
    _printer_thread.start()


def stop_printer_worker() -> None:
    _printer_stop.set()
    if _printer_thread:
        _printer_thread.join(timeout=3.0)


# ── API pública (no bloquea UI) ───────────────────────────────────────────────

def safe_print_ticket(
    ticket_data:  Dict[str, Any],
    on_success:   Optional[Callable[[], None]] = None,
    on_error:     Optional[Callable[[Exception], None]] = None,
) -> None:
    """
    Encola un trabajo de impresión. Retorna INMEDIATAMENTE — no bloquea la UI.

    Args:
        ticket_data: Datos del ticket (mismo formato que siempre).
        on_success:  Callback llamado cuando el ticket se imprime. Corre en hilo background.
        on_error:    Callback llamado si la impresión falla tras reintentos.
    """
    try:
        texto = format_ticket_data(ticket_data)
    except Exception as exc:
        logger.error("safe_print_ticket: error al formatear ticket: %s", exc)
        if on_error:
            on_error(ImpresionError(f"Error formateando ticket: {exc}"))
        return

    job = _PrintJob(texto=texto, on_success=on_success, on_error=on_error)
    try:
        _print_queue.put_nowait(job)
        logger.debug("safe_print_ticket: trabajo encolado (cola=%d)", _print_queue.qsize())
    except queue.Full:
        exc = ImpresionError("Cola de impresión llena (50 trabajos pendientes)")
        logger.error("%s", exc)
        if on_error:
            on_error(exc)


def safe_print_ticket_sync(
    ticket_data: Dict[str, Any],
    timeout:     float = 10.0,
) -> bool:
    """
    Versión bloqueante para contextos donde se necesita confirmar impresión.
    Retorna True si imprimió correctamente.
    """
    resultado: list = []
    evento  = threading.Event()

    def _ok():
        resultado.append(True)
        evento.set()

    def _err(exc: Exception):
        resultado.append(False)
        evento.set()

    safe_print_ticket(ticket_data, on_success=_ok, on_error=_err)
    evento.wait(timeout=timeout)
    return bool(resultado and resultado[0])


# ── Impresión directa (capa de abstracción de hardware) ───────────────────────

def _imprimir_directo(texto: str) -> None:
    """
    Intenta imprimir usando Win32 → ESC/POS → error.
    Lanza ImpresionError tipado según el fallo.
    """
    if HAS_WIN32:
        _imprimir_win32(texto)
        return

    if HAS_ESC_POS:
        _imprimir_escpos(texto)
        return

    raise ImpresionNoDisponibleError(
        "No hay driver de impresión disponible (Win32 o ESC/POS)"
    )


def _imprimir_win32(texto: str) -> None:
    try:
        printer_name = win32print.GetDefaultPrinter()
        hPrinter = win32print.OpenPrinter(printer_name)
        try:
            hJob = win32print.StartDocPrinter(hPrinter, 1, ("Ticket SPJ", None, "RAW"))
            win32print.StartPagePrinter(hPrinter)
            win32print.WritePrinter(hPrinter, texto.encode("utf-8"))
            win32print.EndPagePrinter(hPrinter)
            win32print.EndDocPrinter(hPrinter)
        finally:
            win32print.ClosePrinter(hPrinter)
        logger.info("Ticket impreso vía Win32 (%d bytes)", len(texto))
    except Exception as exc:
        raise ImpresionError(f"Win32 impresión falló: {exc}") from exc


def _imprimir_escpos(texto: str) -> None:
    cfg = _hw_config
    try:
        if cfg.printer_serial:
            p = EscSerial(cfg.printer_serial, baudrate=cfg.printer_baud,
                          timeout=cfg.printer_timeout)
        else:
            p = Usb(cfg.printer_vid, cfg.printer_pid, timeout=cfg.printer_timeout)
        p.text(texto)
        p.cut()
        logger.info("Ticket impreso vía ESC/POS (%.0fx%.0f)", cfg.printer_vid, cfg.printer_pid)
    except Exception as exc:
        if "timeout" in str(exc).lower():
            raise ImpresionTimeoutError(f"ESC/POS timeout: {exc}") from exc
        raise ImpresionError(f"ESC/POS falló: {exc}") from exc


def _log_ticket_consola(texto: str) -> None:
    logger.info("=== TICKET (simulación) ===\n%s\n=== FIN TICKET ===", texto)


# ── Lectura serial (báscula, scanner por COM) ─────────────────────────────────

def safe_serial_read(
    port:     str,
    baud:     int,
    timeout:  float = 2.0,
    encoding: str   = "utf-8",
) -> float:
    """
    Lee un valor numérico (peso) desde un puerto serial.
    Retorna 0.0 si no hay lectura válida — nunca lanza.
    """
    if not HAS_SERIAL:
        logger.warning("safe_serial_read: pyserial no disponible, retornando 0.0")
        return 0.0

    try:
        with serial.Serial(port, baud, timeout=timeout) as ser:
            ser.flushInput()
            line = ser.readline()
            if not line:
                logger.debug("safe_serial_read: sin datos en %s", port)
                return 0.0
            text = line.decode(encoding, errors="replace").strip()
            match = re.search(r"[\d]+\.?[\d]*", text)
            if match:
                peso = float(match.group())
                logger.debug("safe_serial_read: %s → %.3f", text, peso)
                return peso
            logger.warning("safe_serial_read: datos sin número válido: '%s'", text)
            return 0.0
    except serial.SerialTimeoutException as exc:
        logger.warning("safe_serial_read: timeout en %s: %s", port, exc)
        return 0.0
    except serial.SerialException as exc:
        logger.error("safe_serial_read: error serial %s: %s", port, exc)
        return 0.0
    except Exception as exc:
        logger.error("safe_serial_read: error inesperado: %s", exc)
        return 0.0


def safe_serial_send(port: str, data: str, baud: int = 9600, timeout: float = 2.0) -> bool:
    if not HAS_SERIAL:
        logger.warning("safe_serial_send: pyserial no disponible")
        return False
    try:
        with serial.Serial(port, baud, timeout=timeout) as ser:
            ser.write(data.encode("utf-8"))
            logger.debug("safe_serial_send: %d bytes → %s", len(data), port)
            return True
    except serial.SerialException as exc:
        logger.error("safe_serial_send: %s: %s", port, exc)
        return False
    except Exception as exc:
        logger.error("safe_serial_send: error inesperado: %s", exc)
        return False


# ── QR y utilidades ───────────────────────────────────────────────────────────

def safe_qr_generate(data: str, filename: str = "qr_temp.png") -> bool:
    if not HAS_QRCODE:
        logger.warning("safe_qr_generate: qrcode no disponible")
        return False
    try:
        img = qrcode.make(data)
        img.save(filename)
        logger.debug("QR generado: %s", filename)
        return True
    except Exception as exc:
        logger.error("safe_qr_generate: %s", exc)
        return False


def list_available_printers() -> List[str]:
    if not HAS_WIN32:
        logger.debug("list_available_printers: Win32 no disponible")
        return []
    try:
        printers = [p[2] for p in win32print.EnumPrinters(win32print.PRINTER_ENUM_LOCAL, None, 1)]
        logger.debug("Impresoras: %s", printers)
        return printers
    except Exception as exc:
        logger.error("list_available_printers: %s", exc)
        return []


def list_serial_ports() -> List[str]:
    if not HAS_SERIAL:
        logger.debug("list_serial_ports: pyserial no disponible")
        return []
    try:
        ports = [p.device for p in serial.tools.list_ports.comports()]
        logger.debug("Puertos seriales: %s", ports)
        return ports
    except Exception as exc:
        logger.error("list_serial_ports: %s", exc)
        return []


def get_hardware_status() -> Dict[str, bool]:
    return {
        "win32":    HAS_WIN32,
        "esc_pos":  HAS_ESC_POS,
        "serial":   HAS_SERIAL,
        "qrcode":   HAS_QRCODE,
        "pil":      HAS_PIL,
        "print_worker_alive": bool(_printer_thread and _printer_thread.is_alive()),
    }


# ── Formato ticket (sin cambios funcionales) ──────────────────────────────────

def format_ticket_data(ticket_data: Dict[str, Any]) -> str:
    width = _hw_config.ticket_width
    output = []
    empresa = ticket_data.get("empresa", "EMPRESA SPJ")
    folio   = ticket_data.get("folio",   "???")

    output.append("-" * width)
    output.append(empresa.center(width))
    output.append(f"FOLIO: {folio}".center(width))
    fecha = ticket_data.get("fecha", "")
    if fecha:
        output.append(f"Fecha: {fecha}".ljust(width))
    cliente = ticket_data.get("cliente")
    if cliente:
        output.append(f"Cliente: {cliente.get('nombre','')}".ljust(width))
    output.append("-" * width)
    output.append("Producto          Cant   Total")

    for item in ticket_data.get("items", []):
        nombre  = str(item.get("nombre", ""))[:16].ljust(16)
        cant    = f"{item.get('cantidad', 0):.1f}".rjust(5)
        subtotal= f"${item.get('subtotal', 0):.2f}".rjust(7)
        output.append(f"{nombre} {cant} {subtotal}")

    output.append("-" * width)
    total = ticket_data.get("total", 0)
    output.append(f"TOTAL: ${total:.2f}".rjust(width))
    forma = ticket_data.get("forma_pago", "")
    if forma:
        output.append(f"Pago: {forma}".ljust(width))
    cambio = ticket_data.get("cambio", 0)
    if cambio > 0:
        output.append(f"Cambio: ${cambio:.2f}".rjust(width))
    output.append("-" * width)
    footer = ticket_data.get("footer", "¡Gracias por su compra!")
    output.append(footer.center(width))
    output.append("")
    return "\n".join(output)

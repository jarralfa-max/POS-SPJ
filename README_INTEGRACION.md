# SPJ Punto de Venta — Versión Integrada

## Archivos modificados / nuevos

| Archivo | Estado | Qué cambia |
|---|---|---|
| `main.py` | **REEMPLAZADO** | Login con bcrypt, migraciones al arrancar, layout idéntico |
| `database/conexion.py` | **NUEVO** | Ruta absoluta + WAL + FK + migraciones + bcrypt helpers |
| `services.py` | **NUEVO** | SalesEngine + InventoryEngine con transacciones atómicas |
| `modulos/ventas.py` | **PARCHEADO** | `finalizar_venta()` usa SalesEngine (único método cambiado) |
| Todos los demás | **INTACTOS** | Sin cambios — gráficos, layout y UI preservados 100% |

## Cómo correr

```bash
cd spj_integrado
python main.py
```

## Dependencias

```bash
pip install PyQt5 matplotlib bcrypt pyserial
# Para impresora:
pip install python-escpos
```

## Correcciones aplicadas

1. **Ruta BD absoluta** — `data/punto_venta.db` siempre en la misma ubicación
2. **WAL mode** — sin "database is locked" con hilos PyQt
3. **FK activadas** — integridad referencial real
4. **columna `descripcion`** en configuracion — GestorTemas ya no crashea
5. **bcrypt** — passwords hasheados + migración automática al primer login
6. **SalesEngine atómico** — venta+detalles+inventario+caja+puntos en 1 transacción
7. **InventoryEngine** — auditoría completa en `movimientos_inventario`
8. **Ticket con folio real** — ya no usa `random.randint`
9. **Backup usa ruta correcta** — apunta al DB_PATH absoluto

## Usuarios por defecto

| Usuario | Contraseña | Rol |
|---|---|---|
| admin | admin123 | Administrador |
| cajero | cajero123 | Cajero |

Las contraseñas se migran a bcrypt automáticamente al primer login exitoso.

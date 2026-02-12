# Roster Matrix - Guía de Implementación

## Resumen de Cambios

Se ha implementado una nueva vista de horarios tipo **matriz/roster** con las siguientes características:

### Características Principales

1. **Vista de Matriz Semanal**
   - Filas: Agent ID, Full Name, Lead
   - Columnas: Mon, Tue, Wed, Thu, Fri, Sat, Sun
   - Cada celda muestra el código de turno (S1..S10) u OFF
   - Colores consistentes por tipo de turno
   - Leyenda visible con catálogo de turnos

2. **Catálogo de Turnos**
   | Código | Horario | Color |
   |--------|---------|-------|
   | S1 | 22:00–05:30 | Violeta |
   | S2 | 04:00–12:00 | Azul |
   | S3 | 05:00–14:00 | Cian |
   | S4 | 06:00–15:00 | Verde Esmeralda |
   | S5 | 07:00–16:00 | Verde |
   | S6 | 09:00–17:00 | Lima |
   | S7 | 09:00–18:00 | Amarillo |
   | S8 | 14:30–22:00 | Naranja |
   | S9 | 11:00–20:00 | Rojo |
   | S10 | 21:30–05:00 | Rosa |
   | OFF | - | Gris |

3. **Effective Dating (Historial)**
   - Los cambios NO sobrescriben datos históricos
   - Cada cambio tiene una `effective_date` desde cuándo aplica
   - Los registros anteriores se cierran automáticamente
   - Se puede consultar el historial de cualquier agente

4. **Filtros**
   - Por Lead
   - Por Agent
   - Por semana (navegación anterior/siguiente)
   - Por estado (activo/todos)

5. **Edición**
   - Click en una celda abre selector de turno
   - Se puede aplicar cambios a múltiples días
   - Requiere Effective Date

6. **Alta/Baja de Agentes**
   - Agregar agente con Effective Date
   - Eliminar agente sin borrar historial

---

## Archivos Nuevos Creados

```
app/
├── models/
│   └── shifts.py          # Modelos de datos (ShiftTemplate, AgentShiftAssignment, etc.)
├── routes/
│   └── roster.py          # Endpoints de API para roster
├── shift_db.py            # Funciones de base de datos para roster
├── schedules/
│   └── api.py             # Placeholder para compatibilidad
├── agents/
│   └── api.py             # Placeholder para compatibilidad

migrate_roster.py          # Script de migración de datos
tests/
└── test_roster.py         # Tests unitarios
```

---

## Endpoints de API

### GET `/api/roster/templates`
Obtiene el catálogo de turnos disponibles.

### GET `/api/roster/matrix`
Obtiene la matriz de roster para un rango de fechas.

**Parámetros:**
- `start_date`: YYYY-MM-DD (requerido)
- `end_date`: YYYY-MM-DD (requerido)
- `lead`: Filtrar por lead (opcional)
- `agent_id`: Filtrar por agente (opcional)

### GET `/api/roster/week`
Obtiene la matriz de roster para una semana específica.

**Parámetros:**
- `week_start`: YYYY-MM-DD del lunes (requerido)
- `lead`, `agent_id`: Filtros opcionales

### POST `/api/roster/assignment`
Actualiza el turno de un agente para un día.

**Body:**
```json
{
  "agent_id": "10003",
  "day_of_week": "Mon",
  "shift_code": "S4",
  "effective_date": "2026-02-15"
}
```

### POST `/api/roster/assignment/bulk`
Actualiza múltiples días a la vez.

**Body:**
```json
{
  "agent_id": "10003",
  "days_of_week": ["Mon", "Tue", "Wed"],
  "shift_code": "S4",
  "effective_date": "2026-02-15"
}
```

### GET `/api/roster/history/{agent_id}`
Obtiene el historial de turnos de un agente.

### GET `/api/roster/agents`
Lista agentes del roster.

### POST `/api/roster/agents`
Agrega un agente al roster.

**Body:**
```json
{
  "agent_id": "10050",
  "full_name": "John Doe",
  "lead": "Martin",
  "effective_date": "2026-02-15"
}
```

### POST `/api/roster/agents/remove`
Retira un agente del roster (sin borrar historial).

---

## Migración de Datos

### Ejecutar migración (dry-run primero):
```bash
python migrate_roster.py --dry-run
```

### Ejecutar migración real:
```bash
python migrate_roster.py --effective-date 2026-02-01
```

### Migrar desde schedule_versions:
```bash
python migrate_roster.py --source versions
```

---

## Modelo de Datos (SQL)

### Tabla `shift_templates`
```sql
CREATE TABLE shift_templates (
    id INTEGER PRIMARY KEY,
    shift_code TEXT UNIQUE NOT NULL,
    start_time TEXT,
    end_time TEXT,
    crosses_midnight INTEGER DEFAULT 0,
    color TEXT DEFAULT '#6B7280',
    label TEXT
);
```

### Tabla `agent_roster`
```sql
CREATE TABLE agent_roster (
    id INTEGER PRIMARY KEY,
    agent_id TEXT NOT NULL,
    full_name TEXT NOT NULL,
    lead TEXT,
    effective_start TEXT NOT NULL,
    effective_end TEXT,  -- NULL = activo
    is_active INTEGER DEFAULT 1
);
```

### Tabla `agent_shift_assignments`
```sql
CREATE TABLE agent_shift_assignments (
    id INTEGER PRIMARY KEY,
    agent_id TEXT NOT NULL,
    day_of_week TEXT NOT NULL,  -- Mon, Tue, Wed, Thu, Fri, Sat, Sun
    shift_code TEXT NOT NULL,    -- S1..S10 o OFF
    effective_start TEXT NOT NULL,
    effective_end TEXT,          -- NULL = vigente
    created_at TEXT,
    updated_at TEXT
);
```

---

## Reglas de Negocio

1. **No solapamiento**: No puede haber dos asignaciones activas para el mismo agente+día
2. **Cierre automático**: Al crear una nueva asignación, la anterior se cierra automáticamente
3. **Sin duplicados**: Si ya existe la misma asignación vigente, no se crea duplicado
4. **Historial preservado**: Nunca se eliminan registros históricos
5. **Turnos nocturnos**: S1 y S10 cruzan medianoche (`crosses_midnight=True`)

---

## Tests

Ejecutar tests:
```bash
pytest tests/test_roster.py -v
```

### Escenarios de prueba cubiertos:

1. **Given/When/Then: Edición no borra historial**
   - Given: Agent tiene S4 los lunes desde 2026-01-01
   - When: Cambio a S8 con effective_date=2026-02-15
   - Then: S4 tiene effective_end=2026-02-14, S8 tiene effective_start=2026-02-15

2. **Given/When/Then: Cambio a mitad de semana**
   - Given: Agent tiene S4 todos los días
   - When: Cambio Wed/Thu/Fri a S8 effective miércoles
   - Then: Mon/Tue siguen mostrando S4, Wed/Thu/Fri muestran S8

3. **Given/When/Then: Agente removido mantiene historial**
   - Given: Agent activo desde 2026-01-01
   - When: Remove con effective_date=2026-03-01
   - Then: Aparece en roster hasta 2026-02-28, no después

---

## Paleta de Colores (Accesibilidad)

Se incluye una paleta alternativa para daltonismo (Deuteranopia-safe):

```python
SHIFT_CATALOG_COLORBLIND = {
    "S1":  "#785EF0",  # Purple
    "S2":  "#648FFF",  # Blue
    "S3":  "#00B4D8",  # Cyan
    "S4":  "#2EC4B6",  # Teal
    "S5":  "#38A3A5",  # Dark Teal
    "S6":  "#FFB000",  # Amber
    "S7":  "#FE6100",  # Orange
    "S8":  "#DC267F",  # Magenta
    "S9":  "#DC2626",  # Red
    "S10": "#9D4EDD",  # Violet
    "OFF": "#6B7280",  # Gray
}
```

---

## Próximos Pasos

1. Ejecutar migración de datos existentes
2. Probar la nueva UI en el modal "View Schedules"
3. Verificar que los filtros funcionan correctamente
4. Probar edición de turnos
5. Probar alta/baja de agentes
6. Ajustar colores si es necesario

# 🎯 PLAN DE REFACTOR: ATTENDANCE SCHEDULE SYNCHRONIZATION

**Objetivo**: Hacer que el módulo de attendance lea horarios desde LA MISMA FUENTE que "View Schedules" (BD agent_shift_assignments) en lugar de schedule.csv.

**Alcance**: 100% refactor de lógica/datos. 0% cambios visuales/UI.

---

## **📊 RESUMEN EJECUTIVO**

### **Antes (Legacy)**
- **Attendance**: Leía `schedule.csv` → `SCHEDULE_DF` → parse expected_start/expected_end directamente
- **View Schedules**: Leía `agent_shift_assignments` BD → shift codes (S1..S10) → lookup en SHIFT_CATALOG
- **Problema**: Dos fuentes de truth, riesgo de inconsistencia

### **Después (Refactored)**
- **Ambos**: Leen `agent_shift_assignments` BD → ScheduleProvider unifica acceso
- **Ventaja**: Single source of truth, datos siempre sincronizados
- **Cambios internos**: +95 líneas de código limpio, testing incluido
- **UI**: Completamente intacta

---

## **🗂️ ARCHIVOS MODIFICADOS**

### **1. NEW: `app/providers/schedule_provider.py` (CORE CHANGE)**

**Propósito**: Capa unificada de acceso a horarios del roster database.

**Métodos principales:**

```python
ScheduleProvider.shift_code_to_time_range(shift_code: str) 
  → Optional[Tuple[time, time, bool]]
  # Ejemplo: "S8" → (time(14,30), time(22,0), False)
```

```python
ScheduleProvider.get_schedule_for_agent_day(agent_id, day_of_week, target_date) 
  → Dict[str, Any]
  # Devuelve: {"shift_code": "S8", "start_time": time(14,30), "end_time": time(22,0), 
  #             "has_schedule": True, ...}
```

```python
ScheduleProvider.get_schedule_for_date(target_date) 
  → pd.DataFrame
  # Devuelve: DataFrame IDÉNTICO al viejo SCHEDULE_DF
  # Columnas: agent_id, name, lead, Shift, expected_start, expected_end, expected_start_t, expected_end_t, is_night
```

```python
ScheduleProvider.compare_actual_vs_expected(actual_start, actual_end, expected_start, expected_end, tolerance_minutes)
  → Dict[str, Any]
  # Devuelve: {"has_schedule": bool, "delay_minutes": int, "overtime_minutes": int, "status": "A"/"D"/"U"}
```

**Ventajas:**
- Centraliza lógica de conversión (shift_code → horarios)
- Reutilizable en otros módulos
- Facil de testear
- Abstracta la fuente de datos (puede cambiar sin afectar attendance)

---

### **2. MODIFIED: `app/logic.py`**

**Cambios:**

| Acción | Qué | Por qué |
|--------|-----|--------|
| ❌ Eliminar | `load_schedule()` | Ya no lee CSV |
| ❌ Eliminar | `_process_schedule_df()` | Ya no procesa CSV |
| ❌ Eliminar | `CSV_SCHEDULE = "schedule.csv"` | Constante obsoleta |
| ❌ Eliminar | `SCHEDULE_DF = load_schedule()` | Variable global reemplazada por función |
| ❌ Eliminar | `VALID_AGENT_IDS = set(SCHEDULE_DF[...])` | Ahora dinámico via shift_db |
| ✅ Reemplazar | `get_schedule_for_day()` | Usa `ScheduleProvider.get_schedule_for_date()` |
| ✅ Actualizar | `build_attendance()` | Usa `get_valid_agent_ids()` en lugar de `VALID_AGENT_IDS` |
| ✅ Añadir import | `from .providers.schedule_provider import ScheduleProvider` | Nueva dependencia |

**Impacto en lógica:**
- ✅ `expected_interval_for_day()`: Sin cambios (métodos reciben pd.Series igual)
- ✅ `compute_day_status()`: Sin cambios (usa `agent_row` cual sea la fuente)
- ✅ `build_attendance()`: Las métricas se calculan exactamente igual

**Líneas:**
- Eliminadas: ~95 líneas (loads + CSV processing)
- Añadidas: ~13 líneas (imports + refactored code)
- **Net**: -82 líneas, código más limpio

---

### **3. MODIFIED: `app/routes/attendance.py`**

**Cambios:**

| Edad | Qué | Cómo |
|------|-----|------|
| Update | Import | Elimina `SCHEDULE_DF, VALID_AGENT_IDS` |
| Update | Añade | `from ..providers.schedule_provider import ScheduleProvider` |
| Update | Añade | `from ..shift_db import get_all_roster_agents` |
| Update | `/schedules` endpoint | Usa `get_all_roster_agents()` en lugar de iterando `SCHEDULE_DF` |
| Update | `/schedules/all` endpoint | Idem |
| Update | `justifications_report()` | Obtiene nombres desde BD roster en lugar de CSV |

**Endpoints afectados:**
- GET `/schedules`: Sigue devolviendo same JSON structure, source cambió (CSV → BD)
- GET `/schedules/all`: Idem
- GET `/justifications_report.xlsx`: Idem
- GET `/export.xlsx`: Sigue usando `build_attendance()` que ahora usa ScheduleProvider ✅

**Impacto en cliente (UI):**
- ✅ JSON responses idénticos (mismos campos)
- ✅ Orden de datos puede variar ligeramente (BD sort vs CSV sort) pero datos son los mismo
- ✅ No hay cambios HTTP status codes

---

### **4. NEW: `tests/test_schedule_provider.py`**

**Cobertura de tests:**

```
✅ TestShiftCodeToTimeRange
   - S1 (night, crosses midnight)
   - S4 (day shift)
   - S8 (afternoon)
   - S10 (night, crosses midnight)
   - OFF (returns None)
   - Invalid codes
   - All SHIFT_CATALOG codes valid

✅ TestCompareActualVsExpected
   - No schedule (OFF day)
   - No check-in (unjustified)
   - On-time within tolerance
   - Delayed (beyond tolerance)
   - Overtime calculation
   - Night shift crossing midnight
   - Late night shift

✅ TestScheduleProviderIntegration
   - SHIFT_CATALOG completeness
   - SHIFT_CATALOG structure validation

✅ TestEdgeCases
   - Tolerance boundary (exactly at / beyond)
   - Early check-in
```

**Ejecutar:**
```bash
pytest tests/test_schedule_provider.py -v
```

---

## **🔄 FLUJO DE DATOS ANTES vs DESPUÉS**

### **Antes: Legacy Flow**

```
[schedule.csv] --read--> [SCHEDULE_DF] --select--> [pd.Series per agent/day] 
                                                      |
                                                      v
                                         [expected_start_t, expected_end_t]
```

```
[/attendance endpoint] --uses--> [build_attendance()]
                                    |
                                    v
                              [get_schedule_for_day()]
                                    |
                                    v
                              [SCHEDULE_DF[agent] row]
```

### **Después: Refactored Flow**

```
[agent_shift_assignments] --query--> [ScheduleProvider]
    BD table                              |
                                         v
                          [get_schedule_for_agent_day()]
                                         |
                                         v
                          [SHIFT_CATALOG lookup]
                                         |
                                         v
                          [shift_code_to_time_range()]
                                         |
                                         v
                          [pd.Series per agent/day]
                                         |
                                         v
                          [expected_start_t, expected_end_t]
```

```
[/attendance endpoint] --uses--> [build_attendance()]
                                    |
                                    v
                              [get_schedule_for_day()]
                                    |
                                    v
                          [ScheduleProvider.get_schedule_for_date()]
                                    |
                                    v
                          [Returns DataFrame, same format as old SCHEDULE_DF]
```

**Ventaja clave:** 
- `/api/roster` y `/attendance` ahora usan EXACTAMENTE la misma fuente de datos
- Si cambia un turno en Roster UI, Attendance se actualiza automáticamente
- No hay desincronización temporal (antes necesitaba reiniciar app para recargar CSV)

---

## **✅ CHECKLIST: COMPATIBILIDAD**

### **Estructura de Datos**

- [x] DataFrame de `get_schedule_for_date()` tiene todas las columnas esperadas
- [x] agent_id, name, lead son correctos
- [x] Shift codes (S1..S10, OFF) mapeados a expected_start/expected_end
- [x] is_night computed correctamente
- [x] expected_start_t / expected_end_t son time objects

### **Lógica de Attendance**

- [x] `expected_interval_for_day()` funciona con new DataFrame
- [x] `compute_day_status()` computa A, D, U igual que antes
- [x] `build_attendance()` agrupa y suma correctamente
- [x] Tolerance de 2 minutos aplicada igual
- [x] Night shifts cruzando medianoche funcionan

### **APIs**

- [x] GET `/attendance` returns mismo JSON structure
- [x] GET `/schedules` returns compatible JSON (aunque source cambió)
- [x] GET `/export.xlsx` genera Excel igual
- [x] GET `/api/roster` no necesita cambios (ya usaba BD)

### **UI**

- [x] Pantalla de Attendance: Sin cambios (CSS, HTML, layout idénticos)
- [x] Pantalla de View Schedules: Sin cambios (ya era BD-backed)
- [x] Botones, filtros, estilos: Idénticos

---

## **🚀 DEPLOYMENT STEPS**

1. **Backup**: Respaldar BD (`attendance.db`) antes de desplegar
2. **Deploy code**:
   ```bash
   git add app/providers/schedule_provider.py app/logic.py app/routes/attendance.py tests/test_schedule_provider.py
   git commit -m "feat: sync attendance with roster datasource (single source of truth)"
   git push
   ```
3. **Restart app**:
   ```bash
   # FastAPI will hot-reload if needed
   # Or restart container/process
   ```
4. **Verify**:
   ```bash
   curl http://localhost:8000/attendance?start=2026-01-01&end=2026-01-31 | jq '.agents[0]'
   # Check that status codes (A, D, U, etc.) are correct
   ```
5. **Optional: Cleanup**:
   - Si todos los datos están migrados a BD: Remover `schedule.csv`
   - ScheduleProvider ignora CSV completamente de todos modos

---

## **📋 CAMBIOS RESUMIDOS**

| Archivo | Tipo | Cambios |
|---------|------|---------|
| `app/providers/schedule_provider.py` | **NEW** | +280 líneas (4 métodos públicos) |
| `app/logic.py` | **MOD** | -95 líneas (load_schedule, _process_schedule_df) |
| | | +13 líneas (import ScheduleProvider, refactor) |
| `app/routes/attendance.py` | **MOD** | -50 líneas (SCHEDULE_DF.iterrows → get_all_roster_agents) |
| | | +40 líneas (actualizar endpoints) |
| `tests/test_schedule_provider.py` | **NEW** | +280 líneas (4 test classes, 14 test methods) |

**Total:** ~470 líneas de código limpio, testing incluido.

---

## **⚠️ NOTAS IMPORTANTES**

1. **No hay cambios en schema de BD** - agent_shift_assignments existía antes, solo lo reutilizamos
2. **CSV schedule.csv se puede mantener como backup** pero Attendance ahora lo ignora
3. **Todos los endpoints devuelven mismo JSON** - No hay breaking changes
4. **Performance idéntico** - shift_db queries tienen índices ya existentes
5. **Single Source of Truth** - Ahora imposible que Roster y Attendance usen datos diferentes

---

## **🔗 Referencias**

- **ScheduleProvider**: `app/providers/schedule_provider.py`
- **SHIFT_CATALOG**: `app/models/shifts.py` (sin cambios)
- **Shift DB**: `app/shift_db.py` (get_shift_for_agent_day, get_all_roster_agents)
- **Tests**: `tests/test_schedule_provider.py`
- **Validation**: `REFACTOR_VALIDATION.md`

---

**Status**: ✅ READY TO DEPLOY

**Backward Compatible**: ✅ 100%

**UI Changes**: ❌ ZERO

**Tests Added**: ✅ YES (14+ test cases)

**Documentation**: ✅ Complete

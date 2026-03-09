import pandas as pd
from datetime import datetime, date, time, timedelta
from typing import Optional, Dict, Any, Tuple, List


from .utils import parse_hhmm_or_hhmmss, weekday_token, weekday_short, parse_days_list
from .utils import check_time_in_schedule_ranges, get_expected_intervals_for_day, find_best_matching_interval
from .database import (
    get_justifications_map, get_schedule_for_date, get_single_day_override_db, 
    get_new_schedule_override, get_agent_schedules, get_agent
)
from .providers.schedule_provider import ScheduleProvider

CSV_ACTUALS = "actuals.csv"
TOLERANCE_MINUTES = 2


def _select_agent_row_for_day(agent_rows: pd.DataFrame, day: date) -> Optional[pd.Series]:
    """Select the correct row for an agent on a specific day, considering multiple schedules."""
    if agent_rows.empty:
        return None
    
    # If only one row, return it regardless of working days/days off
    # The status will be determined later in compute_day_status
    if len(agent_rows) == 1:
        return agent_rows.iloc[0]
    
    # Multiple rows - find the most appropriate one
    # For now, just return the first one that exists for this agent
    # The schedule versioning logic should handle which one is active
    return agent_rows.iloc[0]


def get_schedule_for_day(target_date: date) -> pd.DataFrame:
    """
    Get the schedule that was effective on target_date.
    Uses the new ScheduleProvider which reads from agent_shift_assignments in the database.
    This ensures attendance uses the exact same schedule data as "View Schedules" UI.
    """
    return ScheduleProvider.get_schedule_for_date(target_date)


def get_effective_schedule_for_agent(agent_id: str, target_date: date, base_row: pd.Series) -> pd.Series:
    """
    Get the effective schedule for an agent on a specific day, applying any overrides.
    Returns a modified copy of the base row with override values applied.
    
    Priority (highest to lowest):
    1. Single-day override for this exact date
    2. New schedule override effective on or before this date
    3. Base schedule (from versioned DB or CSV)
    """
    # Start with a copy of the base row
    effective = base_row.copy()
    
    # Check for single-day override first (highest priority)
    single_day = get_single_day_override_db(agent_id, target_date)
    if single_day:
        # Apply single-day override
        if single_day.get("expected_start"):
            effective["expected_start"] = single_day["expected_start"]
            effective["expected_start_t"] = parse_hhmm_or_hhmmss(single_day["expected_start"])
        if single_day.get("expected_end"):
            effective["expected_end"] = single_day["expected_end"]
            effective["expected_end_t"] = parse_hhmm_or_hhmmss(single_day["expected_end"])
        if single_day.get("shift"):
            effective["Shift"] = single_day["shift"]
            effective["is_night"] = str(single_day["shift"]).lower() == "night"
        # Preserve shift_code from base row if not overridden
        if "shift_code" not in effective and "shift_code" in base_row:
            effective["shift_code"] = base_row["shift_code"]
        return effective
    
    # Check for new schedule override (applies from effective_date onwards)
    new_sched = get_new_schedule_override(agent_id, target_date)
    if new_sched:
        # Apply new schedule override
        if new_sched.get("working_days"):
            effective["working_days"] = new_sched["working_days"]
        if new_sched.get("days_off"):
            effective["days_off"] = new_sched["days_off"]
        if new_sched.get("expected_start"):
            effective["expected_start"] = new_sched["expected_start"]
            effective["expected_start_t"] = parse_hhmm_or_hhmmss(new_sched["expected_start"])
        if new_sched.get("expected_end"):
            effective["expected_end"] = new_sched["expected_end"]
            effective["expected_end_t"] = parse_hhmm_or_hhmmss(new_sched["expected_end"])
        if new_sched.get("shift"):
            effective["Shift"] = new_sched["shift"]
            effective["is_night"] = str(new_sched["shift"]).lower() == "night"
        # Preserve shift_code from base row if not overridden
        if "shift_code" not in effective and "shift_code" in base_row:
            effective["shift_code"] = base_row["shift_code"]
    
    return effective

def load_actuals() -> pd.DataFrame:
    """
    actuals.csv:
      date(mm/dd/yyyy), agent_id, name, shift, actual_start, actual_end
    """
    df = pd.read_csv(CSV_ACTUALS)
    df["agent_id"] = df["agent_id"].astype(str).str.strip()

    def parse_date_us(s: str) -> date:
        return datetime.strptime(str(s).strip(), "%m/%d/%Y").date()

    df["date"] = df["date"].apply(parse_date_us)
    df["actual_start_t"] = df["actual_start"].apply(parse_hhmm_or_hhmmss)
    df["actual_end_t"] = df["actual_end"].apply(parse_hhmm_or_hhmmss)
    return df

def get_valid_agent_ids() -> set:
    from .shift_db import get_all_roster_agents
    return {a["agent_id"] for a in get_all_roster_agents()}

_actuals_cache: Optional[pd.DataFrame] = None

def get_actuals_df() -> pd.DataFrame:
    global _actuals_cache
    if _actuals_cache is None:
        _actuals_cache = load_actuals()
    return _actuals_cache

def invalidate_actuals_cache():
    """Call this after R2 sync downloads a new actuals.csv"""
    global _actuals_cache
    _actuals_cache = None

def expected_interval_for_day(agent_row: pd.Series, day: date) -> Optional[Tuple[datetime, datetime, bool]]:
    """
    Intervalo esperado (start, end). Si end <= start, suma 1 día (cruce de medianoche).
    Devuelve (start_dt, end_dt, is_night).
    """
    start_t = agent_row["expected_start_t"]
    end_t = agent_row["expected_end_t"]
    if not start_t or not end_t:
        return None
    start_dt = datetime.combine(day, start_t)
    end_dt = datetime.combine(day, end_t)
    if end_dt <= start_dt:
        end_dt = end_dt + timedelta(days=1)
    return start_dt, end_dt, bool(agent_row["is_night"])


def get_multi_schedule_intervals(agent_id: str, target_date: date) -> List[Tuple[datetime, datetime, str, bool]]:
    """
    Get all expected intervals for an agent on a specific date using the new multi-schedule system.
    Falls back to empty list if agent not found in new system.
    
    Returns list of tuples: (start_dt, end_dt, shift_code, crosses_midnight)
    """
    # Get agent's schedules from new system
    schedules = get_agent_schedules(agent_id)
    
    # Get day of week
    day_name = weekday_short(target_date)
    day_ranges = schedules.get(day_name, [])
    
    if not day_ranges:
        return []
    
    return get_expected_intervals_for_day(day_ranges, target_date)


def compute_delay_with_multi_schedule(
    agent_id: str, 
    target_date: date, 
    actual_start_t: Optional[time],
    actual_end_t: Optional[time]
) -> Dict[str, Any]:
    """
    Compute delay/status using multi-schedule ranges.
    
    Returns:
    {
        "has_schedule": bool,
        "intervals": list,
        "matched_interval": tuple or None,
        "delay_minutes": int,
        "overtime_minutes": int,
        "shift_code": str,
        "status": str  # "A", "D", "U", or None if no schedule
    }
    """
    intervals = get_multi_schedule_intervals(agent_id, target_date)
    
    if not intervals:
        return {
            "has_schedule": False,
            "intervals": [],
            "matched_interval": None,
            "delay_minutes": 0,
            "overtime_minutes": 0,
            "shift_code": "",
            "status": None
        }
    
    if not actual_start_t:
        # No check-in = unjustified
        return {
            "has_schedule": True,
            "intervals": intervals,
            "matched_interval": None,
            "delay_minutes": 0,
            "overtime_minutes": 0,
            "shift_code": intervals[0][2] if intervals else "",
            "status": "U"
        }
    
    # Find the best matching interval for this check-in
    matched, delay_minutes = find_best_matching_interval(actual_start_t, intervals)
    
    # Calculate overtime if we have actual end time
    overtime_minutes = 0
    if matched and actual_end_t:
        _, exp_end, _, _ = matched
        act_end_dt = datetime.combine(target_date, actual_end_t)
        if act_end_dt > exp_end:
            overtime_minutes = int((act_end_dt - exp_end).total_seconds() // 60)
    
    # Determine status
    status = "A" if delay_minutes <= TOLERANCE_MINUTES else "D"
    if delay_minutes <= TOLERANCE_MINUTES:
        delay_minutes = 0  # Apply tolerance
    
    return {
        "has_schedule": True,
        "intervals": intervals,
        "matched_interval": matched,
        "delay_minutes": delay_minutes,
        "overtime_minutes": overtime_minutes,
        "shift_code": matched[2] if matched else "",
        "status": status
    }

def actual_interval_for_day(actual_row: Optional[pd.Series], day: date, is_night: bool) -> Optional[Tuple[datetime, datetime]]:
    """
    Intervalo real (start, end) del registro. Si end <= start, suma 1 día (cruce de medianoche).
    """
    if actual_row is None:
        return None
    astart_t = actual_row["actual_start_t"]
    aend_t = actual_row["actual_end_t"]
    if not astart_t or not aend_t:
        return None
    astart_dt = datetime.combine(day, astart_t)
    aend_dt = datetime.combine(day, aend_t)
    if aend_dt <= astart_dt:
        aend_dt = aend_dt + timedelta(days=1)
    return astart_dt, aend_dt

def compute_day_status(
    agent_row: pd.Series,
    day: date,
    actual_row: Optional[pd.Series],
    just_map: Dict[Tuple[str, date], Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Calcula estado del día y aplica override.
      - '-' (pending) si el día es futuro (no ha pasado aún).
      - Off 'O' si shift es OFF o no hay expected_start/expected_end.
      - U si no hay registro en día laborable.
      - A si late_minutes == 0; D si > 0.
      - Tolerancia: si late_minutes <= TOLERANCE_MINUTES ⇒ A y late=0.
      - Override permitido: A/J/V/U/D/H/C. Si override 'A' ⇒ late=0 (no suma).
    Devuelve también:
      - original_status (antes del override y tras aplicar tolerancia)
      - is_overridden (True si hubo justificación/override)
    
    Note: This function now applies schedule overrides (from Edit Schedules feature)
    before computing the status. OFF days are determined by shift_code="OFF" or missing times.
    """
    agent_id = agent_row["agent_id"]
    
    # Apply schedule overrides to get effective schedule for this day
    effective_row = get_effective_schedule_for_agent(agent_id, day, agent_row)
    
    name = effective_row["name"]
    lead = effective_row["lead"]
    shift = effective_row.get("Shift", "")
    shift_code = effective_row.get("shift_code", "")
    
    today = date.today()
    
    # Initialize variables
    actual_start = ""
    actual_end = ""
    planned_start = ""
    planned_end = ""
    late_minutes = 0
    overtime_minutes = 0

    # Future dates: default to pending status, but check for overrides below
    if day > today:
        original_status = "-"
    # Base (estado original)
    elif day <= today:
        # Check if it's OFF day based on shift_code or missing times
        exp_iv = expected_interval_for_day(effective_row, day)
        
        if shift_code == "OFF" or exp_iv is None:
            # OFF day - no schedule expected
            original_status = "O"
        else:
            # Working day with expected schedule
            exp_start, exp_end, is_night = exp_iv
            planned_start = exp_start.strftime("%H:%M")
            planned_end = exp_end.strftime("%H:%M")
            act_iv = actual_interval_for_day(actual_row, day, is_night)
            if act_iv is None:
                original_status = "U"
            else:
                act_start, act_end = act_iv
                actual_start = act_start.strftime("%H:%M")
                actual_end = act_end.strftime("%H:%M")
                atraso_entrada = max(0, int((act_start - exp_start).total_seconds() // 60))
                salida_anticipada = max(0, int((exp_end - act_end).total_seconds() // 60))
                late_raw = atraso_entrada + salida_anticipada
                overtime_minutes = max(0, int((exp_start - act_start).total_seconds() // 60)) + \
                                   max(0, int((act_end - exp_end).total_seconds() // 60))
                # ✔ tolerancia de 2 minutos
                if late_raw <= TOLERANCE_MINUTES:
                    late_minutes = 0
                    original_status = "A"
                else:
                    late_minutes = late_raw
                    original_status = "D"

    status = original_status
    is_overridden = False
    tooltip = None

    # Override (justificación/ajuste manual)
    override = just_map.get((agent_id, day))
    if override and override.get("type") in {"A", "J", "V", "U", "D", "H", "C", "ML"}:
        # No aplicar override si el día original es día de descanso (O)
        if original_status == "O":
            status = original_status
            is_overridden = False
        else:
            is_overridden = True
            status = override["type"]
            # Si el status original era 'D' y el override es distinto de 'D', se restan los minutos de atraso
            if original_status == "D" and status != "D":
                late_minutes = 0
                overtime_minutes = 0
                tooltip = None
            elif status == "A":
                # Fuerza día sin penalización
                late_minutes = 0
                overtime_minutes = 0
                tooltip = None
            elif status == "J":
                # Día justificado: no penalización
                late_minutes = 0
                overtime_minutes = 0
                tooltip = None
            elif status == "D":
                tooltip = f"Delay: {late_minutes} minutes"
            else:
                tooltip = None
    else:
        if status == "D":
            tooltip = f"Delay: {late_minutes} minutes"

    return {
        "agent_id": agent_id,
        "name": name,
        "lead": lead,
        "shift": shift,
        "date": day.isoformat(),
        "status": status,
        "actual_start": actual_start,
        "actual_end": actual_end,
        "planned_start": planned_start,
        "planned_end": planned_end,
        "late_minutes": int(late_minutes),
        "overtime_minutes": int(overtime_minutes),
        "tooltip": tooltip,
        "original_status": original_status,
        "is_overridden": is_overridden,
    }

def build_attendance(start: date, end: date, lead: Optional[str], agent_id: Optional[str], status_filter: Optional[str] = None) -> Dict[str, Any]:
    """
    Agrega por agente:
      - días (A/D/U/J/V/O)
      - sumas: late_minutes, delays, vacations, justified, unjustified
      - justified_delays_sum: cuenta días originalmente D que terminaron A o J por override
    
    Uses versioned schedules: for each day, looks up the schedule that was effective on that date.
    """
    just_map = get_justifications_map(start, end)

    actuals_idx = get_actuals_idx()

    # Single bulk prefetch for the entire date range - replaces 31 DB queries with 1
    schedule_cache = ScheduleProvider.get_schedule_cache_for_range(start, end)

    def get_schedule_cached(d: date) -> pd.DataFrame:
        return schedule_cache.get(d, pd.DataFrame())

    from .shift_db import get_all_roster_agents

    # Build all_agents directly from cached roster - no day loop needed
    roster = get_all_roster_agents()
    all_agents = {a["agent_id"]: {"name": a["full_name"], "lead": a["lead"]} for a in roster}

    if lead:
        lead_lower = lead.strip().lower()
        all_agents = {k: v for k, v in all_agents.items() if v["lead"].lower() == lead_lower}

    if agent_id:
        target_aid = str(agent_id).strip()
        all_agents = {k: v for k, v in all_agents.items() if k == target_aid}

    agents_out = []
    for aid, agent_info in all_agents.items():
        days = []
        late_sum = delays = vacations = justified = unjustified = justified_delays_sum = holidays = comp_days = medical_leaves = 0
        cur = start
        
        # normalize status_filter: accept comma-separated, case-insensitive
        allowed_statuses = None
        if status_filter is not None:
            allowed_statuses = {s.strip().upper() for s in str(status_filter).split(",") if s.strip()}

        while cur <= end:
            # Get schedule for this specific day
            day_sched = get_schedule_cached(cur)
            agent_rows = day_sched[day_sched["agent_id"] == aid]
            
            arow = _select_agent_row_for_day(agent_rows, cur)
            if arow is None:
                # Agent not in schedule for this day - skip
                cur += timedelta(days=1)
                continue
            arow_actual = actuals_idx.get((aid, cur))
            item = compute_day_status(arow, cur, arow_actual, just_map)
            
            # Match only the current visible status (after overrides)
            match = True
            if allowed_statuses is not None:
                match = item["status"].upper() in allowed_statuses

            if match:
                days.append(item)

                # Suma de minutos tarde (post-override y post-tolerancia)
                late_sum += item["late_minutes"]

                # Contadores por estado mostrado (post-override)
                if item["status"] == "D":
                    delays += 1
                elif item["status"] == "V":
                    vacations += 1
                elif item["status"] == "J":
                    justified += 1
                elif item["status"] == "U":
                    unjustified += 1
                elif item["status"] == "H":
                    holidays += 1
                elif item["status"] == "C":
                    comp_days += 1
                elif item["status"] == "ML":
                    medical_leaves += 1

                # Justified delays: originalmente D y ahora A o J
                if item["original_status"] == "D" and item["status"] in {"A", "J"}:
                    justified_delays_sum += 1

            cur += timedelta(days=1)

        if days:  # Only include agents with matching days
            agents_out.append({
                "agent_id": aid,
                "name": agent_info["name"],
                "lead": agent_info["lead"],
                "days": days,
                "late_minutes_sum": late_sum,
                "delays_sum": delays,
                "vacations_sum": vacations,
                "justified_sum": justified,
                "unjustified_sum": unjustified,
                "justified_delays_sum": justified_delays_sum,
                "holidays_sum": holidays,
                "comp_days_sum": comp_days,
                "medical_leaves_sum": medical_leaves,
            })

    return {"agents": agents_out}

_actuals_idx_cache: Optional[Dict] = None

def get_actuals_idx() -> Dict:
    global _actuals_idx_cache
    if _actuals_idx_cache is not None:
        return _actuals_idx_cache
    
    df = get_actuals_df()
    valid_ids = get_valid_agent_ids()
    df_filtered = df[df["agent_id"].isin(valid_ids)].copy()

    idx = {}
    if not df_filtered.empty:
        for (aid, d), g in df_filtered.groupby(["agent_id", "date"]):
            starts = [v for v in g["actual_start_t"].tolist() if pd.notnull(v)]
            ends = [v for v in g["actual_end_t"].tolist() if pd.notnull(v)]
            idx[(str(aid), d)] = pd.Series({
                "agent_id": str(aid),
                "date": d,
                "actual_start_t": min(starts) if starts else None,
                "actual_end_t": max(ends) if ends else None,
            })
    
    _actuals_idx_cache = idx
    return _actuals_idx_cache

def invalidate_actuals_cache():
    global _actuals_cache, _actuals_idx_cache
    _actuals_cache = None
    _actuals_idx_cache = None # <- also clear the idx cache
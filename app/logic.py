import pandas as pd
from datetime import datetime, date, time, timedelta
from typing import Optional, Dict, Any, Tuple, List

from .utils import parse_hhmm_or_hhmmss, weekday_token, parse_days_list
from .database import get_justifications_map, get_schedule_for_date

CSV_SCHEDULE = "schedule.csv"
CSV_ACTUALS = "actuals.csv"
TOLERANCE_MINUTES = 2

def load_schedule() -> pd.DataFrame:
    """
    schedule.csv:
      agent_id, Shift, name, lead, working_days, days_off, expected_start, expected_end
    """
    df = pd.read_csv(CSV_SCHEDULE)
    return _process_schedule_df(df)


def _process_schedule_df(df: pd.DataFrame) -> pd.DataFrame:
    """Process a schedule DataFrame, adding computed columns."""
    df = df.copy()
    df["agent_id"] = df["agent_id"].astype(str).str.strip()
    df["Shift"] = df["Shift"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df["lead"] = df["lead"].astype(str).str.strip()
    df["working_days"] = df["working_days"].astype(str).str.strip()
    df["days_off"] = df["days_off"].astype(str).str.strip()
    df["expected_start_t"] = df["expected_start"].apply(parse_hhmm_or_hhmmss)
    df["expected_end_t"] = df["expected_end"].apply(parse_hhmm_or_hhmmss)
    df["is_night"] = df["Shift"].str.lower().eq("night")
    return df


def get_schedule_for_day(target_date: date) -> pd.DataFrame:
    """
    Get the schedule that was effective on target_date.
    First checks the database for versioned schedules, then falls back to CSV.
    """
    # Try to get versioned schedule from database
    db_schedule = get_schedule_for_date(target_date)
    if db_schedule is not None and not db_schedule.empty:
        return _process_schedule_df(db_schedule)
    
    # Fall back to CSV file (for dates before any versioned schedule)
    return SCHEDULE_DF

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

SCHEDULE_DF = load_schedule()
VALID_AGENT_IDS = set(SCHEDULE_DF["agent_id"].tolist())

def get_actuals_df() -> pd.DataFrame:
    """Relee actuals.csv en cada petición para reflejar cambios sin reiniciar."""
    return load_actuals()

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
      - Off 'O' si weekday ∈ days_off.
      - U si no hay registro en día laborable.
      - A si late_minutes == 0; D si > 0.
      - Tolerancia: si late_minutes <= TOLERANCE_MINUTES ⇒ A y late=0.
      - Override permitido: A/J/V/U/D. Si override 'A' ⇒ late=0 (no suma).
    Devuelve también:
      - original_status (antes del override y tras aplicar tolerancia)
      - is_overridden (True si hubo justificación/override)
    """
    agent_id = agent_row["agent_id"]
    name = agent_row["name"]
    lead = agent_row["lead"]
    days_off = set(parse_days_list(agent_row["days_off"]))
    dow = weekday_token(day)
    
    today = date.today()

    # Future dates: default to pending status, but check for overrides below
    if day > today:
        original_status = "-"
        late_minutes = 0
        overtime_minutes = 0
    # Base (estado original)
    elif day <= today:
        if dow in days_off:
            original_status = "O"
            late_minutes = 0
            overtime_minutes = 0
        else:
            exp_iv = expected_interval_for_day(agent_row, day)
            if exp_iv is None:
                original_status = "O"
                late_minutes = 0
                overtime_minutes = 0
            else:
                exp_start, exp_end, is_night = exp_iv
                act_iv = actual_interval_for_day(actual_row, day, is_night)
                if act_iv is None:
                    original_status = "U"
                    late_minutes = 0
                    overtime_minutes = 0
                else:
                    act_start, act_end = act_iv
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
    if override and override.get("type") in {"A", "J", "V", "U", "D"}:
        is_overridden = True
        status = override["type"]
        if status == "A":
            # Fuerza día sin penalización
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
        "date": day.isoformat(),
        "status": status,
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

    # Index de actuals por (agent_id, date)
    # If there are multiple connection rows per day, aggregate them taking
    # the earliest start and the latest end so we don't miss delays.
    actuals_idx: Dict[Tuple[str, date], pd.Series] = {}
    df_act_all = get_actuals_df()
    df_act = df_act_all[df_act_all["agent_id"].isin(VALID_AGENT_IDS)].copy()
    if not df_act.empty:
        grp = df_act.groupby(["agent_id", "date"])
        for (aid, d), g in grp:
            # pick earliest non-null start and latest non-null end
            starts = [v for v in g["actual_start_t"].tolist() if pd.notnull(v)]
            ends = [v for v in g["actual_end_t"].tolist() if pd.notnull(v)]
            astart = min(starts) if starts else None
            aend = max(ends) if ends else None
            # create a Series similar to original rows
            actuals_idx[(str(aid), d)] = pd.Series({
                "agent_id": str(aid),
                "date": d,
                "actual_start_t": astart,
                "actual_end_t": aend,
            })

    # Cache for schedule by date to avoid repeated lookups
    schedule_cache: Dict[date, pd.DataFrame] = {}
    
    def get_schedule_cached(d: date) -> pd.DataFrame:
        if d not in schedule_cache:
            schedule_cache[d] = get_schedule_for_day(d)
        return schedule_cache[d]

    # Collect all unique agents from all schedules in the date range
    all_agents: Dict[str, Dict[str, Any]] = {}  # agent_id -> latest agent info
    cur = start
    while cur <= end:
        sched = get_schedule_cached(cur)
        for _, row in sched.iterrows():
            aid = str(row["agent_id"])
            if aid not in all_agents:
                all_agents[aid] = {"name": row["name"], "lead": row["lead"]}
        cur += timedelta(days=1)

    # Filter agents by lead/agent_id
    if lead:
        lead_lower = lead.strip().lower()
        # Get base schedule to check leads
        base_sched = get_schedule_cached(start)
        valid_agents = set(base_sched[base_sched["lead"].str.lower() == lead_lower]["agent_id"].tolist())
        all_agents = {k: v for k, v in all_agents.items() if k in valid_agents}
    
    if agent_id:
        target_aid = str(agent_id).strip()
        all_agents = {k: v for k, v in all_agents.items() if k == target_aid}

    agents_out = []
    for aid, agent_info in all_agents.items():
        days = []
        late_sum = delays = vacations = justified = unjustified = justified_delays_sum = 0
        cur = start
        
        # normalize status_filter: accept comma-separated, case-insensitive
        allowed_statuses = None
        if status_filter is not None:
            allowed_statuses = {s.strip().upper() for s in str(status_filter).split(",") if s.strip()}

        while cur <= end:
            # Get schedule for this specific day
            day_sched = get_schedule_cached(cur)
            agent_rows = day_sched[day_sched["agent_id"] == aid]
            
            if agent_rows.empty:
                # Agent not in schedule for this day - skip
                cur += timedelta(days=1)
                continue
            
            arow = agent_rows.iloc[0]
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
            })

    return {"agents": agents_out}
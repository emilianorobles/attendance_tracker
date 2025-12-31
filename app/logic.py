import pandas as pd
from datetime import datetime, date, time, timedelta
from typing import Optional, Dict, Any, Tuple, List

from .utils import parse_hhmm_or_hhmmss, weekday_token, parse_days_list
from .database import get_justifications_map

CSV_SCHEDULE = "schedule.csv"
CSV_ACTUALS = "actuals.csv"
TOLERANCE_MINUTES = 2

def load_schedule() -> pd.DataFrame:
    """
    schedule.csv:
      agent_id, Shift, name, lead, working_days, days_off, expected_start, expected_end
    """
    df = pd.read_csv(CSV_SCHEDULE)
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

    # Base (estado original)
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
    """
    sched = SCHEDULE_DF.copy()
    if lead:
        sched = sched[sched["lead"].str.lower() == lead.strip().lower()]
    if agent_id:
        sched = sched[sched["agent_id"] == str(agent_id).strip()]

    just_map = get_justifications_map(start, end)

    # Index de actuals por (agent_id, date)
    actuals_idx: Dict[Tuple[str, date], pd.Series] = {}
    df_act_all = get_actuals_df()
    df_act = df_act_all[df_act_all["agent_id"].isin(VALID_AGENT_IDS)].copy()
    for _, row in df_act.iterrows():
        actuals_idx[(row["agent_id"], row["date"])] = row

    agents_out = []
    for _, arow in sched.iterrows():
        days = []
        late_sum = delays = vacations = justified = unjustified = justified_delays_sum = 0
        cur = start
        while cur <= end:
            arow_actual = actuals_idx.get((arow["agent_id"], cur))
            item = compute_day_status(arow, cur, arow_actual, just_map)
            if status_filter is None or item["status"] == status_filter:
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
                "agent_id": arow["agent_id"],
                "name": arow["name"],
                "lead": arow["lead"],
                "days": days,
                "late_minutes_sum": late_sum,
                "delays_sum": delays,
                "vacations_sum": vacations,
                "justified_sum": justified,
                "unjustified_sum": unjustified,
                "justified_delays_sum": justified_delays_sum,
            })

    return {"agents": agents_out}
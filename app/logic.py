import pandas as pd
import os
from datetime import datetime, date, time, timedelta
from typing import Optional, Dict, Any, Tuple, List
from zoneinfo import ZoneInfo


from .utils import parse_hhmm_or_hhmmss, weekday_token, weekday_short, parse_days_list
from .utils import check_time_in_schedule_ranges, get_expected_intervals_for_day, find_best_matching_interval
from .database import (
    get_justifications_map, get_schedule_for_date, get_single_day_override_db, 
    get_new_schedule_override, get_agent_schedules, get_agent
)
from .providers.schedule_provider import ScheduleProvider

CSV_ACTUALS = "actuals.csv"
TOLERANCE_MINUTES = 2

def _is_midnight_cut(t) -> bool:
    """True when TASKE records 23:59:59 - meaning 'still connected at midnight'."""
    return t is not None and t.hour == 23 and t.minute == 59 and t.second == 59


def compute_night_shift_seconds(
        exp_start: datetime,
        exp_end: datetime,
        actual_row: Optional[pd.Series],
        next_day_actual_row: Optional[pd.Series],
        day: date,
) -> int:
    """
    Calculate total seconds the agent was connected within the expected night shift window.

    Night shift cross midnight so TASKE splits them across two calendar rows:
        - Row on shift date D:  start=21:30, end=23:59:59 (midnight cut)
        - Row on date D+1:      start=0:00,  end=05:00    (continuation)
    
    When an agent never logs off between consecutive nights, TASKE chain rows
    across multiple days (0:00:00 -> 23:59:59 for entire days). Window intersection
    correctly attributes exactly 7.5h to each shift regardless of chain length.
    """
    total = 0
    next_day = day + timedelta(days=1)

    def _interval_overlap(row_date: date, row: pd.Series) -> int:
        start_t = row["actual_start_t"]
        end_t = row["actual_end_t"]
        if start_t is None or end_t is None:
            return 0
        act_start = datetime.combine(row_date, start_t, tzinfo=LOCAL_TZ)
        act_end = datetime.combine(row_date, end_t, tzinfo=LOCAL_TZ)
        if act_end <= act_start:
            act_end += timedelta(days=1)
        overlap_start = max(act_start, exp_start)
        overlap_end = min(act_end, exp_end)
        return max(0, int((overlap_end - overlap_start).total_seconds()))
    
    if actual_row is not None:
        total += _interval_overlap(day, actual_row)
    
    if next_day_actual_row is not None:
        total += _interval_overlap(next_day, next_day_actual_row)
    
    return total

# All schedule and actuals data is in LA time (PST/PDT).
# This ensures PDT transitions and date boundaries are handled correctly
# regardless of what timezone the Render server runs in (UTC).
LOCAL_TZ = ZoneInfo(os.environ.get("APP_TZ", "America/Los_Angeles"))

def local_today() -> date:
    """Today's date in LA time - prevents UTC server from returning
    tomorrow's date during evening/night hours in Pacific time."""
    return datetime.now(LOCAL_TZ).date()


def dst_shift_for_date(day: date) -> timedelta:
    """
    Returns timedelta(hours=1) when LA is in DST (PDT) on the given date,
    timedelta(0) otherwise.
    
    Mexico has no DST so agents work fixed local hours year-round.
    When LA springs forward in March, TASKE-recorded times shift +1h relative
    to the stored schedule times. Shifting the planned window by the same amount
    keeps the comparison correct without touching any stored schedule data.
    """
    dt = datetime(day.year, day.month, day.day, 12, tzinfo=LOCAL_TZ)
    # PST = UTC-8, PDT = UTC-7
    is_dst = dt.utcoffset().total_seconds() == -7 * 3600
    return timedelta(hours=1) if is_dst else timedelta(0)


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
    start_dt = datetime.combine(day, start_t, tzinfo=LOCAL_TZ)
    end_dt = datetime.combine(day, end_t, tzinfo=LOCAL_TZ)
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)
    
    # Shift planned window when LA is in DST - Mexico doesn't observe DST so
    # agents connect 1h later in LA time after the spring-forward transition.
    shift = dst_shift_for_date(day)
    start_dt += shift
    end_dt += shift
    
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
    astart_dt = datetime.combine(day, astart_t, tzinfo=LOCAL_TZ)
    aend_dt = datetime.combine(day, aend_t, tzinfo=LOCAL_TZ)
    if aend_dt <= astart_dt:
        aend_dt = aend_dt + timedelta(days=1)
    return astart_dt, aend_dt

def compute_day_status(
    agent_row: pd.Series,
    day: date,
    actual_row: Optional[pd.Series],
    just_map: Dict[Tuple[str, date], Dict[str, Any]],
    today: Optional[date] = None,
    next_day_actual_row: Optional[pd.Series] = None,
) -> Dict[str, Any]:
    if today is None:
        today = local_today()
  
    agent_id = str(agent_row["agent_id"])    
    name = agent_row["name"]
    lead = agent_row["lead"]
    shift = agent_row.get("Shift", "")
    shift_code = agent_row.get("shift_code", "")

    actual_start = ""
    actual_end = ""
    planned_start = ""
    planned_end = ""
    late_minutes = 0
    overtime_minutes = 0

    if day > today:
        original_status = "-"
    else:
        exp_iv = expected_interval_for_day(agent_row, day)

        if shift_code == "OFF" or exp_iv is None:
            original_status = "O"
        else:
            exp_start, exp_end, is_night = exp_iv
            planned_start = exp_start.strftime("%H:%M")
            planned_end = exp_end.strftime("%H:%M")
            
            if is_night:
                # - Night shift: duration-based rule -
                # TASKE splits night shifts at midnight. We measure total seconds
                # the agent was connected within the expected shift window using
                # both today's row and tomorrow's row (the continuation segment).
                expected_secs = int((exp_end - exp_start).total_seconds())
                night_secs = compute_night_shift_seconds(
                    exp_start, exp_end, actual_row, next_day_actual_row, day
                )

                if night_secs == 0 and actual_row is not None:
                    # No overlap with the evening window — check for a "tail-only session":
                    # the last working day before days off only contains the morning tail
                    # (00:00 → exp_end.time()) which is the completion of the PREVIOUS
                    # night's shift, not the start of a new one.
                    start_t = actual_row["actual_start_t"]
                    end_t = actual_row["actual_end_t"]
                    if start_t is not None and end_t is not None:
                        tail_end_dt = datetime.combine(day, exp_end.time(), tzinfo=LOCAL_TZ)
                        tail_start_dt = datetime.combine(day, time(0, 0, 0), tzinfo=LOCAL_TZ)
                        tail_expected_secs = int((tail_end_dt - tail_start_dt).total_seconds())
                        act_s = datetime.combine(day, start_t, tzinfo=LOCAL_TZ)
                        act_e = datetime.combine(day, end_t, tzinfo=LOCAL_TZ)
                        if act_e <= act_s:
                            act_e += timedelta(days=1)
                        tail_overlap = max(0, int(
                            (min(act_e, tail_end_dt) - max(act_s, tail_start_dt)).total_seconds()
                        ))
                        actual_start = act_s.strftime("%H:%M")
                        actual_end = min(act_e, tail_end_dt).strftime("%H:%M")
                        if tail_overlap >= tail_expected_secs - (TOLERANCE_MINUTES * 60):
                            original_status = "A"
                            late_minutes = 0
                        elif tail_overlap > 0:
                            late_minutes = max(0, (tail_expected_secs - tail_overlap) // 60)
                            original_status = "D"
                        else:
                            original_status = "U"
                    else:
                        original_status = "U"
                elif night_secs == 0:
                    original_status = "U"
                elif night_secs >= expected_secs - (TOLERANCE_MINUTES * 60):
                    original_status = "A"
                    overtime_minutes = max(0, (night_secs - expected_secs) // 60)
                else:
                    late_minutes = (expected_secs - night_secs) // 60
                    original_status = "D"
                
                # Display: show real start from today's row; real end from wherever
                # the shift actually ended (next day's row if shift was midnight-split)
                if actual_row is not None and not actual_start:
                    act_iv = actual_interval_for_day(actual_row, day, is_night)
                    if act_iv:
                        actual_start = act_iv[0].strftime("%H:%M")
                        # If today's row ends at 23:59 (midnight cut) and we have a
                        # next-day row with a real end time, show that as actual_end
                        today_end_t = actual_row.get("actual_end_t")
                        if (_is_midnight_cut(today_end_t)
                                and next_day_actual_row is not None
                                and not _is_midnight_cut(next_day_actual_row.get("actual_end_t"))):
                            next_end_t = next_day_actual_row["actual_end_t"]
                            if next_end_t:
                                actual_end = next_end_t.strftime("%H:%M")
                            else:
                                actual_end = act_iv[1].strftime("%H:%M")
                        else:
                            actual_end = act_iv[1].strftime("%H:%M")
            
            else:
                # - Day / afternoon shift: standard start+end time comparison -
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
                    overtime_minutes = (
                        max(0, int((exp_start - act_start).total_seconds() // 60))
                        + max(0, int((act_end - exp_end).total_seconds() // 60))
                    )
                    if late_raw <= TOLERANCE_MINUTES:
                        late_minutes = 0
                        original_status = "A"
                    else:
                        late_minutes = late_raw
                        original_status = "D"
    
    status = original_status
    is_overridden = False
    tooltip = None

    override = just_map.get((agent_id, day))
    if override and override.get("type") in {"A", "J", "V", "U", "D", "H", "C", "ML"}:
        if original_status == "O":
            status = original_status
            is_overridden = False
        else:
            is_overridden = True
            status = override["type"]
            if status in {"A", "J"} or (original_status == "D" and status != "D"):
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


def build_attendance(start, end, lead, agent_id, status_filter=None):
    from .shift_db import get_all_roster_agents

    just_map = get_justifications_map(start, end)
    actuals_idx = get_actuals_idx()
    today = local_today()

    # Hoist status filter parsing
    allowed_statuses = None
    if status_filter is not None:
        allowed_statuses = {s.strip().upper() for s in str(status_filter).split(",") if s.strip()}

    # Single bulk DB fetch for entire date range
    schedule_cache = ScheduleProvider.get_schedule_cache_for_range(start, end)

    # Pre-index by agent_id for 0(1) lookups
    schedule_by_agent: Dict[str, Dict[date, pd.Series]] = {}
    for d, df in schedule_cache.items():
        if df.empty:
            continue
        for _, row in df.iterrows():
            aid_key = str(row["agent_id"])
            if aid_key not in schedule_by_agent:
                schedule_by_agent[aid_key] = {}
            schedule_by_agent[aid_key][d] = row
    
    # Build agent list from cache
    roster = get_all_roster_agents()
    all_agents = {a["agent_id"]: {"name": a["full_name"], "lead": a["lead"]} for a in roster}

    if lead:
        lead_lower = lead.strip().lower()
        all_agents = {k: v for k, v in all_agents.items() if v["lead"].lower() == lead_lower}
    if agent_id:
        all_agents = {k: v for k, v in all_agents.items() if k == str(agent_id).strip()}
    
    agents_out = []
    for aid, agent_info in all_agents.items():
        days = []
        late_sum = delays = vacations = justified = unjustified = \
            justified_delays_sum = holidays = comp_days = medical_leaves = 0
        cur = start

        while cur <= end:
            arow = schedule_by_agent.get(aid, {}).get(cur) # 0(1)
            if arow is None:
                cur += timedelta(days=1)
                continue

            arow_actual = actuals_idx.get((aid, cur))
            # For night shifts, also fetch the next calendar day's actual row -
            # the shift crosses midnight so TASKE records it across two dates.
            next_arow_actual = None
            if arow is not None and arow.get("is_night"):
                next_arow_actual = actuals_idx.get((aid, cur + timedelta(days=1)))
            item = compute_day_status(arow, cur, arow_actual, just_map, today, next_arow_actual)

            match = allowed_statuses is None or item["status"].upper() in allowed_statuses
            if match:
                days.append(item)
                late_sum += item["late_minutes"]
                s = item["status"]
                if s == "D": delays += 1
                elif s == "V": vacations += 1
                elif s == "J": justified += 1
                elif s == "U": unjustified += 1
                elif s == "H": holidays += 1
                elif s == "C": comp_days += 1
                elif s == "ML": medical_leaves += 1
                if item["original_status"] == "D" and s in {"A", "J"}:
                    justified_delays_sum += 1
            
            cur += timedelta(days=1)
        
        if days:
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
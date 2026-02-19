from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse
from datetime import datetime, date, time, timedelta
from io import BytesIO
from typing import Dict, Tuple, Any, List
import pandas as pd

from ..logic import build_attendance, get_actuals_df, expected_interval_for_day, compute_day_status, get_schedule_for_day, get_valid_agent_ids
from ..providers.schedule_provider import ScheduleProvider
from ..shift_db import get_all_roster_agents
from ..database import (
    get_justifications_map, upsert_justification, delete_justification,
    save_schedule_override, get_all_schedule_overrides, get_unique_shifts
)
from ..models.schemas import JustifyBody, ScheduleOverrideBody

router = APIRouter()

@router.get("/attendance")
def get_attendance(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    lead: str = Query(None),
    agent_id: str = Query(None),
    status: str = Query(None),
):
    # Validación de fechas
    try:
        start_d = datetime.fromisoformat(start).date()
        end_d = datetime.fromisoformat(end).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")
    if end_d < start_d:
        raise HTTPException(status_code=400, detail="The 'end' must be >= 'start'")

    data = build_attendance(start_d, end_d, lead, agent_id, status)
    return data

@router.post("/attendance/justify")
def post_justify(body: JustifyBody):
    try:
        day = datetime.fromisoformat(body.date).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")
    valid_ids = get_valid_agent_ids()
    if body.agent_id not in valid_ids:
        raise HTTPException(status_code=404, detail="agent_id not found in roster")
    upsert_justification(body.agent_id, day, body.type, body.note or "", body.lead or "")
    return {"ok": True, "message": "Justification saved"}

@router.delete("/attendance/justify")
def delete_justify(agent_id: str = Query(...), date: str = Query(..., description="YYYY-MM-DD")):
    try:
        day = datetime.fromisoformat(date).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")
    valid_ids = get_valid_agent_ids()
    if agent_id not in valid_ids:
        raise HTTPException(status_code=404, detail="agent_id not found in roster")
    delete_justification(agent_id, day)
    return {"ok": True, "message": "Justification removed"}


@router.get("/schedules")
def get_schedules(lead: str = Query(None)):
    """Get all agent schedules with their work days, days off, and expected times."""
    from datetime import datetime
    
    # Get all roster agents (from the same source as /api/roster)
    agents_data = get_all_roster_agents()
    
    # Filter by lead if provided
    if lead:
        lead_lower = lead.strip().lower()
        agents_data = [a for a in agents_data if str(a.get("lead", "")).lower() == lead_lower]
    
    # Sort by lead, then by name
    agents_data.sort(key=lambda a: (a.get("lead", ""), a.get("full_name", "")))
    
    agents = []
    for agent in agents_data:
        agent_id = agent["agent_id"]
        agents.append({
            "agent_id": agent_id,
            "name": agent.get("full_name", ""),
            "lead": agent.get("lead", ""),
            "shift": "(from roster)",  # Placeholder - shift varies by day
            "working_days": "",  # Can vary by day in new system
            "days_off": "",     # Can vary by day in new system
            "expected_start": "",  # Can vary by day
            "expected_end": "",    # Can vary by day
        })
    
    return {"agents": agents, "total": len(agents)}


@router.get("/export.xlsx")
def export_excel(
    start: str = Query(..., description="YYYY-MM-DD"),
    end: str = Query(..., description="YYYY-MM-DD"),
    lead: str = Query(None),
    agent_id: str = Query(None),
):
    # Validar fechas
    try:
        start_d = datetime.fromisoformat(start).date()
        end_d = datetime.fromisoformat(end).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")
    if end_d < start_d:
        raise HTTPException(status_code=400, detail="The 'end' must be >= 'start'")

    # --- Attendance (resumen) ---
    data = build_attendance(start_d, end_d, lead, agent_id)
    day_labels: list[str] = []
    cur = start_d
    while cur <= end_d:
        day_labels.append(cur.isoformat())
        cur += timedelta(days=1)

    rows_attendance = []
    for agent in data["agents"]:
        base = {
            "agent_id": agent["agent_id"], "name": agent["name"], "lead": agent["lead"]
        }
        for item in agent["days"]:
            base[item["date"]] = item["status"]
        # Resúmenes (incluye justified_delays_sum)
        base["late_minutes_sum"] = agent["late_minutes_sum"]
        base["delays_sum"] = agent["delays_sum"]
        base["vacations_sum"] = agent["vacations_sum"]
        base["justified_sum"] = agent["justified_sum"]
        base["unjustified_sum"] = agent["unjustified_sum"]
        base["justified_delays_sum"] = agent["justified_delays_sum"]
        base["holidays_sum"] = agent["holidays_sum"]
        base["comp_days_sum"] = agent["comp_days_sum"]
        rows_attendance.append(base)

    cols_attendance = ["agent_id", "name"] + day_labels + [
        "late_minutes_sum", "delays_sum", "vacations_sum", "justified_sum", "unjustified_sum", "justified_delays_sum", "holidays_sum", "comp_days_sum"
    ]
    df_attendance = pd.DataFrame(rows_attendance, columns=cols_attendance)

    # --- Connections (detalle conexiones por día) ---
    df_act_all = get_actuals_df()

    # Cache for schedule by date to avoid repeated lookups
    schedule_cache: Dict[date, pd.DataFrame] = {}
    
    def get_schedule_cached(d: date) -> pd.DataFrame:
        if d not in schedule_cache:
            schedule_cache[d] = get_schedule_for_day(d)
        return schedule_cache[d]

    # Collect all unique agents from all schedules in the date range
    all_agents: Dict[str, Dict[str, Any]] = {}  # agent_id -> latest agent info
    cur = start_d
    while cur <= end_d:
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
        base_sched = get_schedule_cached(start_d)
        valid_agents = set(base_sched[base_sched["lead"].str.lower() == lead_lower]["agent_id"].tolist())
        all_agents = {k: v for k, v in all_agents.items() if k in valid_agents}
    
    if agent_id:
        target_aid = str(agent_id).strip()
        all_agents = {k: v for k, v in all_agents.items() if k == target_aid}

    valid_agents = set(all_agents.keys())
    df_act_all = df_act_all[df_act_all["agent_id"].isin(valid_agents)].copy()

    actuals_by_day: Dict[Tuple[str, date], List[pd.Series]] = {}
    for _, r in df_act_all.iterrows():
        key = (str(r["agent_id"]), r["date"])
        actuals_by_day.setdefault(key, []).append(r)

    def tstr(t: time) -> str:
        return t.strftime("%H:%M") if t else ""

    rows_connections: List[Dict[str, Any]] = []
    for aid, agent_info in all_agents.items():
        cur_day = start_d
        while cur_day <= end_d:
            # Get schedule for this specific day
            day_sched = get_schedule_cached(cur_day)
            agent_rows = day_sched[day_sched["agent_id"] == aid]
            
            if agent_rows.empty:
                # Agent not in schedule for this day - skip
                cur_day += timedelta(days=1)
                continue
            
            arow = agent_rows.iloc[0]
            ashift = str(arow["Shift"])
            exp_iv = expected_interval_for_day(arow, cur_day)
            exp_start_t = exp_iv[0].time() if exp_iv else None
            exp_end_t = exp_iv[1].time() if exp_iv else None

            act_rows = actuals_by_day.get((aid, cur_day), [])
            first_row = act_rows[0] if act_rows else None
            just_map = get_justifications_map(cur_day, cur_day)
            day_item = compute_day_status(arow, cur_day, first_row, just_map)

            if act_rows:
                for r in act_rows:
                    rows_connections.append({
                        "expected_connect_time": tstr(exp_start_t),
                        "expected_disconnect_time": tstr(exp_end_t),
                        "date": cur_day.isoformat(),
                        "agent_id": aid,
                        "name": agent_info["name"],
                        "shift": ashift,
                        "actual_connect_time": tstr(r["actual_start_t"]),
                        "actual_disconnect_time": tstr(r["actual_end_t"]),
                        "status": day_item["status"],
                        "late_minutes_sum": day_item["late_minutes"],
                    })
            else:
                rows_connections.append({
                    "expected_connect_time": tstr(exp_start_t),
                    "expected_disconnect_time": tstr(exp_end_t),
                    "date": cur_day.isoformat(),
                    "agent_id": aid,
                    "name": agent_info["name"],
                    "shift": ashift,
                    "actual_connect_time": "",
                    "actual_disconnect_time": "",
                    "status": day_item["status"],
                    "late_minutes_sum": day_item["late_minutes"],
                })

            cur_day += timedelta(days=1)

    cols_connections = [
        "expected_connect_time", "expected_disconnect_time",
        "date", "agent_id", "name", "shift",
        "actual_connect_time", "actual_disconnect_time",
        "status", "late_minutes_sum"
    ]
    df_connections = pd.DataFrame(rows_connections, columns=cols_connections)

    # --- Escribir ambas hojas al Excel ---
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_attendance.to_excel(writer, index=False, sheet_name="Attendance")
        df_connections.to_excel(writer, index=False, sheet_name="Connections")

    buf.seek(0)
    headers = {"Content-Disposition": 'attachment; filename="export.xlsx"'}
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers
    )

@router.get("/schedules/all")
def get_schedules_all():
    """Return all schedules from the roster database (same source as /api/roster)"""
    agents_data = get_all_roster_agents()
    
    schedules = []
    for agent in agents_data:
        schedules.append({
            "agent_id": agent["agent_id"],
            "name": agent.get("full_name", ""),
            "lead": agent.get("lead", ""),
            "expected_start": "",  # Varies by day of week
            "expected_end": "",    # Varies by day of week
            "shift": "(per day)",
        })
    return {"schedules": schedules}

@router.get("/justifications_report.xlsx")
def justifications_report():
    import sqlite3
    con = sqlite3.connect("attendance.db")
    cur = con.cursor()
    cur.execute(
        "SELECT agent_id, date, type, note, lead, created_at FROM justifications ORDER BY created_at DESC"
    )
    rows = cur.fetchall()
    con.close()

    # Nombres de agentes desde el roster
    agents_data = get_all_roster_agents()
    agent_names = {str(a["agent_id"]): str(a.get("full_name", "")) for a in agents_data}

    data = []
    for agent_id, date_str, typ, note, lead, created_at in rows:
        data.append({
            "agent_id": agent_id,
            "name": agent_names.get(str(agent_id), ""),
            "date": date_str,
            "type": typ,
            "note": note,
            "lead": lead,
            "created_at": created_at,
        })

    df = pd.DataFrame(data, columns=["agent_id", "name", "date", "type", "note", "lead", "created_at"])

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Justifications")
    buf.seek(0)
    headers = {"Content-Disposition": 'attachment; filename="justifications_report.xlsx"'}
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers
    )


# ============ Schedule Override Endpoints ============

@router.get("/schedule-overrides")
def get_schedule_overrides_list():
    """Get all schedule overrides (for viewing history)."""
    overrides = get_all_schedule_overrides()
    return {"overrides": overrides, "total": len(overrides)}


@router.get("/shifts")
def get_shifts():
    """Get all unique shift names."""
    shifts = get_unique_shifts()
    return {"shifts": shifts}


@router.post("/schedule-overrides")
def create_schedule_override(body: ScheduleOverrideBody):
    """
    Create schedule overrides for one or more agents.
    This is append-only - existing data is never modified.
    """
    # Validate lead is provided
    if not body.lead or not body.lead.strip():
        raise HTTPException(status_code=400, detail="Lead name is required")
    
    # Validate agent_ids
    if not body.agent_ids or len(body.agent_ids) == 0:
        raise HTTPException(status_code=400, detail="At least one agent must be selected")
    
    # Validate date format
    try:
        effective_date = datetime.fromisoformat(body.effective_date).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")
    
    # Validate weekly pattern
    wp = body.weekly_pattern
    days = [
        ("mon", wp.mon_enabled, wp.mon_start, wp.mon_end),
        ("tue", wp.tue_enabled, wp.tue_start, wp.tue_end),
        ("wed", wp.wed_enabled, wp.wed_start, wp.wed_end),
        ("thu", wp.thu_enabled, wp.thu_start, wp.thu_end),
        ("fri", wp.fri_enabled, wp.fri_start, wp.fri_end),
        ("sat", wp.sat_enabled, wp.sat_start, wp.sat_end),
        ("sun", wp.sun_enabled, wp.sun_start, wp.sun_end),
    ]
    
    has_working_day = False
    for day_name, enabled, start, end in days:
        if enabled:
            has_working_day = True
            if not start or not end:
                raise HTTPException(status_code=400, detail=f"Start and end times are required for {day_name}")
            if start >= end:
                raise HTTPException(status_code=400, detail=f"End time must be after start time for {day_name}")
    
    if not has_working_day:
        raise HTTPException(status_code=400, detail="At least one day must be working (not a day off)")
    
    # Always use new_schedule scope
    
    # Build working_days and days_off from weekly pattern
    working_days = []
    days_off = []
    day_times = {}
    
    for day_name, enabled, start, end in days:
        if enabled:
            working_days.append(day_name.capitalize())
            if start and end:
                day_times[day_name] = (start, end)
        else:
            days_off.append(day_name.capitalize())
    
    # For simplicity, use the first enabled day's times as expected_start/end
    # In a more complex implementation, you might store per-day times
    first_times = next(iter(day_times.values()), (None, None)) if day_times else (None, None)
    
    created_ids = save_schedule_override(
        agent_ids=body.agent_ids,
        override_type="new_schedule",
        effective_date=body.effective_date,
        end_date=None,  # Open-ended
        shift=None,  # No base shift
        working_days=", ".join(working_days) if working_days else None,
        days_off=", ".join(days_off) if days_off else None,
        expected_start=first_times[0],
        expected_end=first_times[1],
        note=body.note or "",
        lead=body.lead.strip()
    )
    
    return {
        "ok": True,
        "message": f"Schedule override created for {len(body.agent_ids)} agent(s)",
        "created_ids": created_ids
    }
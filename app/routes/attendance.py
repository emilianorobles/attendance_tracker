from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse
from datetime import datetime, date, time, timedelta
from io import BytesIO
from typing import Dict, Tuple, Any, List
import pandas as pd

from ..logic import build_attendance, get_actuals_df, SCHEDULE_DF, VALID_AGENT_IDS, expected_interval_for_day, compute_day_status
from ..database import get_justifications_map, upsert_justification, delete_justification
from ..models.schemas import JustifyBody

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
    if body.agent_id not in VALID_AGENT_IDS:
        raise HTTPException(status_code=404, detail="agent_id not found in schedule.csv")
    upsert_justification(body.agent_id, day, body.type, body.note or "", body.lead or "")
    return {"ok": True, "message": "Justification saved"}

@router.delete("/attendance/justify")
def delete_justify(agent_id: str = Query(...), date: str = Query(..., description="YYYY-MM-DD")):
    try:
        day = datetime.fromisoformat(date).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format (use YYYY-MM-DD)")
    if agent_id not in VALID_AGENT_IDS:
        raise HTTPException(status_code=404, detail="agent_id not found in schedule.csv")
    delete_justification(agent_id, day)
    return {"ok": True, "message": "Justification removed"}


@router.get("/schedules")
def get_schedules(lead: str = Query(None)):
    """Get all agent schedules with their work days, days off, and expected times."""
    sched = SCHEDULE_DF.copy()
    
    if lead:
        sched = sched[sched["lead"].str.lower() == lead.strip().lower()]
    
    # Sort by lead, then by name
    sched = sched.sort_values(["lead", "name"])
    
    agents = []
    for _, row in sched.iterrows():
        agents.append({
            "agent_id": str(row["agent_id"]),
            "name": str(row["name"]),
            "lead": str(row["lead"]),
            "shift": str(row["Shift"]),
            "working_days": str(row["working_days"]),
            "days_off": str(row["days_off"]),
            "expected_start": str(row["expected_start"]),
            "expected_end": str(row["expected_end"]),
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
        rows_attendance.append(base)

    cols_attendance = ["agent_id", "name"] + day_labels + [
        "late_minutes_sum", "delays_sum", "vacations_sum", "justified_sum", "unjustified_sum", "justified_delays_sum"
    ]
    df_attendance = pd.DataFrame(rows_attendance, columns=cols_attendance)

    # --- Connections (detalle conexiones por día) ---
    df_act_all = get_actuals_df()

    sched = SCHEDULE_DF.copy()
    if lead:
        sched = sched[sched["lead"].str.lower() == lead.strip().lower()]
    if agent_id:
        sched = sched[sched["agent_id"] == str(agent_id).strip()]

    valid_agents = set(sched["agent_id"].astype(str).tolist())
    df_act_all = df_act_all[df_act_all["agent_id"].isin(valid_agents)].copy()

    actuals_by_day: Dict[Tuple[str, date], List[pd.Series]] = {}
    for _, r in df_act_all.iterrows():
        key = (str(r["agent_id"]), r["date"])
        actuals_by_day.setdefault(key, []).append(r)

    def tstr(t: time) -> str:
        return t.strftime("%H:%M") if t else ""

    rows_connections: List[Dict[str, Any]] = []
    for _, arow in sched.iterrows():
        aid = str(arow["agent_id"])
        aname = str(arow["name"])
        ashift = str(arow["Shift"])

        cur_day = start_d
        while cur_day <= end_d:
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
                        "name": aname,
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
                    "name": aname,
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

@router.get("/schedules")
def get_schedules():
    """Return all schedules from schedule.csv"""
    schedules = []
    for _, row in SCHEDULE_DF.iterrows():
        schedules.append({
            "agent_id": str(row["agent_id"]),
            "name": str(row["name"]),
            "lead": str(row["lead"]),
            "expected_start": str(row["expected_start"]),
            "expected_end": str(row["expected_end"]),
            "shift": str(row.get("Shift", "")),
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

    # Nombres de agentes desde schedule.csv
    agent_names = {str(r["agent_id"]): str(r["name"]) for _, r in SCHEDULE_DF.iterrows()}

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
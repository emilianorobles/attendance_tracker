from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from datetime import date, timedelta
import json
from pathlib import Path
import csv

from .storage import sync_from_r2
from .database import get_all_agents_and_leads, init_db
from .routes.attendance import router as attendance_router
from .routes.admin import router as admin_router

APP_TITLE = "Attendance"
BASE_DIR = Path(__file__).resolve().parent.parent

app = FastAPI(title=APP_TITLE)

# PWA: servir archivos estáticos
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

# Templates
templates = Jinja2Templates(directory=BASE_DIR / "templates")

# Cache for actuals data
actuals_cache = []

# Load actuals.csv into memory on app startup
@app.on_event("startup")
def load_actuals():
    global actuals_cache
    try:
        with open("actuals.csv", "r") as file:
            reader = csv.DictReader(file)
            actuals_cache = [row for row in reader]
        print("Loaded actuals.csv into memory.")
    except Exception as e:
        print(f"Error loading actuals.csv: {e}")

# Sync files from R2 on startup (before init_db)
sync_from_r2()

# Init DB
init_db()

# Include routes
app.include_router(attendance_router)
app.include_router(admin_router)

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    # Rango por defecto: mes actual del año actual
    today = date.today()
    yr = today.year
    mo = today.month
    start = date(yr, mo, 1)
    # Last day of the month
    if mo == 12:
        end = date(yr, 12, 31)
    else:
        end = date(yr, mo + 1, 1) - timedelta(days=1)

    # Opciones dinámicas (leads y agentes) desde todos los schedules versionados y CSV
    leads, agents = get_all_agents_and_leads()
    options_json = json.dumps({"leads": leads, "agents": agents})

    return templates.TemplateResponse("index.html", {
        "request": request,
        "options_json": options_json,
        "default_start": start.isoformat(),
        "default_end": end.isoformat()
    })

# Helper to find attendance details by agent_id and date
def get_attendance_details(agent_id, date):
    for row in actuals_cache:
        if row['agent_id'] == str(agent_id) and row['date'] == date:
            return {
                "shift": row.get("shift", "—"),
                "actual_start": row.get("actual_start", "—"),
                "actual_end": row.get("actual_end", "—")
            }
    return {"shift": "—", "actual_start": "—", "actual_end": "—"}

SHIFT_STARTS = {
    "Morning": "08:00",
    "Afternoon": "14:00",
    "Night": "22:00"
}

def calculate_delay(actual_start, planned_start=None, shift=None):
    from datetime import datetime

    if not actual_start or actual_start == "—":
        return "—"

    try:
        actual_time = datetime.strptime(actual_start, "%H:%M")
        if planned_start:
            planned_time = datetime.strptime(planned_start, "%H:%M")
        elif shift and shift in SHIFT_STARTS:
            planned_time = datetime.strptime(SHIFT_STARTS[shift], "%H:%M")
        else:
            return "—"

        delay = (actual_time - planned_time).total_seconds() / 60
        return max(0, int(delay))
    except Exception as e:
        print(f"Error calculating delay: {e}")
        return "—"

@app.get("/attendance-details")
async def attendance_details(agent_id: int, date: str):
    details = get_attendance_details(agent_id, date)
    details["delay"] = calculate_delay(details["actual_start"], shift=details["shift"])
    return details
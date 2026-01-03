from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from datetime import date, timedelta
import json
from pathlib import Path

from .storage import sync_from_r2
from .logic import SCHEDULE_DF
from .database import init_db
from .routes.attendance import router as attendance_router
from .routes.admin import router as admin_router

APP_TITLE = "Attendance"
BASE_DIR = Path(__file__).resolve().parent.parent

app = FastAPI(title=APP_TITLE)

# PWA: servir archivos estáticos
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

# Templates
templates = Jinja2Templates(directory=BASE_DIR / "templates")

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

    # Opciones dinámicas (leads y agentes) desde schedule.csv
    leads = sorted(list({str(x) for x in SCHEDULE_DF["lead"].tolist()}))
    agents = [{"id": str(r["agent_id"]), "name": str(r["name"])} for _, r in SCHEDULE_DF.iterrows()]
    options_json = json.dumps({"leads": leads, "agents": agents})

    return templates.TemplateResponse("index.html", {
        "request": request,
        "options_json": options_json,
        "default_start": start.isoformat(),
        "default_end": end.isoformat()
    })
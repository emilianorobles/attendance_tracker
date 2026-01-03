"""Admin routes for database management."""
import os
import shutil
from datetime import date, datetime
from pathlib import Path
from io import StringIO

import pandas as pd
from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Form
from fastapi.responses import FileResponse

from ..storage import sync_actuals_to_r2, sync_schedule_to_r2, is_r2_enabled
from ..database import save_schedule_version, get_all_schedule_versions

router = APIRouter(prefix="/admin", tags=["admin"])

# Password for protecting admin endpoints.
# Set ADMIN_PASSWORD env var on Heroku; defaults to a placeholder for local dev.
# Can be a short memorable password like "supervisor123"
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "changeme")

DB_PATH = Path("attendance.db")
ACTUALS_PATH = Path("actuals.csv")
SCHEDULE_PATH = Path("schedule.csv")


@router.get("/download-db")
def download_db(token: str = Query(..., description="Admin password for authorization")):
    """Download the SQLite database file.

    Usage:
        GET /admin/download-db?token=<ADMIN_PASSWORD>

    Returns the attendance.db file as an attachment.
    """
    if token != ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password")

    if not DB_PATH.exists():
        raise HTTPException(status_code=404, detail="Database file not found")

    return FileResponse(
        path=str(DB_PATH),
        filename="attendance.db",
        media_type="application/octet-stream",
    )


@router.post("/upload-actuals")
async def upload_actuals(
    token: str = Form(..., description="Admin password for authorization"),
    file: UploadFile = File(..., description="The actuals.csv file to upload"),
):
    """Upload a new actuals.csv file.

    Usage:
        POST /admin/upload-actuals
        Form data: token=<ADMIN_PASSWORD>, file=<actuals.csv>

    Replaces the existing actuals.csv with the uploaded file.
    """
    if token != ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password")

    # Validate file extension
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a .csv file")

    # Read and validate content (check for expected columns)
    try:
        content = await file.read()
        # Decode and check header
        text = content.decode("utf-8")
        lines = text.strip().split("\n")
        if not lines:
            raise HTTPException(status_code=400, detail="File is empty")
        
        header = lines[0].lower()
        required_cols = ["date", "agent_id", "actual_start", "actual_end"]
        for col in required_cols:
            if col not in header:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required column: {col}. Header found: {lines[0]}"
                )
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded")

    # Backup existing file if it exists
    if ACTUALS_PATH.exists():
        backup_path = ACTUALS_PATH.with_suffix(".csv.bak")
        shutil.copy2(ACTUALS_PATH, backup_path)

    # Write new file
    with open(ACTUALS_PATH, "wb") as f:
        f.write(content)

    # Sync to R2 for persistence across dyno restarts
    r2_synced = sync_actuals_to_r2()
    
    return {
        "ok": True,
        "message": f"Uploaded {file.filename} successfully",
        "rows": len(lines) - 1,
        "r2_synced": r2_synced,
    }


@router.post("/upload-schedule")
async def upload_schedule(
    token: str = Form(..., description="Admin password for authorization"),
    file: UploadFile = File(..., description="The schedule.csv file to upload"),
    effective_date: str = Form(..., description="Effective start date (YYYY-MM-DD) for this schedule"),
):
    """Upload a new schedule.csv file with an effective date.

    Usage:
        POST /admin/upload-schedule
        Form data: token=<ADMIN_PASSWORD>, file=<schedule.csv>, effective_date=<YYYY-MM-DD>

    The schedule will only affect attendance calculations from the effective_date onwards.
    Historical attendance before this date will use the previous schedule version.
    """
    if token != ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password")

    # Validate effective date
    try:
        eff_date = datetime.strptime(effective_date, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    # Validate file extension
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a .csv file")

    # Read and validate content (check for expected columns)
    try:
        content = await file.read()
        # Decode and check header
        text = content.decode("utf-8")
        lines = text.strip().split("\n")
        if not lines:
            raise HTTPException(status_code=400, detail="File is empty")
        
        header = lines[0].lower()
        required_cols = ["agent_id", "name", "lead", "expected_start", "expected_end"]
        for col in required_cols:
            if col not in header:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required column: {col}. Header found: {lines[0]}"
                )
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="File must be UTF-8 encoded")

    # Parse CSV into DataFrame
    try:
        df = pd.read_csv(StringIO(text))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse CSV: {str(e)}")

    # Save schedule version to database
    version_id = save_schedule_version(df, eff_date, note=f"Uploaded from {file.filename}")

    # Also save to file for backward compatibility and R2 sync
    if SCHEDULE_PATH.exists():
        backup_path = SCHEDULE_PATH.with_suffix(".csv.bak")
        shutil.copy2(SCHEDULE_PATH, backup_path)

    with open(SCHEDULE_PATH, "wb") as f:
        f.write(content)

    # Sync to R2 for persistence across dyno restarts
    r2_synced = sync_schedule_to_r2()
    
    return {
        "ok": True,
        "message": f"Uploaded {file.filename} successfully. Schedule effective from {effective_date}.",
        "rows": len(lines) - 1,
        "version_id": version_id,
        "effective_date": effective_date,
        "r2_synced": r2_synced,
    }


@router.get("/schedule-versions")
async def list_schedule_versions(
    token: str = Query(..., description="Admin password for authorization"),
):
    """List all schedule versions with their effective dates."""
    if token != ADMIN_PASSWORD:
        raise HTTPException(status_code=403, detail="Invalid password")
    
    versions = get_all_schedule_versions()
    return {"versions": versions}
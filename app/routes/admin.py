"""Admin routes for database management."""
import os
import shutil
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Form
from fastapi.responses import FileResponse

router = APIRouter(prefix="/admin", tags=["admin"])

# Token for protecting the download endpoint.
# Set ADMIN_TOKEN env var on Heroku; defaults to a placeholder for local dev.
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "changeme")

DB_PATH = Path("attendance.db")
ACTUALS_PATH = Path("actuals.csv")


@router.get("/download-db")
def download_db(token: str = Query(..., description="Admin token for authorization")):
    """Download the SQLite database file.

    Usage:
        GET /admin/download-db?token=<ADMIN_TOKEN>

    Returns the attendance.db file as an attachment.
    """
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")

    if not DB_PATH.exists():
        raise HTTPException(status_code=404, detail="Database file not found")

    return FileResponse(
        path=str(DB_PATH),
        filename="attendance.db",
        media_type="application/octet-stream",
    )


@router.post("/upload-actuals")
async def upload_actuals(
    token: str = Form(..., description="Admin token for authorization"),
    file: UploadFile = File(..., description="The actuals.csv file to upload"),
):
    """Upload a new actuals.csv file.

    Usage:
        POST /admin/upload-actuals
        Form data: token=<ADMIN_TOKEN>, file=<actuals.csv>

    Replaces the existing actuals.csv with the uploaded file.
    """
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid token")

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

    return {"ok": True, "message": f"Uploaded {file.filename} successfully", "rows": len(lines) - 1}

"""Admin routes for database management."""
import os
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

router = APIRouter(prefix="/admin", tags=["admin"])

# Token for protecting the download endpoint.
# Set ADMIN_TOKEN env var on Heroku; defaults to a placeholder for local dev.
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "changeme")

DB_PATH = Path("attendance.db")


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

"""
Schedules API placeholder.
This module provides schedule-related endpoints.
"""
from fastapi import APIRouter

router = APIRouter(prefix="/api/schedules", tags=["schedules"])


@router.get("")
async def get_schedules():
    """Get schedule data (placeholder)."""
    return {"days": [], "message": "Use /api/roster for the new roster matrix view"}

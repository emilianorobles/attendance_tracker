"""
API Routes for Shift Roster Management.
Provides endpoints for the roster matrix UI with effective dating.
"""
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import date, datetime, timedelta
from io import BytesIO

from ..shift_db import (
    init_shift_tables,
    seed_shift_templates,
    get_all_shift_templates,
    add_agent_to_roster,
    remove_agent_from_roster,
    get_active_agents_on_date,
    get_all_roster_agents,
    get_shift_for_agent_day,
    upsert_shift_assignment,
    bulk_upsert_shift_assignments,
    get_roster_matrix,
    get_shift_history_for_agent,
    sync_agents_from_csv,
    get_all_agents,
    get_agent_status_on_date,
    update_agent_lead,
    add_shift_template,
    update_shift_template,
    delete_shift_template,
)
from ..models.shifts import SHIFT_CATALOG, DayOfWeek

router = APIRouter(prefix="/api/roster", tags=["roster"])


# ============ Request/Response Models ============

class ShiftAssignmentRequest(BaseModel):
    """Request to update a shift assignment."""
    agent_id: str
    day_of_week: str  # Mon, Tue, Wed, Thu, Fri, Sat, Sun
    shift_code: str   # S1..S10 or OFF
    effective_date: str  # YYYY-MM-DD


class BulkShiftAssignmentRequest(BaseModel):
    """Request to update multiple days at once."""
    agent_id: str
    days_of_week: List[str]  # ["Tue", "Wed", "Thu"]
    shift_code: str
    effective_date: str


class AddAgentRequest(BaseModel):
    """Request to add an agent to the roster."""
    agent_id: str
    full_name: str
    lead: str
    effective_date: str


class RemoveAgentRequest(BaseModel):
    """Request to remove an agent from the roster."""
    agent_id: str
    effective_date: str


class UpdateLeadRequest(BaseModel):
    """Request to update an agent's lead."""
    agent_id: str
    new_lead: str
    effective_date: str


class ShiftTemplateResponse(BaseModel):
    """Response with shift template info."""
    shift_code: str
    start_time: Optional[str]
    end_time: Optional[str]
    crosses_midnight: bool
    color: str
    label: str


class ShiftTemplateRequest(BaseModel):
    """Request to create/update a shift template."""
    shift_code: str
    start_time: str
    end_time: str
    crosses_midnight: bool = False
    color: str
    label: Optional[str] = None


# ============ Endpoints ============

@router.get("/templates")
async def get_shift_templates_endpoint():
    """
    Get all available shift templates from database.
    Used to populate the shift selector dropdown and legend.
    """
    templates = get_all_shift_templates()
    return {"templates": templates}


@router.post("/templates")
async def create_shift_template(request: ShiftTemplateRequest):
    """Create a new shift template."""
    if not request.shift_code:
        raise HTTPException(status_code=400, detail="shift_code is required")
    if not request.start_time or not request.end_time:
        raise HTTPException(status_code=400, detail="start_time and end_time are required")
    
    # Generate label if not provided
    label = request.label or f"{request.shift_code} ({request.start_time}–{request.end_time})"
    
    try:
        success = add_shift_template(
            request.shift_code, request.start_time, request.end_time,
            request.crosses_midnight, request.color, label
        )
        return {"status": "created", "shift_code": request.shift_code}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/templates/{shift_code}")
async def update_shift_template_endpoint(shift_code: str, request: ShiftTemplateRequest):
    """Update an existing shift template."""
    if not request.start_time or not request.end_time:
        raise HTTPException(status_code=400, detail="start_time and end_time are required")
    
    # Generate label if not provided
    label = request.label or f"{shift_code} ({request.start_time}–{request.end_time})"
    
    try:
        success = update_shift_template(
            shift_code, request.start_time, request.end_time,
            request.crosses_midnight, request.color, label
        )
        return {"status": "updated", "shift_code": shift_code}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/templates/{shift_code}")
async def delete_shift_template_endpoint(shift_code: str):
    """Delete a shift template. Fails if template is in use by any agent."""
    if shift_code == "OFF":
        raise HTTPException(status_code=400, detail="Cannot delete the OFF template")
    
    success = delete_shift_template(shift_code)
    if success:
        return {"status": "deleted", "shift_code": shift_code}
    else:
        raise HTTPException(status_code=400, detail=f"Cannot delete {shift_code} - it is currently assigned to agents")


@router.get("/matrix")
async def get_matrix(
    week_start: str = Query(..., description="Monday of the week YYYY-MM-DD"),
    lead: Optional[str] = Query(None, description="Filter by lead"),
    agent_id: Optional[str] = Query(None, description="Filter by agent ID"),
    status: str = Query("active", description="Filter by status: 'active' or 'all'")
):
    """
    Get the roster matrix for a week starting from week_start (Monday).
    Returns ALL agents by default showing their shifts for each day.
    
    Response structure:
    {
        "agents": [
            {
                "agent_id": "10003",
                "full_name": "Aaron Gonzalez",
                "lead": "Martin",
                "status": "Active",
                "days": {
                    "Mon": {"shift_code": "S8", "color": "#F97316", ...},
                    "Tue": {"shift_code": "S8", ...},
                    ...
                }
            }
        ],
        "week_start": "2026-02-09",
        "week_dates": {"Mon": "2026-02-09", ...},
        "shift_catalog": {...},
        "total_agents": 50
    }
    """
    try:
        start = date.fromisoformat(week_start)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    
    matrix = get_roster_matrix(start, lead, agent_id, status)
    return matrix


@router.get("/week")
async def get_week_matrix(
    week_start: str = Query(..., description="Monday of the week YYYY-MM-DD"),
    lead: Optional[str] = Query(None, description="Filter by lead"),
    agent_id: Optional[str] = Query(None, description="Filter by agent ID"),
    status: str = Query("active", description="Filter by status: 'active' or 'all'")
):
    """
    Get the roster matrix for a specific week (Mon-Sun).
    Same as /matrix endpoint - provided for backward compatibility.
    """
    try:
        start = date.fromisoformat(week_start)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    
    matrix = get_roster_matrix(start, lead, agent_id, status)
    return matrix


@router.post("/assignment")
async def update_shift_assignment(request: ShiftAssignmentRequest):
    """
    Update a single shift assignment for an agent on a specific day.
    Creates a new version with effective_date, preserving history.
    
    Business Rules:
    - Closes the previous active assignment (effective_end = effective_date - 1)
    - Creates new assignment with effective_start = effective_date
    - If same shift already exists and is active, returns no_change
    """
    # Validate shift code
    if request.shift_code not in SHIFT_CATALOG:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid shift code: {request.shift_code}. Valid codes: {list(SHIFT_CATALOG.keys())}"
        )
    
    # Validate day of week
    valid_days = [d.value for d in DayOfWeek.all_days()]
    if request.day_of_week not in valid_days:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid day_of_week: {request.day_of_week}. Valid days: {valid_days}"
        )
    
    try:
        eff_date = date.fromisoformat(request.effective_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid effective_date format: {e}")
    
    result = upsert_shift_assignment(
        request.agent_id,
        request.day_of_week,
        request.shift_code,
        eff_date
    )
    
    return result


@router.post("/assignment/bulk")
async def update_bulk_shift_assignment(request: BulkShiftAssignmentRequest):
    """
    Update multiple days at once for an agent.
    Example: Set Tue+Wed+Thu to S3 effective from a date.
    """
    # Validate shift code
    if request.shift_code not in SHIFT_CATALOG:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid shift code: {request.shift_code}"
        )
    
    # Validate days
    valid_days = [d.value for d in DayOfWeek.all_days()]
    for day in request.days_of_week:
        if day not in valid_days:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid day: {day}. Valid days: {valid_days}"
            )
    
    try:
        eff_date = date.fromisoformat(request.effective_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid effective_date format: {e}")
    
    results = bulk_upsert_shift_assignments(
        request.agent_id,
        request.days_of_week,
        request.shift_code,
        eff_date
    )
    
    return {"results": results}


@router.get("/history/{agent_id}")
async def get_agent_shift_history(
    agent_id: str,
    day_of_week: Optional[str] = Query(None, description="Filter by day of week")
):
    """
    Get the full history of shift assignments for an agent.
    Useful for auditing and viewing past schedules.
    """
    history = get_shift_history_for_agent(agent_id, day_of_week)
    return {"agent_id": agent_id, "history": history}


# ============ Agent Management ============

@router.get("/agents")
async def get_agents(
    target_date: Optional[str] = Query(None, description="Get agents active on this date"),
    include_inactive: bool = Query(False, description="Include inactive agents")
):
    """
    Get agents from the roster.
    If target_date is provided, returns only agents active on that date.
    """
    if include_inactive:
        agents = get_all_roster_agents()
    elif target_date:
        try:
            t_date = date.fromisoformat(target_date)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
        agents = get_active_agents_on_date(t_date)
    else:
        agents = get_active_agents_on_date(date.today())
    
    return {"agents": agents}


@router.post("/agents")
async def add_agent(request: AddAgentRequest):
    """
    Add a new agent to the roster with an effective start date.
    The agent will appear in the roster starting from effective_date.
    """
    if not request.agent_id or not request.full_name:
        raise HTTPException(status_code=400, detail="agent_id and full_name are required")
    
    try:
        eff_date = date.fromisoformat(request.effective_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid effective_date format: {e}")
    
    try:
        agent_id = add_agent_to_roster(
            request.agent_id,
            request.full_name,
            request.lead,
            eff_date
        )
        return {
            "status": "created",
            "message": f"Agent {request.full_name} added to roster effective {eff_date}",
            "id": agent_id
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/agents/remove")
async def remove_agent(request: RemoveAgentRequest):
    """
    Permanently delete an agent from the roster.
    Removes all history including shift assignments and status records.
    This action cannot be undone.
    """
    if not request.agent_id:
        raise HTTPException(status_code=400, detail="agent_id is required")
    
    try:
        eff_date = date.fromisoformat(request.effective_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid effective_date format: {e}")
    
    success = remove_agent_from_roster(request.agent_id, eff_date)
    
    if success:
        return {
            "status": "deleted",
            "message": f"Agent {request.agent_id} permanently deleted from roster"
        }
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Agent {request.agent_id} not found"
        )


# ============ Initialization ============

@router.post("/init")
async def initialize_roster_db():
    """
    Initialize the roster database tables, seed shift templates, and sync agents from CSV.
    Call this once to set up the new data model.
    """
    try:
        init_shift_tables()
        seed_shift_templates()
        sync_agents_from_csv()
        return {"status": "success", "message": "Roster tables initialized, templates seeded, and agents synced from CSV"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sync-csv")
async def sync_csv_agents():
    """
    Sync agents from schedule.csv to the roster.
    Useful for importing agents without losing existing data.
    """
    try:
        sync_agents_from_csv()
        agents = get_all_agents()
        return {"status": "success", "message": f"Synced {len(agents)} agents from CSV", "total_agents": len(agents)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/agents/update-lead")
async def update_roster_agent_lead(request: UpdateLeadRequest):
    """
    Update the lead for an agent with effective dating.
    """
    if not request.agent_id:
        raise HTTPException(status_code=400, detail="agent_id is required")
    if not request.new_lead:
        raise HTTPException(status_code=400, detail="new_lead is required")
    
    try:
        eff_date = date.fromisoformat(request.effective_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid effective_date format: {e}")
    
    try:
        success = update_agent_lead(request.agent_id, request.new_lead, eff_date)
        if success:
            return {"status": "updated", "message": f"Agent {request.agent_id} lead updated to {request.new_lead} effective {eff_date}"}
        else:
            raise HTTPException(status_code=404, detail=f"Agent {request.agent_id} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/export.xlsx")
async def export_schedules_excel(
    week_start: str = Query(..., description="Monday of the week YYYY-MM-DD"),
    lead: Optional[str] = Query(None, description="Filter by lead"),
):
    """
    Export the roster schedules to an Excel file.
    Returns all agents with their shifts for each day of the week.
    """
    import pandas as pd
    
    try:
        start = date.fromisoformat(week_start)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {e}")
    
    # Get roster matrix data
    matrix = get_roster_matrix(start, lead, None, "active")
    
    DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    week_dates = matrix.get("week_dates", {})
    
    # Build data for Excel
    data = []
    for agent in matrix.get("agents", []):
        row = {
            "Agent ID": agent["agent_id"],
            "Name": agent["full_name"],
            "Lead": agent.get("lead", ""),
        }
        
        for day in DAYS:
            day_data = agent.get("days", {}).get(day, {})
            shift_code = day_data.get("shift_code", "")
            row[f"{day} ({week_dates.get(day, '')})"] = shift_code
        
        data.append(row)
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Create Excel file
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Schedules")
        
        # Auto-adjust column widths
        worksheet = writer.sheets["Schedules"]
        for idx, col in enumerate(df.columns):
            max_length = max(
                df[col].astype(str).map(len).max() if len(df) > 0 else 0,
                len(str(col))
            ) + 2
            worksheet.column_dimensions[chr(65 + idx)].width = min(max_length, 25)
    
    buf.seek(0)
    
    filename = f"schedules_{week_start}.xlsx"
    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"'
    }
    
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers
    )

from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List
import re
import uuid


class JustifyBody(BaseModel):
    agent_id: str = Field(..., description="Agent ID")
    date: str = Field(..., description="YYYY-MM-DD")
    type: str = Field(..., pattern=r"^(A|J|V|U|D|H|C|ML)$")
    note: str = Field(default="", description="Optional note")
    lead: str = Field(default="", description="Optional lead")


# ============ Multi-Schedule Models ============

class TimeRange(BaseModel):
    """A time range with optional label, shift code and midnight crossing support."""
    start_time: str = Field(..., description="Start time in HH:MM format (24h)")
    end_time: str = Field(..., description="End time in HH:MM format (24h)")
    crosses_midnight: bool = Field(default=False, description="True if shift crosses midnight")
    label: Optional[str] = Field(default=None, description="Optional label (e.g., 'morning', 'night')")
    shift_code: Optional[str] = Field(default=None, description="Shift code (e.g., S1, S2). Auto-assigned if matches template.")
    
    @field_validator('start_time', 'end_time')
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        """Validate HH:MM format."""
        if not re.match(r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$', v):
            raise ValueError(f"Invalid time format: {v}. Use HH:MM (24h)")
        # Normalize to HH:MM
        parts = v.split(':')
        return f"{int(parts[0]):02d}:{parts[1]}"


class DaySchedule(BaseModel):
    """Schedule for a single day with multiple time ranges."""
    day: str = Field(..., description="Day of week: mon, tue, wed, thu, fri, sat, sun")
    ranges: List[TimeRange] = Field(default_factory=list, description="List of time ranges")
    
    @field_validator('day')
    @classmethod
    def validate_day(cls, v: str) -> str:
        valid_days = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
        v_lower = v.lower()
        if v_lower not in valid_days:
            raise ValueError(f"Invalid day: {v}. Use: {', '.join(valid_days)}")
        return v_lower


class WeeklySchedule(BaseModel):
    """Complete weekly schedule with multiple ranges per day."""
    mon: List[TimeRange] = Field(default_factory=list)
    tue: List[TimeRange] = Field(default_factory=list)
    wed: List[TimeRange] = Field(default_factory=list)
    thu: List[TimeRange] = Field(default_factory=list)
    fri: List[TimeRange] = Field(default_factory=list)
    sat: List[TimeRange] = Field(default_factory=list)
    sun: List[TimeRange] = Field(default_factory=list)
    
    def get_day(self, day: str) -> List[TimeRange]:
        """Get ranges for a specific day."""
        return getattr(self, day.lower(), [])
    
    def set_day(self, day: str, ranges: List[TimeRange]):
        """Set ranges for a specific day."""
        setattr(self, day.lower(), ranges)


class AgentCreate(BaseModel):
    """Request body for creating a new agent."""
    name: str = Field(..., min_length=1, description="Agent name (unique)")
    id: Optional[str] = Field(default=None, description="Optional UUID, auto-generated if not provided")
    lead: Optional[str] = Field(default="", description="Lead/supervisor name")
    
    @model_validator(mode='before')
    @classmethod
    def generate_id_if_missing(cls, data):
        if isinstance(data, dict):
            if not data.get('id'):
                data['id'] = str(uuid.uuid4())
        return data


class AgentResponse(BaseModel):
    """Agent details in response."""
    id: str
    name: str
    lead: str
    created_at: str


class AgentScheduleUpdate(BaseModel):
    """Request body for updating an agent's weekly schedule."""
    agent_id: str = Field(..., description="Agent ID")
    schedule: WeeklySchedule = Field(..., description="Weekly schedule with multiple ranges per day")
    effective_date: Optional[str] = Field(default=None, description="When this schedule takes effect (YYYY-MM-DD)")
    note: Optional[str] = Field(default="", description="Optional note")


class ScheduleRangeValidation(BaseModel):
    """Validation result for schedule ranges."""
    valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


# ============ Shift Template Models ============

class ShiftTemplateCreate(BaseModel):
    """Request body for creating/updating a shift template."""
    code: str = Field(..., min_length=1, max_length=10, description="Shift code (e.g., S1, S2, NIGHT)")
    name: str = Field(..., min_length=1, description="Human-readable name (e.g., 'Morning Shift')")
    start_time: str = Field(..., description="Start time in HH:MM format")
    end_time: str = Field(..., description="End time in HH:MM format")
    crosses_midnight: bool = Field(default=False, description="True if shift crosses midnight")
    color: Optional[str] = Field(default="#1976d2", description="Color hex code for UI display")
    
    @field_validator('start_time', 'end_time')
    @classmethod
    def validate_time_format(cls, v: str) -> str:
        if not re.match(r'^([01]?[0-9]|2[0-3]):[0-5][0-9]$', v):
            raise ValueError(f"Invalid time format: {v}. Use HH:MM (24h)")
        parts = v.split(':')
        return f"{int(parts[0]):02d}:{parts[1]}"
    
    @field_validator('code')
    @classmethod
    def validate_code(cls, v: str) -> str:
        # Allow alphanumeric codes
        if not re.match(r'^[A-Za-z0-9_-]+$', v):
            raise ValueError("Shift code must be alphanumeric (letters, numbers, _ or -)")
        return v.upper()


class ShiftTemplateResponse(BaseModel):
    """Shift template response."""
    id: int
    code: str
    name: str
    start_time: str
    end_time: str
    crosses_midnight: bool
    color: str
    created_at: str


class ScheduleSaveRequest(BaseModel):
    """Request body for saving agent schedules."""
    schedule: 'WeeklySchedule' = Field(..., description="Weekly schedule with time ranges per day")
    effective_date: Optional[str] = Field(default=None, description="When this schedule takes effect (YYYY-MM-DD)")
    note: Optional[str] = Field(default="", description="Optional note")


class ScheduleValidationResult(BaseModel):
    """Result of schedule validation."""
    valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


# ============ Legacy Models (kept for backward compatibility) ============

class WeeklyPattern(BaseModel):
    """Weekly pattern for new schedule overrides (legacy)."""
    mon_enabled: bool = False
    mon_start: Optional[str] = None
    mon_end: Optional[str] = None
    tue_enabled: bool = False
    tue_start: Optional[str] = None
    tue_end: Optional[str] = None
    wed_enabled: bool = False
    wed_start: Optional[str] = None
    wed_end: Optional[str] = None
    thu_enabled: bool = False
    thu_start: Optional[str] = None
    thu_end: Optional[str] = None
    fri_enabled: bool = False
    fri_start: Optional[str] = None
    fri_end: Optional[str] = None
    sat_enabled: bool = False
    sat_start: Optional[str] = None
    sat_end: Optional[str] = None
    sun_enabled: bool = False
    sun_start: Optional[str] = None
    sun_end: Optional[str] = None


class ScheduleOverrideBody(BaseModel):
    """Body for creating schedule overrides (legacy)."""
    agent_ids: List[str] = Field(..., description="List of agent IDs to apply override to")
    lead: str = Field(..., min_length=1, description="Lead name (required)")
    effective_date: str = Field(..., description="YYYY-MM-DD - Effective date for the new schedule")
    weekly_pattern: WeeklyPattern = Field(..., description="Weekly pattern for new schedule")
    note: str = Field(default="", description="Optional note/reason")
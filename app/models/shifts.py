"""
Shift Management Models for Roster Matrix View
Implements effective dating pattern for shift assignments with history preservation.
"""
from datetime import date, time, datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from enum import Enum


class DayOfWeek(str, Enum):
    """Days of the week for shift assignments."""
    MON = "Mon"
    TUE = "Tue"
    WED = "Wed"
    THU = "Thu"
    FRI = "Fri"
    SAT = "Sat"
    SUN = "Sun"

    @classmethod
    def from_date(cls, d: date) -> "DayOfWeek":
        """Get day of week from a date."""
        days = [cls.MON, cls.TUE, cls.WED, cls.THU, cls.FRI, cls.SAT, cls.SUN]
        return days[d.weekday()]

    @classmethod
    def all_days(cls) -> List["DayOfWeek"]:
        return [cls.MON, cls.TUE, cls.WED, cls.THU, cls.FRI, cls.SAT, cls.SUN]


# Shift Template Catalog - Standard shift codes with times
SHIFT_CATALOG: Dict[str, Dict[str, Any]] = {
    "S1":  {"start": "22:00", "end": "05:30", "crosses_midnight": True,  "color": "#8B5CF6", "label": "S1 (22:00–05:30)"},
    "S2":  {"start": "04:00", "end": "12:00", "crosses_midnight": False, "color": "#3B82F6", "label": "S2 (04:00–12:00)"},
    "S3":  {"start": "05:00", "end": "14:00", "crosses_midnight": False, "color": "#06B6D4", "label": "S3 (05:00–14:00)"},
    "S4":  {"start": "06:00", "end": "15:00", "crosses_midnight": False, "color": "#10B981", "label": "S4 (06:00–15:00)"},
    "S5":  {"start": "07:00", "end": "16:00", "crosses_midnight": False, "color": "#22C55E", "label": "S5 (07:00–16:00)"},
    "S6":  {"start": "09:00", "end": "17:00", "crosses_midnight": False, "color": "#84CC16", "label": "S6 (09:00–17:00)"},
    "S7":  {"start": "09:00", "end": "18:00", "crosses_midnight": False, "color": "#EAB308", "label": "S7 (09:00–18:00)"},
    "S8":  {"start": "14:30", "end": "22:00", "crosses_midnight": False, "color": "#F97316", "label": "S8 (14:30–22:00)"},
    "S9":  {"start": "11:00", "end": "20:00", "crosses_midnight": False, "color": "#EF4444", "label": "S9 (11:00–20:00)"},
    "S10": {"start": "21:30", "end": "05:00", "crosses_midnight": True,  "color": "#EC4899", "label": "S10 (21:30–05:00)"},
    "OFF": {"start": None,    "end": None,    "crosses_midnight": False, "color": "#6B7280", "label": "OFF (Day Off)"},
}

# Colorblind-friendly alternative palette (Deuteranopia-safe)
SHIFT_CATALOG_COLORBLIND: Dict[str, str] = {
    "S1":  "#785EF0",  # Purple
    "S2":  "#648FFF",  # Blue
    "S3":  "#00B4D8",  # Cyan
    "S4":  "#2EC4B6",  # Teal
    "S5":  "#38A3A5",  # Dark Teal
    "S6":  "#FFB000",  # Amber
    "S7":  "#FE6100",  # Orange
    "S8":  "#DC267F",  # Magenta
    "S9":  "#DC2626",  # Red
    "S10": "#9D4EDD",  # Violet
    "OFF": "#6B7280",  # Gray
}


@dataclass
class ShiftTemplate:
    """
    Represents a shift type in the catalog.
    These are predefined shifts (S1-S10, OFF) with their time ranges.
    """
    shift_code: str
    start_time: Optional[str]  # HH:MM format
    end_time: Optional[str]    # HH:MM format
    crosses_midnight: bool = False
    color: str = "#6B7280"
    label: str = ""

    @classmethod
    def from_code(cls, code: str) -> "ShiftTemplate":
        """Create a ShiftTemplate from a shift code."""
        if code not in SHIFT_CATALOG:
            raise ValueError(f"Unknown shift code: {code}")
        info = SHIFT_CATALOG[code]
        return cls(
            shift_code=code,
            start_time=info["start"],
            end_time=info["end"],
            crosses_midnight=info["crosses_midnight"],
            color=info["color"],
            label=info["label"]
        )

    @classmethod
    def all_shifts(cls) -> List["ShiftTemplate"]:
        """Get all available shift templates."""
        return [cls.from_code(code) for code in SHIFT_CATALOG.keys()]


@dataclass
class AgentShiftAssignment:
    """
    Represents a shift assignment for an agent on a specific day of the week.
    Uses effective dating to preserve history.
    
    Business Rules:
    - No overlapping effective date ranges for same agent_id + day_of_week
    - effective_end is NULL for current/active assignments
    - When updating, close previous range (effective_end = new_effective_start - 1 day)
    """
    id: Optional[int] = None
    agent_id: str = ""
    day_of_week: str = ""  # Mon, Tue, Wed, Thu, Fri, Sat, Sun
    shift_code: str = ""   # S1..S10 or OFF
    effective_start: Optional[date] = None
    effective_end: Optional[date] = None  # NULL means currently active
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    def is_active_on(self, target_date: date) -> bool:
        """Check if this assignment is active on a specific date."""
        if self.effective_start is None:
            return False
        if self.effective_start > target_date:
            return False
        if self.effective_end is not None and self.effective_end < target_date:
            return False
        return True


@dataclass
class AgentRosterEntry:
    """
    Represents an agent in the roster with their employment dates.
    Used for tracking when agents join/leave the roster.
    """
    id: Optional[int] = None
    agent_id: str = ""
    full_name: str = ""
    lead: str = ""
    effective_start: Optional[date] = None  # When they joined
    effective_end: Optional[date] = None    # When they left (NULL = active)
    is_active: bool = True
    created_at: Optional[datetime] = None

    def is_active_on(self, target_date: date) -> bool:
        """Check if agent is active on the roster for a specific date."""
        if self.effective_start is None:
            return False
        if self.effective_start > target_date:
            return False
        if self.effective_end is not None and self.effective_end < target_date:
            return False
        return True


@dataclass
class RosterMatrixCell:
    """
    Represents a single cell in the roster matrix view.
    Contains the shift code and metadata for tooltip display.
    """
    agent_id: str
    date: date
    day_of_week: str
    shift_code: str
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    effective_start: Optional[date] = None
    effective_end: Optional[date] = None
    color: str = "#6B7280"
    crosses_midnight: bool = False


@dataclass
class RosterMatrixRow:
    """
    Represents a row in the roster matrix (one agent's weekly schedule).
    """
    agent_id: str
    full_name: str
    lead: str
    cells: Dict[str, RosterMatrixCell] = field(default_factory=dict)  # day_of_week -> cell


def map_legacy_shift_to_code(expected_start: str, expected_end: str) -> str:
    """
    Map legacy schedule format (expected_start/expected_end) to shift code (S1-S10).
    Used for migration from old schedule.csv format.
    """
    if not expected_start or not expected_end:
        return "OFF"
    
    # Normalize time format (handle H:MM vs HH:MM)
    def normalize_time(t: str) -> str:
        if not t:
            return ""
        t = t.strip()
        if len(t) == 4 and t[1] == ':':  # H:MM
            t = '0' + t
        return t[:5]  # Take HH:MM
    
    start = normalize_time(expected_start)
    end = normalize_time(expected_end)
    
    # Direct mapping based on times
    time_to_code = {
        ("22:00", "05:30"): "S1",
        ("04:00", "12:00"): "S2",
        ("05:00", "14:00"): "S3",
        ("06:00", "15:00"): "S4",
        ("07:00", "16:00"): "S5",
        ("09:00", "17:00"): "S6",
        ("09:00", "18:00"): "S7",
        ("14:30", "22:00"): "S8",
        ("11:00", "20:00"): "S9",
        ("21:30", "05:00"): "S10",
    }
    
    # Try exact match
    key = (start, end)
    if key in time_to_code:
        return time_to_code[key]
    
    # Fuzzy match based on start time
    start_hour = int(start.split(':')[0]) if start else -1
    
    if start_hour >= 21 or start_hour < 4:
        if start_hour >= 22:
            return "S1"
        return "S10"
    elif start_hour >= 4 and start_hour < 5:
        return "S2"
    elif start_hour >= 5 and start_hour < 6:
        return "S3"
    elif start_hour >= 6 and start_hour < 7:
        return "S4"
    elif start_hour >= 7 and start_hour < 9:
        return "S5"
    elif start_hour >= 9 and start_hour < 11:
        return "S7"  # Default to longer shift
    elif start_hour >= 11 and start_hour < 14:
        return "S9"
    elif start_hour >= 14:
        return "S8"
    
    return "S4"  # Default fallback


def parse_working_days(working_days_str: str) -> List[str]:
    """
    Parse working days string from CSV format.
    Input: "Mon, Tue, Wed, Thu, Fri" or "Mon,Tue,Wed"
    Output: ["Mon", "Tue", "Wed", "Thu", "Fri"]
    """
    if not working_days_str:
        return []
    
    days = []
    for part in working_days_str.split(','):
        day = part.strip()
        # Normalize day names
        day_map = {
            'monday': 'Mon', 'mon': 'Mon', 'm': 'Mon',
            'tuesday': 'Tue', 'tue': 'Tue', 't': 'Tue',
            'wednesday': 'Wed', 'wed': 'Wed', 'w': 'Wed',
            'thursday': 'Thu', 'thu': 'Thu', 'th': 'Thu',
            'friday': 'Fri', 'fri': 'Fri', 'f': 'Fri',
            'saturday': 'Sat', 'sat': 'Sat', 's': 'Sat',
            'sunday': 'Sun', 'sun': 'Sun', 'su': 'Sun',
        }
        normalized = day_map.get(day.lower(), day.capitalize()[:3])
        if normalized in ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']:
            days.append(normalized)
    
    return days

from datetime import date, time, datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple


def parse_hhmm_or_hhmmss(s: str) -> Optional[time]:
    """Convierte 'HH:MM' o 'HH:MM:SS' a objeto time; devuelve None si está vacío o inválido."""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            pass
    return None

def weekday_token(d: date) -> str:
    """Regresa 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'."""
    return d.strftime("%a")

def weekday_short(d: date) -> str:
    """Regresa 'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun' (lowercase)."""
    return d.strftime("%a").lower()

def parse_days_list(s: str) -> list[str]:
    """Convierte 'Mon, Tue, Wed, Thu, Fri' -> ['Mon','Tue','Wed','Thu','Fri']"""
    return [x.strip() for x in str(s).split(",") if str(x).strip()]


# ============ Multi-Schedule Utilities ============

def check_time_in_schedule_ranges(
    check_time: time,
    day_ranges: List[Dict[str, Any]],
    prev_day_ranges: List[Dict[str, Any]] = None
) -> Tuple[bool, Optional[Dict[str, Any]], int]:
    """
    Check if a time falls within any of the schedule ranges.
    Also checks previous day's midnight-crossing shifts.
    
    Returns: (is_valid, matched_range, delay_minutes)
    - is_valid: True if time is within a valid range
    - matched_range: The range that matched (or closest range for delay calc)
    - delay_minutes: Minutes late if outside range, 0 if on time
    """
    if not day_ranges and not prev_day_ranges:
        return False, None, 0
    
    check_dt = datetime.combine(date.today(), check_time)
    
    # Check current day ranges
    for r in (day_ranges or []):
        start_t = parse_hhmm_or_hhmmss(r["start_time"])
        end_t = parse_hhmm_or_hhmmss(r["end_time"])
        if not start_t or not end_t:
            continue
        
        crosses_midnight = r.get("crosses_midnight", False)
        
        if crosses_midnight:
            # For midnight-crossing shift on same day: start_time to 23:59
            if check_time >= start_t:
                return True, r, 0
        else:
            # Normal range: start_time to end_time
            if start_t <= check_time <= end_t:
                return True, r, 0
    
    # Check previous day's midnight-crossing ranges (for early morning check-ins)
    for r in (prev_day_ranges or []):
        if not r.get("crosses_midnight", False):
            continue
        end_t = parse_hhmm_or_hhmmss(r["end_time"])
        if not end_t:
            continue
        # For midnight-crossing shift from previous day: 00:00 to end_time
        if check_time <= end_t:
            return True, r, 0
    
    # Not in any range - calculate delay to closest range
    min_delay = None
    closest_range = None
    
    for r in (day_ranges or []):
        start_t = parse_hhmm_or_hhmmss(r["start_time"])
        if not start_t:
            continue
        
        start_dt = datetime.combine(date.today(), start_t)
        
        if check_dt > start_dt:
            # Check-in is after start time (late)
            delay = int((check_dt - start_dt).total_seconds() // 60)
            if min_delay is None or delay < min_delay:
                min_delay = delay
                closest_range = r
    
    return False, closest_range, min_delay or 0


def get_expected_intervals_for_day(
    day_ranges: List[Dict[str, Any]],
    target_date: date
) -> List[Tuple[datetime, datetime, str, bool]]:
    """
    Get all expected intervals for a day from multi-schedule ranges.
    
    Returns list of tuples: (start_dt, end_dt, shift_code, crosses_midnight)
    """
    intervals = []
    
    for r in day_ranges:
        start_t = parse_hhmm_or_hhmmss(r["start_time"])
        end_t = parse_hhmm_or_hhmmss(r["end_time"])
        if not start_t or not end_t:
            continue
        
        start_dt = datetime.combine(target_date, start_t)
        end_dt = datetime.combine(target_date, end_t)
        crosses_midnight = r.get("crosses_midnight", False)
        
        if crosses_midnight or end_dt <= start_dt:
            end_dt = end_dt + timedelta(days=1)
        
        shift_code = r.get("shift_code", "") or r.get("label", "")
        intervals.append((start_dt, end_dt, shift_code, crosses_midnight))
    
    return intervals


def find_best_matching_interval(
    check_in_time: time,
    intervals: List[Tuple[datetime, datetime, str, bool]]
) -> Tuple[Optional[Tuple[datetime, datetime, str, bool]], int]:
    """
    Find the interval that best matches a check-in time.
    Returns (matched_interval, delay_minutes).
    """
    if not intervals:
        return None, 0
    
    check_dt = datetime.combine(intervals[0][0].date(), check_in_time)
    
    # First, check if time falls within any interval
    for interval in intervals:
        start_dt, end_dt, _, _ = interval
        if start_dt <= check_dt <= end_dt:
            return interval, 0
    
    # Not in any interval - find the closest one (by start time)
    best_match = None
    min_delay = None
    
    for interval in intervals:
        start_dt, _, _, _ = interval
        if check_dt > start_dt:
            delay = int((check_dt - start_dt).total_seconds() // 60)
            if min_delay is None or delay < min_delay:
                min_delay = delay
                best_match = interval
    
    return best_match, min_delay or 0
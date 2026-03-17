"""
Schedule Provider for Attendance Module
Unified access to agent schedules from the Roster Database (agent_shift_assignments table).
Replaces direct CSV reading (schedule.csv) with BD-backed schedule queries.

Key Features:
- Single source of truth: Uses same data as /api/roster screen
- Converts shift codes (S1..S10, OFF) to time ranges
- Handles OFF, missing schedules, and midnight-crossing shifts
- Provides DataFrame interface compatible with legacy code
"""

from typing import Optional, Dict, Any, Tuple
from datetime import date, datetime, time
import pandas as pd

from ..shift_db import get_shifts_for_agents_bulk, get_all_roster_agents
from ..models.shifts import SHIFT_CATALOG, DayOfWeek
from ..utils import parse_hhmm_or_hhmmss


class ScheduleProvider:
    """
    Provides agent schedules from the Roster Database (agent_shift_assignments).
    Converts entre shift codes and time ranges.
    """

    @staticmethod
    def shift_code_to_time_range(shift_code: str) -> Optional[Tuple[time, time, bool]]:
        """
        Convert shift code (S1..S10, OFF) to (start_time, end_time, crosses_midnight).
        
        Args:
            shift_code: "S1", "S2", ..., "S10", "OFF"
        
        Returns:
            (start_time, end_time, crosses_midnight) or None if shift_code is OFF or invalid
        """
        if shift_code == "OFF" or shift_code not in SHIFT_CATALOG:
            return None
        
        info = SHIFT_CATALOG.get(shift_code, {})
        if not info.get("start") or not info.get("end"):
            return None
        
        start_t = parse_hhmm_or_hhmmss(info["start"])
        end_t = parse_hhmm_or_hhmmss(info["end"])
        crosses_midnight = info.get("crosses_midnight", False)
        
        return start_t, end_t, crosses_midnight


    @staticmethod
    def get_schedule_for_date(target_date: date) -> pd.DataFrame:
        """
        Get all agent schedules for a specific date.
        Uses a single bulk DB query for all agents instead of one per agent.
        """
        roster_agents = get_all_roster_agents()
        if not roster_agents:
            return pd.DataFrame()
        
        agent_ids = [a["agent_id"] for a in roster_agents]
        agent_info = {a["agent_id"]: a for a in roster_agents} #lookup by id

        # Single DB query for all agents for this date
        day_of_week = DayOfWeek.from_date(target_date).value
        bulk_shifts = get_shifts_for_agents_bulk(agent_ids, target_date, target_date)

        rows = []
        for agent_id in agent_ids:
            agent = agent_info[agent_id]
            shift_assignment = bulk_shifts.get((agent_id, day_of_week))

            if not shift_assignment:
                shift_code = "OFF"
                start_t = None
                end_t = None
                crosses_midnight = False
                color = "#6B7280"
                label = "No schedule"
            else:
                shift_code = shift_assignment.get("shift_code", "OFF")
                time_range = ScheduleProvider.shift_code_to_time_range(shift_code)
                if shift_code == "OFF" or time_range is None:
                    start_t = None
                    end_t = None
                    crosses_midnight = False
                else:
                    start_t, end_t, crosses_midnight = time_range
                color = shift_assignment.get("color", "#6B7280")
                label = shift_assignment.get("label", shift_code)
            
            expected_start_str = start_t.strftime("%H:%M") if start_t else None
            expected_end_str = end_t.strftime("%H:%M") if end_t else None
            is_night = bool(SHIFT_CATALOG.get(shift_code, {}).get("crosses_midnight", False)) if shift_code else False

            rows.append({
                "agent_id": agent_id,
                "name": agent["full_name"],
                "lead": agent.get("lead", ""),
                "Shift": shift_code,
                "expected_start": expected_start_str,
                "expected_end": expected_end_str,
                "expected_start_t": start_t,
                "expected_end_t": end_t,
                "is_night": is_night,
                "working_days": "",
                "days_off": "",
                "color": color,
                "shift_code": shift_code,
                "crosses_midnight": crosses_midnight,
            })

        df = pd.DataFrame(rows)
        column_order = [
            "agent_id", "name", "lead", "Shift", "expected_start", "expected_end",
            "expected_start_t", "expected_end_t", "is_night", "working_days",
            "days_off", "color", "shift_code", "crosses_midnight"
        ]
        return df[[col for col in column_order if col in df.columns]]
        

    @staticmethod
    def compare_actual_vs_expected(
        actual_start: Optional[datetime],
        actual_end: Optional[datetime],
        expected_start: Optional[datetime],
        expected_end: Optional[datetime],
        tolerance_minutes: int = 2
    ) -> Dict[str, Any]:
        """
        Compare actual work times against expected schedule.
        
        Args:
            actual_start: When agent actually started (or None)
            actual_end: When agent actually ended (or None)
            expected_start: Expected start time
            expected_end: Expected end time (optional)
            tolerance_minutes: Minutes of grace for delays
        
        Returns:
            Dict with:
            {
                "has_schedule": bool,
                "actual_start": datetime or None,
                "actual_end": datetime or None,
                "expected_start": datetime or None,
                "expected_end": datetime or None,
                "delay_minutes": int,
                "overtime_minutes": int,
                "status": "A", "D", or "U"  (On-time, Delayed, Unjustified)
            }
        """
        # No schedule means no expected_start (expected_end is optional)
        if not expected_start:
            return {
                "has_schedule": False,
                "actual_start": actual_start,
                "actual_end": actual_end,
                "expected_start": None,
                "expected_end": None,
                "delay_minutes": 0,
                "overtime_minutes": 0,
                "status": None  # No schedule defined
            }
        
        if not actual_start:
            # No check-in = unjustified (but we have a schedule)
            return {
                "has_schedule": True,
                "actual_start": None,
                "actual_end": actual_end,
                "expected_start": expected_start,
                "expected_end": expected_end,
                "delay_minutes": 0,
                "overtime_minutes": 0,
                "status": "U"  # Unjustified
            }
        
        # Calculate delay (can be negative if early)
        delay_delta = actual_start - expected_start
        delay_minutes = int(delay_delta.total_seconds() / 60)
        
        # Apply tolerance: if delay <= tolerance_minutes, clamp to 0 and mark as On-time
        if delay_minutes <= tolerance_minutes:
            delay_minutes = 0
            status = "A"  # On-time
        else:
            status = "D"  # Delayed
        
        # Calculate overtime only if expected_end is provided
        overtime_minutes = 0
        if expected_end and actual_end:
            overtime_delta = actual_end - expected_end
            if overtime_delta.total_seconds() > 0:
                overtime_minutes = int(overtime_delta.total_seconds() / 60)
        
        return {
            "has_schedule": True,
            "actual_start": actual_start,
            "actual_end": actual_end,
            "expected_start": expected_start,
            "expected_end": expected_end,
            "delay_minutes": max(delay_minutes, 0),  # Never negative in output
            "overtime_minutes": overtime_minutes,
            "status": status
        }


    @staticmethod
    def invalidate_cache():
        """
        No persistent in-memory cache exists in ScheduleProvider - each call
        queries the DB fresh. This method exists so roster.py can call it after
        shift assignment changes without raising AttributeError.
        """
        pass


    @staticmethod
    def get_schedule_cache_for_range(start_date: date, end_date: date) -> Dict[date, pd.DataFrame]:
        """
        Replaces 31 per-day DB calls with a single bulk query.
        Returns {date: DataFrame} for the entire data range.
        """
        from ..shift_db import get_all_roster_agents, get_shifts_for_date_range_bulk
        from datetime import timedelta

        roster_agents = get_all_roster_agents()
        if not roster_agents:
            return {}
        
        agent_ids = [a["agent_id"] for a in roster_agents]
        agent_info = {a["agent_id"]: a for a in roster_agents}

        # One DB query for all agents, all days, entire date range

        all_shifts = get_shifts_for_date_range_bulk(agent_ids, start_date, end_date)

        result = {}
        cur = start_date
        while cur <= end_date:
            day_of_week = DayOfWeek.from_date(cur).value
            rows = []
            for agent_id in agent_ids:
                agent = agent_info[agent_id]
                #Find effective assignment for this specific date (list is newest-first)
                assignments = all_shifts.get((agent_id, day_of_week), [])
                shift_code = "OFF"
                color = "#6B7280"
                label = "No schedule"
                for a in assignments:
                    eff_start = a["effective_start"]
                    eff_end = a["effective_end"]
                    if isinstance(eff_start, str):
                        eff_start = date.fromisoformat(eff_start)
                    if eff_end and isinstance(eff_end, str):
                        eff_end = date.fromisoformat(eff_end)
                    if eff_start <= cur and (eff_end is None or eff_end >= cur):
                        shift_code = a["shift_code"]
                        break
                
                time_range = ScheduleProvider.shift_code_to_time_range(shift_code)
                if time_range:
                    start_t, end_t, crosses_midnight = time_range
                else:
                    start_t = end_t = None
                    crosses_midnight = False
                
                rows.append({
                    "agent_id": agent_id,
                    "name": agent["full_name"],
                    "lead": agent.get("lead", ""),
                    "Shift": shift_code,
                    "expected_start": start_t.strftime("%H:%M") if start_t else None,
                    "expected_end": end_t.strftime("%H:%M") if end_t else None,
                    "expected_start_t": start_t,
                    "expected_end_t": end_t,
                    "is_night": crosses_midnight,
                    "working_days": "",
                    "days_off": "",
                    "color": color,
                    "shift_code": shift_code,
                    "crosses_midnight": crosses_midnight,
                })
        
            result[cur] = pd.DataFrame(rows)
            cur += timedelta(days=1)
        
        return result
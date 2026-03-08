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
    def get_schedule_for_agent_day(
        agent_id: str,
        day_of_week: str,
        target_date: date
    ) -> Optional[Dict[str, Any]]:
        """
        Get schedule for an agent on a specific day.
        
        Args:
            agent_id: Agent ID
            day_of_week: "Mon", "Tue", ..., "Sun" (from DayOfWeek enum)
            target_date: The date to look up
        
        Returns:
            Dict with:
            {
                "agent_id": str,
                "day_of_week": str,
                "shift_code": str,
                "start_time": Optional[time],
                "end_time": Optional[time],
                "crosses_midnight": bool,
                "color": str,
                "label": str,
                "has_schedule": bool  # False if OFF or missing
            }
            Or None if agent not found
        """
        # Query the database
        shift_assignment = get_shifts_for_agents_bulk(agent_id, day_of_week, target_date)
        
        if not shift_assignment:
            # No shift found for this agent/day
            return {
                "agent_id": agent_id,
                "day_of_week": day_of_week,
                "shift_code": None,
                "start_time": None,
                "end_time": None,
                "crosses_midnight": False,
                "color": "#6B7280",
                "label": "No schedule",
                "has_schedule": False
            }
        
        shift_code = shift_assignment.get("shift_code", "OFF")
        time_range = ScheduleProvider.shift_code_to_time_range(shift_code)
        
        if shift_code == "OFF" or time_range is None:
            # Day off
            return {
                "agent_id": agent_id,
                "day_of_week": day_of_week,
                "shift_code": "OFF",
                "start_time": None,
                "end_time": None,
                "crosses_midnight": False,
                "color": shift_assignment.get("color", "#6B7280"),
                "label": "OFF",
                "has_schedule": False
            }
        
        start_t, end_t, crosses_midnight = time_range
        
        return {
            "agent_id": agent_id,
            "day_of_week": day_of_week,
            "shift_code": shift_code,
            "start_time": start_t,
            "end_time": end_t,
            "crosses_midnight": crosses_midnight,
            "color": shift_assignment.get("color", "#6B7280"),
            "label": shift_assignment.get("label", shift_code),
            "has_schedule": True
        }

    @staticmethod
    def get_schedule_for_date(target_date: date) -> pd.DataFrame:
        """
        Get all agent schedules for a specific date.
        Returns a DataFrame compatible with the legacy SCHEDULE_DF format.
        
        Columns:
        - agent_id
        - name
        - lead
        - Shift (shift_code, like "S1", "S10", "OFF")
        - expected_start (HH:MM string, or None if OFF)
        - expected_end (HH:MM string, or None if OFF)
        - expected_start_t (time object, or None)
        - expected_end_t (time object, or None)
        - is_night (boolean)
        - working_days (placeholder for compatibility)
        - days_off (placeholder for compatibility)
        
        Args:
            target_date: The date to look up schedules for
        
        Returns:
            pd.DataFrame with all agent schedules for that date
        """
        # Get all roster agents
        roster_agents = get_all_roster_agents()
        
        rows = []
        for agent in roster_agents:
            agent_id = agent["agent_id"]
            name = agent["full_name"]
            lead = agent.get("lead", "")
            
            # Get day of week
            day_of_week = DayOfWeek.from_date(target_date).value
            
            # Get schedule for this day
            schedule = ScheduleProvider.get_schedule_for_agent_day(
                agent_id, day_of_week, target_date
            )
            
            if not schedule:
                # Agent not found, skip
                continue
            
            shift_code = schedule.get("shift_code", "OFF")
            start_t = schedule.get("start_time")
            end_t = schedule.get("end_time")
            crosses_midnight = schedule.get("crosses_midnight", False)
            
            # Format times as strings (HH:MM) for compatibility
            expected_start_str = start_t.strftime("%H:%M") if start_t else None
            expected_end_str = end_t.strftime("%H:%M") if end_t else None
            
            # Determine if night shift (S1, S10, or "Night" string)
            is_night = False
            if shift_code and isinstance(shift_code, str):
                is_night = shift_code.lower() == "night" or (shift_code in ["S1", "S10"])
            
            row = {
                "agent_id": agent_id,
                "name": name,
                "lead": lead,
                "Shift": shift_code,
                "expected_start": expected_start_str,
                "expected_end": expected_end_str,
                "expected_start_t": start_t,
                "expected_end_t": end_t,
                "is_night": is_night,
                "working_days": "",  # Placeholder
                "days_off": "",     # Placeholder
                "color": schedule.get("color", "#6B7280"),
                "shift_code": shift_code,
                "crosses_midnight": crosses_midnight,
            }
            rows.append(row)
        
        df = pd.DataFrame(rows) if rows else pd.DataFrame()
        
        # Ensure column order for compatibility
        if not df.empty:
            column_order = [
                "agent_id", "name", "lead", "Shift", "expected_start", "expected_end",
                "expected_start_t", "expected_end_t", "is_night", "working_days",
                "days_off", "color", "shift_code", "crosses_midnight"
            ]
            df = df[[col for col in column_order if col in df.columns]]
        
        return df

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

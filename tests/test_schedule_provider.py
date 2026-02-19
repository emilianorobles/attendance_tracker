"""
Unit tests for Schedule Provider and Attendance logic.
Tests cover shift code conversion, schedule comparison, and edge cases.
"""

import pytest
from datetime import date, datetime, time
from app.providers.schedule_provider import ScheduleProvider
from app.models.shifts import SHIFT_CATALOG


class TestShiftCodeToTimeRange:
    """Test conversion of shift codes to time ranges."""
    
    def test_shift_code_s1(self):
        """Test S1 shift (22:00-05:30, night shift crossing midnight)."""
        result = ScheduleProvider.shift_code_to_time_range("S1")
        assert result is not None
        start_t, end_t, crosses_midnight = result
        assert start_t == time(22, 0)
        assert end_t == time(5, 30)
        assert crosses_midnight is True
    
    def test_shift_code_s4(self):
        """Test S4 shift (06:00-15:00, normal day shift)."""
        result = ScheduleProvider.shift_code_to_time_range("S4")
        assert result is not None
        start_t, end_t, crosses_midnight = result
        assert start_t == time(6, 0)
        assert end_t == time(15, 0)
        assert crosses_midnight is False
    
    def test_shift_code_s8(self):
        """Test S8 shift (14:30-22:00, afternoon shift)."""
        result = ScheduleProvider.shift_code_to_time_range("S8")
        assert result is not None
        start_t, end_t, crosses_midnight = result
        assert start_t == time(14, 30)
        assert end_t == time(22, 0)
        assert crosses_midnight is False
    
    def test_shift_code_s10(self):
        """Test S10 shift (21:30-05:00, night shift crossing midnight)."""
        result = ScheduleProvider.shift_code_to_time_range("S10")
        assert result is not None
        start_t, end_t, crosses_midnight = result
        assert start_t == time(21, 30)
        assert end_t == time(5, 0)
        assert crosses_midnight is True
    
    def test_shift_code_off(self):
        """Test OFF (day off) returns None."""
        result = ScheduleProvider.shift_code_to_time_range("OFF")
        assert result is None
    
    def test_shift_code_invalid(self):
        """Test invalid shift code returns None."""
        result = ScheduleProvider.shift_code_to_time_range("INVALID")
        assert result is None
    
    def test_all_shift_codes_valid(self):
        """Test that all shift codes in SHIFT_CATALOG have valid times or are OFF."""
        for shift_code in SHIFT_CATALOG.keys():
            result = ScheduleProvider.shift_code_to_time_range(shift_code)
            # Either should return a tuple (start, end, crosses_midnight) or None for OFF
            if shift_code == "OFF":
                assert result is None
            elif result is not None:
                assert isinstance(result, tuple)
                assert len(result) == 3
                assert isinstance(result[0], time)
                assert isinstance(result[1], time)
                assert isinstance(result[2], bool)


class TestCompareActualVsExpected:
    """Test comparison of actual work times vs expected schedule."""
    
    def test_no_schedule(self):
        """Test when there is no expected schedule (OFF day)."""
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=datetime(2026, 1, 5, 9, 0),
            actual_end=datetime(2026, 1, 5, 17, 0),
            expected_start=None,
            expected_end=None
        )
        assert result["has_schedule"] is False
        assert result["status"] is None
        assert result["delay_minutes"] == 0
    
    def test_no_checkin(self):
        """Test when agent didn't check in (unjustified)."""
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=None,
            actual_end=None,
            expected_start=datetime(2026, 1, 5, 9, 0),
            expected_end=datetime(2026, 1, 5, 17, 0)
        )
        assert result["has_schedule"] is True
        assert result["status"] == "U"  # Unjustified
        assert result["delay_minutes"] == 0
    
    def test_on_time_checkin(self):
        """Test when agent checks in on time (within tolerance)."""
        expected_start = datetime(2026, 1, 5, 9, 0)
        actual_start = datetime(2026, 1, 5, 9, 1)  # 1 minute late (within 2-min tolerance)
        expected_end = datetime(2026, 1, 5, 17, 0)
        
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=actual_start,
            actual_end=datetime(2026, 1, 5, 17, 5),
            expected_start=expected_start,
            expected_end=expected_end,
            tolerance_minutes=2
        )
        assert result["has_schedule"] is True
        assert result["status"] == "A"  # On time
        assert result["delay_minutes"] == 0
    
    def test_delayed_checkin(self):
        """Test when agent is late (beyond tolerance)."""
        expected_start = datetime(2026, 1, 5, 9, 0)
        actual_start = datetime(2026, 1, 5, 9, 5)  # 5 minutes late (beyond 2-min tolerance)
        expected_end = datetime(2026, 1, 5, 17, 0)
        
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=actual_start,
            actual_end=datetime(2026, 1, 5, 17, 0),
            expected_start=expected_start,
            expected_end=expected_end,
            tolerance_minutes=2
        )
        assert result["has_schedule"] is True
        assert result["status"] == "D"  # Delayed
        assert result["delay_minutes"] == 5
    
    def test_overtime(self):
        """Test when agent works overtime."""
        expected_start = datetime(2026, 1, 5, 9, 0)
        expected_end = datetime(2026, 1, 5, 17, 0)
        
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=datetime(2026, 1, 5, 9, 0),
            actual_end=datetime(2026, 1, 5, 17, 30),  # 30 min overtime
            expected_start=expected_start,
            expected_end=expected_end
        )
        assert result["has_schedule"] is True
        assert result["status"] == "A"  # On time
        assert result["overtime_minutes"] == 30
    
    def test_night_shift_crossing_midnight(self):
        """Test night shift that crosses midnight (e.g., S1: 22:00-05:30)."""
        # Simulate night shift: starts at 22:00 on day 4, ends at 05:30 on day 5
        expected_start = datetime(2026, 1, 4, 22, 0)
        expected_end = datetime(2026, 1, 5, 5, 30)  # Next day
        
        # Agent checks in at 21:58 (2 min early)
        actual_start = datetime(2026, 1, 4, 21, 58)
        actual_end = datetime(2026, 1, 5, 5, 30)
        
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=actual_start,
            actual_end=actual_end,
            expected_start=expected_start,
            expected_end=expected_end
        )
        assert result["has_schedule"] is True
        assert result["status"] == "A"  # On time (2 min early within tolerance)
        assert result["delay_minutes"] == 0
        assert result["overtime_minutes"] == 0
    
    def test_night_shift_late(self):
        """Test night shift with late check-in."""
        expected_start = datetime(2026, 1, 4, 22, 0)
        expected_end = datetime(2026, 1, 5, 5, 30)
        
        # Agent checks in at 22:10 (10 min late)
        actual_start = datetime(2026, 1, 4, 22, 10)
        actual_end = datetime(2026, 1, 5, 5, 30)
        
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=actual_start,
            actual_end=actual_end,
            expected_start=expected_start,
            expected_end=expected_end,
            tolerance_minutes=2
        )
        assert result["has_schedule"] is True
        assert result["status"] == "D"  # Delayed
        assert result["delay_minutes"] == 10


class TestScheduleProviderIntegration:
    """Integration tests for ScheduleProvider methods."""
    
    def test_shift_catalog_completeness(self):
        """Verify SHIFT_CATALOG has all expected codes."""
        expected_codes = {"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8", "S9", "S10", "OFF"}
        actual_codes = set(SHIFT_CATALOG.keys())
        assert actual_codes == expected_codes
    
    def test_shift_catalog_structure(self):
        """Verify each SHIFT_CATALOG entry has required fields."""
        for code, info in SHIFT_CATALOG.items():
            if code != "OFF":
                assert "start" in info
                assert "end" in info
                assert info["start"] is not None
                assert info["end"] is not None
            assert "crosses_midnight" in info
            assert isinstance(info["crosses_midnight"], bool)
            assert "color" in info
            assert "label" in info


class TestEdgeCases:
    """Test edge cases and error conditions."""
    
    def test_tolerance_boundary(self):
        """Test that tolerance is applied correctly at boundary."""
        expected_start = datetime(2026, 1, 5, 9, 0)
        tolerance = 2
        
        # Exactly at tolerance (should still be A)
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=datetime(2026, 1, 5, 9, 2),
            actual_end=None,
            expected_start=expected_start,
            expected_end=None,
            tolerance_minutes=tolerance
        )
        assert result["status"] == "A"
        assert result["delay_minutes"] == 0
        
        # One minute beyond tolerance (should be D)
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=datetime(2026, 1, 5, 9, 3),
            actual_end=None,
            expected_start=expected_start,
            expected_end=None,
            tolerance_minutes=tolerance
        )
        assert result["status"] == "D"
        assert result["delay_minutes"] == 3
    
    def test_early_checkin(self):
        """Test early check-in (should be on time)."""
        expected_start = datetime(2026, 1, 5, 9, 0)
        actual_start = datetime(2026, 1, 5, 8, 50)  # 10 min early
        
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=actual_start,
            actual_end=None,
            expected_start=expected_start,
            expected_end=None
        )
        assert result["status"] == "A"
        # Delay should be 0 or negative (clamped to 0)
        assert result["delay_minutes"] == 0
    
    def test_off_day_no_shift_no_checkin(self):
        """Test OFF day (no schedule) - agent shouldn't check in."""
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=None,
            actual_end=None,
            expected_start=None,
            expected_end=None
        )
        assert result["has_schedule"] is False
        assert result["status"] is None
        assert result["delay_minutes"] == 0
    
    def test_shift_code_to_time_range_s1_night(self):
        """Test S1 (night shift crossing midnight) conversion."""
        result = ScheduleProvider.shift_code_to_time_range("S1")
        assert result is not None
        start_t, end_t, crosses_midnight = result
        assert start_t == time(22, 0)
        assert end_t == time(5, 30)
        assert crosses_midnight is True
    
    def test_shift_code_to_time_range_s3_morning(self):
        """Test S3 (morning shift) conversion."""
        result = ScheduleProvider.shift_code_to_time_range("S3")
        assert result is not None
        start_t, end_t, crosses_midnight = result
        assert start_t == time(5, 0)
        assert end_t == time(14, 0)
        assert crosses_midnight is False
    
    def test_shift_code_off_returns_none(self):
        """Test that OFF shift returns None (no schedule)."""
        result = ScheduleProvider.shift_code_to_time_range("OFF")
        assert result is None
    
    def test_no_actual_end_time(self):
        """Test scenario where agent checked in but didn't check out."""
        expected_start = datetime(2026, 1, 5, 9, 0)
        expected_end = datetime(2026, 1, 5, 17, 0)
        
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=datetime(2026, 1, 5, 9, 0),
            actual_end=None,  # No check-out
            expected_start=expected_start,
            expected_end=expected_end
        )
        assert result["has_schedule"] is True
        assert result["status"] == "A"  # On time for start
        assert result["overtime_minutes"] == 0  # Can't calculate without end time
    
    def test_very_late_checkin(self):
        """Test significant delay (>30 minutes)."""
        expected_start = datetime(2026, 1, 5, 9, 0)
        actual_start = datetime(2026, 1, 5, 9, 45)
        
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=actual_start,
            actual_end=None,
            expected_start=expected_start,
            expected_end=None,
            tolerance_minutes=2
        )
        assert result["status"] == "D"
        assert result["delay_minutes"] == 45
    
    def test_no_schedule_but_has_actual_checkin(self):
        """Test when there's no schedule (OFF) but agent checked in."""
        # This might happen if agent worked on a day off
        result = ScheduleProvider.compare_actual_vs_expected(
            actual_start=datetime(2026, 1, 5, 9, 0),
            actual_end=datetime(2026, 1, 5, 17, 0),
            expected_start=None,  # No schedule
            expected_end=None
        )
        assert result["has_schedule"] is False
        assert result["status"] is None  # No schedule to compare against


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

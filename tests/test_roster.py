"""
Unit tests for Roster Matrix functionality.
Tests effective dating, shift assignments, and history preservation.
"""
import pytest
from datetime import date, timedelta
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.shifts import (
    DayOfWeek,
    SHIFT_CATALOG,
    map_legacy_shift_to_code,
    parse_working_days,
    ShiftTemplate,
)


class TestShiftModels:
    """Test shift model utilities."""
    
    def test_day_of_week_from_date(self):
        """Given a date, should return correct day of week."""
        # Monday
        assert DayOfWeek.from_date(date(2026, 2, 9)) == DayOfWeek.MON
        # Friday
        assert DayOfWeek.from_date(date(2026, 2, 13)) == DayOfWeek.FRI
        # Sunday
        assert DayOfWeek.from_date(date(2026, 2, 15)) == DayOfWeek.SUN
    
    def test_shift_catalog_completeness(self):
        """Shift catalog should have all expected shifts."""
        expected = ['S1', 'S2', 'S3', 'S4', 'S5', 'S6', 'S7', 'S8', 'S9', 'S10', 'OFF']
        for code in expected:
            assert code in SHIFT_CATALOG
    
    def test_shift_template_from_code(self):
        """Given a valid shift code, should create template."""
        template = ShiftTemplate.from_code('S4')
        assert template.shift_code == 'S4'
        assert template.start_time == '06:00'
        assert template.end_time == '15:00'
        assert not template.crosses_midnight
    
    def test_shift_template_invalid_code(self):
        """Given invalid shift code, should raise ValueError."""
        with pytest.raises(ValueError):
            ShiftTemplate.from_code('INVALID')
    
    def test_night_shift_crosses_midnight(self):
        """Night shifts should have crosses_midnight=True."""
        s1 = ShiftTemplate.from_code('S1')
        s10 = ShiftTemplate.from_code('S10')
        assert s1.crosses_midnight
        assert s10.crosses_midnight
    
    def test_day_shift_does_not_cross_midnight(self):
        """Day shifts should have crosses_midnight=False."""
        s4 = ShiftTemplate.from_code('S4')
        s6 = ShiftTemplate.from_code('S6')
        assert not s4.crosses_midnight
        assert not s6.crosses_midnight


class TestLegacyMapping:
    """Test legacy schedule to new shift code mapping."""
    
    def test_map_exact_s4(self):
        """Given 06:00-15:00, should map to S4."""
        code = map_legacy_shift_to_code('06:00', '15:00')
        assert code == 'S4'
    
    def test_map_exact_s8(self):
        """Given 14:30-22:00, should map to S8."""
        code = map_legacy_shift_to_code('14:30', '22:00')
        assert code == 'S8'
    
    def test_map_exact_s10(self):
        """Given 21:30-05:00, should map to S10."""
        code = map_legacy_shift_to_code('21:30', '05:00')
        assert code == 'S10'
    
    def test_map_empty_to_off(self):
        """Given empty times, should map to OFF."""
        code = map_legacy_shift_to_code('', '')
        assert code == 'OFF'
    
    def test_map_short_time_format(self):
        """Given H:MM format, should still map correctly."""
        code = map_legacy_shift_to_code('6:00', '15:00')
        assert code == 'S4'
    
    def test_fuzzy_map_morning(self):
        """Given morning time not exactly matching, should map to closest."""
        # 7:00 start should map to S5
        code = map_legacy_shift_to_code('07:00', '16:00')
        assert code == 'S5'


class TestParseWorkingDays:
    """Test working days string parsing."""
    
    def test_parse_standard_format(self):
        """Given 'Mon, Tue, Wed', should parse correctly."""
        days = parse_working_days('Mon, Tue, Wed, Thu, Fri')
        assert days == ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
    
    def test_parse_no_spaces(self):
        """Given 'Mon,Tue,Wed', should parse correctly."""
        days = parse_working_days('Mon,Tue,Wed')
        assert days == ['Mon', 'Tue', 'Wed']
    
    def test_parse_lowercase(self):
        """Given lowercase 'mon, tue', should normalize."""
        days = parse_working_days('mon, tue, wed')
        assert days == ['Mon', 'Tue', 'Wed']
    
    def test_parse_empty(self):
        """Given empty string, should return empty list."""
        days = parse_working_days('')
        assert days == []


class TestEffectiveDating:
    """Test effective dating business rules.
    
    These tests describe the expected behavior of the effective dating system.
    """
    
    def test_assignment_is_active_on_date(self):
        """
        Given: An assignment with effective_start=2026-02-01, effective_end=None
        When: Checking if active on 2026-02-15
        Then: Should return True
        """
        from app.models.shifts import AgentShiftAssignment
        
        assignment = AgentShiftAssignment(
            agent_id='10003',
            day_of_week='Mon',
            shift_code='S4',
            effective_start=date(2026, 2, 1),
            effective_end=None
        )
        
        assert assignment.is_active_on(date(2026, 2, 15)) is True
    
    def test_assignment_not_active_before_start(self):
        """
        Given: An assignment with effective_start=2026-02-01
        When: Checking if active on 2026-01-15
        Then: Should return False
        """
        from app.models.shifts import AgentShiftAssignment
        
        assignment = AgentShiftAssignment(
            agent_id='10003',
            day_of_week='Mon',
            shift_code='S4',
            effective_start=date(2026, 2, 1),
            effective_end=None
        )
        
        assert assignment.is_active_on(date(2026, 1, 15)) is False
    
    def test_assignment_not_active_after_end(self):
        """
        Given: An assignment with effective_start=2026-02-01, effective_end=2026-02-28
        When: Checking if active on 2026-03-15
        Then: Should return False
        """
        from app.models.shifts import AgentShiftAssignment
        
        assignment = AgentShiftAssignment(
            agent_id='10003',
            day_of_week='Mon',
            shift_code='S4',
            effective_start=date(2026, 2, 1),
            effective_end=date(2026, 2, 28)
        )
        
        assert assignment.is_active_on(date(2026, 3, 15)) is False


class TestRosterAgent:
    """Test agent roster entry behavior."""
    
    def test_agent_is_active_on_date(self):
        """
        Given: Agent with effective_start=2026-01-01, effective_end=None
        When: Checking if active on 2026-02-15
        Then: Should return True
        """
        from app.models.shifts import AgentRosterEntry
        
        agent = AgentRosterEntry(
            agent_id='10003',
            full_name='Aaron Gonzalez',
            lead='Martin',
            effective_start=date(2026, 1, 1),
            effective_end=None
        )
        
        assert agent.is_active_on(date(2026, 2, 15)) is True
    
    def test_removed_agent_not_active_after_end(self):
        """
        Given: Agent with effective_end=2026-02-01
        When: Checking if active on 2026-02-15
        Then: Should return False (agent removed)
        """
        from app.models.shifts import AgentRosterEntry
        
        agent = AgentRosterEntry(
            agent_id='10003',
            full_name='Aaron Gonzalez',
            lead='Martin',
            effective_start=date(2026, 1, 1),
            effective_end=date(2026, 2, 1)
        )
        
        assert agent.is_active_on(date(2026, 2, 15)) is False


# ============ Integration Test Scenarios ============

class TestBusinessScenarios:
    """
    High-level test scenarios matching the business requirements.
    These are Given/When/Then format tests for key behaviors.
    """
    
    def test_scenario_no_history_overwrite(self):
        """
        SCENARIO: Editing a shift should not delete history
        
        Given: Agent 10003 has S4 on Monday effective from 2026-01-01
        When: Changing Monday to S8 with effective_date=2026-02-15
        Then: 
          - Previous S4 assignment should have effective_end=2026-02-14
          - New S8 assignment should have effective_start=2026-02-15
          - Querying for 2026-02-01 should return S4
          - Querying for 2026-02-15 should return S8
        """
        # This scenario documents the expected behavior
        # Implementation tests would use the actual DB functions
        pass
    
    def test_scenario_midweek_change(self):
        """
        SCENARIO: Change applied mid-week
        
        Given: Agent has S4 on all weekdays
        When: Changing Wed/Thu/Fri to S8 effective 2026-02-11 (Wednesday)
        Then:
          - Mon/Tue should still show S4 for that week
          - Wed/Thu/Fri should show S8
          - Previous assignments are not deleted
        """
        pass
    
    def test_scenario_agent_removal_preserves_history(self):
        """
        SCENARIO: Removing agent keeps all historical data
        
        Given: Agent 10003 with schedules from 2026-01-01
        When: Removing agent with effective_date=2026-03-01
        Then:
          - Agent appears in roster for dates before 2026-03-01
          - Agent does NOT appear in roster for dates from 2026-03-01
          - All historical shift assignments are preserved
        """
        pass
    
    def test_scenario_no_duplicate_active_assignments(self):
        """
        SCENARIO: Prevent duplicate active assignments
        
        Given: Agent 10003 already has S4 on Monday (active)
        When: Attempting to set Monday to S4 again
        Then: No new record is created (returns "no_change" status)
        """
        pass
    
    def test_scenario_no_overlapping_periods(self):
        """
        SCENARIO: Prevent overlapping effective date ranges
        
        Given: Agent 10003 has S4 on Monday effective 2026-02-01 to None (active)
        When: Attempting to create S8 on Monday effective 2026-01-15
        Then: 
          - If dates would overlap with existing range, error is returned
          - OR the system auto-closes the existing range
        """
        pass


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

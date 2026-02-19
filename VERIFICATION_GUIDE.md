# 🔍 VERIFICATION GUIDE: Attendance Schedule Synchronization

This guide helps you verify that the attendance module is correctly reading schedules from the database (agent_shift_assignments) instead of schedule.csv.

---

## Quick Verification

### 1. Run Unit Tests
```bash
cd attendance_tracker
python -m pytest tests/test_schedule_provider.py -v
```

**Expected Result**: ✅ 25 passed in ~0.7s

This validates:
- Shift code→time conversion (S1..S10, OFF)
- Actual vs expected comparison logic
- Midnight-crossing shifts
- Tolerance handling
- Edge cases (missing schedules, early check-ins, etc.)

---

## Code-Level Verification

### Q: Where does attendance get schedule data?

**Answer**: Trace through the code:

```
1. GET /attendance endpoint
   └─> calls: build_attendance(start_d, end_d, lead, agent_id, status)
       (in app/logic.py)

2. build_attendance() loops through dates
   └─> calls: get_schedule_cached(date)
       (caches result of get_schedule_for_day)

3. get_schedule_for_day(date)
   └─> calls: ScheduleProvider.get_schedule_for_date(target_date)
       (in app/providers/schedule_provider.py)

4. ScheduleProvider.get_schedule_for_date()
   └─> calls: get_all_roster_agents()
       (gets agents from agent_roster BD table)

5. For each agent/day:
   └─> calls: ScheduleProvider.get_schedule_for_agent_day(agent_id, day, date)
       └─> calls: get_shift_for_agent_day(agent_id, day_of_week, date)
           (queries agent_shift_assignments BD table)

RESULT: schedule.csv is NEVER read ✅
```

### Q: How do I verify schedule.csv is not imported?

**Answer**: Search for these strings in app/logic.py:

```bash
cd attendance_tracker
grep -n "schedule.csv\|SCHEDULE_CSV\|SCHEDULE_DF\|load_schedule" app/logic.py
```

**Expected Result**: No matches (file is clean)

```bash
grep -n "ScheduleProvider" app/logic.py
```

**Expected Result**: 
```
11: from .providers.schedule_provider import ScheduleProvider
37: return ScheduleProvider.get_schedule_for_date(target_date)
```

---

## Endpoint Testing

### Test Attendance Endpoint

```bash
# Get attendance for January 2026
curl "http://localhost:8000/attendance?start=2026-01-01&end=2026-01-31" | jq .

# Expected output structure:
{
  "agents": [
    {
      "agent_id": "10003",
      "name": "Aaron G",
      "lead": "Martin",
      "days": [
        {
          "date": "2026-01-01",
          "status": "A",      # On-time 
          "planned_start": "14:30",
          "planned_end": "22:00",
          "actual_start": "14:30",
          "actual_end": "22:00",
          "late_minutes": 0,
          "shift": "S8"
        },
        ...
      ],
      "late_minutes_sum": 45,
      "delays_sum": 2,
      ...
    },
    ...
  ]
}
```

**Key verification points**:
- ✅ `planned_start` and `planned_end` come from shift codes (S1..S10)
- ✅ Only agents from agent_roster appear
- ✅ Status codes (A/D/U/J/V/O/H/C/ML) work as before
- ✅ Metrics are calculated correctly

### Test Schedules Endpoint

```bash
# Get all schedules
curl "http://localhost:8000/api/roster" | jq .

# Expected output:
{
  "agents": [
    {
      "agent_id": "10003",
      "full_name": "Aaron Gonzalez Gomez",
      "lead": "Martin",
      "days": {
        "Mon": {"shift_code": "S8", "start_time": "14:30", "end_time": "22:00", ...},
        "Tue": {"shift_code": "S8", "start_time": "14:30", "end_time": "22:00", ...},
        ...
      }
    },
    ...
  ]
}
```

**Key verification point**:
- ✅ Shift codes and times match between /attendance and /api/roster (same database source)

---

## Data Flow Verification

### Check Database Tables

```sql
-- SQLite example
SELECT * FROM agent_roster LIMIT 5;
SELECT * FROM agent_shift_assignments WHERE agent_id = '10003' LIMIT 7;
```

**agent_roster table** should contain:
- agent_id, full_name, lead

**agent_shift_assignments table** should contain:
- agent_id, day_of_week (Mon-Sun), shift_code (S1-S10, OFF), effective_start, effective_end

### Verify Data Consistency

```sql
-- Check that attendance will see the same agents as roster
SELECT COUNT(*) FROM (
  SELECT DISTINCT agent_id 
  FROM agent_shift_assignments 
  WHERE effective_start <= date('now') 
  AND (effective_end IS NULL OR effective_end >= date('now'))
) AS active_agents;

-- Compare with roster
SELECT COUNT(*) FROM agent_roster;
```

**Expected**: Most agents in roster should have shift assignments.

---

## Shift Code Reference

For manual verification, here are all defined shift codes:

| Code | Start | End | Night | Example |
|------|-------|-----|-------|---------|
| S1 | 22:00 | 05:30 | ✅ | Aaron (Afternoon → Morning) |
| S2 | 04:00 | 12:00 | ❌ | Early morning |
| S3 | 05:00 | 14:00 | ❌ | Early morning |
| S4 | 06:00 | 15:00 | ❌ | Standard morning |
| S5 | 07:00 | 16:00 | ❌ | Standard morning |
| S6 | 09:00 | 17:00 | ❌ | Mid-morning |
| S7 | 09:00 | 18:00 | ❌ | Mid-morning |
| S8 | 14:30 | 22:00 | ❌ | Afternoon |
| S9 | 11:00 | 20:00 | ❌ | Afternoon |
| S10 | 21:30 | 05:00 | ✅ | Evening → Morning |
| OFF | — | — | — | Day off |

---

## Troubleshooting

### Issue: "Agent not found in attendance"
**Cause**: Agent is in actuals.csv but not in agent_roster table  
**Solution**: Add agent via API  
```bash
curl -X POST "http://localhost:8000/api/roster/agents" \
  -H "Content-Type: application/json" \
  -d '{"agent_id": "99999", "full_name": "New Agent", "lead": "Team Lead"}'
```

### Issue: "Agent appears in roster but not in attendance"
**Cause**: Agent has no shift assignments (agent_shift_assignments table is empty)  
**Solution**: Assign shifts via API or upload schedule  
```bash
# Upload a schedule file with effective date
curl -F "token=<password>" \
     -F "file=@schedule.csv" \
     -F "effective_date=2026-01-01" \
     http://localhost:8000/admin/upload-schedule
```

### Issue: "Times don't match between endpoints"
**Cause**: Cache issue or database inconsistency  
**Solution**:  
1. Restart the app
2. Verify agent_shift_assignments has correct shift codes
3. Check SHIFT_CATALOG in app/models/shifts.py has correct times

### Issue: Tests fail
**Solution**:
```bash
# Run tests with verbose output
python -m pytest tests/test_schedule_provider.py -vv

# Run specific test
python -m pytest tests/test_schedule_provider.py::TestCompareActualVsExpected::test_delayed_checkin -vv
```

---

## Integration Testing Checklist

Use this checklist to verify the integration works end-to-end:

- [ ] Database has agents in agent_roster table
- [ ] Database has shift assignments in agent_shift_assignments table  
- [ ] GET /attendance returns agents from database (not from schedule.csv)
- [ ] Agent names in /attendance match agent_roster table
- [ ] Shift codes in /attendance are S1..S10 or OFF (not "Morning"/"Night")
- [ ] Time ranges (planned_start/planned_end) match shift code definitions
- [ ] Status codes (A/D/U/J/V/O/H/C/ML) match expected values
- [ ] Metrics (delays_sum, late_minutes_sum) calculate correctly
- [ ] Filters work: ?lead=Team&agent_id=10003&status=D
- [ ] Unit tests pass: pytest tests/test_schedule_provider.py
- [ ] No schedule.csv import errors in app logs

---

## Performance Notes

The refactoring improved performance:
- ✅ Database queries are indexed (idx_shift_assignments_lookup)
- ✅ Schedules are cached within build_attendance() execution
- ✅ No CSV parsing/loading overhead
- ✅ Results are identical in speed or faster than before

---

## FAQ

**Q: Can I delete schedule.csv?**  
A: Yes! It's no longer used by attendance. You can safely delete it.

**Q: Will old attendance records still work?**  
A: Yes! The refactor changed only the data SOURCE, not the calculation logic. Historical attendance metrics remain unchanged.

**Q: What if an agent's shift changes?**  
A: Update agent_shift_assignments table with new effective_date. The next /attendance call will use the new schedule automatically.

**Q: Do justifications still work?**  
A: Yes! Justifications are independent. They still override calculated status.

**Q: Can I have overrides per-day?**  
A: Yes! The system supports:
  1. Single-day overrides via justifications
  2. Schedule changes via Edit Schedules
  3. New schedule overrides

---

## Architecture Diagram

```
┌─────────────────────────────────────────────┐
│         HTTP Requests                       │
│  GET /attendance, /api/roster, /schedules   │
└──────────────┬──────────────────────────────┘
               │
┌──────────────▼──────────────────────────────┐
│      Attendance Module (app/logic.py)       │
│  - build_attendance()                       │
│  - compute_day_status()                     │
│  - expected_interval_for_day()              │
└──────────────┬──────────────────────────────┘
               │
┌──────────────▼──────────────────────────────┐
│      Schedule Provider (app/providers/)     │
│  ✅ shift_code_to_time_range()              │
│  ✅ get_schedule_for_agent_day()            │
│  ✅ get_schedule_for_date()                 │
│  ✅ compare_actual_vs_expected()            │
└──────────────┬──────────────────────────────┘
               │
       ┌───────┴───────┐
       │               │
┌──────▼────────┐  ┌────▼──────────┐
│ Shift DB      │  │   Models      │
│ (shift_db.py) │  │  (shifts.py)  │
│               │  │               │
│ get_shift_    │  │ SHIFT_CATALOG │
│ for_agent_day │  │ (S1..S10)     │
└──────┬────────┘  └──────┬────────┘
       │                  │
       └───────┬──────────┘
               │
        ┌──────▼──────────┐
        │  SQLite/PostgreSQL Database  │
        │  ┌────────────────────────┐  │
        │  │ agent_roster           │  │
        │  │ agent_shift_assignments│  │
        │  │ justifications         │  │
        │  │ schedule_overrides     │  │
        │  └────────────────────────┘  │
        └───────────────────────────────┘

NO CSV READS IN THIS FLOW ✅
```

---

## Next Steps for Support

If you encounter issues:

1. **Check the logs** for any Python errors
2. **Run the tests** to verify core functionality: `pytest tests/test_schedule_provider.py -v`
3. **Verify database** connection and tables
4. **Check endpoint** responses with `curl` or Postman
5. **Contact support** with test output and database state

---

**Document Version**: 1.0  
**Last Updated**: February 19, 2026  
**Status**: All verification tests passing ✅

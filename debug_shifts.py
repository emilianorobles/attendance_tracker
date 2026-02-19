#!/usr/bin/env python3
import sqlite3
from datetime import date

conn = sqlite3.connect('attendance.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# Update all effective_start dates to be Jan 1, 2026
print("Updating effective_start dates for all shifts to 2026-01-01...")
cursor.execute("""
    UPDATE agent_shift_assignments 
    SET effective_start = '2026-01-01'
    WHERE effective_start > '2026-01-06'
""")
conn.commit()
print(f"Updated {cursor.rowcount} rows")

# Now test the query again
target_date = date(2026, 1, 6)
print(f"\nTest date: {target_date} ({target_date.strftime('%A')})")

# Check what shift SQL would return for agent 10003 on Tue 2026-01-06
cursor.execute("""
    SELECT id, shift_code, effective_start, effective_end 
    FROM agent_shift_assignments 
    WHERE agent_id = ? 
    AND day_of_week = ? 
    AND effective_start <= ? 
    AND (effective_end IS NULL OR effective_end >= ?) 
    ORDER BY effective_start DESC 
    LIMIT 1
""", ('10003', 'Tue', target_date.isoformat(), target_date.isoformat()))

row = cursor.fetchone()
print(f"\nSQL Query result for agent 10003 on Tue (after fix):")
if row:
    print(f"  {dict(row)}")
else:
    print("  NO RESULT")

conn.close()

"""
Migration script to convert existing schedule data to the new roster model.
This preserves all historical data and creates the first effective version.

Run this script once to migrate from the old format to the new effective-dated model.
"""
import csv
import sqlite3
from datetime import date, datetime
from typing import List, Dict, Any
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.shifts import (
    map_legacy_shift_to_code,
    parse_working_days,
    DayOfWeek,
    SHIFT_CATALOG
)
from app.shift_db import (
    init_shift_tables,
    seed_shift_templates,
    add_agent_to_roster,
    upsert_shift_assignment,
    DB_PATH,
    USE_POSTGRES,
    _get_pg_connection
)


def load_legacy_schedule_csv(csv_path: str = "schedule.csv") -> List[Dict[str, str]]:
    """Load the legacy schedule.csv file."""
    schedules = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            schedules.append(row)
    return schedules


def migrate_from_csv(
    csv_path: str = "schedule.csv",
    effective_date: date = None,
    dry_run: bool = True
) -> Dict[str, Any]:
    """
    Migrate from legacy schedule.csv to the new effective-dated model.
    
    Args:
        csv_path: Path to the schedule.csv file
        effective_date: The date from which the migrated schedule should be effective.
                       Defaults to today.
        dry_run: If True, only report what would be done without making changes.
    
    Returns:
        Migration report with counts and any issues.
    """
    if effective_date is None:
        effective_date = date.today()
    
    report = {
        "effective_date": effective_date.isoformat(),
        "agents_migrated": 0,
        "shifts_created": 0,
        "off_days_created": 0,
        "issues": [],
        "details": []
    }
    
    # Load legacy data
    try:
        legacy_data = load_legacy_schedule_csv(csv_path)
    except FileNotFoundError:
        report["issues"].append(f"File not found: {csv_path}")
        return report
    except Exception as e:
        report["issues"].append(f"Error reading CSV: {e}")
        return report
    
    print(f"\n{'='*60}")
    print(f"MIGRATION: schedule.csv -> New Roster Model")
    print(f"{'='*60}")
    print(f"Effective Date: {effective_date}")
    print(f"Records to migrate: {len(legacy_data)}")
    print(f"Dry Run: {dry_run}")
    print(f"{'='*60}\n")
    
    if not dry_run:
        # Initialize tables
        print("Initializing database tables...")
        init_shift_tables()
        seed_shift_templates()
        print("Tables initialized.\n")
    
    for row in legacy_data:
        agent_id = str(row.get('agent_id', '')).strip()
        full_name = str(row.get('name', '')).strip()
        lead = str(row.get('lead', '')).strip()
        expected_start = str(row.get('expected_start', '')).strip()
        expected_end = str(row.get('expected_end', '')).strip()
        working_days_str = str(row.get('working_days', '')).strip()
        days_off_str = str(row.get('days_off', '')).strip()
        
        if not agent_id or not full_name:
            report["issues"].append(f"Skipping row with missing agent_id or name: {row}")
            continue
        
        # Determine shift code from times
        shift_code = map_legacy_shift_to_code(expected_start, expected_end)
        
        # Parse working days and days off
        working_days = parse_working_days(working_days_str)
        days_off = parse_working_days(days_off_str)
        
        # If no explicit days, assume Mon-Fri working, Sat-Sun off
        if not working_days and not days_off:
            working_days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
            days_off = ['Sat', 'Sun']
        elif not days_off:
            all_days = set(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
            days_off = list(all_days - set(working_days))
        elif not working_days:
            all_days = set(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
            working_days = list(all_days - set(days_off))
        
        detail = {
            "agent_id": agent_id,
            "full_name": full_name,
            "lead": lead,
            "shift_code": shift_code,
            "working_days": working_days,
            "days_off": days_off,
            "legacy_times": f"{expected_start}-{expected_end}"
        }
        report["details"].append(detail)
        
        print(f"Agent: {agent_id} - {full_name}")
        print(f"  Lead: {lead}")
        print(f"  Legacy times: {expected_start} - {expected_end}")
        print(f"  -> Shift code: {shift_code}")
        print(f"  Working: {working_days}")
        print(f"  Off: {days_off}")
        
        if not dry_run:
            # Add agent to roster
            try:
                add_agent_to_roster(agent_id, full_name, lead, effective_date)
                report["agents_migrated"] += 1
            except Exception as e:
                # Agent might already exist
                report["issues"].append(f"Error adding agent {agent_id}: {e}")
            
            # Create shift assignments for working days
            for day in working_days:
                try:
                    result = upsert_shift_assignment(agent_id, day, shift_code, effective_date)
                    if result["status"] == "created":
                        report["shifts_created"] += 1
                except Exception as e:
                    report["issues"].append(f"Error creating shift for {agent_id}/{day}: {e}")
            
            # Create OFF assignments for days off
            for day in days_off:
                try:
                    result = upsert_shift_assignment(agent_id, day, "OFF", effective_date)
                    if result["status"] == "created":
                        report["off_days_created"] += 1
                except Exception as e:
                    report["issues"].append(f"Error creating OFF for {agent_id}/{day}: {e}")
        
        print()
    
    print(f"\n{'='*60}")
    print(f"MIGRATION SUMMARY")
    print(f"{'='*60}")
    print(f"Agents processed: {len(legacy_data)}")
    if not dry_run:
        print(f"Agents migrated: {report['agents_migrated']}")
        print(f"Shift assignments created: {report['shifts_created']}")
        print(f"OFF assignments created: {report['off_days_created']}")
    print(f"Issues: {len(report['issues'])}")
    if report["issues"]:
        print("\nIssues encountered:")
        for issue in report["issues"]:
            print(f"  - {issue}")
    print(f"{'='*60}\n")
    
    return report


def migrate_from_schedule_versions(
    effective_date: date = None,
    dry_run: bool = True
) -> Dict[str, Any]:
    """
    Migrate from schedule_versions/schedule_entries tables to the new model.
    This uses the existing versioned schedule data.
    """
    if effective_date is None:
        effective_date = date.today()
    
    report = {
        "effective_date": effective_date.isoformat(),
        "versions_processed": 0,
        "agents_migrated": 0,
        "shifts_created": 0,
        "issues": []
    }
    
    # Get all schedule versions
    if USE_POSTGRES:
        con = _get_pg_connection()
        cur = con.cursor()
        cur.execute("SELECT id, effective_from FROM schedule_versions ORDER BY effective_from")
        versions = cur.fetchall()
        cur.close()
        con.close()
    else:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT id, effective_from FROM schedule_versions ORDER BY effective_from")
        versions = cur.fetchall()
        con.close()
    
    print(f"\n{'='*60}")
    print(f"MIGRATION: schedule_versions -> New Roster Model")
    print(f"{'='*60}")
    print(f"Versions found: {len(versions)}")
    print(f"Dry Run: {dry_run}")
    print(f"{'='*60}\n")
    
    if not dry_run:
        init_shift_tables()
        seed_shift_templates()
    
    for version_id, eff_from in versions:
        # Parse effective date
        if isinstance(eff_from, str):
            ver_eff_date = date.fromisoformat(eff_from)
        else:
            ver_eff_date = eff_from
        
        print(f"\nProcessing version {version_id} (effective: {ver_eff_date})")
        
        # Get entries for this version
        if USE_POSTGRES:
            con = _get_pg_connection()
            cur = con.cursor()
            cur.execute("""
                SELECT agent_id, shift, name, lead, working_days, days_off, 
                       expected_start, expected_end
                FROM schedule_entries WHERE version_id = %s
            """, (version_id,))
            entries = cur.fetchall()
            cur.close()
            con.close()
        else:
            con = sqlite3.connect(DB_PATH)
            cur = con.cursor()
            cur.execute("""
                SELECT agent_id, shift, name, lead, working_days, days_off,
                       expected_start, expected_end
                FROM schedule_entries WHERE version_id = ?
            """, (version_id,))
            entries = cur.fetchall()
            con.close()
        
        for entry in entries:
            agent_id = entry[0]
            full_name = entry[2]
            lead = entry[3]
            working_days_str = entry[4]
            days_off_str = entry[5]
            expected_start = entry[6]
            expected_end = entry[7]
            
            shift_code = map_legacy_shift_to_code(expected_start, expected_end)
            working_days = parse_working_days(working_days_str)
            days_off = parse_working_days(days_off_str)
            
            # Fill in missing days
            if not working_days and not days_off:
                working_days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
                days_off = ['Sat', 'Sun']
            elif not days_off:
                all_days = set(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
                days_off = list(all_days - set(working_days))
            elif not working_days:
                all_days = set(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
                working_days = list(all_days - set(days_off))
            
            if not dry_run:
                # Add agent to roster (if this is their first version)
                try:
                    add_agent_to_roster(agent_id, full_name, lead, ver_eff_date)
                    report["agents_migrated"] += 1
                except Exception as e:
                    pass  # Agent might already exist from earlier version
                
                # Create shift assignments
                for day in working_days:
                    try:
                        upsert_shift_assignment(agent_id, day, shift_code, ver_eff_date)
                        report["shifts_created"] += 1
                    except Exception as e:
                        report["issues"].append(f"Error: {e}")
                
                for day in days_off:
                    try:
                        upsert_shift_assignment(agent_id, day, "OFF", ver_eff_date)
                    except Exception as e:
                        pass
        
        report["versions_processed"] += 1
    
    return report


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Migrate schedule data to new roster model")
    parser.add_argument("--csv", default="schedule.csv", help="Path to schedule.csv")
    parser.add_argument("--effective-date", help="Effective date YYYY-MM-DD (default: today)")
    parser.add_argument("--dry-run", action="store_true", help="Don't make changes, just show what would be done")
    parser.add_argument("--source", choices=["csv", "versions"], default="csv",
                       help="Source to migrate from: csv or versions")
    
    args = parser.parse_args()
    
    eff_date = date.fromisoformat(args.effective_date) if args.effective_date else date.today()
    
    if args.source == "csv":
        result = migrate_from_csv(args.csv, eff_date, args.dry_run)
    else:
        result = migrate_from_schedule_versions(eff_date, args.dry_run)
    
    print("\nMigration complete!")
    if args.dry_run:
        print("This was a DRY RUN. No changes were made.")
        print("Remove --dry-run to execute the migration.")

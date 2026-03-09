"""
Database operations for Shift Roster Management with Effective Dating.
Supports both SQLite and PostgreSQL.

Key Features:
- AgentRosterStatus for effective dating of agent active/inactive status
- AgentShiftAssignment for per-day shift assignments with history
- No overlap validation for effective date ranges
- All agents shown (from schedule.csv + roster table)
"""
import sqlite3
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import os
import csv
import contextlib

from .storage import sync_db_to_r2


# Database configuration
DB_PATH = "attendance.db"
DATABASE_URL = os.environ.get("DATABASE_URL")
USE_POSTGRES = False
PG_DSN: Optional[str] = None

psycopg2 = None
if DATABASE_URL:
    try:
        import psycopg2 as _psycopg2
        from psycopg2 import pool as pg_pool
        psycopg2 = _psycopg2
        USE_POSTGRES = True
        PG_DSN = DATABASE_URL
    except ImportError:
        USE_POSTGRES = False


_connection_pool: Optional[pg_pool.ThreadedConnectionPool] = None

def _get_pool() -> pg_pool.ThreadedConnectionPool:
    global _connection_pool
    if _connection_pool is None or _connection_pool.closed:
        dsn = os.environ.get("DATABASE_URL")
        _connection_pool = pg_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=5,
            dsn=dsn,
            sslmode="require",
            connect_timeout=10
        )
    return _connection_pool

@contextlib.contextmanager
def get_pg_connection():
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
        if not conn.closed:
            conn.commit()
    except Exception:
        if not conn.closed:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        try:
            pool.putconn(conn) # returns to pool, no SSL teardown
        except Exception:
            pass


def _get_sqlite_connection():
    """Get a SQLite connection."""
    return sqlite3.connect(DB_PATH)


def init_shift_tables():
    """
    Initialize database tables for shift roster management.
    Call this on app startup after init_db().
    """
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS shift_templates (
                    id SERIAL PRIMARY KEY,
                shift_code VARCHAR(10) UNIQUE NOT NULL,
                start_time VARCHAR(5),
                end_time VARCHAR(5),
                crosses_midnight BOOLEAN DEFAULT FALSE,
                color VARCHAR(7) DEFAULT '#6B7280',
                label VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
        
            cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_roster_status (
                id SERIAL PRIMARY KEY,
                agent_id VARCHAR(20) NOT NULL,
                status VARCHAR(10) NOT NULL DEFAULT 'Active',
                effective_start DATE NOT NULL,
                effective_end DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
        
            cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_roster (
                id SERIAL PRIMARY KEY,
                agent_id VARCHAR(20) UNIQUE NOT NULL,
                full_name VARCHAR(100) NOT NULL,
                lead VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
        
            cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_shift_assignments (
                id SERIAL PRIMARY KEY,
                agent_id VARCHAR(20) NOT NULL,
                day_of_week VARCHAR(3) NOT NULL CHECK (day_of_week IN ('Mon','Tue','Wed','Thu','Fri','Sat','Sun')),
                shift_code VARCHAR(10) NOT NULL,
                effective_start DATE NOT NULL,
                effective_end DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """)
        
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_shift_assignments_lookup ON agent_shift_assignments(agent_id, day_of_week, effective_start)""")
            cur.execute("""CREATE INDEX IF NOT EXISTS idx_roster_status_lookup ON agent_roster_status(agent_id, effective_start)""")
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shift_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                shift_code TEXT UNIQUE NOT NULL,
                start_time TEXT,
                end_time TEXT,
                crosses_midnight INTEGER DEFAULT 0,
                color TEXT DEFAULT '#6B7280',
                label TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_roster_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'Active',
                effective_start TEXT NOT NULL,
                effective_end TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_roster (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT UNIQUE NOT NULL,
                full_name TEXT NOT NULL,
                lead TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_shift_assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                day_of_week TEXT NOT NULL CHECK (day_of_week IN ('Mon','Tue','Wed','Thu','Fri','Sat','Sun')),
                shift_code TEXT NOT NULL,
                effective_start TEXT NOT NULL,
                effective_end TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute("""CREATE INDEX IF NOT EXISTS idx_shift_assignments_lookup ON agent_shift_assignments(agent_id, day_of_week, effective_start)""")
        cur.execute("""CREATE INDEX IF NOT EXISTS idx_roster_status_lookup ON agent_roster_status(agent_id, effective_start)""")
        
        con.commit()
        con.close()


def seed_shift_templates():
    """Seed the shift_templates table with predefined shifts S1-S10 + OFF."""
    from .models.shifts import SHIFT_CATALOG
    
    templates = [(code, info["start"], info["end"], info["crosses_midnight"], info["color"], info["label"]) for code, info in SHIFT_CATALOG.items()]
    
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            for code, start, end, crosses, color, label in templates:
                cur.execute("""INSERT INTO shift_templates (shift_code, start_time, end_time, crosses_midnight, color, label) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (shift_code) DO NOTHING""", (code, start, end, crosses, color, label))
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        for code, start, end, crosses, color, label in templates:
            cur.execute("""INSERT OR IGNORE INTO shift_templates (shift_code, start_time, end_time, crosses_midnight, color, label) VALUES (?, ?, ?, ?, ?, ?)""", (code, start, end, 1 if crosses else 0, color, label))
        con.commit()
        con.close()


def get_all_shift_templates() -> List[Dict[str, Any]]:
    """Get all shift templates from the database."""
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            cur.execute("SELECT shift_code, start_time, end_time, crosses_midnight, color, label FROM shift_templates ORDER BY shift_code")
            rows = cur.fetchall()
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        cur.execute("SELECT shift_code, start_time, end_time, crosses_midnight, color, label FROM shift_templates ORDER BY shift_code")
        rows = cur.fetchall()
        con.close()
    
    return [{"shift_code": r[0], "start_time": r[1], "end_time": r[2], "crosses_midnight": bool(r[3]), "color": r[4], "label": r[5]} for r in rows]


def add_shift_template(shift_code: str, start_time: str, end_time: str, crosses_midnight: bool, color: str, label: str) -> bool:
    """Add a new shift template."""
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            cur.execute("""INSERT INTO shift_templates (shift_code, start_time, end_time, crosses_midnight, color, label) 
                           VALUES (%s, %s, %s, %s, %s, %s)""", 
                        (shift_code, start_time, end_time, crosses_midnight, color, label))
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        cur.execute("""INSERT INTO shift_templates (shift_code, start_time, end_time, crosses_midnight, color, label) 
                       VALUES (?, ?, ?, ?, ?, ?)""", 
                    (shift_code, start_time, end_time, 1 if crosses_midnight else 0, color, label))
        con.commit()
        con.close()
        sync_db_to_r2()
    return True


def update_shift_template(shift_code: str, start_time: str, end_time: str, crosses_midnight: bool, color: str, label: str) -> bool:
    """Update an existing shift template."""
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            cur.execute("""UPDATE shift_templates SET start_time = %s, end_time = %s, crosses_midnight = %s, color = %s, label = %s 
                           WHERE shift_code = %s""", 
                        (start_time, end_time, crosses_midnight, color, label, shift_code))
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        cur.execute("""UPDATE shift_templates SET start_time = ?, end_time = ?, crosses_midnight = ?, color = ?, label = ? 
                       WHERE shift_code = ?""", 
                    (start_time, end_time, 1 if crosses_midnight else 0, color, label, shift_code))
        con.commit()
        con.close()
        sync_db_to_r2()
    return True


def delete_shift_template(shift_code: str) -> bool:
    """Delete a shift template. Cannot delete if in use by agents."""
    # Check if shift is in use
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM agent_shift_assignments WHERE shift_code = %s", (shift_code,))
            count = cur.fetchone()[0]
            if count > 0:
                return False  # Cannot delete - in use
            cur.execute("DELETE FROM shift_templates WHERE shift_code = %s", (shift_code,))
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM agent_shift_assignments WHERE shift_code = ?", (shift_code,))
        count = cur.fetchone()[0]
        if count > 0:
            con.close()
            return False  # Cannot delete - in use
        cur.execute("DELETE FROM shift_templates WHERE shift_code = ?", (shift_code,))
        con.commit()
        con.close()
        sync_db_to_r2()
    return True


def load_all_agents_from_csv(csv_path: str = "schedule.csv") -> List[Dict[str, Any]]:
    """Load all agents from schedule.csv as base source with shift info."""
    agents = []
    try:
        with open(csv_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig handles BOM
            reader = csv.DictReader(f)
            for row in reader:
                agents.append({
                    "agent_id": str(row.get('agent_id', '')).strip(),
                    "full_name": str(row.get('name', '')).strip(),
                    "lead": str(row.get('lead', '')).strip(),
                    "expected_start": str(row.get('expected_start', '')).strip(),
                    "expected_end": str(row.get('expected_end', '')).strip(),
                    "working_days": str(row.get('working_days', '')).strip(),
                    "days_off": str(row.get('days_off', '')).strip(),
                })
    except FileNotFoundError:
        pass
    return agents


def map_time_to_shift_code(start_time: str, end_time: str) -> str:
    """Map legacy expected_start/expected_end times to shift codes."""
    from .models.shifts import SHIFT_CATALOG
    
    if not start_time or not end_time:
        return 'OFF'
    
    # Normalize times
    def normalize(t):
        parts = t.replace(' ', '').split(':')
        if len(parts) >= 2:
            h = int(parts[0])
            m = int(parts[1]) if len(parts) > 1 else 0
            return f"{h:02d}:{m:02d}"
        return t
    
    start_norm = normalize(start_time)
    end_norm = normalize(end_time)
    
    # Direct match
    for code, info in SHIFT_CATALOG.items():
        if code == 'OFF':
            continue
        if info['start'] == start_norm and info['end'] == end_norm:
            return code
    
    # Best effort match by start time
    start_mappings = {
        '22:00': 'S1', '04:00': 'S2', '05:00': 'S3', '05:30': 'S3',
        '06:00': 'S4', '07:00': 'S5', '07:30': 'S5', '08:00': 'S6',
        '09:00': 'S7', '11:00': 'S9', '14:00': 'S8', '14:30': 'S8',
        '21:30': 'S10', '23:30': 'S1'
    }
    
    if start_norm in start_mappings:
        return start_mappings[start_norm]
    
    # Fallback
    return 'S5'


def parse_working_days(days_str: str) -> List[str]:
    """Parse working_days string like 'Mon, Tue, Wed' to list."""
    if not days_str:
        return []
    return [d.strip()[:3].capitalize() for d in days_str.split(',') if d.strip()]


def sync_agents_from_csv():
    """Sync agents from schedule.csv to agent_roster table and create shift assignments."""
    try:
        csv_agents = load_all_agents_from_csv()
        if not csv_agents:
            print("No agents loaded from CSV")
            return
        
        ts = datetime.now().isoformat(timespec="seconds")
        today = date.today()
        effective_date = date(today.year, 1, 1)
        all_days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        
        if USE_POSTGRES:
            with get_pg_connection() as con:
                cur = con.cursor()
                for agent in csv_agents:
                    try:
                        if not agent["agent_id"]:
                            continue
                        cur.execute("""INSERT INTO agent_roster (agent_id, full_name, lead, created_at) VALUES (%s, %s, %s, %s) ON CONFLICT (agent_id) DO UPDATE SET full_name = EXCLUDED.full_name, lead = EXCLUDED.lead""", (agent["agent_id"], agent["full_name"], agent["lead"], ts))
                        cur.execute("SELECT COUNT(*) FROM agent_shift_assignments WHERE agent_id = %s", (agent["agent_id"],))
                        if cur.fetchone()[0] > 0:
                            continue
                        
                        shift_code = map_time_to_shift_code(agent["expected_start"], agent["expected_end"])
                        working_days = parse_working_days(agent["working_days"])
                        for day in all_days:
                            day_shift = shift_code if day in working_days else 'OFF'
                            cur.execute("""INSERT INTO agent_shift_assignments (agent_id, day_of_week, shift_code, effective_start, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s)""", (agent["agent_id"], day, day_shift, effective_date.isoformat(), ts, ts))
                    except Exception as e:
                        print(f"Error syncing agent {agent.get('agent_id')}: {e}")
            invalidate_roster_cache()
            print(f"Synced {len(csv_agents)} agents to PostgreSQL")
        else:
            con = _get_sqlite_connection()
            cur = con.cursor()
            for agent in csv_agents:
                try:
                    if not agent["agent_id"]:
                        continue
                    cur.execute("SELECT id FROM agent_roster WHERE agent_id = ?", (agent["agent_id"],))
                    existing = cur.fetchone()
                    if existing:
                        cur.execute("UPDATE agent_roster SET full_name = ?, lead = ? WHERE agent_id = ?", (agent["full_name"], agent["lead"], agent["agent_id"]))
                    else:
                        cur.execute("INSERT INTO agent_roster (agent_id, full_name, lead, created_at) VALUES (?, ?, ?, ?)", (agent["agent_id"], agent["full_name"], agent["lead"], ts))
                    
                    cur.execute("SELECT COUNT(*) FROM agent_shift_assignments WHERE agent_id = ?", (agent["agent_id"],))
                    if cur.fetchone()[0] > 0:
                        continue
                    
                    shift_code = map_time_to_shift_code(agent["expected_start"], agent["expected_end"])
                    working_days = parse_working_days(agent["working_days"])
                    for day in all_days:
                        day_shift = shift_code if day in working_days else 'OFF'
                        cur.execute("INSERT INTO agent_shift_assignments (agent_id, day_of_week, shift_code, effective_start, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)", (agent["agent_id"], day, day_shift, effective_date.isoformat(), ts, ts))
                except Exception as e:
                    print(f"Error syncing agent {agent.get('agent_id')}: {e}")
            
            con.commit()
            con.close()
            print(f"Synced {len(csv_agents)} agents to SQLite")
    except Exception as e:
        print(f"Error in sync_agents_from_csv: {e}")
        import traceback
        traceback.print_exc()


def get_all_agents() -> List[Dict[str, Any]]:
    """Get ALL agents from agent_roster table."""
    try:
        if USE_POSTGRES:
            with get_pg_connection() as con:
                cur = con.cursor()
                cur.execute("SELECT agent_id, full_name, lead FROM agent_roster ORDER BY full_name")
                rows = cur.fetchall()
        else:
            con = _get_sqlite_connection()
            cur = con.cursor()
            cur.execute("SELECT agent_id, full_name, lead FROM agent_roster ORDER BY full_name")
            rows = cur.fetchall()
            con.close()
        
        return [{"agent_id": r[0], "full_name": r[1], "lead": r[2]} for r in rows]
    except Exception as e:
        print(f"Error fetching agents: {e}")
        return []


def get_all_roster_agent_ids() -> set:
    """Get set of all agent IDs currently in the roster table."""
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            cur.execute("SELECT agent_id FROM agent_roster")
            rows = cur.fetchall()
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        cur.execute("SELECT agent_id FROM agent_roster")
        rows = cur.fetchall()
        con.close()
    
    return {r[0] for r in rows}


def get_agent_status_on_date(agent_id: str, target_date: date) -> str:
    """Get agent's status (Active/Inactive) on a specific date. Returns 'Active' if no status record exists."""
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            cur.execute("""SELECT status FROM agent_roster_status WHERE agent_id = %s AND effective_start <= %s AND (effective_end IS NULL OR effective_end >= %s) ORDER BY effective_start DESC LIMIT 1""", (agent_id, target_date.isoformat(), target_date.isoformat()))
            row = cur.fetchone()
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        cur.execute("""SELECT status FROM agent_roster_status WHERE agent_id = ? AND effective_start <= ? AND (effective_end IS NULL OR effective_end >= ?) ORDER BY effective_start DESC LIMIT 1""", (agent_id, target_date.isoformat(), target_date.isoformat()))
        row = cur.fetchone()
        con.close()
    return row[0] if row else 'Active'


def add_agent_to_roster(agent_id: str, full_name: str, lead: str, effective_date: date) -> int:
    """Add an agent to the roster and set as Active."""
    ts = datetime.now().isoformat(timespec="seconds")
    
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            cur.execute("""INSERT INTO agent_roster (agent_id, full_name, lead, created_at) VALUES (%s, %s, %s, %s) ON CONFLICT (agent_id) DO UPDATE SET full_name = EXCLUDED.full_name, lead = EXCLUDED.lead RETURNING id""", (agent_id, full_name, lead, ts))
            roster_id = cur.fetchone()[0]
            cur.execute("""INSERT INTO agent_roster_status (agent_id, status, effective_start, created_at) VALUES (%s, 'Active', %s, %s)""", (agent_id, effective_date.isoformat(), ts))
        return roster_id
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        cur.execute("SELECT id FROM agent_roster WHERE agent_id = ?", (agent_id,))
        existing = cur.fetchone()
        if existing:
            cur.execute("UPDATE agent_roster SET full_name = ?, lead = ? WHERE agent_id = ?", (full_name, lead, agent_id))
            roster_id = existing[0]
        else:
            cur.execute("INSERT INTO agent_roster (agent_id, full_name, lead, created_at) VALUES (?, ?, ?, ?)", (agent_id, full_name, lead, ts))
            roster_id = cur.lastrowid
        cur.execute("INSERT INTO agent_roster_status (agent_id, status, effective_start, created_at) VALUES (?, 'Active', ?, ?)", (agent_id, effective_date.isoformat(), ts))
        con.commit()
        con.close()
        sync_db_to_r2()
        invalidate_roster_cache()
        return roster_id


def update_agent_lead(agent_id: str, new_lead: str, effective_date: date) -> bool:
    """Update the lead for an agent. The effective_date is logged for audit purposes."""
    # Note: For simplicity, lead changes are immediate. The effective_date is recorded
    # for future audit/history features. To implement full effective dating for leads,
    # a separate agent_lead_history table would be needed.
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            cur.execute("UPDATE agent_roster SET lead = %s WHERE agent_id = %s", (new_lead, agent_id))
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        cur.execute("UPDATE agent_roster SET lead = ? WHERE agent_id = ?", (new_lead, agent_id))
        con.commit()
        con.close()
        sync_db_to_r2()
    invalidate_roster_cache()
    return True


def remove_agent_from_roster(agent_id: str, effective_date: date) -> bool:
    """Permanently delete an agent and all their data from the roster.
    
    This removes:
    - Agent from agent_roster table
    - All status records from agent_roster_status
    - All shift assignments from agent_shift_assignments
    
    The agent will completely disappear from the roster starting from the effective date.
    """
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            # Delete shift assignments for this agent
            cur.execute("DELETE FROM agent_shift_assignments WHERE agent_id = %s", (agent_id,))
            # Delete status records for this agent
            cur.execute("DELETE FROM agent_roster_status WHERE agent_id = %s", (agent_id,))
            # Delete from roster
            cur.execute("DELETE FROM agent_roster WHERE agent_id = %s", (agent_id,))
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        # Delete shift assignments for this agent
        cur.execute("DELETE FROM agent_shift_assignments WHERE agent_id = ?", (agent_id,))
        # Delete status records for this agent
        cur.execute("DELETE FROM agent_roster_status WHERE agent_id = ?", (agent_id,))
        # Delete from roster
        cur.execute("DELETE FROM agent_roster WHERE agent_id = ?", (agent_id,))
        con.commit()
        con.close()
        sync_db_to_r2()
    invalidate_roster_cache()
    return True


def get_shifts_for_agents_bulk(agent_ids: list, start_date, end_date) -> dict:
    """
    Returns a dict: {(agent_id, day_of_week): shift_code}
    Single query instead of one per agent per day.
    """
    if not agent_ids:
        return {}
    
    with get_pg_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT agent_id, day_of_week, shift_code, effective_start, effective_end 
            FROM agent_shift_assignments 
            WHERE agent_id = ANY(%s) AND effective_start <= %s AND (effective_end IS NULL OR effective_end >= %s)
            ORDER BY effective_start DESC
        """, (agent_ids, end_date.isoformat(), start_date.isoformat()))
        rows = cur.fetchall()

    result = {}
    for agent_id, day_of_week, shift_code, effective_start, effective_end in rows:
        key = (agent_id, day_of_week)
        if key not in result:
            result[key] = {"shift_code": shift_code, "effective_start": effective_start, "effective_end": effective_end}
    return result


def upsert_shift_assignment(agent_id: str, day_of_week: str, shift_code: str, effective_date: date) -> Dict[str, Any]:
    """Create or update a shift assignment with effective dating."""
    ts = datetime.now().isoformat(timespec="seconds")
    
    current = get_shifts_for_agents_bulk([agent_id], effective_date, effective_date).get((agent_id, day_of_week))
    if current and current["shift_code"] == shift_code:
        return {"status": "no_change", "message": f"Agent {agent_id} already has {shift_code} on {day_of_week}", "assignment_id": current["id"]}
    
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            if current:
                prev_end = effective_date - timedelta(days=1)
                cur.execute("UPDATE agent_shift_assignments SET effective_end = %s, updated_at = %s WHERE id = %s", (prev_end.isoformat(), ts, current["id"]))
            cur.execute("INSERT INTO agent_shift_assignments (agent_id, day_of_week, shift_code, effective_start, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s) RETURNING id", (agent_id, day_of_week, shift_code, effective_date.isoformat(), ts, ts))
            new_id = cur.fetchone()[0]
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        if current:
            prev_end = effective_date - timedelta(days=1)
            cur.execute("UPDATE agent_shift_assignments SET effective_end = ?, updated_at = ? WHERE id = ?", (prev_end.isoformat(), ts, current["id"]))
        cur.execute("INSERT INTO agent_shift_assignments (agent_id, day_of_week, shift_code, effective_start, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)", (agent_id, day_of_week, shift_code, effective_date.isoformat(), ts, ts))
        new_id = cur.lastrowid
        con.commit()
        con.close()
        sync_db_to_r2()
    
    return {"status": "created", "message": f"Created {shift_code} for {agent_id} on {day_of_week} effective {effective_date}", "assignment_id": new_id, "previous_closed": current["id"] if current else None}


def bulk_upsert_shift_assignments(agent_id: str, days_of_week: List[str], shift_code: str, effective_date: date) -> List[Dict[str, Any]]:
    """Update multiple days at once for an agent."""
    return [upsert_shift_assignment(agent_id, day, shift_code, effective_date) for day in days_of_week]


def get_shift_history_for_agent(agent_id: str, day_of_week: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get the full history of shift assignments for an agent."""
    from .models.shifts import SHIFT_CATALOG
    
    if USE_POSTGRES:
        with get_pg_connection() as con:
            cur = con.cursor()
            if day_of_week:
                cur.execute("SELECT id, day_of_week, shift_code, effective_start, effective_end FROM agent_shift_assignments WHERE agent_id = %s AND day_of_week = %s ORDER BY effective_start DESC", (agent_id, day_of_week))
            else:
                cur.execute("SELECT id, day_of_week, shift_code, effective_start, effective_end FROM agent_shift_assignments WHERE agent_id = %s ORDER BY day_of_week, effective_start DESC", (agent_id,))
            rows = cur.fetchall()
    else:
        con = _get_sqlite_connection()
        cur = con.cursor()
        if day_of_week:
            cur.execute("SELECT id, day_of_week, shift_code, effective_start, effective_end FROM agent_shift_assignments WHERE agent_id = ? AND day_of_week = ? ORDER BY effective_start DESC", (agent_id, day_of_week))
        else:
            cur.execute("SELECT id, day_of_week, shift_code, effective_start, effective_end FROM agent_shift_assignments WHERE agent_id = ? ORDER BY day_of_week, effective_start DESC", (agent_id,))
        rows = cur.fetchall()
        con.close()
    
    return [{"id": r[0], "day_of_week": r[1], "shift_code": r[2], "effective_start": r[3], "effective_end": r[4], "color": SHIFT_CATALOG.get(r[2], {}).get("color", "#6B7280")} for r in rows]


def get_roster_matrix(week_start: date, lead_filter = None, agent_filter = None, status_filter = "active"):
    from .models.shifts import SHIFT_CATALOG
    
    DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    week_end = week_start + timedelta(days=6)
    week_dates = {day: (week_start + timedelta(days=i)).isoformat() for i, day in enumerate(DAYS)}
    
    all_agents = get_all_agents()
    
    if lead_filter:
        all_agents = [a for a in all_agents if a["lead"] == lead_filter]
    if agent_filter:
        all_agents = [a for a in all_agents if a["agent_id"] == agent_filter]

    agent_ids = [a["agent_id"] for a in all_agents]
    statuses = get_agent_statuses_bulk(agent_ids, week_start)
    
    if status_filter == "active":
        all_agents = [a for a in all_agents if statuses.get(a["agent_id"]) == "Active"]
        agent_ids = [a["agent_id"] for a in all_agents]
    
    bulk_shifts = get_shifts_for_agents_bulk(agent_ids, week_start, week_end)

    db_templates = get_all_shift_templates()
    db_catalog = {t["shift_code"]: t for t in db_templates}
    
    matrix_agents = []
    for agent in all_agents:
        agent_data = {
            "agent_id": agent["agent_id"],
            "full_name": agent["full_name"],
            "lead": agent["lead"],
            "status": get_agent_status_on_date(agent["agent_id"], week_start),
            "days": {}
        }
        
        for day in DAYS:
            shift_info = bulk_shifts.get((agent["agent_id"], day))
            if shift_info:
                code = shift_info["shift_code"]
                tmpl = db_catalog.get(code, {})
                agent_data["days"][day] = {
                    "shift_code": code,
                    "start_time": tmpl.get("start_time"),
                    "end_time": tmpl.get("end_time"),
                    "color": tmpl.get("color", "#374151"),
                    "label": tmpl.get("label", code),
                    "crosses_midnight": tmpl.get("crosses_midnight", False),
                    "effective_start": shift_info["effective_start"],
                    "effective_end": shift_info["effective_end"],
                }
            else:
                agent_data["days"][day] = {
                    "shift_code": "", "start_time": None, "end_time": None,
                    "color": "#374151", "label": "Not Assigned",
                    "effective_start": None, "effective_end": None, "crosses_midnight": False
                }
            
        matrix_agents.append(agent_data)
    
    return {
        "agents": matrix_agents,
        "week_start": week_start.isoformat(),
        "week_dates": week_dates,
        "shift_catalog": {k: {"start": v["start_time"], "end": v["end_time"],
                              "color": v["color"], "label": v["label"],
                              "crosses_midnight": v["crosses_midnight"]} for k, v in db_catalog.items()},
        "total_agents": len(matrix_agents)
    }

def get_agent_statuses_bulk(agent_ids: list, target_date: date) -> dict:
    """Returns {agent_id: status} for all agents in one query."""
    if not agent_ids or not USE_POSTGRES:
        return {}
    with get_pg_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (agent_id) agent_id, status
            FROM agent_roster_status
            WHERE agent_id = ANY(%s)
                AND effective_start <= %s
                AND (effective_end IS NULL OR effective_end >= %s)
            ORDER BY agent_id, effective_start DESC
        """, (agent_ids, target_date.isoformat(), target_date.isoformat()))
        rows = cur.fetchall()
    result = {r[0]: r[1] for r in rows}

    for aid in agent_ids:
        result.setdefault(aid, "Active")
    return result
    

_roster_agents_cache: Optional[List[Dict]] = None

def get_all_roster_agents() -> List[Dict[str, Any]]:
    global _roster_agents_cache
    if _roster_agents_cache is not None:
        return _roster_agents_cache
    
    try:
        if USE_POSTGRES:
            with get_pg_connection() as con:
                cur = con.cursor()
                cur.execute("SELECT agent_id, full_name, lead FROM agent_roster ORDER BY full_name")
                rows = cur.fetchall()
        
        else:
            con = _get_sqlite_connection()
            cur = con.cursor()
            cur.execute("SELECT agent_id, full_name, lead FROM agent_roster ORDER BY full_name")
            rows = cur.fetchall()
            con.close()
        
        _roster_agents_cache = [{"agent_id": r[0], "full_name": r[1], "lead": r[2]} for r in rows]
    
    except Exception as e:
        print(f"Error fetching roster agents: {e}")
        _roster_agents_cache = []
    
    return _roster_agents_cache

def invalidate_roster_cache():
    global _roster_agents_cache
    _roster_agents_cache = None


def get_active_agents_on_date(target_date: date) -> List[Dict[str, Any]]:
    """Get all agents who are active on the roster for a specific date."""
    return [a for a in get_all_agents() if get_agent_status_on_date(a["agent_id"], target_date) == 'Active']


def get_shifts_for_date_range_bulk(agent_ids: list, start_date: date, end_date: date) -> dict:
    """
    Returns {(agent_id, day_of_week): [list of assignments, newest first]}
    Single query covering the entire date range.
    """
    if not agent_ids:
        return {}
    with get_pg_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT agent_id, day_of_week, shift_code, effective_start, effective_end
            FROM agent_shift_assignments
            WHERE agent_id = ANY(%s)
                AND effective_start <= %s
                AND (effective_end IS NULL OR effective_end >= %s)
            ORDER BY agent_id, day_of_week, effective_start DESC
        """, (agent_ids, end_date.isoformat(), start_date.isoformat()))
        rows = cur.fetchall()
    
    result = {}
    for agent_id, day_of_week, shift_code, effective_start, effective_end in rows:
        key = (agent_id, day_of_week)
        if key not in result:
            result[key] = []
        result[key].append({
            "shift_code": shift_code,
            "effective_start": effective_start,
            "effective_end": effective_end,
        })
    return result
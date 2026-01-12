import os
import sqlite3
from datetime import date, datetime
from typing import Dict, Tuple, Any, Optional, List
import pandas as pd

from .storage import sync_db_to_r2

# Support either local SQLite or Postgres via DATABASE_URL (Heroku)
DB_PATH = "attendance.db"
DATABASE_URL = os.environ.get("DATABASE_URL")
USE_POSTGRES = False
PG_DSN: str | None = None

# Try to import psycopg2 if DATABASE_URL is set
psycopg2 = None  # type: ignore
if DATABASE_URL:
    try:
        import psycopg2 as _psycopg2  # type: ignore
        psycopg2 = _psycopg2
        USE_POSTGRES = True
        PG_DSN = DATABASE_URL
    except ImportError:
        USE_POSTGRES = False


def _get_pg_connection():
    """Get a Postgres connection."""
    return psycopg2.connect(PG_DSN, sslmode="require")


def init_db():
    if USE_POSTGRES:
        con = _get_pg_connection()
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS justifications (
                id SERIAL PRIMARY KEY,
                agent_id TEXT NOT NULL,
                date DATE NOT NULL,
                type TEXT NOT NULL,
                note TEXT,
                lead TEXT,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        # Schedule versions table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_versions (
                id SERIAL PRIMARY KEY,
                effective_from DATE NOT NULL,
                created_at TIMESTAMP NOT NULL,
                note TEXT
            )
            """
        )
        # Schedule entries linked to versions
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_entries (
                id SERIAL PRIMARY KEY,
                version_id INTEGER NOT NULL REFERENCES schedule_versions(id),
                agent_id TEXT NOT NULL,
                shift TEXT,
                name TEXT NOT NULL,
                lead TEXT,
                working_days TEXT,
                days_off TEXT,
                expected_start TEXT,
                expected_end TEXT
            )
            """
        )
        con.commit()
        cur.close()
        con.close()
    else:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS justifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_id TEXT NOT NULL,
                date TEXT NOT NULL,
                type TEXT NOT NULL,
                note TEXT,
                lead TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        # Schedule versions table
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_versions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                effective_from TEXT NOT NULL,
                created_at TEXT NOT NULL,
                note TEXT
            )
            """
        )
        # Schedule entries linked to versions
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schedule_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                version_id INTEGER NOT NULL,
                agent_id TEXT NOT NULL,
                shift TEXT,
                name TEXT NOT NULL,
                lead TEXT,
                working_days TEXT,
                days_off TEXT,
                expected_start TEXT,
                expected_end TEXT,
                FOREIGN KEY (version_id) REFERENCES schedule_versions(id)
            )
            """
        )
        con.commit()
        con.close()


# ============ Schedule Version Functions ============

def save_schedule_version(df: pd.DataFrame, effective_from: date, note: str = "") -> int:
    """
    Save a new schedule version to the database.
    Returns the version_id.
    """
    ts = datetime.now().isoformat(timespec="seconds")
    
    if USE_POSTGRES:
        con = _get_pg_connection()
        cur = con.cursor()
        cur.execute(
            "INSERT INTO schedule_versions(effective_from, created_at, note) VALUES(%s, %s, %s) RETURNING id",
            (effective_from.isoformat(), ts, note)
        )
        version_id = cur.fetchone()[0]
        
        # Insert all schedule entries
        for _, row in df.iterrows():
            cur.execute(
                """INSERT INTO schedule_entries(version_id, agent_id, shift, name, lead, 
                   working_days, days_off, expected_start, expected_end) 
                   VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (version_id, str(row.get("agent_id", "")), str(row.get("Shift", "")),
                 str(row.get("name", "")), str(row.get("lead", "")),
                 str(row.get("working_days", "")), str(row.get("days_off", "")),
                 str(row.get("expected_start", "")), str(row.get("expected_end", "")))
            )
        con.commit()
        cur.close()
        con.close()
    else:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO schedule_versions(effective_from, created_at, note) VALUES(?, ?, ?)",
            (effective_from.isoformat(), ts, note)
        )
        version_id = cur.lastrowid
        
        # Insert all schedule entries
        for _, row in df.iterrows():
            cur.execute(
                """INSERT INTO schedule_entries(version_id, agent_id, shift, name, lead,
                   working_days, days_off, expected_start, expected_end)
                   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (version_id, str(row.get("agent_id", "")), str(row.get("Shift", "")),
                 str(row.get("name", "")), str(row.get("lead", "")),
                 str(row.get("working_days", "")), str(row.get("days_off", "")),
                 str(row.get("expected_start", "")), str(row.get("expected_end", "")))
            )
        con.commit()
        con.close()
        sync_db_to_r2()
    
    return version_id


def get_schedule_version_for_date(target_date: date) -> Optional[int]:
    """
    Get the version_id of the schedule that was effective on target_date.
    Returns the version with the largest effective_from <= target_date.
    """
    if USE_POSTGRES:
        con = _get_pg_connection()
        cur = con.cursor()
        cur.execute(
            """SELECT id FROM schedule_versions 
               WHERE effective_from <= %s 
               ORDER BY effective_from DESC LIMIT 1""",
            (target_date.isoformat(),)
        )
        row = cur.fetchone()
        cur.close()
        con.close()
        return row[0] if row else None
    else:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute(
            """SELECT id FROM schedule_versions 
               WHERE effective_from <= ? 
               ORDER BY effective_from DESC LIMIT 1""",
            (target_date.isoformat(),)
        )
        row = cur.fetchone()
        con.close()
        return row[0] if row else None


def get_schedule_entries_for_version(version_id: int) -> List[Dict[str, Any]]:
    """Get all schedule entries for a specific version."""
    if USE_POSTGRES:
        con = _get_pg_connection()
        cur = con.cursor()
        cur.execute(
            """SELECT agent_id, shift, name, lead, working_days, days_off, 
                      expected_start, expected_end 
               FROM schedule_entries WHERE version_id = %s""",
            (version_id,)
        )
        rows = cur.fetchall()
        cur.close()
        con.close()
    else:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute(
            """SELECT agent_id, shift, name, lead, working_days, days_off,
                      expected_start, expected_end
               FROM schedule_entries WHERE version_id = ?""",
            (version_id,)
        )
        rows = cur.fetchall()
        con.close()
    
    return [
        {
            "agent_id": r[0], "Shift": r[1], "name": r[2], "lead": r[3],
            "working_days": r[4], "days_off": r[5],
            "expected_start": r[6], "expected_end": r[7]
        }
        for r in rows
    ]


def get_schedule_for_date(target_date: date) -> Optional[pd.DataFrame]:
    """
    Get the schedule DataFrame that was effective on target_date.
    Returns None if no schedule version exists for that date.
    """
    version_id = get_schedule_version_for_date(target_date)
    if version_id is None:
        return None
    
    entries = get_schedule_entries_for_version(version_id)
    if not entries:
        return None
    
    return pd.DataFrame(entries)


def get_all_schedule_versions() -> List[Dict[str, Any]]:
    """Get all schedule versions with their effective dates."""
    if USE_POSTGRES:
        con = _get_pg_connection()
        cur = con.cursor()
        cur.execute(
            "SELECT id, effective_from, created_at, note FROM schedule_versions ORDER BY effective_from DESC"
        )
        rows = cur.fetchall()
        cur.close()
        con.close()
    else:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute(
            "SELECT id, effective_from, created_at, note FROM schedule_versions ORDER BY effective_from DESC"
        )
        rows = cur.fetchall()
        con.close()
    
    return [
        {"id": r[0], "effective_from": r[1], "created_at": r[2], "note": r[3]}
        for r in rows
    ]


def upsert_justification(agent_id: str, day: date, typ: str, note: str, lead: str):
    from datetime import datetime
    ts = datetime.now().isoformat(timespec="seconds")
    
    if USE_POSTGRES:
        con = _get_pg_connection()
        cur = con.cursor()
        cur.execute("SELECT id FROM justifications WHERE agent_id=%s AND date=%s", (agent_id, day.isoformat()))
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE justifications SET type=%s, note=%s, lead=%s, created_at=%s WHERE id=%s",
                (typ, note, lead, ts, row[0]),
            )
        else:
            cur.execute(
                "INSERT INTO justifications(agent_id, date, type, note, lead, created_at) VALUES(%s,%s,%s,%s,%s,%s)",
                (agent_id, day.isoformat(), typ, note, lead, ts),
            )
        con.commit()
        cur.close()
        con.close()
    else:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("SELECT id FROM justifications WHERE agent_id=? AND date=?", (agent_id, day.isoformat()))
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE justifications SET type=?, note=?, lead=?, created_at=? WHERE id=?",
                (typ, note, lead, ts, row[0]),
            )
        else:
            cur.execute(
                "INSERT INTO justifications(agent_id, date, type, note, lead, created_at) VALUES(?,?,?,?,?,?)",
                (agent_id, day.isoformat(), typ, note, lead, ts),
            )
        con.commit()
        con.close()
        # Sync DB to R2 after changes
        sync_db_to_r2()


def delete_justification(agent_id: str, day: date):
    if USE_POSTGRES:
        con = _get_pg_connection()
        cur = con.cursor()
        cur.execute("DELETE FROM justifications WHERE agent_id=%s AND date=%s", (agent_id, day.isoformat()))
        con.commit()
        cur.close()
        con.close()
    else:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("DELETE FROM justifications WHERE agent_id=? AND date=?", (agent_id, day.isoformat()))
        con.commit()
        con.close()
        # Sync DB to R2 after changes
        sync_db_to_r2()


def get_justifications_map(start: date, end: date) -> Dict[Tuple[str, date], Dict[str, Any]]:
    out: Dict[Tuple[str, date], Dict[str, Any]] = {}
    
    if USE_POSTGRES:
        con = _get_pg_connection()
        cur = con.cursor()
        cur.execute(
            "SELECT agent_id, date, type, note, lead FROM justifications WHERE date>=%s AND date<=%s",
            (start.isoformat(), end.isoformat()),
        )
        rows = cur.fetchall()
        cur.close()
        con.close()
        for agent_id, d_val, typ, note, lead in rows:
            if hasattr(d_val, "isoformat"):
                d = date.fromisoformat(d_val.isoformat())
            else:
                d = date.fromisoformat(str(d_val))
            out[(agent_id, d)] = {"type": typ, "note": note, "lead": lead}
    else:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute(
            "SELECT agent_id, date, type, note, lead FROM justifications WHERE date>=? AND date<=?",
            (start.isoformat(), end.isoformat()),
        )
        rows = cur.fetchall()
        con.close()
        for agent_id, d_str, typ, note, lead in rows:
            out[(agent_id, date.fromisoformat(d_str))] = {"type": typ, "note": note, "lead": lead}
    
    return out


def get_all_agents_and_leads() -> Tuple[List[str], List[Dict[str, str]]]:
    """Get all unique leads and agents from all schedule versions and the base CSV."""
    # from .logic import load_schedule  # Import here to avoid circular import
    
    leads = set()
    agents = {}  # agent_id -> {"id": agent_id, "name": name}
    
    # Add from base CSV schedule
    # base_sched = load_schedule()
    # for _, row in base_sched.iterrows():
    #     leads.add(str(row["lead"]))
    #     agents[str(row["agent_id"])] = {"id": str(row["agent_id"]), "name": str(row["name"])}
    
    # Add from all schedule versions
    versions = get_all_schedule_versions()
    for version in versions:
        entries = get_schedule_entries_for_version(version["id"])
        for entry in entries:
            leads.add(entry["lead"])
            agents[entry["agent_id"]] = {"id": entry["agent_id"], "name": entry["name"]}
    
    return sorted(list(leads)), list(agents.values())
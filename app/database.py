import os
import sqlite3
from datetime import date
from typing import Dict, Tuple, Any

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
        con.commit()
        con.close()


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
import sqlite3
from datetime import date
from typing import Dict, Tuple, Any, List

DB_PATH = "attendance.db"
import os
DATABASE_URL = os.environ.get("DATABASE_URL")
USE_POSTGRES = False
PG_DSN = None

if DATABASE_URL:
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor

        USE_POSTGRES = True
        PG_DSN = DATABASE_URL
    except Exception:
        USE_POSTGRES = False

def init_db():
    if USE_POSTGRES:
        import psycopg2

        con = psycopg2.connect(PG_DSN, sslmode="require")
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
                date TEXT NOT NULL,          -- YYYY-MM-DD
                type TEXT NOT NULL,          -- 'A'|'J'|'V'|'U'|'D'
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
        import psycopg2

        con = psycopg2.connect(PG_DSN, sslmode="require")
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

def delete_justification(agent_id: str, day: date):
    if USE_POSTGRES:
        import psycopg2

        con = psycopg2.connect(PG_DSN, sslmode="require")
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

def get_justifications_map(start: date, end: date) -> Dict[Tuple[str, date], Dict[str, Any]]:
    out: Dict[Tuple[str, date], Dict[str, Any]] = {}
    if USE_POSTGRES:
        import psycopg2

        con = psycopg2.connect(PG_DSN, sslmode="require")
        cur = con.cursor()
        cur.execute(
            "SELECT agent_id, date, type, note, lead FROM justifications WHERE date>=%s AND date<=%s",
            (start.isoformat(), end.isoformat()),
        )
        rows = cur.fetchall()
        cur.close()
        con.close()
        for agent_id, d_val, typ, note, lead in rows:
            # d_val may be a date object
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
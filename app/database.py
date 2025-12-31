import sqlite3
from datetime import date
from typing import Dict, Tuple, Any, List

DB_PATH = "attendance.db"

def init_db():
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
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("DELETE FROM justifications WHERE agent_id=? AND date=?", (agent_id, day.isoformat()))
    con.commit()
    con.close()

def get_justifications_map(start: date, end: date) -> Dict[Tuple[str, date], Dict[str, Any]]:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "SELECT agent_id, date, type, note, lead FROM justifications WHERE date>=? AND date<=?",
        (start.isoformat(), end.isoformat()),
    )
    rows = cur.fetchall()
    con.close()
    out: Dict[Tuple[str, date], Dict[str, Any]] = {}
    for agent_id, d_str, typ, note, lead in rows:
        out[(agent_id, date.fromisoformat(d_str))] = {"type": typ, "note": note, "lead": lead}
    return out
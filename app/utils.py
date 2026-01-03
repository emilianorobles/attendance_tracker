from datetime import date, time, datetime
from typing import Optional

def parse_hhmm_or_hhmmss(s: str) -> Optional[time]:
    """Convierte 'HH:MM' o 'HH:MM:SS' a objeto time; devuelve None si está vacío o inválido."""
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(s, fmt).time()
        except ValueError:
            pass
    return None

def weekday_token(d: date) -> str:
    """Regresa 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'."""
    return d.strftime("%a")

def parse_days_list(s: str) -> list[str]:
    """Convierte 'Mon, Tue, Wed, Thu, Fri' -> ['Mon','Tue','Wed','Thu','Fri']"""
    return [x.strip() for x in str(s).split(",") if str(x).strip()]
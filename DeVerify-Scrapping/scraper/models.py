from dataclasses import dataclass, asdict
from typing import List, Optional
from datetime import datetime

@dataclass
class HackathonItem:
    id: str
    name: str
    startDate: str  # ISO-ish
    endDate: str
    status: str  # "upcoming" | "running" | "ended"
    testHack: bool
    tags: Optional[List[str]] = None

    def to_dict(self):
        return asdict(self)

def parse_iso_date(s: str) -> str:
    # Try to parse common formats; keep as-is if parsing fails
    s = s.strip()
    for fmt in ("%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(s, fmt).isoformat()
        except Exception:
            pass
    return s  # fallback

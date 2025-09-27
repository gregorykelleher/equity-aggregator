# authoritative_feeds/__init__.py

from .sec import fetch_equity_records as fetch_equity_records_sec
from .turquoise import fetch_equity_records as fetch_equity_records_turquoise
from .xetra import fetch_equity_records as fetch_equity_records_xetra

__all__ = [
    "fetch_equity_records_turquoise",
    "fetch_equity_records_xetra",
    "fetch_equity_records_sec",
]

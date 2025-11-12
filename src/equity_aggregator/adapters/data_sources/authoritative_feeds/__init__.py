# authoritative_feeds/__init__.py

from .lseg import fetch_equity_records as fetch_equity_records_lseg
from .sec import fetch_equity_records as fetch_equity_records_sec
from .xetra import fetch_equity_records as fetch_equity_records_xetra

__all__ = [
    "fetch_equity_records_lseg",
    "fetch_equity_records_xetra",
    "fetch_equity_records_sec",
]

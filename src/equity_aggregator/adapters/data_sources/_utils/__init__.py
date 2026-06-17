# _utils/__init__.py

from ._client import make_client
from .backoff import backoff_delays
from .dedup import deduplicate_records

__all__ = [
    "backoff_delays",
    "deduplicate_records",
    "make_client",
]

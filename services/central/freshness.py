"""This is the central server view of recently stored bus states"""
import os
from datetime import UTC, datetime

FRESH_SECONDS = float(os.getenv("CENTRAL_FRESHNESS_SECONDS", "30"))

def freshness(last_updated, now=None):
    """'current' or 'stale' by when central stored the state; None if never reported"""
    if last_updated is None:
        return None
    if isinstance(last_updated, str):
        last_updated = datetime.fromisoformat(last_updated)
    if last_updated.tzinfo is None:
        last_updated = last_updated.replace(tzinfo=UTC)
    age = ((now or datetime.now(UTC)) - last_updated).total_seconds()
    return "current" if age < FRESH_SECONDS else "stale"


from datetime import UTC, datetime, timedelta

from services.central.freshness import FRESH_SECONDS, freshness

NOW = datetime(2026, 11, 22, 12, 0, tzinfo=UTC)


def test_recent_state_is_current():
    assert freshness(NOW - timedelta(seconds=5), now=NOW) == "current"


def test_old_state_is_stale():
    assert freshness(NOW - timedelta(seconds=FRESH_SECONDS + 1), now=NOW) == "stale"


def test_never_reported_bus_has_no_freshness():
    assert freshness(None, now=NOW) is None


def test_sqlite_text_timestamp_is_read_as_utc():
    assert freshness("2026-11-22 11:59:55", now=NOW) == "current"
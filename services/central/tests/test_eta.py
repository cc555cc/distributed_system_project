from types import SimpleNamespace

from services.central.eta import DEFAULT_SPEED, distance_km, eta_bus

# Four stops due north of each other; the bus is halfway between S2 and S3.
STOPS = [("S1", 43.00, -79.0), ("S2", 43.01, -79.0), ("S3", 43.02, -79.0), ("S4", 43.03, -79.0)]


class FakeConnection:
    """Answer eta_bus's two queries in order: the bus row, then the route stops."""

    def __init__(self, bus, stops):
        self.results = [bus, stops]

    def execute(self, sql, params):
        result = self.results.pop(0)
        return SimpleNamespace(fetchone=lambda: result, fetchall=lambda: result)


def test_eta_only_covers_stops_ahead_and_follows_the_route():
    etas = eta_bus(FakeConnection((43.015, -79.0, "S3", "R1", 0, 30.0), STOPS), "A1")
    assert [e["stop_id"] for e in etas] == ["S3", "S4"]
    to_s3 = distance_km(43.015, -79.0, 43.02, -79.0)
    to_s4 = to_s3 + distance_km(43.02, -79.0, 43.03, -79.0)
    assert etas[0]["eta_minutes"] == round(to_s3 / 30 * 60, 1)
    assert etas[1]["eta_minutes"] == round(to_s4 / 30 * 60, 1)


def test_stopped_bus_uses_default_speed():
    etas = eta_bus(FakeConnection((43.015, -79.0, "S3", "R1", 0, 0.0), STOPS), "A1")
    to_s3 = distance_km(43.015, -79.0, 43.02, -79.0)
    assert etas[0]["eta_minutes"] == round(to_s3 / DEFAULT_SPEED * 60, 1)


def test_unknown_next_stop_gives_no_etas():
    assert eta_bus(FakeConnection((43.015, -79.0, "X9", "R1", 0, 30.0), STOPS), "A1") == []


def test_unknown_bus_returns_none():
    assert eta_bus(FakeConnection(None, STOPS), "nope") is None
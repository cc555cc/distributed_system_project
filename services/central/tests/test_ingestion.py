"""Regression checks for the central changes merged into ali; no live database."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from services.central.ingestion_check import check_update, next_stop_check


@pytest.mark.parametrize("current,following,expected", [
    (None, "S1", True), (None, "other", False),
    ("S1", "S1", True), ("S1", "S2", True),
    ("S1", "S3", False), ("S2", "S1", False),
])
def test_next_stop_membership_and_progress(current, following, expected):
    connection = Mock()
    connection.execute.return_value.fetchall.return_value = [("S1",), ("S2",), ("S3",)]
    assert next_stop_check(connection, "302", current, following, 0) is expected


def test_region_rejection_stops_before_route_lookup():
    connection = Mock()
    connection.execute.return_value.fetchone.return_value = None
    assert check_update(connection, "bus-302-1", "east", Mock()) is not None
    assert connection.execute.call_count == 1


def test_first_update_checks_stop_membership():
    connection = Mock()
    responses = [Mock(), Mock(), Mock()]
    responses[0].fetchone.return_value = (1,)
    responses[1].fetchone.return_value = ("302", None)
    responses[2].fetchall.return_value = [("S1",), ("S2",)]
    connection.execute.side_effect = responses
    state = SimpleNamespace(next_stop_id="other", direction_id=0)
    assert "invalid" in check_update(connection, "bus-302-1", "west", state)


@pytest.fixture
def bus_states(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test_unused")
    from services.central.routers import bus_states
    return bus_states


@pytest.mark.parametrize("rowcount,expected", [(1, (True, None)), (0, (False, "stale update"))])
def test_apply_update_returns_consistent_result(bus_states, rowcount, expected):
    state = bus_states.BusStateUpdate(lat=43.9, lon=-78.9, speed=20, direction_id=0,
                                     bus_status="travelling", occupancy=10, sequence_number=2, lamport=2)
    connection = Mock()
    connection.execute.return_value.rowcount = rowcount
    with patch.object(bus_states.psycopg, "connect") as connect, patch.object(bus_states, "check_update", return_value=None):
        connect.return_value.__enter__.return_value = connection
        assert bus_states.apply_bus_state("bus-302-1", "west", state) == expected


def test_rejected_ingestion_does_not_write(bus_states):
    connection = Mock()
    with patch.object(bus_states.psycopg, "connect") as connect, patch.object(bus_states, "check_update", return_value="wrong region"):
        connect.return_value.__enter__.return_value = connection
        assert bus_states.apply_bus_state("bus-302-1", "east", SimpleNamespace(edge_metadata=None)) == (False, "wrong region")
    connection.execute.assert_not_called()


def test_mqtt_passes_region_to_central_ingestion(bus_states):
    from services.messaging import mqtt_client
    payload = dict(lat=43.9, lon=-78.9, speed=20, direction_id=0,
                   bus_status="travelling", occupancy=10, sequence_number=2, lamport=2)
    import json
    message = SimpleNamespace(topic="transit/west/bus/bus-302-1/state", payload=json.dumps(payload).encode())
    with patch.object(mqtt_client, "apply_bus_state", return_value=(True, None)) as apply:
        mqtt_client.on_message(None, None, message)
    assert apply.call_args.args[:2] == ("bus-302-1", "west")


def test_list_endpoints_use_expected_route_fields(bus_states):
    from services.central.routers import routes, stops
    for module, function in [(routes, routes.get_all_route), (stops, stops.get_all_stops)]:
        connection = Mock()
        connection.execute.return_value.fetchall.return_value = [{"id": "example"}]
        with patch.object(module.psycopg, "connect") as connect:
            connect.return_value.__enter__.return_value = connection
            assert function() == [{"id": "example"}]
        assert "row_factory" in connect.call_args.kwargs
        assert "routes_id" not in connection.execute.call_args.args[0]

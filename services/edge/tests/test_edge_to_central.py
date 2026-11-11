"""Check the outgoing boundary against Carson's actual model, without networking."""

import json
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError

from services.edge.communication.edge_to_central import EdgeToCentral
from services.edge.config import EdgeConfig
from services.edge.validation.bus_registry import BusAssignment, BusRegistry


@pytest.fixture
def sender(monkeypatch):
    # Importing Carson's router needs a URL, but these tests never query a database.
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/test_unused")
    from services.central.routers.bus_states import BusStateUpdate

    registry = BusRegistry({"bus-302-1": BusAssignment("west", "302", "DURHAM", "edge-west-1")})
    return EdgeToCentral(EdgeConfig("DURHAM", "west", "edge-west-1"), registry, BusStateUpdate)


@pytest.fixture
def payload():
    # Values are explicitly supplied; the sender must not invent status or clocks.
    return dict(lat=43.9, lon=-78.9, speed=30.0, direction_id=0,
                next_stop_id="166", bus_status="travelling", occupancy=40,
                sequence_number=2, lamport=3)


def connected_client():
    client = Mock()
    client.is_connected.return_value = True
    receipt = client.publish.return_value
    receipt.rc = 0
    receipt.is_published.return_value = True
    return client


# ==================== RECHECK THE CENTRAL CONTRACT BEFORE SENDING ====================
def test_sends_validated_payload_to_central_topic(sender, payload):
    client = connected_client()
    sender._client = client
    sender.send("bus-302-1", payload)
    args, kwargs = client.publish.call_args
    assert args[0] == "transit/west/bus/bus-302-1/state"
    assert json.loads(args[1]) == payload
    assert kwargs == {"qos": 1, "retain": False}


@pytest.mark.parametrize("missing", ["bus_status", "lamport", "occupancy"])
def test_missing_required_value_cannot_publish(sender, payload, missing):
    sender._client = connected_client()
    del payload[missing]
    with pytest.raises(ValidationError):
        sender.send("bus-302-1", payload)
    sender._client.publish.assert_not_called()


@pytest.mark.parametrize("changes", [{"occupancy": "40"}, {"lamport": True}])
def test_wrong_types_cannot_publish(sender, payload, changes):
    sender._client = connected_client()
    payload.update(changes)
    with pytest.raises(ValidationError):
        sender.send("bus-302-1", payload)
    sender._client.publish.assert_not_called()


def test_unknown_bus_cannot_publish(sender, payload):
    sender._client = connected_client()
    with pytest.raises(ValueError):
        sender.send("unknown", payload)
    sender._client.publish.assert_not_called()


@pytest.mark.parametrize("changes", [{"latitude": 43.9}, {"city_id": "DURHAM"}, {"speed": float("nan")}])
def test_unsupported_fields_and_non_json_values_rejected(sender, payload, changes):
    sender._client = connected_client()
    payload.update(changes)
    with pytest.raises(ValueError):
        sender.send("bus-302-1", payload)
    sender._client.publish.assert_not_called()


# ==================== CONNECTION AND BROKER RECEIPT FAILURES ====================
def test_send_requires_connection(sender, payload):
    with pytest.raises(ConnectionError):
        sender.send("bus-302-1", payload)


def test_publish_failure_and_timeout_are_reported(sender, payload):
    sender._client = connected_client()
    sender._client.publish.return_value.rc = 4
    with pytest.raises(ConnectionError):
        sender.send("bus-302-1", payload)
    sender._client.publish.return_value.rc = 0
    sender._client.publish.return_value.is_published.return_value = False
    with pytest.raises(TimeoutError):
        sender.send("bus-302-1", payload)


def test_connection_lifecycle(sender):
    with patch("paho.mqtt.client.Client") as factory:
        client = factory.return_value
        sender.connect("localhost")
        client.connect.assert_called_once_with("localhost", 1883, keepalive=60)
        client.loop_start.assert_called_once()
        with pytest.raises(RuntimeError):
            sender.connect("localhost")
        sender.close()
        client.disconnect.assert_called_once()
        client.loop_stop.assert_called_once()
        assert sender._client is None

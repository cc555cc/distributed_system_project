"""Check the outgoing boundary against Carson's actual model, without networking."""

import json
from unittest.mock import Mock, patch

import pytest
from pydantic import ValidationError

from services.edge.communication.edge_to_central import EdgeToCentral
from services.edge.config import EdgeConfig, NetworkConfig, RouteDefinition
from services.edge.processing.processor import EdgeProcessor
from services.edge.transformation.central_mapper import map_bus_update
from shared.schema.bus_state import BusStateUpdate
from services.edge.validation.bus_registry import BusAssignment, BusRegistry


@pytest.fixture
def sender():
    # Use the shared contract directly. Edge tests do not import central services.

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


# ==================== EDGE MAPPING ONLY: NO CENTRAL DATABASE OR API ====================
@pytest.fixture
def accepted_output(sender):
    network = NetworkConfig(sender.registry, (
        RouteDefinition("DURHAM", "west", "302", ("166", "167")),
    ))
    processor = EdgeProcessor(sender.config, network)
    source = dict(bus_id="bus-302-1", region_id="west", route_id="302",
                  latitude=43.9, longitude=-78.9, speed_kmh=30.0,
                  passengers=40, capacity=50, sequence_number=2,
                  timestamp="2026-11-11T12:00:00Z", bus_status="travelling",
                  lamport_clock=3, direction_id=0, next_stop_id="166")
    return processor.process(source).output


def test_mapper_produces_the_expected_outgoing_fields(sender, accepted_output):
    payload = map_bus_update(accepted_output, "bus-302-1", "passengers")
    encoded = sender.validate_payload("bus-302-1", payload)
    checked = BusStateUpdate.model_validate_json(encoded, strict=True)
    assert (checked.lat, checked.lon, checked.speed) == (43.9, -78.9, 30.0)
    assert (checked.occupancy, checked.lamport, checked.sequence_number) == (40, 3, 2)
    assert checked.bus_status == "travelling"
    assert checked.edge_metadata.city_id == "DURHAM"
    assert checked.edge_metadata.region_id == "west"
    assert checked.edge_metadata.occupancy_percent == 80.0
    assert checked.edge_metadata.statistics.scope == "edge_assigned_buses"


@pytest.mark.parametrize("field", ["bus_status", "lamport_clock"])
def test_mapper_does_not_invent_missing_information(accepted_output, field):
    del accepted_output["buses"][0][field]
    with pytest.raises(ValueError):
        map_bus_update(accepted_output, "bus-302-1", "passengers")


def test_mapper_respects_explicit_occupancy_units(accepted_output):
    assert map_bus_update(accepted_output, "bus-302-1", "percent")["occupancy"] == 80
    with pytest.raises(ValueError):
        map_bus_update(accepted_output, "bus-302-1", "unknown")

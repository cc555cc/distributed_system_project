"""EDGE-01 acceptance tests 01–12: subscriber -> validator -> edge ingress.

These invoke the actual subscriber callback with a simulated MQTT message.
They need neither a live MQTT broker nor Paho installed.
"""

import json
from dataclasses import dataclass

import pytest

from services.edge.edge_server import EdgeIngress
from services.edge.config import EdgeConfig, NetworkConfig, RouteDefinition
from services.edge.processing.processor import EdgeProcessor, ProcessingStatus
from services.edge.validation.bus_registry import BusRegistry, BusAssignment
from services.edge.communication.mqtt_subscriber import TelemetrySubscriber


@dataclass
class FakeMQTTMessage:
    """Minimal substitute for a broker-delivered Paho message."""

    topic: str
    payload: bytes


@pytest.fixture
def subscriber() -> TelemetrySubscriber:
    """Build a subscriber connected to the temporary North registry."""
    return TelemetrySubscriber(region_id="north", edge_id="north-edge-test")


@pytest.fixture
def edge() -> EdgeIngress:
    """Real EDGE-01 receiver (not a mock), initially empty."""
    network = NetworkConfig(BusRegistry({
        "BUS-001": BusAssignment("north", "R1", "CITY-A", "north-edge-test"),
        "BUS-002": BusAssignment("north", "R1", "CITY-A", "north-edge-test"),
    }), (RouteDefinition("CITY-A", "north", "R1", ("STOP-01", "STOP-02", "STOP-03")),))
    return EdgeIngress(EdgeProcessor(EdgeConfig("CITY-A", "north", "north-edge-test"), network))


@pytest.fixture
def valid_data() -> dict[str, object]:
    """A bus payload compatible with the agreed temporary field names."""
    return {
        "bus_id": "BUS-001",
        "route_id": "R1",
        "region_id": "north",
        "latitude": 43.945,
        "longitude": -78.895,
        "speed_kmh": 35.0,
        "passengers": 32,
        "capacity": 50,
        "sequence_number": 1,
        "timestamp": "2026-11-10T15:00:00Z",
    }


def publish_to_callback(
    subscriber: TelemetrySubscriber,
    data: dict[str, object] | bytes,
    *,
    topic: str = "transit/buses/north/BUS-001/telemetry",
) -> None:
    """Simulate the exact Paho on_message entry point without a broker."""
    raw = data if isinstance(data, bytes) else json.dumps(data).encode("utf-8")
    subscriber.on_message(
        None,
        None,
        FakeMQTTMessage(topic=topic, payload=raw),
    )


def assert_never_reaches_edge(
    subscriber: TelemetrySubscriber, edge: EdgeIngress
) -> None:
    """Check the boundary, not just the validator's return value."""
    assert edge.drain_pending(subscriber) == []
    assert edge.accepted == []
    assert subscriber.messages.empty()


# TEST-01: A valid bus message must reach the real EDGE-01 receiver.
def test_01_valid_data_reaches_edge(
    subscriber: TelemetrySubscriber,
    edge: EdgeIngress,
    valid_data: dict[str, object],
) -> None:
    publish_to_callback(subscriber, valid_data)
    assert [r.status for r in edge.drain_pending(subscriber)] == [ProcessingStatus.ACCEPTED]
    assert edge.accepted[0]["bus_id"] == "BUS-001"


# TEST-02: Broken JSON must be rejected before the processing queue.
def test_02_invalid_json_is_rejected(
    subscriber: TelemetrySubscriber, edge: EdgeIngress
) -> None:
    publish_to_callback(subscriber, b'{"bus_id":')
    assert_never_reaches_edge(subscriber, edge)


# TEST-03: Every mandatory field must be present.
@pytest.mark.parametrize(
    "field",
    [
        "bus_id", "route_id", "region_id", "latitude", "longitude",
        "speed_kmh", "passengers", "capacity", "sequence_number", "timestamp",
    ],
)
def test_03_missing_required_field_is_rejected(
    subscriber: TelemetrySubscriber,
    edge: EdgeIngress,
    valid_data: dict[str, object],
    field: str,
) -> None:
    valid_data.pop(field)
    publish_to_callback(subscriber, valid_data)
    assert_never_reaches_edge(subscriber, edge)


# TEST-04: GPS coordinates must remain within geographic bounds.
@pytest.mark.parametrize(
    ("field", "value"),
    [("latitude", 91), ("latitude", -91), ("longitude", 181), ("longitude", -181)],
)
def test_04_invalid_gps_is_rejected(
    subscriber: TelemetrySubscriber,
    edge: EdgeIngress,
    valid_data: dict[str, object],
    field: str,
    value: int,
) -> None:
    valid_data[field] = value
    publish_to_callback(subscriber, valid_data)
    assert_never_reaches_edge(subscriber, edge)


# TEST-05: Speed cannot be negative or of the wrong type.
@pytest.mark.parametrize("value", [-1, "fast", True, 250])
def test_05_invalid_speed_is_rejected(
    subscriber: TelemetrySubscriber,
    edge: EdgeIngress,
    valid_data: dict[str, object],
    value: object,
) -> None:
    valid_data["speed_kmh"] = value
    publish_to_callback(subscriber, valid_data)
    assert_never_reaches_edge(subscriber, edge)


# TEST-06: Passenger readings cannot violate bus capacity.
@pytest.mark.parametrize(
    ("passengers", "capacity"), [(60, 50), (-1, 50), (1, 0)]
)
def test_06_invalid_occupancy_is_rejected(
    subscriber: TelemetrySubscriber,
    edge: EdgeIngress,
    valid_data: dict[str, object],
    passengers: int,
    capacity: int,
) -> None:
    valid_data["passengers"] = passengers
    valid_data["capacity"] = capacity
    publish_to_callback(subscriber, valid_data)
    assert_never_reaches_edge(subscriber, edge)


# TEST-07: A fabricated bus ID cannot pass the temporary registry.
def test_07_unknown_bus_id_is_rejected(
    subscriber: TelemetrySubscriber,
    edge: EdgeIngress,
    valid_data: dict[str, object],
) -> None:
    valid_data["bus_id"] = "BUS-999"
    publish_to_callback(
        subscriber,
        valid_data,
        topic="transit/buses/north/BUS-999/telemetry",
    )
    assert_never_reaches_edge(subscriber, edge)


# TEST-08: A registered bus cannot claim a different region.
def test_08_bus_with_wrong_region_is_rejected(
    valid_data: dict[str, object],
) -> None:
    south_subscriber = TelemetrySubscriber("south", "south-edge-test")
    network = NetworkConfig(BusRegistry({}), ())
    south_edge = EdgeIngress(EdgeProcessor(EdgeConfig("CITY-A", "south", "south-edge-test"), network))
    valid_data["region_id"] = "south"  # Topic and payload agree, registry does not.
    publish_to_callback(
        south_subscriber,
        valid_data,
        topic="transit/buses/south/BUS-001/telemetry",
    )
    assert_never_reaches_edge(south_subscriber, south_edge)


# TEST-08 (route variant): even a known bus cannot claim an unassigned route.
def test_08_registered_bus_wrong_route_is_rejected(
    subscriber: TelemetrySubscriber,
    edge: EdgeIngress,
    valid_data: dict[str, object],
) -> None:
    valid_data["route_id"] = "R9"
    publish_to_callback(subscriber, valid_data)
    assert_never_reaches_edge(subscriber, edge)


# TEST-09: A message on BUS-002's topic cannot impersonate BUS-001.
def test_09_topic_bus_mismatch_is_rejected(
    subscriber: TelemetrySubscriber,
    edge: EdgeIngress,
    valid_data: dict[str, object],
) -> None:
    publish_to_callback(
        subscriber,
        valid_data,
        topic="transit/buses/north/BUS-002/telemetry",
    )
    assert_never_reaches_edge(subscriber, edge)


# TEST-10: Timestamps and sequence numbers must be well formed.
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("sequence_number", -1),
        ("sequence_number", "two"),
        ("sequence_number", True),
        ("timestamp", "not-a-time"),
        ("timestamp", "2026-11-10T15:00:00"),
        ("timestamp", None),
    ],
)
def test_10_invalid_timestamp_or_sequence_is_rejected(
    subscriber: TelemetrySubscriber,
    edge: EdgeIngress,
    valid_data: dict[str, object],
    field: str,
    value: object,
) -> None:
    valid_data[field] = value
    publish_to_callback(subscriber, valid_data)
    assert_never_reaches_edge(subscriber, edge)


# TEST-11: Several independently valid messages can reach edge ingress.
def test_11_multiple_valid_messages_reach_edge(
    subscriber: TelemetrySubscriber,
    edge: EdgeIngress,
    valid_data: dict[str, object],
) -> None:
    publish_to_callback(subscriber, valid_data)
    second = dict(valid_data)
    second["sequence_number"] = 2
    publish_to_callback(subscriber, second)
    assert [r.status for r in edge.drain_pending(subscriber)] == [ProcessingStatus.ACCEPTED] * 2
    assert [m["sequence_number"] for m in edge.accepted] == [2]


# TEST-12: One invalid message must not block the next valid message.
def test_12_invalid_followed_by_valid(
    subscriber: TelemetrySubscriber,
    edge: EdgeIngress,
    valid_data: dict[str, object],
) -> None:
    broken = dict(valid_data)
    broken["latitude"] = 1000
    publish_to_callback(subscriber, broken)
    publish_to_callback(subscriber, valid_data)
    assert [r.status for r in edge.drain_pending(subscriber)] == [ProcessingStatus.ACCEPTED]
    assert edge.accepted[0]["latitude"] == 43.945


# Sequence freshness and cache tests live in test_edge_processing.py.
#
# TODO TEST-14 (Julian/integration): start a real Mosquitto broker,
# disconnect it, restart it and confirm subscriber reconnection,
# topic resubscription and resumed receipt with real MQTT networking.


@pytest.mark.parametrize("topic", [
    "other/buses/north/BUS-001/telemetry",
    "transit/buses/south/BUS-001/telemetry",
    "transit/buses/north//telemetry",
    "transit/buses/north/BUS-001/state",
    "transit/buses/north/BUS-001/telemetry/extra",
])
def test_invalid_topic_rejected(subscriber, edge, valid_data, topic):
    publish_to_callback(subscriber, valid_data, topic=topic)
    assert_never_reaches_edge(subscriber, edge)


@pytest.mark.parametrize("raw", [b"\xff", b"[]", b"null", b"x" * 65537], ids=["utf8", "array", "null", "oversize"])
def test_invalid_payload_rejected(subscriber, edge, raw):
    publish_to_callback(subscriber, raw)
    assert_never_reaches_edge(subscriber, edge)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), 10**1000], ids=["nan", "infinity", "huge-int"])
def test_extreme_number_rejected(subscriber, edge, valid_data, value):
    valid_data["latitude"] = value
    publish_to_callback(subscriber, valid_data)
    assert_never_reaches_edge(subscriber, edge)


def test_full_queue_rejects_without_displacing_valid_message(valid_data):
    subscriber = TelemetrySubscriber("north", "bounded-edge", queue_size=1)
    raw = json.dumps(valid_data).encode()
    topic = "transit/buses/north/BUS-001/telemetry"
    assert subscriber.handle_raw_message(topic, raw)
    assert not subscriber.handle_raw_message(topic, raw)
    assert subscriber.messages.qsize() == 1


def test_reconnect_resubscribes_and_failed_connection_does_not():
    from types import SimpleNamespace
    from unittest.mock import Mock
    subscriber = TelemetrySubscriber("north", "edge-test")
    client = Mock()
    client.subscribe.return_value = (0, 1)
    subscriber.on_connect(client, None, None, SimpleNamespace(is_failure=True), None)
    client.subscribe.assert_not_called()
    for _ in range(2):
        subscriber.on_connect(client, None, None, SimpleNamespace(is_failure=False), None)
    assert client.subscribe.call_count == 2
    client.subscribe.assert_called_with(subscriber.topic_filter, qos=1)

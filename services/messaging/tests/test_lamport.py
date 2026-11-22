"""MQTT-03: Lamport send/receive rules, per-bus sequences and ordering metadata.

Offline tests. The bus -> edge -> central chain uses the real simulator,
forwarder and central MQTT callback with an in-memory transport and no database.
"""

import json
from datetime import datetime, timedelta
from itertools import pairwise
from threading import Thread
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from services.data_simulation.generate_data import generate_messages
from services.edge.communication.edge_to_central import CentralForwarder, EdgeToCentral
from services.edge.config import EdgeConfig, NetworkConfig, RouteDefinition
from services.edge.edge_server import EdgeIngress
from services.edge.processing.processor import EdgeProcessor, ProcessingStatus
from services.edge.validation.bus_registry import BusAssignment, BusRegistry
from services.messaging.lamport import LamportClock
from shared.schema.bus_state import BusStateUpdate


# ==================== THE CLOCK ITSELF ====================
def test_send_rule_increments_by_one():
    clock = LamportClock()
    assert [clock.tick(), clock.tick(), clock.tick()] == [1, 2, 3]
    assert clock.value == 3


@pytest.mark.parametrize(
    "local, received, expected",
    [
        (0, 0, 1),  # equal clocks still move forward
        (5, 2, 6),  # receiver ahead: local + 1
        (2, 9, 10),  # sender ahead: jump past the sender
        (7, 7, 8),
    ],
)
def test_receive_rule_is_max_plus_one(local, received, expected):
    clock = LamportClock(local)
    assert clock.receive(received) == expected
    assert clock.value == expected


@pytest.mark.parametrize("bad", [-1, 1.5, "3", None, True])
def test_invalid_clock_values_are_rejected_without_changing_the_clock(bad):
    clock = LamportClock(4)
    with pytest.raises(ValueError):
        clock.receive(bad)
    with pytest.raises(ValueError):
        LamportClock(bad)
    assert clock.value == 4


def test_concurrent_events_in_one_process_get_unique_stamps():
    clock = LamportClock()
    stamps = []

    def work():
        for index in range(500):
            stamps.append(clock.tick() if index % 2 else clock.receive(0))

    threads = [Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(set(stamps)) == len(stamps) == 4000
    assert clock.value == 4000


# ==================== BUS: SEND RULE AND ORDERING METADATA ====================
def bus_messages(**settings):
    return list(generate_messages("302", 0, "bus-302-1", update_seconds=0, **settings))


def test_every_bus_message_carries_increasing_ordering_metadata():
    messages = bus_messages(max_updates=25)
    assert len(messages) == 25
    for earlier, later in pairwise(messages):
        assert later["sequence_number"] == earlier["sequence_number"] + 1
        assert later["lamport_clock"] > earlier["lamport_clock"]
    for message in messages:
        assert message["sequence_number"] >= 1 and message["lamport_clock"] >= 1
        assert datetime.fromisoformat(message["timestamp"]).utcoffset() == timedelta(0)


def test_dropped_sensor_readings_leave_gaps_but_never_reorder():
    messages = bus_messages(max_updates=40, sensor_failure_rate=0.5)
    assert 0 < len(messages) < 40
    sequences = [message["sequence_number"] for message in messages]
    clocks = [message["lamport_clock"] for message in messages]
    assert sequences == sorted(set(sequences)) and clocks == sorted(set(clocks))


def test_message_received_by_the_bus_raises_its_later_send_stamps():
    clock = LamportClock()
    stream = generate_messages(
        "302", 0, "bus-302-1", update_seconds=0, max_updates=4, clock=clock
    )
    assert [next(stream)["lamport_clock"], next(stream)["lamport_clock"]] == [1, 2]
    # A command stamped 50 arrives on this bus (the consumer is MQTT-05).
    assert clock.receive(50) == 51
    after = next(stream)
    assert after["lamport_clock"] == 52
    assert after["sequence_number"] == 3  # sequence counts readings only


# ==================== BUS -> EDGE -> CENTRAL ====================
class Transport:
    """Hand published bytes straight to central's real MQTT callback."""

    def __init__(self, callback):
        self.callback, self.sent, self.fail = callback, [], False

    def is_connected(self):
        return not self.fail

    def publish(self, topic, payload, **kwargs):
        self.sent.append(json.loads(payload))
        self.callback(
            None, None, SimpleNamespace(topic=topic, payload=payload.encode())
        )
        return SimpleNamespace(
            rc=0, wait_for_publish=lambda **kwargs: None, is_published=lambda: True
        )


@pytest.fixture
def chain(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/offline")
    from services.messaging import mqtt_client

    monkeypatch.setattr(mqtt_client, "clock", LamportClock())
    monkeypatch.setattr(
        mqtt_client, "apply_bus_state", lambda bus, region, state: (True, None)
    )
    registry = BusRegistry(
        {bus: BusAssignment("north", "R1", "CITY-A", "edge-a") for bus in ("A1", "A2")}
    )
    config = EdgeConfig("CITY-A", "north", "edge-a")
    network = NetworkConfig(
        registry, (RouteDefinition("CITY-A", "north", "R1", ("S1", "S2")),)
    )
    sender = EdgeToCentral(config, registry, BusStateUpdate)
    transport = Transport(mqtt_client.on_message)
    sender._client = transport
    forwarder = CentralForwarder(
        EdgeIngress(EdgeProcessor(config, network, clock=lambda: 0.0)),
        sender,
        "passengers",
    )
    return SimpleNamespace(
        forwarder=forwarder, transport=transport, central=mqtt_client
    )


def reading(bus="A1", sequence=1, lamport=1):
    return {
        "bus_id": bus,
        "city_id": "CITY-A",
        "region_id": "north",
        "route_id": "R1",
        "latitude": 43.9,
        "longitude": -78.9,
        "speed_kmh": 30.0,
        "passengers": 40,
        "capacity": 50,
        "sequence_number": sequence,
        "timestamp": "2026-11-11T12:00:00Z",
        "bus_status": "travelling",
        "lamport_clock": lamport,
        "direction_id": 0,
        "current_stop_id": "S1",
        "next_stop_id": "S2",
        "sensor_status": "healthy",
    }


def forward(chain, source):
    result = chain.forwarder.process(source)
    chain.forwarder.flush_one()
    return result


def test_stamps_increase_along_bus_edge_central(chain):
    assert forward(chain, reading(lamport=7)).status == ProcessingStatus.ACCEPTED
    sent = chain.transport.sent[-1]
    assert sent["lamport"] == 7  # bus's stamp is preserved
    assert sent["edge_metadata"]["edge_lamport"] == 9  # receive -> 8, send -> 9
    assert chain.central.clock.value == 10  # receive: max(0, 9) + 1


def test_receiver_ahead_of_sender_keeps_its_own_clock(chain):
    forward(chain, reading("A1", lamport=100))
    first = chain.transport.sent[-1]["edge_metadata"]["edge_lamport"]
    central_after_first = chain.central.clock.value
    # A2's own clock is far behind, yet its update is still ordered after A1's.
    forward(chain, reading("A2", lamport=1))
    second = chain.transport.sent[-1]
    assert second["lamport"] == 1
    assert second["edge_metadata"]["edge_lamport"] > first
    assert chain.central.clock.value > central_after_first


def test_stale_and_duplicate_deliveries_advance_the_edge_clock_but_are_not_sent(chain):
    forward(chain, reading(sequence=5, lamport=5))
    before = chain.forwarder.clock.value
    for repeat in (reading(sequence=5, lamport=5), reading(sequence=4, lamport=4)):
        assert forward(chain, repeat).status != ProcessingStatus.ACCEPTED
    assert chain.forwarder.clock.value == before + 2  # two receive events
    assert len(chain.transport.sent) == 1
    forward(chain, reading(sequence=6, lamport=6))
    stamps = [sent["edge_metadata"]["edge_lamport"] for sent in chain.transport.sent]
    assert stamps == sorted(set(stamps))


def test_retried_delivery_keeps_its_original_stamp(chain):
    chain.transport.fail = True
    forward(chain, reading(lamport=3))
    assert not chain.transport.sent
    stamped = chain.forwarder.pending["A1"][0]["edge_metadata"]["edge_lamport"]
    chain.transport.fail = False
    assert chain.forwarder.flush_one()
    assert chain.transport.sent[-1]["edge_metadata"]["edge_lamport"] == stamped
    assert chain.forwarder.clock.value == stamped  # the retry is not a new event


def test_central_applies_the_receive_rule_even_when_it_rejects_the_update(
    chain, monkeypatch
):
    monkeypatch.setattr(
        chain.central,
        "apply_bus_state",
        lambda bus, region, state: (False, "stale update"),
    )
    forward(chain, reading(lamport=20))
    assert (
        chain.central.clock.value
        == chain.transport.sent[-1]["edge_metadata"]["edge_lamport"] + 1
    )


def test_central_falls_back_to_the_bus_stamp_for_senders_without_an_edge_stamp(chain):
    forward(chain, reading(lamport=30))
    payload = chain.transport.sent[-1]
    del payload["edge_metadata"]["edge_lamport"]
    chain.central.clock = LamportClock(4)
    chain.central.on_message(
        None,
        None,
        SimpleNamespace(
            topic="transit/north/bus/A1/state", payload=json.dumps(payload).encode()
        ),
    )
    assert chain.central.clock.value == 31


def test_invalid_payload_does_not_move_the_central_clock(chain):
    chain.central.on_message(
        None,
        None,
        SimpleNamespace(
            topic="transit/north/bus/A1/state", payload=b'{"lamport": 999}'
        ),
    )
    assert chain.central.clock.value == 0


def test_contract_rejects_an_edge_stamp_that_does_not_follow_the_bus_stamp(chain):
    forward(chain, reading(lamport=10))
    payload = chain.transport.sent[-1]
    for impossible in (10, 3):
        payload["edge_metadata"]["edge_lamport"] = impossible
        with pytest.raises(ValidationError):
            BusStateUpdate.model_validate(payload, strict=True)

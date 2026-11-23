"""MQTT-04: delayed and duplicate deliveries never move accepted state backward.

Offline. A seeded faulty channel duplicates and reorders messages on both hops:
bus -> edge (real subscriber, processor and forwarder) and edge -> central
(real MQTT callback and SQL statements, run on SQLite). The live reconnection
checks are in test_live_reconnect.py.
"""

import json
import random
import sqlite3
from types import SimpleNamespace

import pytest
from psycopg.types.json import Jsonb

from services.edge.communication.data_to_edge import TelemetrySubscriber
from services.edge.communication.edge_to_central import CentralForwarder, EdgeToCentral
from services.edge.config import EdgeConfig, NetworkConfig, RouteDefinition
from services.edge.edge_server import EdgeIngress
from services.edge.processing.processor import EdgeProcessor, ProcessingStatus
from services.edge.validation.bus_registry import BusAssignment, BusRegistry
from shared.schema.bus_state import BusStateUpdate

BUSES = ("A1", "A2")


class SqliteCentral:
    """Run central's real statements on SQLite (placeholders, time and JSON adapted)."""

    def __init__(self, path):
        self.connection = sqlite3.connect(path)

    def __enter__(self):
        return self

    def __exit__(self, kind, error, trace):
        self.connection.commit() if error is None else self.connection.rollback()
        self.connection.close()

    def execute(self, sql, parameters=()):
        values = tuple(
            json.dumps(p.obj) if isinstance(p, Jsonb) else p for p in parameters
        )
        return self.connection.execute(
            sql.replace("%s", "?")
            .replace("now()", "CURRENT_TIMESTAMP")
            .replace(" FOR UPDATE", ""),
            values,
        )


class Outbox:
    """Collect what the edge publishes so the test controls when central gets it."""

    def __init__(self):
        self.sent = []

    def is_connected(self):
        return True

    def publish(self, topic, payload, **kwargs):
        self.sent.append((topic, payload))
        return SimpleNamespace(
            rc=0, wait_for_publish=lambda **kwargs: None, is_published=lambda: True
        )


@pytest.fixture
def system(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/offline")
    from services.central.routers import bus_states
    from services.messaging import mqtt_client

    path = tmp_path / "central.sqlite"
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE routes(route_id TEXT PRIMARY KEY, region_id TEXT);
            CREATE TABLE buses(bus_id TEXT PRIMARY KEY, route_id TEXT);
            CREATE TABLE route_stops(route_id TEXT, direction_id INTEGER, position INTEGER, stop_id TEXT);
            CREATE TABLE bus_states(bus_id TEXT PRIMARY KEY, lat REAL, lon REAL, speed REAL,
                direction_id INTEGER, current_stop_id TEXT, next_stop_id TEXT, bus_status TEXT, occupancy INTEGER,
                sequence_number INTEGER, lamport INTEGER, edge_metadata TEXT, last_updated TEXT);
            INSERT INTO routes VALUES ('R1','north');
            INSERT INTO buses VALUES ('A1','R1'), ('A2','R1');
            INSERT INTO route_stops VALUES ('R1',0,0,'S1'),('R1',0,1,'S2');
        """)
    monkeypatch.setattr(
        bus_states.database.psycopg, "connect", lambda url, **kwargs: SqliteCentral(path)
    )
    registry = BusRegistry(
        {bus: BusAssignment("north", "R1", "CITY-A", "edge-a") for bus in BUSES}
    )
    config = EdgeConfig("CITY-A", "north", "edge-a")
    network = NetworkConfig(
        registry, (RouteDefinition("CITY-A", "north", "R1", ("S1", "S2")),)
    )
    receiver = EdgeIngress(EdgeProcessor(config, network, clock=lambda: 0.0))
    sender = EdgeToCentral(config, registry, BusStateUpdate)
    sender._client = outbox = Outbox()
    forwarder = CentralForwarder(receiver, sender, "passengers")
    subscriber = TelemetrySubscriber(
        "north", "edge-a", city_id="CITY-A", registry=registry
    )

    def to_edge(source):
        """One MQTT delivery to the edge. Return the processing status."""
        topic = f"transit/buses/north/{source['bus_id']}/telemetry"
        assert subscriber.handle_raw_message(topic, json.dumps(source).encode())
        item = subscriber.messages.get_nowait()
        try:
            status = forwarder.process(item).status
            forwarder.flush_one()
            return status
        finally:
            subscriber.messages.task_done()

    def edge_sequences():
        return {
            entry["bus_id"]: entry["sequence_number"] for entry in receiver.accepted
        }

    def to_central(topic, payload):
        mqtt_client.on_message(
            None, None, SimpleNamespace(topic=topic, payload=payload.encode())
        )

    def central_row(bus):
        with sqlite3.connect(path) as connection:
            row = connection.execute(
                "SELECT sequence_number, lamport, edge_metadata FROM bus_states WHERE bus_id = ?",
                (bus,),
            ).fetchone()
        return (
            None
            if row is None
            else {"sequence": row[0], "lamport": row[1], "metadata": json.loads(row[2])}
        )

    return SimpleNamespace(
        to_edge=to_edge,
        edge_sequences=edge_sequences,
        outbox=outbox,
        to_central=to_central,
        central_row=central_row,
    )


def reading(bus, sequence):
    # Lamport stamps differ from sequences so a mix-up between them would show.
    return {
        "bus_id": bus,
        "city_id": "CITY-A",
        "region_id": "north",
        "route_id": "R1",
        "latitude": 43.9 + sequence / 10000,
        "longitude": -78.9,
        "speed_kmh": 30.0,
        "passengers": sequence % 40,
        "capacity": 50,
        "sequence_number": sequence,
        "timestamp": "2026-11-11T12:00:00Z",
        "bus_status": "travelling",
        "lamport_clock": sequence * 3 + 100,
        "direction_id": 0,
        "current_stop_id": "S1",
        "next_stop_id": "S2",
        "sensor_status": "healthy",
    }


def faulty(messages, rng, delay):
    """At-least-once channel: 1-3 copies of each message, each held up to `delay` slots."""
    arrivals = []
    for index, message in enumerate(messages):
        for _ in range(rng.choice((1, 1, 2, 3))):
            arrivals.append((index + rng.uniform(0, delay), message))
    arrivals.sort(key=lambda arrival: arrival[0])
    return [message for _, message in arrivals]


# ==================== BUS -> EDGE ====================
@pytest.mark.parametrize("seed", range(5))
@pytest.mark.parametrize(
    "delay", [0, 4, 60]
)  # duplicates only / short delays / heavy reordering
def test_edge_state_never_moves_backward_and_every_fault_is_identified(
    system, seed, delay
):
    rng = random.Random(seed)
    sent = [reading(bus, sequence) for sequence in range(1, 21) for bus in BUSES]
    newest, counts = {}, {status: 0 for status in ProcessingStatus}
    deliveries = faulty(sent, rng, delay)
    for message in deliveries:
        bus, sequence = message["bus_id"], message["sequence_number"]
        status = system.to_edge(message)
        counts[status] += 1
        # Identification: the status names exactly what kind of delivery this was.
        if sequence > newest.get(bus, 0):
            assert status == ProcessingStatus.ACCEPTED
            newest[bus] = sequence
        elif sequence == newest[bus]:
            assert status == ProcessingStatus.DUPLICATE
        else:
            assert status == ProcessingStatus.STALE
        assert system.edge_sequences() == newest  # never backward, never ahead
    assert newest == {bus: 20 for bus in BUSES}
    assert sum(counts.values()) == len(deliveries) > len(sent)
    assert (
        counts[ProcessingStatus.DUPLICATE] + counts[ProcessingStatus.STALE]
        == len(deliveries) - counts[ProcessingStatus.ACCEPTED]
    )
    # Only accepted readings were forwarded; rejected deliveries produce no new send.
    assert len(system.outbox.sent) == counts[ProcessingStatus.ACCEPTED]


def test_readings_held_back_by_an_outage_arrive_stale_and_keep_their_metadata(system):
    for sequence in (1, 2, 3):
        assert system.to_edge(reading("A1", sequence)) == ProcessingStatus.ACCEPTED
    held = [
        reading("A1", sequence) for sequence in (4, 5, 6)
    ]  # queued at the bus while offline
    assert system.to_edge(reading("A1", 7)) == ProcessingStatus.ACCEPTED
    for late in held:
        # Delivered unchanged: original sequence and Lamport stamp identify it as old.
        assert late["lamport_clock"] < reading("A1", 7)["lamport_clock"]
        assert system.to_edge(late) == ProcessingStatus.STALE
        assert system.edge_sequences() == {"A1": 7}
    forwarded = [
        json.loads(payload)["sequence_number"] for _, payload in system.outbox.sent
    ]
    assert forwarded == [1, 2, 3, 7]


# ==================== EDGE -> CENTRAL ====================
@pytest.mark.parametrize("seed", range(5))
@pytest.mark.parametrize("delay", [0, 4, 60])
def test_central_state_never_moves_backward_and_keeps_matching_clock_data(
    system, seed, delay
):
    for sequence in range(1, 16):
        for bus in BUSES:
            assert system.to_edge(reading(bus, sequence)) == ProcessingStatus.ACCEPTED
    by_key = {}
    for topic, payload in system.outbox.sent:
        body = json.loads(payload)
        by_key[topic.split("/")[3], body["sequence_number"]] = body
    newest = {}
    for topic, payload in faulty(system.outbox.sent, random.Random(seed), delay):
        bus, sequence = topic.split("/")[3], json.loads(payload)["sequence_number"]
        system.to_central(topic, payload)
        newest[bus] = max(newest.get(bus, 0), sequence)
        row = system.central_row(bus)
        assert row["sequence"] == newest[bus]
        # Sequence and both Lamport stamps stored together belong to one message.
        original = by_key[bus, row["sequence"]]
        assert (
            row["lamport"]
            == original["lamport"]
            == reading(bus, row["sequence"])["lamport_clock"]
        )
        assert (
            row["metadata"]["edge_lamport"] == original["edge_metadata"]["edge_lamport"]
        )
    assert newest == {bus: 15 for bus in BUSES}

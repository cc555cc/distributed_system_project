"""Full code-path tests: local MQTT transport and SQLite, not a live broker/PG test."""

import json
import sqlite3
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from psycopg.types.json import Jsonb

from services.edge.communication.edge_to_central import CentralForwarder, EdgeToCentral
from services.edge.communication.mqtt_subscriber import TelemetrySubscriber
from services.edge.config import EdgeConfig, NetworkConfig, RouteDefinition
from services.edge.edge_server import EdgeIngress
from services.edge.processing.processor import EdgeProcessor, ProcessingStatus
from shared.schema.bus_state import BusStateUpdate


class TestDatabase:
    """Execute central's real queries with only placeholder/time/JSON adaptation."""

    __test__ = False

    def __init__(self, path, row_factory=None):
        self.connection = sqlite3.connect(path)
        self.dictionary_rows = row_factory is not None

    def __enter__(self):
        return self

    def __exit__(self, kind, error, trace):
        if error is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.connection.close()

    def execute(self, sql, parameters=()):
        params = tuple(json.dumps(p.obj) if isinstance(p, Jsonb) else p for p in parameters)
        cursor = self.connection.execute(sql.replace("%s", "?").replace("now()", "CURRENT_TIMESTAMP"), params)
        dictionary_rows = self.dictionary_rows

        class Cursor:
            rowcount = cursor.rowcount

            def fetchone(self):
                row = cursor.fetchone()
                if row is None or not dictionary_rows:
                    return row
                result = dict(zip([column[0] for column in cursor.description], row))
                if result.get("edge_metadata"):
                    result["edge_metadata"] = json.loads(result["edge_metadata"])
                return result

            def fetchall(self):
                return cursor.fetchall()

        return Cursor()


class LocalTransport:
    """Deliver bytes to the real central MQTT callback, without a network broker."""

    def __init__(self, callback):
        self.callback = callback
        self.sent = []
        self.fail = False

    def is_connected(self):
        return not self.fail

    def publish(self, topic, payload, **kwargs):
        self.sent.append((topic, json.loads(payload)))
        self.callback(None, None, SimpleNamespace(topic=topic, payload=payload.encode()))
        return SimpleNamespace(rc=0, wait_for_publish=lambda **kwargs: None, is_published=lambda: True)


@pytest.fixture
def pipeline(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/offline")
    from services.central.routers import bus_states
    from services.messaging import mqtt_client
    from services.central.main import app

    path = tmp_path / "central.sqlite"
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            CREATE TABLE routes(route_id TEXT PRIMARY KEY, region_id TEXT);
            CREATE TABLE buses(bus_id TEXT PRIMARY KEY, route_id TEXT);
            CREATE TABLE route_stops(route_id TEXT, direction_id INTEGER, position INTEGER, stop_id TEXT);
            CREATE TABLE bus_states(bus_id TEXT PRIMARY KEY, lat REAL, lon REAL, speed REAL,
                direction_id INTEGER, next_stop_id TEXT, bus_status TEXT, occupancy INTEGER,
                sequence_number INTEGER, lamport INTEGER, edge_metadata TEXT, last_updated TEXT);
            INSERT INTO routes VALUES ('R1','north');
            INSERT INTO buses VALUES ('A1','R1'), ('A2','R1');
            INSERT INTO route_stops VALUES ('R1',0,0,'S1'),('R1',0,1,'S2');
        """)
    monkeypatch.setattr(bus_states.psycopg, "connect", lambda url, **kwargs: TestDatabase(path, **kwargs))
    from services.edge.validation.bus_registry import BusRegistry, BusAssignment
    registry = BusRegistry({bus: BusAssignment("north", "R1", "CITY-A", "edge-a") for bus in ("A1", "A2")})
    network = NetworkConfig(registry, (RouteDefinition("CITY-A", "north", "R1", ("S1", "S2")),))
    config = EdgeConfig("CITY-A", "north", "edge-a")
    now = [0.0]
    receiver = EdgeIngress(EdgeProcessor(config, network, clock=lambda: now[0]))
    sender = EdgeToCentral(config, registry, BusStateUpdate)
    transport = LocalTransport(mqtt_client.on_message)
    sender._client = transport
    forwarder = CentralForwarder(receiver, sender, "passengers")
    subscriber = TelemetrySubscriber("north", "edge-a", city_id="CITY-A", registry=registry)
    return SimpleNamespace(subscriber=subscriber, forwarder=forwarder, transport=transport,
                           api=TestClient(app), now=now, receiver=receiver)


def telemetry(bus="A1", sequence=1):
    return dict(bus_id=bus, city_id="CITY-A", region_id="north", route_id="R1",
                latitude=43.9, longitude=-78.9, speed_kmh=30.0, passengers=40,
                capacity=50, sequence_number=sequence, timestamp="2026-11-11T12:00:00Z",
                bus_status="travelling", lamport_clock=sequence + 10, direction_id=0,
                current_stop_id="S1", next_stop_id="S2", sensor_status="healthy")


def deliver(pipeline, source):
    topic = f"transit/buses/north/{source['bus_id']}/telemetry"
    accepted = pipeline.subscriber.handle_raw_message(topic, json.dumps(source).encode())
    if not accepted:
        return None
    item = pipeline.subscriber.messages.get_nowait()
    try:
        result = pipeline.forwarder.process(item)
        pipeline.forwarder.flush_one()
        return result
    finally:
        pipeline.subscriber.messages.task_done()


# ==================== SOURCE -> QUEUE -> PROCESS -> CENTRAL SQL -> API ====================
@pytest.mark.parametrize("bus", ["A1", "A2"])
def test_all_fields_survive_the_full_code_path(pipeline, bus):
    source = telemetry(bus)
    assert deliver(pipeline, source).status == ProcessingStatus.ACCEPTED
    response = pipeline.api.get(f"/bus_states/{bus}")
    assert response.status_code == 200
    stored = response.json()
    assert stored["lat"] == source["latitude"]
    assert stored["lon"] == source["longitude"]
    assert stored["speed"] == source["speed_kmh"]
    assert stored["occupancy"] == source["passengers"]
    assert stored["lamport"] == source["lamport_clock"]
    assert stored["sequence_number"] == source["sequence_number"]
    assert stored["bus_status"] == source["bus_status"]
    metadata = stored["edge_metadata"]
    for key in ("city_id", "region_id", "route_id", "capacity", "passengers", "current_stop_id"):
        assert metadata[key] == source[key]
    assert metadata["source_timestamp"] == source["timestamp"]
    assert metadata["occupancy_percent"] == 80.0
    assert metadata["crowding_status"] == "high"
    assert metadata["statistics"]["scope"] == "edge_assigned_buses"
    assert metadata["statistics"]["active_buses"] == 1


def test_stale_duplicate_conflict_and_invalid_data_do_not_publish(pipeline):
    deliver(pipeline, telemetry(sequence=2))
    before = pipeline.api.get("/bus_states/A1").json()
    deliver(pipeline, telemetry(sequence=1))
    deliver(pipeline, telemetry(sequence=2))
    conflicting = telemetry(sequence=2)
    conflicting["passengers"] = 5
    deliver(pipeline, conflicting)
    invalid = telemetry(sequence=3)
    invalid["city_id"] = "CITY-B"
    assert deliver(pipeline, invalid) is None
    assert len(pipeline.transport.sent) == 1
    assert pipeline.api.get("/bus_states/A1").json() == before


def test_retry_keeps_only_newest_and_drops_expired_updates(pipeline):
    pipeline.transport.fail = True
    deliver(pipeline, telemetry(sequence=1))
    deliver(pipeline, telemetry(sequence=2))
    assert len(pipeline.forwarder.pending) == 1
    pipeline.transport.fail = False
    assert pipeline.forwarder.flush_one()
    assert pipeline.api.get("/bus_states/A1").json()["sequence_number"] == 2
    pipeline.transport.fail = True
    deliver(pipeline, telemetry(sequence=3))
    pipeline.now[0] = 30
    pipeline.transport.fail = False
    assert not pipeline.forwarder.flush_one()
    assert not pipeline.forwarder.pending
    assert pipeline.api.get("/bus_states/A1").json()["sequence_number"] == 2


@pytest.mark.parametrize("missing", ["bus_status", "lamport_clock"])
def test_required_source_fields_are_not_guessed(pipeline, missing):
    source = telemetry()
    del source[missing]
    with pytest.raises(ValueError):
        deliver(pipeline, source)
    assert not pipeline.transport.sent


def test_faulty_new_reading_supersedes_pending_healthy_reading(pipeline):
    pipeline.transport.fail = True
    deliver(pipeline, telemetry())
    faulty = telemetry(sequence=2)
    faulty["sensor_status"] = "faulty"
    with pytest.raises(ValueError):
        deliver(pipeline, faulty)
    assert not pipeline.forwarder.pending


def test_percent_mapping_requires_exact_integer(pipeline):
    pipeline.forwarder.occupancy_unit = "percent"
    deliver(pipeline, telemetry())
    assert pipeline.api.get("/bus_states/A1").json()["occupancy"] == 80
    fractional = telemetry(sequence=2)
    fractional.update(passengers=1, capacity=3)
    with pytest.raises(ValueError, match="fractional"):
        deliver(pipeline, fractional)


# ==================== THE ACTUAL AUTOMATIC RUNTIME LOOP ====================
def test_runtime_loop_forwards_and_marks_queue_complete(pipeline, monkeypatch):
    from services.edge.main import process_pending_messages
    source = telemetry()
    assert pipeline.subscriber.handle_raw_message("transit/buses/north/A1/telemetry", json.dumps(source).encode())
    original_get = pipeline.subscriber.messages.get
    calls = [0]
    def get_once(*args, **kwargs):
        calls[0] += 1
        if calls[0] > 1:
            raise KeyboardInterrupt
        return original_get(*args, **kwargs)
    monkeypatch.setattr(pipeline.subscriber.messages, "get", get_once)
    with pytest.raises(KeyboardInterrupt):
        process_pending_messages(pipeline.subscriber, pipeline.receiver, pipeline.forwarder)
    assert pipeline.subscriber.messages.unfinished_tasks == 0
    assert pipeline.api.get("/bus_states/A1").status_code == 200


def test_central_rechecks_region_route_and_types(pipeline):
    from shared.schema.bus_state import BusStateUpdate
    deliver(pipeline, telemetry())
    valid = pipeline.transport.sent[-1][1]
    wrong_region = json.loads(json.dumps(valid))
    wrong_region["edge_metadata"]["region_id"] = "wrong"
    wrong_route = json.loads(json.dumps(valid))
    wrong_route["edge_metadata"]["route_id"] = "wrong"
    wrong_type = json.loads(json.dumps(valid))
    wrong_type["occupancy"] = "40"
    for invalid in (wrong_region, wrong_route, wrong_type):
        invalid["sequence_number"] = 2
        pipeline.transport.publish("transit/north/bus/A1/state", json.dumps(invalid))
        assert pipeline.api.get("/bus_states/A1").json()["sequence_number"] == 1
    assert BusStateUpdate.model_validate(valid, strict=True).occupancy == 40


@pytest.mark.parametrize("field", ["city_id", "region_id", "edge_id", "route_id"])
def test_outgoing_metadata_must_match_registered_ownership(pipeline, field):
    deliver(pipeline, telemetry())
    payload = json.loads(json.dumps(pipeline.transport.sent[-1][1]))
    payload["edge_metadata"][field] = "wrong"
    with pytest.raises(ValueError, match="ownership"):
        pipeline.forwarder.sender.validate_payload("A1", payload)

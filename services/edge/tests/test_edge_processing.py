"""EDGE-02 acceptance cases; fake monotonic time and no broker/database needed."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime, timezone
import json

import pytest

from services.edge.config import EdgeConfig, NetworkConfig, RouteDefinition
from services.edge.processing.processor import EdgeProcessor, ProcessingStatus as Status
from services.edge.validation.bus_registry import BusAssignment, BusRegistry
from services.edge.validation.validator import parse_telemetry
from services.edge.communication.mqtt_subscriber import TelemetrySubscriber
from services.edge.edge_server import EdgeIngress


@pytest.fixture
def network():
    # Same region/route/stop names in two cities deliberately exercise isolation.
    assignments = {
        "A1": BusAssignment("north", "R1", "CITY-A", "edge-a"),
        "A2": BusAssignment("north", "R1", "CITY-A", "edge-a"),
        "A3": BusAssignment("north", "R2", "CITY-A", "edge-a"),
        "OTHER": BusAssignment("north", "R1", "CITY-A", "edge-other"),
        "B1": BusAssignment("north", "R1", "CITY-B", "edge-b"),
    }
    return NetworkConfig(BusRegistry(assignments), (
        RouteDefinition("CITY-A", "north", "R1", ("S1", "S2", "S3")),
        RouteDefinition("CITY-A", "north", "R2", ("T1", "T2")),
        RouteDefinition("CITY-B", "north", "R1", ("S1", "S2", "S3")),
    ))


@pytest.fixture
def clock():
    return [0.0]


@pytest.fixture
def processor(network, clock):
    return EdgeProcessor(EdgeConfig("CITY-A", "north", "edge-a", max_buses=2,
                                   freshness_seconds=10, expire_seconds=20), network,
                         clock=lambda: clock[0],
                         utc_now=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc))


def message(bus="A1", sequence=1, **changes):
    return dict(bus_id=bus, region_id="north", route_id="R1", latitude=43.9,
                longitude=-78.9, speed_kmh=30, passengers=40, capacity=50,
                sequence_number=sequence, timestamp="2026-01-01T00:00:00Z",
                **changes)


# ==================== TEST-01/02: NEW AND NEWER STATE ====================
def test_new_and_newer_state(processor):
    assert processor.process(message()).status == Status.ACCEPTED
    assert processor.process(message(sequence=2)).status == Status.ACCEPTED
    state = processor.snapshot()["buses"][0]
    assert state["sequence_number"] == 2
    assert state["speed_kmh"] == 30
    assert list(processor.cache.snapshot()) == [("CITY-A", "north", "A1")]


# ==================== TEST-03/04/05: FRESHNESS AND CONFLICT ====================
@pytest.mark.parametrize("sequence,changes,status", [
    (0, {}, Status.STALE), (1, {}, Status.DUPLICATE),
    (1, {"passengers": 20}, Status.CONFLICT),
])
def test_rejected_freshness_keeps_state(processor, sequence, changes, status):
    processor.process(message())
    before = processor.cache.snapshot()
    payload = message(sequence=sequence)
    payload.update(changes)
    result = processor.process(payload)
    assert result.status == status
    assert result.output is None
    assert processor.cache.snapshot() == before


# ==================== TEST-06/13: TWO CITIES USE THE SAME CODE ====================
def test_city_isolation(processor, network):
    other = EdgeProcessor(EdgeConfig("CITY-B", "north", "edge-b"), network)
    assert processor.process(message()).status == Status.ACCEPTED
    assert other.process(message("B1")).status == Status.ACCEPTED
    assert processor.process(message("B1")).status == Status.NOT_ASSIGNED
    assert other.process(message()).status == Status.NOT_ASSIGNED
    assert processor.snapshot()["buses"][0]["city_id"] == "CITY-A"
    assert other.snapshot()["buses"][0]["city_id"] == "CITY-B"


# ==================== TEST-07/18: REJECTIONS NEVER MUTATE STATE ====================
@pytest.mark.parametrize("changes,status", [
    ({"city_id": "CITY-B"}, Status.NOT_ASSIGNED),
    ({"region_id": "south"}, Status.NOT_ASSIGNED),
    ({"route_id": "R2"}, Status.INVALID_ROUTE),
    ({"current_stop_id": "X"}, Status.INVALID_ROUTE),
    ({"current_stop_id": "S2", "next_stop_id": "S1"}, Status.INVALID_ROUTE),
    ({"current_stop_id": "S1", "next_stop_id": "S3"}, Status.INVALID_ROUTE),
    ({"direction_id": 99}, Status.INVALID_ROUTE),
])
def test_invalid_relationship(processor, changes, status):
    processor.process(message())
    before = processor.cache.snapshot()
    payload = message(sequence=2)
    payload.update(changes)
    result = processor.process(payload)
    assert result.status == status and result.output is None
    assert processor.cache.snapshot() == before


# ==================== TEST-08: OCCUPANCY AND SENSOR BOUNDARIES ====================
@pytest.mark.parametrize("count,level", [(0, "low"), (24, "low"), (25, "medium"),
                                         (39, "medium"), (40, "high"), (50, "high")])
def test_occupancy_boundaries(processor, count, level):
    payload = message()
    payload["passengers"] = count
    result = processor.process(payload)
    bus = result.output["buses"][0]
    assert bus["occupancy_percent"] == count / 50 * 100
    assert bus["crowding_status"] == level


@pytest.mark.parametrize("sensor", ["faulty", "offline"])
def test_faulty_occupancy_is_unavailable(processor, sensor):
    processor.process(message(sensor_status=sensor))
    output = processor.snapshot()
    assert output["buses"][0]["occupancy_percent"] is None
    assert output["statistics"]["average_occupancy_percent"] is None
    assert output["statistics"]["active_buses"] == (sensor != "offline")


# ==================== TEST-09/17: LOCAL ASSIGNMENT-SCOPED STATISTICS ====================
def test_statistics_scope_and_ownership(processor):
    processor.process(message())
    payload = message("A2")
    payload["passengers"] = 20
    processor.process(payload)
    assert processor.process(message("OTHER")).status == Status.NOT_ASSIGNED
    stats = processor.snapshot()["statistics"]
    assert stats == {"scope": "edge_assigned_buses", "active_buses": 2,
                     "crowded_buses": 1, "average_occupancy_percent": 60.0,
                     "occupancy_samples": 2, "active_buses_per_route": {"R1": 2}}


# ==================== TEST-10: CAPACITY AND RETAINED SEQUENCE FLOORS ====================
def test_capacity_preserves_active_and_sequence_floor(processor, clock):
    processor.process(message(sequence=10))
    processor.process(message("A2", sequence=10))
    third = message("A3")
    third["route_id"] = "R2"
    assert processor.process(third).status == Status.CAPACITY_EXCEEDED
    assert processor.process(message(sequence=11)).status == Status.ACCEPTED
    clock[0] = 20
    assert processor.process(third).status == Status.ACCEPTED
    assert processor.process(message(sequence=10)).status == Status.STALE
    assert processor.process(message(sequence=11)).status == Status.STALE
    assert processor.process(message(sequence=12)).status == Status.ACCEPTED


def test_invalid_new_bus_cannot_evict_expired_entry(processor, clock):
    processor.process(message())
    processor.process(message("A2"))
    clock[0] = 20
    before = processor.cache.snapshot()
    payload = message("A3", next_stop_id="INVALID")
    payload["route_id"] = "R2"
    assert processor.process(payload).status == Status.INVALID_ROUTE
    assert processor.cache.snapshot() == before


# ==================== TEST-11/16: MONOTONIC STALENESS WITHOUT NEW TRAFFIC ====================
def test_stale_excluded_and_duplicates_do_not_refresh(processor, clock):
    processor.process(message())
    clock[0] = 9
    assert processor.process(message()).status == Status.DUPLICATE
    clock[0] = 10
    output = processor.snapshot()
    assert output["buses"][0]["freshness"] == "stale"
    assert output["statistics"]["active_buses"] == 0
    assert output["statistics"]["average_occupancy_percent"] is None


# ==================== TEST-12: VERSIONED JSON OUTPUT AND ISOLATED SNAPSHOTS ====================
def test_output_contract_and_route_progress(processor):
    payload = message(current_stop_id="S2", next_stop_id="S3")
    result = processor.process(payload)
    output = result.output
    assert json.loads(json.dumps(output, allow_nan=False)) == output
    assert output["schema_version"] == "edge-state-proposal-v1"
    assert output["edge_id"] == "edge-a"
    state = output["buses"][0]
    assert state["route_progress_percent"] == 50
    assert state["progress_basis"] == "reported_current_stop"
    assert state["source_timestamp"] == payload["timestamp"]
    assert state["freshness"] == "current"
    for key in ("bus_id", "route_id", "city_id", "region_id", "latitude", "longitude",
                "speed_kmh", "passengers", "capacity", "sequence_number",
                "current_stop_id", "next_stop_id", "occupancy_percent", "crowding_status",
                "processing_timestamp"):
        assert key in state
    output["buses"][0]["sequence_number"] = 99
    payload["sequence_number"] = 100
    assert processor.snapshot()["buses"][0]["sequence_number"] == 1


def test_missing_progress_not_invented(processor):
    output = processor.process(message()).output
    assert output["buses"][0]["route_progress_percent"] is None


# ==================== TEST-14: CONCURRENT WRITERS CANNOT ROLL STATE BACK ====================
def test_concurrent_updates(processor):
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda seq: processor.process(message(sequence=seq)),
                      list(range(100, 0, -1)) + list(range(1, 101))))
    assert processor.snapshot()["buses"][0]["sequence_number"] == 100


# ==================== TEST-15: EXISTING SUBSCRIBER -> QUEUE -> PROCESSOR ====================
def test_existing_ingress_pipeline(processor, network):
    subscriber = TelemetrySubscriber("north", "edge-a", city_id="CITY-A", registry=network.registry)
    receiver = EdgeIngress(processor)
    assert subscriber.handle_raw_message("transit/buses/north/A1/telemetry", json.dumps(message()).encode())
    results = receiver.drain_pending(subscriber)
    assert results[0].status == Status.ACCEPTED
    assert results[0].output["statistics"]["active_buses"] == 1
    assert subscriber.messages.unfinished_tasks == 0
    assert not subscriber.handle_raw_message("transit/buses/north/B1/telemetry", json.dumps(message("B1")).encode())


def test_city_validation_and_optional_legacy_city(network):
    payload = message()
    assert parse_telemetry(payload, expected_region="north", expected_bus="A1", registry=network.registry)[0]
    payload["city_id"] = "CITY-B"
    assert parse_telemetry(payload, expected_region="north", expected_bus="A1", registry=network.registry)[0] is None


def test_network_loads_once_and_rejects_ambiguous_configuration(tmp_path):
    path = tmp_path / "network.json"
    data = {"schema_version": "edge-network-proposal-v1",
            "routes": [{"city_id": "Durham", "region_id": "west", "route_id": "302", "stops": ["2576", "166"]}],
            "buses": [{"bus_id": "bus-302-1", "city_id": "Durham", "region_id": "west", "route_id": "302", "edge_id": "edge-west-1"}]}
    path.write_text(json.dumps(data))
    network = NetworkConfig.load(path)
    path.unlink()
    assert network.registry.get("bus-302-1").route_id == "302"
    data["buses"].append(deepcopy(data["buses"][0]))
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="globally unique"):
        NetworkConfig.load(path)


# ==================== CONFIGURATION AND SHARED SEED COMPATIBILITY ====================
def test_shared_seed_example_can_be_processed():
    from pathlib import Path
    path = Path(__file__).resolve().parents[3] / "shared/examples/edge_network.json"
    network = NetworkConfig.load(path.resolve())
    processor = EdgeProcessor(EdgeConfig("DURHAM", "west", "edge-west-1"), network)
    payload = message("bus-302-1", next_stop_id="166", current_stop_id="2576")
    payload.update(route_id="302", region_id="west")
    result = processor.process(payload)
    assert result.status == Status.ACCEPTED
    assert result.output["buses"][0]["city_id"] == "DURHAM"


def test_multiple_routes_and_missing_progress(processor):
    processor.process(message())
    payload = message("A3", next_stop_id="T2")
    payload["route_id"] = "R2"
    result = processor.process(payload)
    assert result.output["statistics"]["active_buses_per_route"] == {"R1": 1, "R2": 1}
    assert result.output["buses"][1]["route_progress_percent"] is None


@pytest.mark.parametrize("settings", [
    {"freshness_seconds": 0}, {"expire_seconds": 5}, {"max_buses": 0},
    {"freshness_seconds": float("nan")}, {"medium_threshold": 90},
])
def test_invalid_cache_policy_rejected(settings):
    values = dict(city_id="CITY-A", region_id="north", edge_id="edge-a", freshness_seconds=10)
    values.update(settings)
    with pytest.raises(ValueError):
        EdgeConfig(**values)


def test_custom_crowding_thresholds(network):
    processor = EdgeProcessor(EdgeConfig("CITY-A", "north", "edge-a",
                                        medium_threshold=20, high_threshold=90), network)
    state = processor.process(message()).output["buses"][0]
    assert state["crowding_status"] == "medium"

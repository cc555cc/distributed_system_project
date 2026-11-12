"""Check the shared BUS-01 format and BUS-02 network without a running broker."""

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from services.data_simulation import export_routes, generate_data, run_buses
from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.processing.processor import EdgeProcessor, ProcessingStatus
from services.edge.transformation.central_mapper import map_bus_update
from services.edge.validation.validator import parse_telemetry
from shared.schema.bus_state import BusStateUpdate
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network, route_stops, validate_network


def messages(bus, **settings):
    """Use deterministic reporting time; zero interval skips real waiting only."""
    return list(generate_data.generate_messages(
        bus["route_id"], bus.get("direction_id", 0), bus["bus_id"],
        max_updates=3, update_seconds=0,
        utc_now=lambda: datetime(2026, 1, 1, tzinfo=timezone.utc), **settings))


# ==================== TWO CITIES / FIVE STOPS / SIX ROUTES PER CITY ====================
def test_network_topology():
    network = load_network()
    assert len(network["cities"]) == 2
    assert len(network["stops"]) == 24
    assert len(network["routes"]) == 11
    assert sum(route["intercity"] for route in network["routes"]) == 1
    for city in network["cities"]:
        city_id = city["city_id"]
        stops = [stop for stop in network["stops"] if stop["city_id"] == city_id]
        assert len(stops) == 12
        assert "Neighborhood 1" in {stop["name"] for stop in stops}
        assert sum(city_id in route["served_city_ids"] for route in network["routes"]) == 6
    assert all(len(route["stops"]) == 5 for route in network["routes"])
    assert len({bus["bus_id"] for bus in network["buses"]}) == len(network["buses"])


# ==================== EVERY CONFIGURED BUS PASSES THE EXISTING EDGE AND CENTRAL MODEL ====================
@pytest.mark.parametrize("bus_id", [bus["bus_id"] for bus in load_network()["buses"]])
def test_simulator_to_edge_to_central_contract(bus_id):
    network = load_network()
    edge_network = NetworkConfig.load(DEFAULT_NETWORK_PATH)
    bus = next(bus for bus in network["buses"] if bus["bus_id"] == bus_id)
    processor = EdgeProcessor(EdgeConfig(bus["city_id"], bus["region_id"], bus["edge_id"]), edge_network)
    readings = messages(bus)
    assert len(readings) == 3
    for source in readings:
        decoded = json.loads(json.dumps(source, allow_nan=False))
        valid, errors = parse_telemetry(decoded, expected_region=bus["region_id"],
                                        expected_bus=bus_id, registry=edge_network.registry)
        assert not errors
        assert valid is not None
        result = processor.process(valid)
        assert result.status == ProcessingStatus.ACCEPTED
        payload = map_bus_update(result.output, bus_id, "passengers")
        central = BusStateUpdate.model_validate(payload, strict=True)
        assert central.occupancy == source["passengers"]
        assert central.lamport == source["lamport_clock"]
        assert central.edge_metadata.telemetry_schema_version == "bus-telemetry-v1"
        assert central.edge_metadata.city_id == source["city_id"]
    assert [source["sequence_number"] for source in readings] == [1, 2, 3]


# ==================== INTERCITY COVERAGE IS EXPLICIT, NOT ACCIDENTAL CITY MIXING ====================
def test_intercity_stop_coverage_and_processing_owner():
    network = load_network()
    bus = next(bus for bus in network["buses"] if bus["route_id"] == "AB-R6")
    readings = list(generate_data.generate_messages("AB-R6", 0, bus["bus_id"],
                                                   max_updates=3000, update_seconds=0))
    across = next(source for source in readings if source["current_stop_city_id"] == "CITY-B")
    assert across["city_id"] == "CITY-A"  # Registered owner remains fixed for this ticket.
    edge_network = NetworkConfig.load(DEFAULT_NETWORK_PATH)
    processor = EdgeProcessor(EdgeConfig(bus["city_id"], bus["region_id"], bus["edge_id"]), edge_network)
    valid, errors = parse_telemetry(across, expected_region=bus["region_id"], expected_bus=bus["bus_id"], registry=edge_network.registry)
    assert not errors
    result = processor.process(valid)
    assert result.status == ProcessingStatus.ACCEPTED
    metadata = map_bus_update(result.output, bus["bus_id"], "passengers")["edge_metadata"]
    assert metadata["current_stop_city_id"] == "CITY-B"
    assert metadata["served_city_ids"] == ["CITY-A", "CITY-B"]
    invalid = deepcopy(across)
    invalid["sequence_number"] += 1
    invalid["current_stop_city_id"] = "CITY-A"
    assert processor.process(invalid).status == ProcessingStatus.INVALID_ROUTE


def test_local_route_cannot_silently_cross_cities():
    network = load_network()
    network["routes"][0]["stops"][0] = "B-HOME"
    with pytest.raises(ValueError):
        validate_network(network)


def test_direction_uses_same_five_stops_in_reverse_order():
    network = load_network()
    forward = route_stops(network, "A-R1", 0)
    reverse = route_stops(network, "A-R1", 1)
    assert [stop["stop_id"] for stop in reverse] == list(reversed([stop["stop_id"] for stop in forward]))
    assert len(reverse) == 5


# ==================== ZACH'S SCENARIOS STILL CHANGE REAL READINGS ====================
def test_peak_demand_and_traffic_slowdown():
    bus = load_network()["buses"][0]
    normal = messages(bus)
    peak = messages(bus, demand="peak")
    slow = messages(bus, traffic_factor=0.5)
    assert peak[0]["passengers"] > normal[0]["passengers"]
    assert slow[1]["speed_kmh"] == normal[1]["speed_kmh"] / 2
    assert all(source["passengers"] <= source["capacity"] for source in peak)


def test_failed_sensor_does_not_invent_a_reading(capsys):
    assert messages(load_network()["buses"][0], sensor_failure_rate=1.0) == []
    assert "sensor failure" in capsys.readouterr().err


def test_repeatable_measurements_except_report_time():
    bus = load_network()["buses"][0]
    assert messages(bus) == messages(bus)


def test_sql_seed_is_generated_from_the_same_network():
    seed = DEFAULT_NETWORK_PATH.parent / "schema/seed.sql"
    assert seed.read_text(encoding="utf-8") == export_routes.seed_sql(load_network())


@pytest.mark.parametrize("change", ["duplicate_bus", "unknown_stop", "wrong_region", "wrong_edge"])
def test_invalid_assignments_are_rejected(change):
    network = load_network()
    if change == "duplicate_bus":
        network["buses"].append(deepcopy(network["buses"][0]))
    elif change == "unknown_stop":
        network["routes"][0]["stops"][0] = "missing"
    elif change == "wrong_region":
        network["buses"][0]["region_id"] = "B-north"
    else:
        network["buses"][0]["edge_id"] = "not-registered"
    with pytest.raises(ValueError):
        validate_network(network)


def test_unknown_bus_cannot_generate_telemetry():
    with pytest.raises(ValueError, match="not registered"):
        list(generate_data.generate_messages("A-R1", 0, "unknown", update_seconds=0))

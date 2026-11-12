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


# ==================== CARSON'S DURHAM MAP AND REGISTERED BUSES ====================
def test_network_topology():
    network = load_network()
    assert {city["city_id"] for city in network["cities"]} == {"DURHAM"}
    assert len(network["stops"]) == 373
    assert {region["region_id"] for region in network["regions"]} == {"west", "east"}
    assert {route["route_id"] for route in network["routes"]} == {"302", "407", "409", "900", "901"}
    assert len(network["buses"]) == 10
    assert not any(route["intercity"] for route in network["routes"])
    assert all(len(route_stops(network, route["route_id"], direction)) >= 2
               for route in network["routes"] for direction in (0, 1))
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


# ==================== REGIONAL SEPARATION AND REAL DIRECTIONAL PATTERNS ====================
def test_bus_cannot_claim_another_region():
    network = load_network()
    bus = network["buses"][0]
    edge_network = NetworkConfig.load(DEFAULT_NETWORK_PATH)
    source = messages(bus)[0]
    source["region_id"] = "east" if bus["region_id"] == "west" else "west"
    valid, errors = parse_telemetry(source, expected_region=source["region_id"],
                                    expected_bus=bus["bus_id"], registry=edge_network.registry)
    assert valid is None and errors


def test_local_route_cannot_silently_cross_cities():
    network = load_network()
    network["cities"].append(dict(city_id="OTHER", name="Other"))
    network["stops"][0]["city_id"] = "OTHER"
    with pytest.raises(ValueError):
        validate_network(network)


def test_return_direction_uses_carsons_distinct_pattern():
    network = load_network()
    forward = route_stops(network, "302", 0)
    reverse = route_stops(network, "302", 1)
    assert len(forward) == 50 and len(reverse) == 53
    assert [stop["stop_id"] for stop in reverse] != list(reversed([stop["stop_id"] for stop in forward]))


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
    import sqlite3
    seed = DEFAULT_NETWORK_PATH.parent / "schema/seed.sql"
    def records(script):
        with sqlite3.connect(":memory:") as connection:
            connection.executescript("""CREATE TABLE regions(region_id TEXT,name TEXT,primary_edge TEXT,backup_edge TEXT);
                CREATE TABLE routes(route_id TEXT,name TEXT,region_id TEXT);
                CREATE TABLE stops(stop_id TEXT,name TEXT,lat REAL,lon REAL);
                CREATE TABLE route_stops(route_id TEXT,direction_id INTEGER,position INTEGER,stop_id TEXT);
                CREATE TABLE buses(bus_id TEXT,route_id TEXT);""")
            connection.executescript(script)
            return {table: sorted(connection.execute("SELECT * FROM " + table).fetchall())
                    for table in ("regions", "routes", "stops", "route_stops", "buses")}
    assert records(seed.read_text(encoding="utf-8")) == records(export_routes.seed_sql(load_network()))


@pytest.mark.parametrize("change", ["duplicate_bus", "unknown_stop", "wrong_region", "wrong_edge"])
def test_invalid_assignments_are_rejected(change):
    network = load_network()
    if change == "duplicate_bus":
        network["buses"].append(deepcopy(network["buses"][0]))
    elif change == "unknown_stop":
        network["routes"][0]["stops"][0] = "missing"
    elif change == "wrong_region":
        network["buses"][0]["region_id"] = "east"
    else:
        network["buses"][0]["edge_id"] = "not-registered"
    with pytest.raises(ValueError):
        validate_network(network)


def test_unknown_bus_cannot_generate_telemetry():
    with pytest.raises(ValueError, match="not registered"):
        list(generate_data.generate_messages("302", 0, "unknown", update_seconds=0))

"""Known-format interoperability, invalid input and unchanged processing results."""

from copy import deepcopy
import csv
import io
import json

import pytest

from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.data_to_json_converter import convert_to_telemetry, data_to_json, decode_json
from services.edge.communication.data_to_edge import TelemetrySubscriber
from services.edge.processing.processor import EdgeProcessor
from shared.schema.network import DEFAULT_NETWORK_PATH
from services.data_simulation.generate_data import generate_messages


@pytest.fixture
def source():
    return next(generate_messages("302", 0, "bus-302-1", max_updates=1, update_seconds=0))


@pytest.fixture
def network():
    return NetworkConfig.load(DEFAULT_NETWORK_PATH)


def options(network):
    return dict(expected_region="west", expected_bus="bus-302-1", registry=network.registry)


def vendor(source):
    return dict(version="vendor-v1", vehicleId=source["bus_id"], city=source["city_id"], area=source["region_id"],
        route=source["route_id"], location=dict(lat=source["latitude"], lng=source["longitude"]),
        speed=dict(value=source["speed_kmh"], unit="km/h"), load=dict(passengers=source["passengers"], capacity=source["capacity"]),
        observedAt=source["timestamp"], sequence=source["sequence_number"], lamport=source["lamport_clock"],
        direction=source["direction_id"], state="stopped", sensorState="ok",
        currentStop=source["current_stop_id"], nextStop=source["next_stop_id"])


def csv_message(source):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(source))
    writer.writeheader()
    writer.writerow(source)
    return output.getvalue()


def test_canonical_json_round_trip_keeps_types_values_and_source_unchanged(source, network):
    original = deepcopy(source)
    converted = json.loads(data_to_json(json.dumps(source).encode(), **options(network)))
    assert converted == source == original
    converted["passengers"] = 999
    assert source == original


@pytest.mark.parametrize("source_format", ["canonical-v1", "vendor-v1", "csv-v1"])
def test_formats_produce_equivalent_occupancy_route_and_central_payload(source, network, source_format):
    from services.edge.transformation.central_mapper import map_bus_update
    from datetime import datetime, timezone
    raw = source if source_format == "canonical-v1" else vendor(source) if source_format == "vendor-v1" else csv_message(source)
    converted = convert_to_telemetry(raw, source_format=source_format, **options(network))
    now = datetime.now(timezone.utc)
    def process(value):
        processor = EdgeProcessor(EdgeConfig("DURHAM", "west", "edge-west-1"), network, utc_now=lambda: now)
        return map_bus_update(processor.process(value).output, source["bus_id"], "passengers")
    assert process(converted) == process(source)


@pytest.mark.parametrize("unit,value,expected", [("mph", 10, 16.09344), ("m/s", 10, 36), ("km/h", 10, 10)])
def test_explicit_speed_units_are_normalized(source, network, unit, value, expected):
    raw = vendor(source)
    raw["speed"] = dict(value=value, unit=unit)
    assert convert_to_telemetry(raw, source_format="vendor-v1", **options(network))["speed_kmh"] == pytest.approx(expected)


def test_unix_seconds_and_timezone_offsets_become_utc(source, network):
    raw = vendor(source)
    raw["observedAt"] = 1735689600
    assert convert_to_telemetry(raw, source_format="vendor-v1", **options(network))["timestamp"] == "2025-01-01T00:00:00+00:00"
    raw["observedAt"] = "2024-12-31T19:00:00-05:00"
    assert convert_to_telemetry(raw, source_format="vendor-v1", **options(network))["timestamp"] == "2025-01-01T00:00:00+00:00"


@pytest.mark.parametrize("change", ["unit", "boolean_speed", "timezone", "missing", "city", "capacity", "state", "extra"])
def test_unknown_or_invalid_vendor_values_are_rejected(source, network, change):
    raw = vendor(source)
    if change == "unit": raw["speed"]["unit"] = "knots"
    if change == "boolean_speed": raw["speed"]["value"] = True
    if change == "timezone": raw["observedAt"] = "2025-01-01T00:00:00"
    if change == "missing": del raw["load"]
    if change == "city": raw["area"] = "east"
    if change == "capacity": raw["load"]["capacity"] = 0
    if change == "state": raw["state"] = "guess"
    if change == "extra": raw["guess"] = 1
    with pytest.raises(ValueError):
        convert_to_telemetry(raw, source_format="vendor-v1", **options(network))


@pytest.mark.parametrize("raw", ['{"bus_id":"one","bus_id":"two"}', '{"value":NaN}', b"\xff", "x" * 65537], ids=["duplicate", "nan", "invalid-utf8", "oversized"])
def test_ambiguous_nonstandard_or_oversized_json_is_rejected(raw):
    with pytest.raises((ValueError, UnicodeError)):
        decode_json(raw)


@pytest.mark.parametrize("raw", ["bus_id,bus_id\na,b", "unknown\na", "bus_id\na\nb", "bus_id,route_id\na"])
def test_csv_rejects_duplicate_unknown_multiple_or_incomplete_records(raw, network):
    with pytest.raises(ValueError):
        convert_to_telemetry(raw, source_format="csv-v1", **options(network))


def test_ingress_accepts_vendor_and_csv_envelopes_and_rejects_mismatched_bus(source, network):
    subscriber = TelemetrySubscriber("west", "edge-west-1", city_id="DURHAM", registry=network.registry)
    topic = "transit/buses/west/bus-302-1/telemetry"
    for format_name, data in (("vendor-v1", vendor(source)), ("csv-v1", csv_message(source))):
        assert subscriber.handle_raw_message(topic, json.dumps(dict(format=format_name, data=data)).encode())
        assert subscriber.messages.get_nowait()["bus_id"] == source["bus_id"]
    raw = vendor(source)
    raw["vehicleId"] = "bus-302-2"
    assert not subscriber.handle_raw_message(topic, json.dumps(dict(format="vendor-v1", data=raw)).encode())
    assert subscriber.messages.empty()

"""Convert supported external formats into our validated shared telemetry JSON."""

import csv
import io
import json
import math
from copy import deepcopy
from datetime import UTC, datetime

from services.edge.validation.validator import BusTelemetry, parse_telemetry

MAX_INPUT_BYTES = 64 * 1024


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field")
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError("Invalid JSON number")


def decode_json(raw):
    """Bound input and reject ambiguous objects and nonstandard JSON numbers."""
    if isinstance(raw, bytes):
        if len(raw) > MAX_INPUT_BYTES:
            raise ValueError("Input exceeds 64 KiB")
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        if len(raw.encode("utf-8")) > MAX_INPUT_BYTES:
            raise ValueError("Input exceeds 64 KiB")
        return json.loads(
            raw, object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
    if isinstance(raw, dict):
        encoded = json.dumps(raw, allow_nan=False)
        if len(encoded.encode("utf-8")) > MAX_INPUT_BYTES:
            raise ValueError("Input exceeds 64 KiB")
        return deepcopy(raw)
    raise ValueError("Expected a JSON object or UTF-8 JSON")


def _timestamp(value):
    if isinstance(value, bool):
        raise TypeError("Timestamp cannot be a boolean")
    if isinstance(value, (float, int)):
        if not math.isfinite(value):
            raise ValueError("Timestamp must be finite Unix seconds")
        return datetime.fromtimestamp(value, UTC).isoformat()
    if not isinstance(value, str):
        raise TypeError("Expected Unix seconds or timezone-aware ISO timestamp")
    timestamp = datetime.fromisoformat(value)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("Timestamp requires a timezone")
    return timestamp.astimezone(UTC).isoformat()


def _vendor(raw):
    # This is an explicit vendor-v1 adapter, not a guess at arbitrary fields.
    value = decode_json(raw)
    required = {
        "version",
        "vehicleId",
        "city",
        "area",
        "route",
        "location",
        "speed",
        "load",
        "observedAt",
        "sequence",
        "lamport",
        "direction",
        "state",
        "sensorState",
        "currentStop",
    }
    if (
        not isinstance(value, dict)
        or not required <= value.keys()
        or value["version"] != "vendor-v1"
    ):
        raise ValueError("Missing vendor-v1 fields or unsupported version")
    if value.keys() - (required | {"nextStop"}):
        raise ValueError("Unsupported vendor-v1 field")
    for name, fields in (
        ("location", {"lat", "lng"}),
        ("speed", {"value", "unit"}),
        ("load", {"passengers", "capacity"}),
    ):
        if not isinstance(value[name], dict) or set(value[name]) != fields:
            raise ValueError("Invalid vendor-v1 nested fields")
    speed = value["speed"]["value"]
    factors = {"km/h": 1.0, "mph": 1.609344, "m/s": 3.6}
    unit = value["speed"]["unit"]
    if (
        not isinstance(unit, str)
        or unit not in factors
        or isinstance(speed, bool)
        or not isinstance(speed, (int, float))
        or not math.isfinite(speed)
    ):
        raise ValueError("Speed requires a finite number and supported unit")
    states = {
        "moving": "moving",
        "stopped": "at_stop",
        "boarding": "boarding",
        "out_of_service": "out_of_service",
    }
    sensors = {"ok": "healthy", "fault": "faulty", "offline": "offline"}
    if (
        not isinstance(value["state"], str)
        or value["state"] not in states
        or not isinstance(value["sensorState"], str)
        or value["sensorState"] not in sensors
    ):
        raise ValueError("Unsupported vendor-v1 state")
    result = {
        "schema_version": "bus-telemetry-v1",
        "bus_id": value["vehicleId"],
        "city_id": value["city"],
        "region_id": value["area"],
        "route_id": value["route"],
        "latitude": value["location"]["lat"],
        "longitude": value["location"]["lng"],
        "speed_kmh": speed * factors[unit],
        "passengers": value["load"]["passengers"],
        "capacity": value["load"]["capacity"],
        "timestamp": _timestamp(value["observedAt"]),
        "sequence_number": value["sequence"],
        "lamport_clock": value["lamport"],
        "direction_id": value["direction"],
        "bus_status": states[value["state"]],
        "sensor_status": sensors[value["sensorState"]],
        "current_stop_id": value["currentStop"],
    }
    if "nextStop" in value:
        result["next_stop_id"] = value["nextStop"]
    return result


def _csv(raw):
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_INPUT_BYTES:
        raise ValueError("CSV must be UTF-8 text of at most 64 KiB")
    reader = csv.DictReader(io.StringIO(raw), strict=True)
    headers = reader.fieldnames
    if (
        not headers
        or len(headers) != len(set(headers))
        or set(headers)
        - (BusTelemetry.__required_keys__ | BusTelemetry.__optional_keys__)
    ):
        raise ValueError("Missing, duplicate or unsupported CSV headers")
    rows = list(reader)
    if (
        len(rows) != 1
        or None in rows[0]
        or any(value is None for value in rows[0].values())
    ):
        raise ValueError("CSV requires a header and exactly one complete record")
    value = {key: item for key, item in rows[0].items() if item != ""}
    integers = {
        "passengers",
        "capacity",
        "sequence_number",
        "lamport_clock",
        "direction_id",
    }
    numbers = {"latitude", "longitude", "speed_kmh", "heading_degrees"}
    for key in integers & value.keys():
        value[key] = int(value[key])
    for key in numbers & value.keys():
        value[key] = float(value[key])
    return value


def convert_to_telemetry(
    raw, *, source_format="canonical-v1", expected_region, expected_bus, registry
):
    """Normalize known formats, then run the same EDGE-01 validator."""
    try:
        if source_format == "canonical-v1":
            value = decode_json(raw)
        elif source_format == "vendor-v1":
            value = _vendor(raw)
        elif source_format == "csv-v1":
            value = _csv(raw)
        else:
            raise ValueError("Unsupported input format")
        telemetry, errors = parse_telemetry(
            value,
            expected_region=expected_region,
            expected_bus=expected_bus,
            registry=registry,
        )
    except (
        TypeError,
        OverflowError,
        OSError,
        RecursionError,
        csv.Error,
        UnicodeError,
    ) as error:
        raise ValueError("Malformed telemetry input") from error
    if telemetry is None:
        raise ValueError("; ".join(errors))
    return telemetry


def data_to_json(raw, **options):
    """Return consistent JSON with the field meanings expected by the pipeline."""
    return json.dumps(
        convert_to_telemetry(raw, **options),
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )

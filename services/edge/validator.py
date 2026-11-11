"""Stateless validation of untrusted bus telemetry payloads.

Validation lives outside MQTT so it can be tested and updated when Zachary
finalizes the shared JSON schema. No route processing or cache writes here.
"""

import math
from datetime import datetime
from typing import NotRequired, TypedDict, cast

from services.edge.bus_registry import BusRegistry


class BusTelemetry(TypedDict):
    """Fields known to exist after successful EDGE-01 validation."""

    bus_id: str
    route_id: str
    region_id: str
    latitude: float
    longitude: float
    speed_kmh: float
    passengers: int
    capacity: int
    sequence_number: int
    timestamp: str
    lamport_clock: NotRequired[int]
    heading_degrees: NotRequired[float]
    current_stop_id: NotRequired[str]
    next_stop_id: NotRequired[str]
    sensor_status: NotRequired[str]


REQUIRED_FIELDS = frozenset(
    {
        "bus_id",
        "route_id",
        "region_id",
        "latitude",
        "longitude",
        "speed_kmh",
        "passengers",
        "capacity",
        "sequence_number",
        "timestamp",
    }
)

# Preliminary simulation limit; revisit with Zachary's finalized schema.
MAX_SPEED_KMH = 200.0


def _valid_number(value: object, lower: float, upper: float) -> bool:
    """Accept finite int/float measurements, but not Python booleans."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return lower <= value <= upper and math.isfinite(value)


def _valid_integer(value: object, minimum: int) -> bool:
    """Reject bool values, which Python otherwise treats as integers."""
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _valid_timestamp(value: object) -> bool:
    """Require parseable ISO 8601 timestamps with an explicit UTC offset."""
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def parse_telemetry(
    payload: object,
    *,
    expected_region: str,
    expected_bus: str,
    registry: BusRegistry,
) -> tuple[BusTelemetry | None, list[str]]:
    """Validate a decoded JSON value against temporary EDGE-01 rules.

    Args:
        payload: Decoded JSON (untrusted; may not be a dictionary).
        expected_region: Region extracted from the MQTT topic.
        expected_bus: Bus ID extracted from the MQTT topic.
        registry: Temporary list of allowed bus assignments.

    Returns:
        Valid typed data and no errors, or None and rejection reasons.

    Security note: registry checks alone cannot authenticate a publisher.
    Broker credentials and topic permissions will be configured by Julian.
    """
    if not isinstance(payload, dict):
        return None, ["Telemetry must be a JSON object"]

    errors: list[str] = []
    missing = REQUIRED_FIELDS - payload.keys()
    for key in sorted(missing):
        errors.append(f"Missing required field: {key}")
    if missing:
        return None, errors

    for key in ("bus_id", "route_id", "region_id"):
        value: object = payload[key]
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{key} must be a non-empty string")

    # The topic and payload must identify the same bus and regional stream.
    if payload["region_id"] != expected_region:
        errors.append("Payload region_id does not match MQTT topic")
    if payload["bus_id"] != expected_bus:
        errors.append("Payload bus_id does not match MQTT topic")

    if not _valid_number(payload["latitude"], -90, 90):
        errors.append("latitude must be a finite number between -90 and 90")
    if not _valid_number(payload["longitude"], -180, 180):
        errors.append("longitude must be a finite number between -180 and 180")
    if not _valid_number(payload["speed_kmh"], 0, MAX_SPEED_KMH):
        errors.append("speed_kmh must be a finite number from 0 to 200")

    passengers: object = payload["passengers"]
    capacity: object = payload["capacity"]
    if not _valid_integer(passengers, 0):
        errors.append("passengers must be a non-negative integer")
    if not _valid_integer(capacity, 1):
        errors.append("capacity must be a positive integer")
    if (
        isinstance(passengers, int)
        and not isinstance(passengers, bool)
        and isinstance(capacity, int)
        and not isinstance(capacity, bool)
        and passengers >= 0
        and capacity > 0
        and passengers > capacity
    ):
        errors.append("passengers cannot exceed capacity")

    if not _valid_integer(payload["sequence_number"], 0):
        errors.append("sequence_number must be a non-negative integer")
    if not _valid_timestamp(payload["timestamp"]):
        errors.append("timestamp must be an ISO 8601 datetime with timezone")

    if "lamport_clock" in payload and not _valid_integer(
        payload["lamport_clock"], 0
    ):
        errors.append("lamport_clock must be a non-negative integer")
    if "heading_degrees" in payload and not _valid_number(
        payload["heading_degrees"], 0, 359.999999
    ):
        errors.append("heading_degrees must be between 0 and less than 360")
    for key in ("current_stop_id", "next_stop_id"):
        if key in payload and (
            not isinstance(payload[key], str) or not payload[key].strip()
        ):
            errors.append(f"{key} must be a non-empty string")
    if "sensor_status" in payload and payload["sensor_status"] not in (
        "healthy", "faulty", "offline"
    ):
        errors.append("sensor_status is not recognized")

    # Only consult the registry after ensuring identifiers are usable strings.
    if all(
        isinstance(payload[key], str) and payload[key].strip()
        for key in ("bus_id", "route_id", "region_id")
    ):
        errors.extend(
            registry.check_identity(
                payload["bus_id"], payload["region_id"], payload["route_id"]
            )
        )

    if errors:
        return None, errors

    # All required fields have now been checked at runtime.
    return cast(BusTelemetry, payload), []

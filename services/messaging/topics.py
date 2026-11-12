"""Agreed MQTT topic names. See infrastructure/README.md; change them only through TEAM-02."""

ALL_BUSES = "all"

STATE_FILTER = "transit/+/bus/+/state"
ALERT_FILTER = "transit/alerts/#"


def telemetry(region_id: str, bus_id: str) -> str:
    return f"transit/buses/{region_id}/{bus_id}/telemetry"


def telemetry_filter(region_id: str) -> str:
    return f"transit/buses/{region_id}/+/telemetry"


def state(region_id: str, bus_id: str) -> str:
    return f"transit/{region_id}/bus/{bus_id}/state"


def alert(region_id: str, source_id: str) -> str:
    return f"transit/alerts/{region_id}/{source_id}"


def command(region_id: str, bus_id: str = ALL_BUSES) -> str:
    return f"transit/commands/{region_id}/{bus_id}"

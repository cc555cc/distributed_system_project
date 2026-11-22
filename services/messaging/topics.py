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


EDGE_HEARTBEAT_FILTER = "transit/heartbeats/edge/+/+"


def bus_heartbeat(region_id: str, bus_id: str) -> str:
    return f"transit/heartbeats/bus/{region_id}/{bus_id}"


def bus_heartbeat_filter(region_id: str) -> str:
    return f"transit/heartbeats/bus/{region_id}/+"


def edge_heartbeat(region_id: str, edge_id: str) -> str:
    return f"transit/heartbeats/edge/{region_id}/{edge_id}"


# Central publishes which edge owns each bus in a region; edges follow it.
ASSIGNMENT_FILTER = "transit/assignments/+"


def assignment(region_id: str) -> str:
    return f"transit/assignments/{region_id}"

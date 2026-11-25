"""Helpers shared by the messaging tests."""


def reading(sequence, **fields):
    """A valid bus-telemetry payload; ``fields`` override the defaults."""
    return {
        "bus_id": "A1",
        "city_id": "CITY-A",
        "region_id": "north",
        "route_id": "R1",
        "latitude": 43.9,
        "longitude": -78.9,
        "speed_kmh": 30.0,
        "passengers": 10,
        "capacity": 50,
        "sequence_number": sequence,
        "timestamp": "2026-11-11T12:00:00Z",
        "bus_status": "travelling",
        "lamport_clock": sequence,
        "direction_id": 0,
        "current_stop_id": "S1",
        "next_stop_id": "S2",
        "sensor_status": "healthy",
    } | fields

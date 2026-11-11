"""Map an accepted edge snapshot to the same model central actually imports."""

from typing import Literal

from shared.schema.bus_state import BusStateUpdate

OccupancyUnit = Literal["passengers", "percent"]


def map_bus_update(output: dict, bus_id: str, occupancy_unit: OccupancyUnit) -> dict:
    """Return checked central fields without inventing missing sensor/clock data."""
    if occupancy_unit not in ("passengers", "percent"):
        raise ValueError("Agree occupancy units: passengers or percent")
    bus = next((bus for bus in output["buses"] if bus["bus_id"] == bus_id), None)
    if bus is None or bus["freshness"] != "current":
        raise ValueError("Only the current accepted bus update can be forwarded")
    if bus.get("sensor_status", "healthy") != "healthy":
        raise ValueError("Faulty sensor occupancy cannot become a valid central reading")
    # These must come from the simulator/messaging contract, not edge guesses.
    if not bus.get("bus_status") or "lamport_clock" not in bus:
        raise ValueError("Forwarding requires bus_status and lamport_clock")
    percent = bus["occupancy_percent"]
    occupancy = bus["passengers"]
    if occupancy_unit == "percent":
        if percent is None or not float(percent).is_integer():
            raise ValueError("Central integer occupancy cannot represent fractional percent exactly")
        occupancy = int(percent)
    metadata = {
        "city_id": output["city_id"], "region_id": output["region_id"],
        "edge_id": output["edge_id"], "route_id": bus["route_id"],
        "capacity": bus["capacity"], "passengers": bus["passengers"],
        "occupancy_unit": occupancy_unit, "current_stop_id": bus["current_stop_id"],
        "occupancy_percent": percent, "crowding_status": bus["crowding_status"],
        "sensor_status": bus.get("sensor_status", "healthy"),
        "source_timestamp": bus["source_timestamp"], "processing_timestamp": bus["processing_timestamp"],
        "freshness": bus["freshness"], "route_progress_percent": bus["route_progress_percent"],
        "statistics": output["statistics"],
    }
    payload = dict(lat=bus["latitude"], lon=bus["longitude"], speed=bus["speed_kmh"],
                   direction_id=bus["direction_id"], next_stop_id=bus["next_stop_id"],
                   bus_status=bus["bus_status"], occupancy=occupancy,
                   sequence_number=bus["sequence_number"], lamport=bus["lamport_clock"],
                   edge_metadata=metadata)
    return BusStateUpdate.model_validate(payload, strict=True).model_dump(mode="json", exclude_none=True)

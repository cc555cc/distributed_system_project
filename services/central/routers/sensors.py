"""Read-only sensor health. Silence is not proof that a sensor is broken."""

from typing import Literal

from fastapi import APIRouter, HTTPException

from services.central.health import find_bus_health
from shared.schema.health import SENSORS

router = APIRouter(tags=["sensors"])


@router.get("/sensors")
def list_sensors():
    return {"sensors": list(SENSORS)}


@router.get("/buses/{bus_id}/sensors")
def get_bus_sensors(bus_id: str, source: Literal["live", "demo"] = "live"):
    bus = find_bus_health(bus_id, source)
    return {"bus_id": bus_id, "source": source, "sensors": bus["sensors"]}


@router.get("/buses/{bus_id}/sensors/{sensor_name}")
def get_sensor_health(bus_id: str, sensor_name: str,
                      source: Literal["live", "demo"] = "live"):
    if sensor_name not in SENSORS:
        raise HTTPException(404, "Unknown sensor")
    bus = find_bus_health(bus_id, source)
    sensor = bus["sensors"].get(sensor_name, {"health_status": "unknown", "age_seconds": None})
    return {"bus_id": bus_id, "sensor": sensor_name, "source": source, **sensor}

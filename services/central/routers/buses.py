from services.central.database import transit as database
import os
from typing import Literal

from fastapi import APIRouter, HTTPException

from services.central.health import bus_health_snapshot, find_bus_health

router = APIRouter(tags=["buses"])
db_url = os.environ["DATABASE_URL"]


@router.get("/buses/health")
def list_bus_health(source: Literal["live", "demo"] = "live",
                    region_id: str | None = None, route_id: str | None = None):
    """Dropdown choices include registered buses that have never reported."""
    return [
        {key: value for key, value in bus.items() if key != "sensors"}
        for bus in bus_health_snapshot(source)
        if (region_id is None or bus["region_id"] == region_id)
        and (route_id is None or bus["route_id"] == route_id)
    ]


@router.get("/buses/{bus_id}/health")
def get_bus_health(bus_id: str, source: Literal["live", "demo"] = "live"):
    return {key: value for key, value in find_bus_health(bus_id, source).items() if key != "sensors"}


# list every bus with its latest state (state fields are null if the bus has not reported yet)
# optional filters: /buses?region_id=west, /buses?route_id=302
@router.get("/buses")
def get_all_buses(region_id: str | None = None, route_id: str | None = None):
    try:
        return database.get_all_buses(db_url, region_id=region_id, route_id=route_id)
    except ValueError as error:
        raise HTTPException(404, str(error)) from error

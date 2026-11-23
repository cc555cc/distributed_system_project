import os

from fastapi import APIRouter, HTTPException

from services.central.database import transit as database
from services.central.freshness import freshness
from services.central.ingestion_check import check_update  # ingestion check function
from shared.schema.bus_state import BusStateUpdate

router = APIRouter()
db_url = os.environ["DATABASE_URL"]


def apply_bus_state(
    bus_id: str, region_id: str, state: BusStateUpdate
) -> tuple[bool, str | None]:
    # Central ingestion and HTTP share this database operation.
    return database.apply_bus_state(db_url, bus_id, region_id, state, validator=check_update)


# HTTP route to update bus state
@router.put("/regions/{region_id}/bus_states/{bus_id}")
def update_bus_state(region_id: str, bus_id: str, state: BusStateUpdate):
    applied, reason = apply_bus_state(bus_id, region_id, state)
    if not applied and reason != "stale update":
        raise HTTPException(422, reason)
    return {"bus_id": bus_id, "applied": applied}


# fetch a bus's status by id
@router.get("/bus_states/{bus_id}")
def get_bus_state(bus_id: str):
    try:
        row = database.get_bus_state(db_url, bus_id)
    except ValueError as error:
        raise HTTPException(404, str(error)) from error
    return {**row, "freshness": freshness(row["last_updated"])}


# remove bus from active network when it reaches the terminal
@router.put("/bus_states/{bus_id}/terminal")
def remove_bus_from_network(bus_id: str):
    try:
        return database.remove_bus_from_network(db_url, bus_id)
    except ValueError as error:
        raise HTTPException(404, str(error)) from error

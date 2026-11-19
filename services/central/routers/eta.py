#load data from database and call eta.py
import os
import psycopg
from fastapi import APIRouter, HTTPException
from psycopg.types.json import Jsonb
from shared.schema.bus_state import BusStateUpdate
from psycopg.rows import dict_row
from services.central.eta import eta_bus

router = APIRouter()
db_url = os.environ["DATABASE_URL"]

@router.get("/bus/{bus_id}/eta")
def get_stops_eta(bus_id: str):
    with psycopg.connect(db_url) as conn:
        etas = eta_bus(conn, bus_id)
    if etas is None:
        raise HTTPException(404, f"Bus: {bus_id} not found")
    return {"bus_id": bus_id, "stops": etas}

    
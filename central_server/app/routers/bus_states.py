import os

import psycopg
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()
db_url = os.environ["DATABASE_URL"]


class BusStateUpdate(BaseModel):
    lat: float
    lon: float
    speed: float
    direction_id: int
    next_stop_id: str | None = None
    bus_status: str
    occupancy: int
    sequence_number: int
    lamport: int


#update a bus's live state (insert if this is the bus's first report)
@router.put("/bus_states/{bus_id}")
def update_bus_state(bus_id: str, state: BusStateUpdate):
    with psycopg.connect(db_url) as connection:
        cursor = connection.execute(
            """
            INSERT INTO bus_states (bus_id, lat, lon, speed, direction_id, next_stop_id,
                                    bus_status, occupancy, sequence_number, lamport, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (bus_id) DO UPDATE SET
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                speed = EXCLUDED.speed,
                direction_id = EXCLUDED.direction_id,
                next_stop_id = EXCLUDED.next_stop_id,
                bus_status = EXCLUDED.bus_status,
                occupancy = EXCLUDED.occupancy,
                sequence_number = EXCLUDED.sequence_number,
                lamport = EXCLUDED.lamport,
                last_updated = now()
            WHERE bus_states.sequence_number < EXCLUDED.sequence_number
            """,
            (
                bus_id, state.lat, state.lon, state.speed, state.direction_id, state.next_stop_id,
                state.bus_status, state.occupancy, state.sequence_number, state.lamport,
            ),
        )
        if cursor.rowcount == 0:
            raise HTTPException(status_code=409, detail="Stale update: newer sequence_number already stored")

    return {"bus_id": bus_id, "applied": True}

#fetch a bus's status by id
@router.get("/bus_state/{bus_id}")
def get_bus_state(bus_id: str):
    connection = connect_db()
    rows = connection.execute("SELECT * FROM bus_states WHERE bus_id = %s", (bus_id,)).fetchall()
    return rows.item().lat, rows.item().lon, rows.item().speed, rows.item().direction_id, rows.item().next_stop_id, rows.item().bus_status, rows.item().occupancy, rows.item().sequence_number, rows.item().lamport, rows.item().last_updated

def connect_db():
    return psycopg.connect(db_url)
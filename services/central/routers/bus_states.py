import os

import psycopg
from fastapi import APIRouter, HTTPException
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from services.central.ingestion_check import check_update  # ingestion check function
from shared.schema.bus_state import BusStateUpdate

router = APIRouter()
db_url = os.environ["DATABASE_URL"]


def apply_bus_state(
    bus_id: str, region_id: str, state: BusStateUpdate
) -> tuple[bool, str | None]:
    with psycopg.connect(db_url) as connection:
        if (
            state.edge_metadata is not None
            and state.edge_metadata.region_id != region_id
        ):
            return False, "metadata region differs from MQTT/API region"

        # validate sequence number first
        row = connection.execute(
            "SELECT sequence_number FROM bus_states WHERE bus_id = %s FOR UPDATE",
            (bus_id,),
        ).fetchone()
        if (
            row is not None and row[0] >= state.sequence_number
        ):  # a late message, drop it
            return False, "stale update"

        # use ingestion check first before pushing update to the database
        reason = check_update(connection, bus_id, region_id, state)
        if reason is not None:
            return False, reason

        cursor = connection.execute(
            """
            INSERT INTO bus_states (bus_id, lat, lon, speed, direction_id, next_stop_id,
                                    bus_status, occupancy, sequence_number, lamport, edge_metadata, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
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
                edge_metadata = EXCLUDED.edge_metadata,
                last_updated = now()
            WHERE bus_states.sequence_number < EXCLUDED.sequence_number
            """,
            (
                bus_id,
                state.lat,
                state.lon,
                state.speed,
                state.direction_id,
                state.next_stop_id,
                state.bus_status,
                state.occupancy,
                state.sequence_number,
                state.lamport,
                Jsonb(state.edge_metadata.model_dump(mode="json"))
                if state.edge_metadata
                else None,
            ),
        )
        if cursor.rowcount == 0:
            return False, "stale update"
        return True, None


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
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        row = connection.execute(
            "SELECT * FROM bus_states WHERE bus_id = %s", (bus_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(404, f"Bus: {bus_id} not found")
    return row


# remove bus from active network when it reaches the terminal
@router.put("/bus_states/{bus_id}/terminal")
def remove_bus_from_network(bus_id: str):
    with psycopg.connect(db_url) as connection:
        cursor = connection.execute(
            """
            UPDATE bus_states s SET
                current_stop_id = last.stop_id,
                next_stop_id    = last.stop_id,
                bus_status      = 'idle',
                occupancy       = 0,
                speed           = 0,
                edge_metadata   = NULL,
                last_updated    = now()
            FROM buses b,
                 LATERAL (
                     SELECT rs.stop_id FROM route_stops rs
                     WHERE rs.route_id = b.route_id AND rs.direction_id = s.direction_id
                     ORDER BY rs.position DESC LIMIT 1
                 ) AS last
            WHERE s.bus_id = %s AND b.bus_id = s.bus_id
            """,
            (bus_id,),
        )
    if cursor.rowcount == 0:
        raise HTTPException(404, f"Bus: {bus_id} not found")
    return {"bus_id": bus_id, "bus_status": "idle"}


def connect_db():
    return psycopg.connect(db_url)

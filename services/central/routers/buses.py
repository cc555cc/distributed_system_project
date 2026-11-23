import os
from typing import Literal

import psycopg
from fastapi import APIRouter
from psycopg.rows import dict_row

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
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        rows = connection.execute(
            """
            SELECT b.bus_id, b.route_id, r.region_id,
                   s.lat, s.lon, s.speed, s.direction_id, s.next_stop_id, s.bus_status,
                   s.occupancy, s.sequence_number, s.edge_metadata, s.last_updated
            FROM buses b
            JOIN routes r ON r.route_id = b.route_id
            LEFT JOIN bus_states s ON s.bus_id = b.bus_id
            WHERE (CAST(%s AS TEXT) IS NULL OR r.region_id = %s)
              AND (CAST(%s AS TEXT) IS NULL OR b.route_id = %s)
            ORDER BY b.bus_id
            """,
            (region_id, region_id, route_id, route_id),
        ).fetchall()
    return rows

import os
import psycopg
from psycopg.rows import dict_row
from fastapi import APIRouter, HTTPException, Query

router = APIRouter()
db_url = os.environ["DATABASE_URL"]


@router.get("/routes/{route_id}/stops")
def get_route_stops(route_id: str, direction: int | None = Query(default=None, ge=0, le=1)):
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        rows = connection.execute(
            """
            SELECT rs.direction_id, rs.position, s.stop_id, s.name, s.lat, s.lon
            FROM route_stops rs
            JOIN stops s ON s.stop_id = rs.stop_id
            WHERE rs.route_id = %s
              AND (%s::integer IS NULL OR rs.direction_id = %s)
            ORDER BY rs.direction_id, rs.position
            """,
            (route_id, direction, direction),
        ).fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail="No stops found for this route and direction")
    return rows

#get a route's name and region by id
@router.get("/routes/{route_id}")
def get_route_info(route_id: str):
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        row = connection.execute(
            "SELECT route_id, name, region_id FROM routes WHERE route_id = %s", (route_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Route not found")
    return row

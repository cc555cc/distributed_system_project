import os
import psycopg
from psycopg.rows import dict_row
from fastapi import APIRouter, HTTPException

router = APIRouter()
db_url = os.environ["DATABASE_URL"]

#list all
@router.get("/routes")
def get_all_route():
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        row = connection.execute("SELECT route_id, name, region_id FROM routes ORDER BY route_id").fetchall()
    return row

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

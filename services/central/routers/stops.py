import os
import psycopg
from psycopg.rows import dict_row
from fastapi import APIRouter, HTTPException

router = APIRouter()
db_url = os.environ["DATABASE_URL"]

#get all stops 
@router.get("/stops")
def get_all_stops():
    with psycopg.connect(db_url) as connection:
        row = connection.execute("SELECT stop_id, name, lat, lon FROM stops ORDER BY stop_id").fetchall()
    return row

#get a stop's latitude and longitude by stop_id
@router.get("/stops/{stop_id}")
def get_stop_loc(stop_id: str):
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        row = connection.execute(
            "SELECT stop_id, lat, lon FROM stops WHERE stop_id = %s", (stop_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Stop not found")
    return row


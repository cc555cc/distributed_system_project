from services.central.database import transit as database
import os

from fastapi import APIRouter, HTTPException

router = APIRouter()
db_url = os.environ["DATABASE_URL"]


# get all stops
@router.get("/stops")
def get_all_stops():
    try:
        return database.get_all_stops(db_url)
    except ValueError as error:
        raise HTTPException(404, str(error)) from error


# get a stop's latitude and longitude by stop_id
@router.get("/stops/{stop_id}")
def get_stop_loc(stop_id: str):
    try:
        return database.get_stop_loc(db_url, stop_id)
    except ValueError as error:
        raise HTTPException(404, str(error)) from error

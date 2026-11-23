from services.central.database import transit as database
import os
from typing import Literal

from fastapi import APIRouter, HTTPException, Query

from services.central import health

router = APIRouter(tags=["routes"])
db_url = os.environ["DATABASE_URL"]


@router.get("/routes/{route_id}/health")
def get_route_health(route_id: str, source: Literal["live", "demo"] = "live"):
    return health.route_health(route_id, source)


# list all
@router.get("/routes")
def get_all_route():
    try:
        return database.get_all_route(db_url)
    except ValueError as error:
        raise HTTPException(404, str(error)) from error


# get all stops on a route in order, optionally filtered by direction
@router.get("/routes/{route_id}/stops")
def get_route_stops(
    route_id: str, direction: int | None = Query(default=None, ge=0, le=1)
):
    try:
        return database.get_route_stops(db_url, route_id, direction)
    except ValueError as error:
        raise HTTPException(404, str(error)) from error


# get a route's name and region by id
@router.get("/routes/{route_id}")
def get_route_info(route_id: str):
    try:
        return database.get_route_info(db_url, route_id)
    except ValueError as error:
        raise HTTPException(404, str(error)) from error

"""Public passenger APIs: map data, stop arrivals, journeys and explained choices."""

from typing import Literal

import psycopg
from fastapi import APIRouter, HTTPException, Query

from services.central.journeys import arrivals, patterns, plan_journeys, transit_context
from services.central.routers.eta import get_stops_eta
from services.central.suggestions import rank_options
from shared.schema.network import load_network, route_stops

router = APIRouter(prefix="/passenger", tags=["passenger"])
Source = Literal["live", "demo"]


def context(source):
    try:
        return transit_context(source)
    except psycopg.Error as error:
        raise HTTPException(503, "Live transit data is unavailable") from error


@router.get("/routes")
def routes(source: Source = "demo"):
    return [{key: route[key] for key in ("route_id", "name", "region_id")} for route in load_network()["routes"]]


@router.get("/routes/{route_id}/stops")
def stops_on_route(route_id: str, source: Source = "demo"):
    try:
        network = load_network()
        return route_stops(network, route_id, 0) + route_stops(network, route_id, 1)
    except ValueError as error:
        raise HTTPException(404, "Unknown route") from error


@router.get("/stops")
def stops(region_id: str | None = None, source: Source = "demo"):
    network = load_network()
    if region_id is not None and region_id not in {r["region_id"] for r in network["regions"]}:
        raise HTTPException(422, "Unknown city area")
    ordered = patterns({"network": network}, region_id)
    unique = {stop["stop_id"]: stop for route in ordered.values() for stop in route}
    return sorted(unique.values(), key=lambda stop: (stop["name"], stop["stop_id"]))


def validate_stops(origin, destination, region, source):
    allowed = {stop["stop_id"] for stop in stops(region, source)}
    if origin not in allowed or destination is not None and destination not in allowed:
        raise HTTPException(422, "Choose stops in the selected city area")
    if destination == origin:
        raise HTTPException(422, "Origin and destination must be different")


@router.get("/buses/{bus_id}/eta")
def bus_eta(bus_id: str):
    """Public copy of /bus/{id}/eta for the passenger map's bus popup (live buses)."""
    return get_stops_eta(bus_id)


@router.get("/snapshot")
def snapshot(source: Source = "demo"):
    data = context(source)
    public = []
    for definition in data["definitions"]:
        bus_id = definition["bus_id"]
        state = data["states"].get(bus_id)
        clean = None
        if state and state.get("lat") is not None:
            clean = {key: state.get(key) for key in ("lat", "lon", "speed", "direction_id", "next_stop_id", "bus_status", "occupancy", "sequence_number")}
            metadata = state.get("edge_metadata") or {}
            clean["edge_metadata"] = {key: metadata.get(key) for key in ("passengers", "capacity", "occupancy_percent", "crowding_status", "current_stop_id", "route_progress_percent")} if metadata.get("capacity") and metadata.get("passengers") is not None else None
            clean.update(data_age_seconds=data["ages"].get(bus_id, 30), service_status=data["status"].get(bus_id, "unknown"))
            stamp = state.get("last_updated")
            clean["last_updated"] = stamp.isoformat() if hasattr(stamp, "isoformat") else metadata.get("source_timestamp")
        public.append(dict(bus_id=bus_id, route_id=definition["route_id"], region_id=definition["region_id"], state=clean))
    from services.central import health
    return {"source": source, "buses": public, "clock": dict(seconds=data["time"], **health.simulation_clock.snapshot()) if source == "demo" else None}


@router.get("/arrivals")
def stop_arrivals(stop_id: str, region_id: str | None = None, party_size: int = Query(1, ge=1, le=20), source: Source = "demo"):
    validate_stops(stop_id, None, region_id, source)
    choices = arrivals(context(source), stop_id, region_id)
    return {"stop_id": stop_id, "arrivals": choices, **rank_options(choices, party_size),
            "message": "No approaching buses with reliable predictions" if not choices else "Current approaching buses"}


@router.get("/journeys")
def journeys(origin: str, destination: str, region_id: str | None = None,
             party_size: int = Query(1, ge=1, le=20), source: Source = "demo"):
    validate_stops(origin, destination, region_id, source)
    options = plan_journeys(context(source), origin, destination, region_id)
    return {"origin": origin, "destination": destination, **rank_options(options, party_size),
            "message": "No supported journey with current bus predictions" if not options else "Direct and one-transfer journeys"}


@router.get("/recommendations")
def recommendations(origin: str, destination: str | None = None, region_id: str | None = None,
                    party_size: int = Query(1, ge=1, le=20), source: Source = "demo"):
    return journeys(origin, destination, region_id, party_size, source) if destination else stop_arrivals(origin, region_id, party_size, source)


@router.get("/nearby")
def nearby(lat: float = Query(ge=-90, le=90), lon: float = Query(ge=-180, le=180),
           region_id: str | None = None, party_size: int = Query(1, ge=1, le=20), source: Source = "demo"):
    from services.central.eta import distance_km
    network_stops = stops(region_id, source)
    data = context(source)
    closest = sorted(network_stops, key=lambda stop: distance_km(lat, lon, stop["lat"], stop["lon"]))[:5]
    choices = []
    for stop in closest:
        walking_km = distance_km(lat, lon, stop["lat"], stop["lon"])
        for bus in arrivals(data, stop["stop_id"], region_id):
            # Allow walking at 4.5 km/h plus a boarding margin. Do not suggest a bus already missed.
            walking_minutes = walking_km / 4.5 * 60
            if walking_minutes + .5 > bus["eta_minutes"]:
                continue
            choices.append(dict(bus, stop_id=stop["stop_id"], stop_name=stop["name"],
                walking_metres=round(walking_km*1000), walking_minutes=round(walking_minutes, 2)))
    return dict(nearest_stops=[dict(stop_id=s["stop_id"], name=s["name"], distance_metres=round(distance_km(lat,lon,s["lat"],s["lon"])*1000)) for s in closest],
                **rank_options(choices, party_size), message="No bus can currently be reached at the nearest five stops" if not choices else "Nearby stop options")

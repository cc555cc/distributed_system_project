"""Read the same city, stop, route and bus configuration in every service."""

import json
import math
from pathlib import Path

DEFAULT_NETWORK_PATH = Path(__file__).resolve().parents[1] / "routes.json"
NETWORK_VERSION = "transit-network-v1"


def validate_network(data: dict) -> dict:
    """Reject ambiguous assignments before a simulator or edge starts running."""
    if data.get("schema_version") != NETWORK_VERSION:
        raise ValueError("Unsupported transit network version")
    cities = {city["city_id"] for city in data["cities"]}
    if len(cities) != len(data["cities"]):
        raise ValueError("City IDs must be unique")
    stops = {stop["stop_id"]: stop for stop in data["stops"]}
    if len(stops) != len(data["stops"]):
        raise ValueError("Stop IDs must be globally unique")
    for stop in stops.values():
        if stop["city_id"] not in cities:
            raise ValueError("Stop belongs to an unknown city")
        for field, bound in (("lat", 90), ("lon", 180)):
            value = stop[field]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not -bound <= value <= bound or not math.isfinite(value):
                raise ValueError("Stop coordinates are invalid")
    regions = {region["region_id"]: region for region in data["regions"]}
    if len(regions) != len(data["regions"]):
        raise ValueError("Region IDs must be globally unique for MQTT topics")
    if any(region["city_id"] not in cities for region in regions.values()):
        raise ValueError("Region belongs to an unknown city")
    routes = {route["route_id"]: route for route in data["routes"]}
    if len(routes) != len(data["routes"]):
        raise ValueError("Route IDs must be globally unique")
    for route in routes.values():
        owner = regions.get(route["region_id"])
        if owner is None or owner["city_id"] != route["city_id"]:
            raise ValueError("Route processing owner does not match its city/region")
        if len(route["stops"]) != 5 or len(set(route["stops"])) != 5:
            raise ValueError("Each route needs exactly five distinct ordered stops")
        if any(stop not in stops for stop in route["stops"]):
            raise ValueError("Route contains an unknown stop")
        served = set(route["served_city_ids"])
        actual = {stops[stop]["city_id"] for stop in route["stops"]}
        if served != actual or not served <= cities or route["city_id"] not in served:
            raise ValueError("Route served cities do not match its stops")
        if len(served) > 1 and not route.get("intercity", False):
            raise ValueError("Cross-city stops require an explicit intercity route")
        if route.get("intercity", False) != (len(served) > 1):
            raise ValueError("Intercity flag does not match route coverage")
    buses = {bus["bus_id"]: bus for bus in data["buses"]}
    if len(buses) != len(data["buses"]):
        raise ValueError("Bus IDs must be globally unique")
    for bus in buses.values():
        route = routes.get(bus["route_id"])
        if route is None or (bus["city_id"], bus["region_id"]) != (route["city_id"], route["region_id"]):
            raise ValueError("Bus must use its route's configured processing assignment")
        region = regions[bus["region_id"]]
        if bus["edge_id"] not in (region["primary_edge"], region["backup_edge"]):
            raise ValueError("Bus has an unregistered regional edge owner")
        if isinstance(bus["capacity"], bool) or not isinstance(bus["capacity"], int) or bus["capacity"] < 1:
            raise ValueError("Bus capacity must be a positive integer")
        if bus.get("direction_id", 0) not in (0, 1):
            raise ValueError("Bus direction must be 0 or 1")
    return data


def load_network(path: str | Path = DEFAULT_NETWORK_PATH) -> dict:
    """Load once at startup; all callers use the same validation rules."""
    return validate_network(json.loads(Path(path).read_text(encoding="utf-8")))


def route_stops(network: dict, route_id: str, direction: int = 0) -> list[dict]:
    """Return the configured stop order, including explicit intercity stops."""
    if direction not in (0, 1):
        raise ValueError("Direction must be 0 or 1")
    route = next((route for route in network["routes"] if route["route_id"] == route_id), None)
    if route is None:
        raise ValueError("Unknown route ID")
    stops = {stop["stop_id"]: stop for stop in network["stops"]}
    ids = route["stops"] if direction == 0 else list(reversed(route["stops"]))
    return [dict(stops[stop], direction_id=direction, position=position)
            for position, stop in enumerate(ids)]

"""Validate configured route membership without guessing arrival or ETA."""

from services.edge.config import RouteDefinition


class RouteProcessor:
    def __init__(self, routes: tuple[RouteDefinition, ...]) -> None:
        self.routes = {}
        self.positions = {}
        for route in routes:
            key = (route.city_id, route.region_id, route.route_id, route.direction_id)
            if key in self.routes:
                raise ValueError("Duplicate directional route")
            self.routes[key] = route
            self.positions[key] = {stop: i for i, stop in enumerate(route.stops)}

    def process(self, telemetry: dict, city: str, region: str) -> dict:
        """Return only progress explicitly supported by reported stop IDs."""
        direction = telemetry.get("direction_id", 0)
        key = (city, region, telemetry["route_id"], direction)
        route = self.routes.get(key)
        if route is None:
            raise ValueError("Route/city/region/direction is not configured")
        positions = self.positions[key]
        current, following = telemetry.get("current_stop_id"), telemetry.get("next_stop_id")
        for stop in (current, following):
            if stop is not None and stop not in positions:
                raise ValueError("Reported stop does not belong to route")
        if current is not None and following is not None:
            if positions[following] != positions[current] + 1:
                raise ValueError("Current and next stop must be adjacent in travel order")
        progress = None
        if current is not None and len(route.stops) > 1:
            progress = positions[current] / (len(route.stops) - 1) * 100
        return {"direction_id": direction, "current_stop_id": current,
                "next_stop_id": following, "route_progress_percent": progress,
                "progress_basis": "reported_current_stop" if progress is not None else "unavailable"}

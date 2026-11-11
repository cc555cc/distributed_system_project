"""Load the edge's versioned network configuration once at startup."""

import json
import math
from dataclasses import dataclass
from pathlib import Path

from services.edge.validation.bus_registry import BusAssignment, BusRegistry


@dataclass(frozen=True)
class EdgeConfig:
    """One process owns an explicitly configured city/region assignment."""

    city_id: str
    region_id: str
    edge_id: str
    max_buses: int = 1000
    freshness_seconds: float = 30.0
    expire_seconds: float = 300.0
    medium_threshold: float = 50.0
    high_threshold: float = 80.0

    def __post_init__(self) -> None:
        if not all(isinstance(x, str) and x.strip() for x in
                   (self.city_id, self.region_id, self.edge_id)):
            raise ValueError("City, region and edge identifiers are required")
        if isinstance(self.max_buses, bool) or not isinstance(self.max_buses, int) or self.max_buses < 1:
            raise ValueError("max_buses must be a positive integer")
        values = (self.freshness_seconds, self.expire_seconds,
                  self.medium_threshold, self.high_threshold)
        if not all(isinstance(x, (int, float)) and not isinstance(x, bool)
                   and math.isfinite(x) for x in values):
            raise ValueError("Timeouts and thresholds must be finite numbers")
        if not 0 < self.freshness_seconds <= self.expire_seconds:
            raise ValueError("Require 0 < freshness_seconds <= expire_seconds")
        if not 0 <= self.medium_threshold < self.high_threshold <= 100:
            raise ValueError("Require 0 <= medium < high <= 100")


@dataclass(frozen=True)
class RouteDefinition:
    """An ordered directional route, scoped to one city and region."""

    city_id: str
    region_id: str
    route_id: str
    stops: tuple[str, ...]
    direction_id: int = 0

    def __post_init__(self) -> None:
        if not all(isinstance(x, str) and x.strip() for x in
                   (self.city_id, self.region_id, self.route_id, *self.stops)):
            raise ValueError("Route identifiers and stop IDs must be nonempty strings")
        if not self.stops or len(set(self.stops)) != len(self.stops):
            raise ValueError("A directional route needs distinct ordered stop IDs")
        if isinstance(self.direction_id, bool) or not isinstance(self.direction_id, int) or self.direction_id < 0:
            raise ValueError("direction_id must be a nonnegative integer")


@dataclass(frozen=True)
class NetworkConfig:
    """Immutable route definitions and the registry shared by ingress/processing."""

    registry: BusRegistry
    routes: tuple[RouteDefinition, ...]

    @classmethod
    def load(cls, path: str | Path) -> "NetworkConfig":
        """Load proposal version 1; reject ambiguous assignments before startup."""
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if data.get("schema_version") != "edge-network-proposal-v1":
            raise ValueError("Unsupported edge network schema")
        routes = tuple(RouteDefinition(**{**r, "stops": tuple(r["stops"])})
                       for r in data["routes"])
        route_keys = {(r.city_id, r.region_id, r.route_id, r.direction_id) for r in routes}
        if len(route_keys) != len(routes):
            raise ValueError("Duplicate directional route")
        # The current MQTT contract identifies buses globally by bus_id.
        assignments: dict[str, BusAssignment] = {}
        for bus in data["buses"]:
            bus = dict(bus)
            bus_id = bus.pop("bus_id")
            if not isinstance(bus_id, str) or not bus_id.strip() or bus_id in assignments:
                raise ValueError("Bus IDs must be nonempty and globally unique")
            assignment = BusAssignment(**bus)
            if not assignment.edge_id:
                raise ValueError("Each configured bus needs an explicit edge owner")
            if not any(key[:3] == (assignment.city_id, assignment.region_id, assignment.route_id)
                       for key in route_keys):
                raise ValueError("Bus assignment has no matching route")
            assignments[bus_id] = assignment
        return cls(BusRegistry(assignments), routes)

"""Temporary, explicit bus-to-route/region assignments for EDGE-01.

Replace these fixtures with Zachary's agreed registry once it is available.
A registry match is a consistency check, NOT publisher authentication.
"""

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class BusAssignment:
    """The one route and region currently assigned to a simulated bus."""

    region_id: str
    route_id: str
    city_id: str = "CITY-A"
    edge_id: str | None = None

    def __post_init__(self) -> None:
        if not all(isinstance(x, str) and x.strip() for x in (self.city_id, self.region_id, self.route_id)):
            raise ValueError("Bus assignment identifiers must be nonempty strings")
        if self.edge_id is not None and (not isinstance(self.edge_id, str) or not self.edge_id.strip()):
            raise ValueError("Edge owner must be a nonempty string")


class BusRegistry:
    """Look up known bus IDs and check their declared assignments."""

    def __init__(self, assignments: Mapping[str, BusAssignment]) -> None:
        self._assignments = dict(assignments)

    def get(self, bus_id: str) -> BusAssignment | None:
        """Resolve the single authoritative configured assignment."""
        return self._assignments.get(bus_id)

    def check_identity(
        self, bus_id: str, region_id: str, route_id: str, city_id: str | None = None
    ) -> list[str]:
        """Return human-readable errors; empty list means a valid assignment."""
        assigned = self._assignments.get(bus_id)
        if assigned is None:
            return [f"Unknown bus ID: {bus_id}"]

        errors: list[str] = []
        if city_id is not None and assigned.city_id != city_id:
            errors.append("Bus is not assigned to declared city")
        if assigned.region_id != region_id:
            errors.append(f"Bus {bus_id} is not assigned to region {region_id}")
        if assigned.route_id != route_id:
            errors.append(f"Bus {bus_id} is not assigned to route {route_id}")
        return errors


# TODO (Zachary integration): load assignments from agreed shared route data.
DEFAULT_BUS_REGISTRY = BusRegistry(
    {
        "BUS-001": BusAssignment(region_id="north", route_id="R1"),
        "BUS-002": BusAssignment(region_id="north", route_id="R1"),
        "BUS-003": BusAssignment(region_id="south", route_id="R2"),
    }
)

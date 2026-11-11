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


class BusRegistry:
    """Look up known bus IDs and check their declared assignments."""

    def __init__(self, assignments: Mapping[str, BusAssignment]) -> None:
        self._assignments = dict(assignments)

    def check_identity(
        self, bus_id: str, region_id: str, route_id: str
    ) -> list[str]:
        """Return human-readable errors; empty list means a valid assignment."""
        assigned = self._assignments.get(bus_id)
        if assigned is None:
            return [f"Unknown bus ID: {bus_id}"]

        errors: list[str] = []
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

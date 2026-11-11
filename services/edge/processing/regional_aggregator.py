"""Statistics for the local edge's assigned buses, never a claimed regional total."""

from collections import Counter


def aggregate(states: list[dict]) -> dict:
    """Exclude stale/offline readings; keep unknown occupancy out of the mean."""
    active = [s for s in states if s["freshness"] == "current"
              and s.get("sensor_status", "healthy") != "offline"]
    occupancy = [s["occupancy_percent"] for s in active
                 if s["occupancy_percent"] is not None]
    return {
        "scope": "edge_assigned_buses",
        "active_buses": len(active),
        "crowded_buses": sum(s["crowding_status"] == "high" for s in active),
        "average_occupancy_percent": sum(occupancy) / len(occupancy) if occupancy else None,
        "occupancy_samples": len(occupancy),
        "active_buses_per_route": dict(Counter(s["route_id"] for s in active)),
    }

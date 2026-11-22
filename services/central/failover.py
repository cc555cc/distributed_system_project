"""Central is responsible for moving buses away from failed edges and back after recovery.

Central is the single ownership authority, it:
  1. records edge status changes
  2. recomputes who should own each bus
  3. logs each bus that moved
  4. publishes the full, retained assignment of every region that changed,
     edges should follow the published list and start/stop processing the moved buses
"""

import logging

logger = logging.getLogger(__name__)


def run_round(monitor, ownership, publish) -> set[str]:
    """One failover round; returns the regions whose assignment changed.

    publish(region_id, payload) sends the assignment to the edges (MQTT in production).
    """
    monitor.poll()
    before = ownership.owners()
    changed = ownership.reconcile(monitor.statuses())
    for bus_id, owner in sorted(ownership.owners().items()):
        if before[bus_id] != owner:
            monitor.events.append("bus_reassigned", bus_id=bus_id, region_id=ownership.region_of[bus_id],
                                  from_edge=before[bus_id], to_edge=owner)
            logger.warning("Bus %s reassigned from %s to %s", bus_id, before[bus_id], owner)
    for region_id in sorted(changed):
        publish(region_id, ownership.assignment(region_id))
    return changed

"""Small EDGE-01 receiver proving validation occurs before edge processing.

EDGE-02 will replace the diagnostic in-memory list with real per-bus
caching, sequence freshness, route processing and occupancy calculations.
"""

import logging
import os
from dataclasses import dataclass, field
from queue import Empty

from services.edge.mqtt_subscriber import TelemetrySubscriber
from services.edge.validator import BusTelemetry

logger = logging.getLogger(__name__)


@dataclass
class EdgeIngress:
    """Record messages that have passed subscriber/validator checks."""

    region_id: str
    accepted: list[BusTelemetry] = field(default_factory=list)

    def process_telemetry(self, telemetry: BusTelemetry) -> None:
        """EDGE-01 placeholder for later route/cache business logic."""
        if telemetry["region_id"] != self.region_id:
            raise ValueError("Wrong region passed to edge ingress")
        self.accepted.append(telemetry)
        logger.info("Edge received validated bus=%s", telemetry["bus_id"])

    def drain_pending(self, subscriber: TelemetrySubscriber) -> int:
        """Move all currently queued valid messages into the edge receiver."""
        if subscriber.region_id != self.region_id:
            raise ValueError("Subscriber and edge must belong to same region")
        count = 0
        while True:
            try:
                item = subscriber.messages.get_nowait()
            except Empty:
                return count
            self.process_telemetry(item)
            count += 1


def main() -> None:
    """Run a regional edge ingestion loop against a live MQTT broker."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    region = os.getenv("REGION_ID", "north")
    subscriber = TelemetrySubscriber(
        region_id=region,
        edge_id=os.getenv("EDGE_ID", "north-edge-a"),
        broker_host=os.getenv("MQTT_HOST", "127.0.0.1"),
        broker_port=int(os.getenv("MQTT_PORT", "1883")),
    )
    receiver = EdgeIngress(region_id=region)
    subscriber.start()
    try:
        while True:
            # Blocking queue read keeps CPU use low when no bus is publishing.
            try:
                telemetry = subscriber.messages.get(timeout=1.0)
            except Empty:
                # Idle periods are normal when no buses send telemetry.
                continue
            receiver.process_telemetry(telemetry)
    except KeyboardInterrupt:
        logger.info("Stopping edge ingestion")
    finally:
        subscriber.stop()


if __name__ == "__main__":
    main()

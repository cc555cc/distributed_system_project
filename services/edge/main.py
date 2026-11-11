"""Run one configured edge instance, without EDGE-03 forwarding."""

import logging
import os
from queue import Empty

from services.edge.communication.mqtt_subscriber import TelemetrySubscriber
from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.edge_server import EdgeIngress
from services.edge.processing.processor import EdgeProcessor


def main() -> None:
    """Load network definitions once and consume the existing ingress queue."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    network = NetworkConfig.load(os.environ["EDGE_NETWORK_CONFIG"])
    config = EdgeConfig(
        city_id=os.environ["CITY_ID"], region_id=os.environ["REGION_ID"],
        edge_id=os.environ["EDGE_ID"],
        max_buses=int(os.getenv("EDGE_MAX_BUSES", "1000")),
        freshness_seconds=float(os.getenv("EDGE_FRESHNESS_SECONDS", "30")),
        expire_seconds=float(os.getenv("EDGE_EXPIRE_SECONDS", "300")),
        medium_threshold=float(os.getenv("EDGE_MEDIUM_THRESHOLD", "50")),
        high_threshold=float(os.getenv("EDGE_HIGH_THRESHOLD", "80")),
    )
    subscriber = TelemetrySubscriber(
        config.region_id, config.edge_id, city_id=config.city_id,
        registry=network.registry, broker_host=os.getenv("MQTT_HOST", "127.0.0.1"),
        broker_port=int(os.getenv("MQTT_PORT", "1883")),
    )
    receiver = EdgeIngress(EdgeProcessor(config, network))
    subscriber.start()
    try:
        while True:
            try:
                telemetry = subscriber.messages.get(timeout=1.0)
            except Empty:
                continue
            try:
                result = receiver.process_telemetry(telemetry)
                logging.info("Processing result=%s", result.status.value)
                # result.output is returned to callers; no MQTT send yet.
            finally:
                subscriber.messages.task_done()
    except KeyboardInterrupt:
        logging.info("Stopping edge ingestion")
    finally:
        subscriber.stop()


if __name__ == "__main__":
    main()

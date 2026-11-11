"""Run one configured edge instance, with validated automatic EDGE-03 forwarding."""

import logging
import os
from queue import Empty

from services.edge.communication.mqtt_subscriber import TelemetrySubscriber
from services.edge.communication.edge_to_central import CentralForwarder, EdgeToCentral
from shared.schema.bus_state import BusStateUpdate
from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.edge_server import EdgeIngress
from services.edge.processing.processor import EdgeProcessor


def load_edge_config() -> EdgeConfig:
    """Read this server's city, region and cache settings from the environment."""
    return EdgeConfig(
        city_id=os.environ["CITY_ID"], region_id=os.environ["REGION_ID"],
        edge_id=os.environ["EDGE_ID"],
        max_buses=int(os.getenv("EDGE_MAX_BUSES", "1000")),
        freshness_seconds=float(os.getenv("EDGE_FRESHNESS_SECONDS", "30")),
        expire_seconds=float(os.getenv("EDGE_EXPIRE_SECONDS", "300")),
        medium_threshold=float(os.getenv("EDGE_MEDIUM_THRESHOLD", "50")),
        high_threshold=float(os.getenv("EDGE_HIGH_THRESHOLD", "80")),
    )


def create_subscriber(config: EdgeConfig, network: NetworkConfig) -> TelemetrySubscriber:
    """Use the same bus registry for incoming validation and local processing."""
    return TelemetrySubscriber(
        config.region_id, config.edge_id, city_id=config.city_id,
        registry=network.registry, broker_host=os.getenv("MQTT_HOST", "127.0.0.1"),
        broker_port=int(os.getenv("MQTT_PORT", "1883")),
    )


def process_pending_messages(subscriber: TelemetrySubscriber, receiver: EdgeIngress,
                             forwarder: CentralForwarder | None = None) -> None:
    """Wait for checked telemetry and process it outside the MQTT callback."""
    while True:
        try:
            telemetry = subscriber.messages.get(timeout=1.0)
        except Empty:
            if forwarder is not None:
                forwarder.flush_one()
            continue
        try:
            result = (forwarder.process(telemetry) if forwarder is not None
                      else receiver.process_telemetry(telemetry))
            logging.info("Processing result=%s", result.status.value)
            if forwarder is not None:
                forwarder.flush_one()
        except ValueError as error:
            logging.warning("Not forwarded: validation failed (%s)", type(error).__name__)
        finally:
            subscriber.messages.task_done()


def main() -> None:
    """Start the subscriber, run local processing and clean up on shutdown."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    # Load definitions once. Do not fetch routes from the database per message.
    network = NetworkConfig.load(os.environ["EDGE_NETWORK_CONFIG"])
    config = load_edge_config()
    subscriber = create_subscriber(config, network)
    receiver = EdgeIngress(EdgeProcessor(config, network))
    # Units must be explicitly selected; an integer alone does not define them.
    sender = EdgeToCentral(config, network.registry, BusStateUpdate)
    forwarder = CentralForwarder(receiver, sender, os.environ["CENTRAL_OCCUPANCY_UNIT"])
    try:
        sender.connect(os.getenv("CENTRAL_MQTT_HOST", os.getenv("MQTT_HOST", "127.0.0.1")),
                       int(os.getenv("CENTRAL_MQTT_PORT", os.getenv("MQTT_PORT", "1883"))))
        subscriber.start()
        process_pending_messages(subscriber, receiver, forwarder)
    except KeyboardInterrupt:
        logging.info("Stopping edge ingestion")
    finally:
        subscriber.stop()
        sender.close()


if __name__ == "__main__":
    main()

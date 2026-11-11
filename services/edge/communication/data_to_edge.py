"""MQTT ingress boundary: decode, validate and queue accepted telemetry.

The broker client is imported only when start() runs. Consequently the
validation tests can run without a broker or the Paho package installed.
"""

from __future__ import annotations

import json
import os
import logging
from queue import Empty, Full, Queue
from typing import Any, Protocol, TYPE_CHECKING

from services.edge.config import EdgeConfig, NetworkConfig

if TYPE_CHECKING:
    from services.edge.edge_server import EdgeIngress
    from services.edge.communication.edge_to_central import CentralForwarder

from services.edge.validation.bus_registry import DEFAULT_BUS_REGISTRY, BusRegistry
from services.edge.validation.validator import BusTelemetry, parse_telemetry

logger = logging.getLogger(__name__)

MAX_PAYLOAD_BYTES = 64 * 1024


class MQTTMessageLike(Protocol):
    """The attributes the subscriber needs from a Paho MQTT message."""

    topic: str
    payload: bytes


class TelemetrySubscriber:
    """Subscribe to one region and queue only successfully checked messages."""

    def __init__(
        self,
        region_id: str,
        edge_id: str,
        *,
        broker_host: str = "127.0.0.1",
        broker_port: int = 1883,
        registry: BusRegistry = DEFAULT_BUS_REGISTRY,
        queue_size: int = 1000,
        city_id: str | None = None,
    ) -> None:
        if not region_id or not edge_id:
            raise ValueError("region_id and edge_id are required")
        if queue_size <= 0:
            raise ValueError("queue_size must be positive")

        self.city_id = city_id
        self.region_id = region_id
        self.edge_id = edge_id
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.registry = registry
        self.topic_filter = f"transit/buses/{region_id}/+/telemetry"
        self.messages: Queue[BusTelemetry] = Queue(maxsize=queue_size)
        self._client: Any | None = None  # Paho's external client API boundary.

    def handle_raw_message(self, topic: str, raw_payload: bytes) -> bool:
        """Handle one broker-delivered message; True iff queued for the edge.

        Deliberately does not calculate occupancy, mutate bus state, or
        compare a sequence number with prior readings; that is EDGE-02.
        """
        parts = topic.split("/")
        if (
            len(parts) != 5
            or parts[0:2] != ["transit", "buses"]
            or parts[2] != self.region_id
            or not parts[3]
            or parts[4] != "telemetry"
        ):
            logger.warning("Rejected message: unexpected MQTT topic %s", topic)
            return False

        if len(raw_payload) > MAX_PAYLOAD_BYTES:
            logger.warning("Rejected message: MQTT payload exceeds 64 KiB")
            return False

        try:
            decoded: object = json.loads(raw_payload.decode("utf-8"))
        except (UnicodeDecodeError, ValueError, RecursionError):
            logger.warning("Rejected message: malformed UTF-8 or JSON")
            return False

        # Check the decoded message before letting it enter the processing queue.
        telemetry, errors = parse_telemetry(
            decoded,
            expected_region=parts[2],
            expected_bus=parts[3],
            registry=self.registry,
        )
        if telemetry is None:
            # Avoid logging raw, potentially sensitive or malformed payloads.
            logger.warning("Rejected telemetry: %s", "; ".join(errors))
            return False

        assignment = self.registry.get(telemetry["bus_id"])
        if self.city_id is not None and (assignment is None or assignment.city_id != self.city_id):
            logger.warning("Rejected telemetry: bus belongs to another city")
            return False

        try:
            # Do not block the MQTT callback while waiting for processing space.
            self.messages.put_nowait(telemetry)
        except Full:
            # EDGE-02/08 will add a deliberate backpressure/retry policy.
            logger.error("Rejected telemetry: edge ingress queue is full")
            return False

        logger.info(
            "Accepted bus=%s region=%s sequence=%d",
            telemetry["bus_id"],
            telemetry["region_id"],
            telemetry["sequence_number"],
        )
        return True

    def on_message(
        self, client: object, userdata: object, message: MQTTMessageLike
    ) -> None:
        """Paho callback: delegate transport events to the testable handler."""
        self.handle_raw_message(message.topic, message.payload)

    def on_connect(
        self,
        client: Any,
        userdata: object,
        flags: object,
        reason_code: Any,
        properties: object,
    ) -> None:
        """Subscribe after successful connection, including reconnects."""
        if reason_code.is_failure:
            logger.error("MQTT connection refused: %s", reason_code)
            return
        result, _mid = client.subscribe(self.topic_filter, qos=1)
        if result != 0:
            logger.error("MQTT subscribe request failed: %s", result)
        else:
            logger.info("Subscribed to %s", self.topic_filter)

    def on_disconnect(
        self,
        client: object,
        userdata: object,
        disconnect_flags: object,
        reason_code: object,
        properties: object,
    ) -> None:
        """Report a disconnect; Paho manages automatic reconnect attempts."""
        logger.warning("MQTT disconnected: %s", reason_code)

    def start(self) -> None:
        """Start the real MQTT client (requires Paho and a running broker)."""
        try:
            import paho.mqtt.client as mqtt
        except ImportError as exc:
            raise RuntimeError("Install paho-mqtt before connecting to MQTT") from exc

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self.edge_id,
            protocol=mqtt.MQTTv311,
        )
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.on_disconnect = self.on_disconnect
        client.reconnect_delay_set(min_delay=1, max_delay=10)

        # TODO (Julian, security): broker authentication, ACLs and TLS.
        client.connect(self.broker_host, self.broker_port, keepalive=60)
        client.loop_start()
        self._client = client

    def stop(self) -> None:
        """Disconnect and stop the network loop, if one was started."""
        if self._client is not None:
            self._client.disconnect()
            self._client.loop_stop()
            self._client = None


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


def run() -> None:
    """Start the subscriber, run local processing and clean up on shutdown."""
    # Import the processing/sending side when the worker starts. Receiving can
    # still be imported and tested without starting connections or central code.
    from services.edge.communication.edge_to_central import CentralForwarder, EdgeToCentral
    from services.edge.edge_server import EdgeIngress
    from services.edge.processing.processor import EdgeProcessor
    from shared.schema.bus_state import BusStateUpdate

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
    run()

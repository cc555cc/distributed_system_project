"""Take input from the publisher and validate it so it is ready for the edge
server to do some processing on it. Valid messages are added to the queue,
and invalid messages are rejected.
"""

# Used to delay typing 
from __future__ import annotations

# Import libuaries that will be used in the code
import json
import os
import logging
import time
from queue import Empty, Full, Queue
from typing import Any, Protocol, TYPE_CHECKING


from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.edge_server_health import EdgeMonitor
from services.messaging.topics import bus_heartbeat_filter

# So type checkers, now what classes i am refering to and what type of data it those class need
if TYPE_CHECKING:
    from services.edge.edge_server import EdgeIngress
    from services.edge.communication.edge_to_central import CentralForwarder

# This imports the existing default bus registry object and the BusRegistry class 
# used to create custom registries for other buses.
from services.edge.validation.bus_registry import DEFAULT_BUS_REGISTRY, BusRegistry
from services.edge.validation.validator import BusTelemetry, parse_telemetry

# Log Events Happening Without Stopping Code
logger = logging.getLogger(__name__)

# MQTT Messages Have A Specific Size Limit, So We Define A Constant For That
MAX_PAYLOAD_BYTES = 64 * 1024


class MQTTMessageLike(Protocol):
    """The data the MQTT Subscriber needs from the MQTT Publisher."""
    topic: str  # Where Message Came From
    payload: bytes # It's Data In Bytes

# Basically it is the class that sets up and manages the MQTT subscriber and allows it to deal with bus messages
class TelemetrySubscriber:
    """Receive bus messages from a specific region, validate them, 
    and store valid messages in a queue for further processing."""

    def __init__(self,  # Refers to the current subscriber object
        region_id: str,  # Region the subscriber listens to (e.g., north)
        edge_id: str,  # Unique identifier of this edge server
        *,  # All parameters below must be passed by name
        broker_host: str = "127.0.0.1",  # MQTT broker IP (localhost by default)
        broker_port: int = 1883,  # MQTT connection port
        registry: BusRegistry = DEFAULT_BUS_REGISTRY,  # Known bus information
        queue_size: int = 1000,  # Maximum messages waiting for processing
        city_id: str | None = None,  # City ID (optional)
        monitor: EdgeMonitor | None = None,
    ) -> None:  # Constructor does not return a value

        # Check that the required identifiers exist
        if not region_id or not edge_id:
            raise ValueError("region_id and edge_id don't exist and are required")

        # Ensure the queue can hold at least one message
        if queue_size <= 0:
            raise ValueError("queue_size must be positive")

        # Store configuration inside the subscriber object
        self.monitor = monitor
        self.city_id = city_id  # City this subscriber belongs to
        self.region_id = region_id  # Region to monitor
        self.edge_id = edge_id  # Edge server identifier
        self.broker_host = broker_host  # Broker address
        self.broker_port = broker_port  # Broker port
        self.registry = registry  # Registry used to validate buses

        # Subscribe to telemetry from all buses in this region
        # '+' represents any single bus ID
        self.topic_filter = f"transit/buses/{region_id}/+/telemetry"

        # Create a queue to temporarily hold validated telemetry
        self.messages: Queue[BusTelemetry] = Queue(maxsize=queue_size)

        # MQTT client is initially empty; start() creates it later
        # This is what connets to the MQTT broker and handles incoming messages
        self._client: Any | None = None


    # This is the main function that handles input data and to make it ready for processing
    def handle_raw_message(self, topic: str, raw_payload: bytes) -> bool:
        """Recieve a raw MQTT message, validate it, and enqueue it for processing if valid.
        Returns True if the message was accepted, False if rejected."""

        # Split the MQTT Identification Label into parts to validate its structure and content
        parts = topic.split("/")

        # Check if the Identification Label parts matches the expected structure and content for a valid message
        if (len(parts) != 5 or parts[0:2] != ["transit", "buses"] or parts[2] != self.region_id
            or not parts[3] or parts[4] != "telemetry"):

            # Log a Warning if Idenfication Label mismatches expected structure and content for a valid message
            logger.warning("Rejected message: unexpected MQTT topic %s", topic)
            return False

        # Log a Warning if the MQTT message exceeds the maximum allowed size of 64 KiB
        if len(raw_payload) > MAX_PAYLOAD_BYTES:
            logger.warning("Rejected message: MQTT payload exceeds 64 KiB")
            return False

        # Attempt to decode the raw payload as UTF-8 and then as JSON so it can be used in future.
        try:
            decoded: object = json.loads(raw_payload.decode("utf-8"))

        # Could not transform into UTF-8 or JSON.
        except (UnicodeDecodeError, ValueError, RecursionError):
            logger.warning("Rejected message: malformed UTF-8 or JSON")
            return False


        # Check the decoded to see if it is valid
        telemetry, errors = parse_telemetry(
            decoded, # Checks if sensor data is valid
            expected_region=parts[2], # Checks if expected region matches the topic region
            expected_bus=parts[3], # Checks if expected bus matches the topic bus
            registry=self.registry, # Uses the bus registry to validate the bus infromation
        )

        # If data is invalid, the code logs a warning and tells why it failed to validate data
        if telemetry is None:
            # Avoid logging raw, potentially sensitive or malformed payloads.
            logger.warning("Rejected telemetry: %s", "; ".join(errors))
            return False

        # Checking whether the bus belongs to the correct city using the bus registry.
        assignment = self.registry.get(telemetry["bus_id"])
        if self.city_id is not None and (assignment is None or assignment.city_id != self.city_id):
            logger.warning("Rejected telemetry: bus belongs to another city")
            return False


        # A regional MQTT topic can reach both workers. Queue only our owned
        # buses so the partner's work does not fill this instance's queue.
        if assignment is not None and assignment.edge_id is not None and assignment.edge_id != self.edge_id:
            return False

        try:
            # Attempts to add valdiated data to the queue for future processing.
            self.messages.put_nowait(telemetry)

        except Full:
            # If the queue is full, it logs an error and returns False to indicate that the message was rejected.
            logger.error("Rejected telemetry: edge ingress queue is full")
            return False

        if self.monitor is not None:
            self.monitor.record_received()
        logger.info(
            # Records the accepted bus_id, region_id and sequence number of the msg
            "Accepted bus=%s region=%s sequence=%d",
            telemetry["bus_id"],
            telemetry["region_id"],
            telemetry["sequence_number"],
        )
        return True

    # So basically, when message arrives this is the validation steps it takes on header and payload.
    def on_message(self, client: object, userdata: object, message: MQTTMessageLike) -> None:
        """Paho callback: delegate transport events to the testable handler."""
        if self.monitor is not None and message.topic.startswith("transit/heartbeats/bus/"):
            self.monitor.receive(message.topic, message.payload)
            return
        self.handle_raw_message(message.topic, message.payload)

    # So it checks what is openning when MQTT Subscriber is connecting to MQTT Broker.
    def on_connect(self, client: Any, userdata: object, flags: object, reason_code: Any, properties: object,) -> None:
        """Subscribe after successful connection, including reconnects."""

        # When Subscriber cannot be connected to MQTT Broker.
        if reason_code.is_failure:
            logger.error("MQTT connection refused: %s", reason_code)
            return
        # Fails then Stop, otherwise subscribe to the topic filter and log the result.
        result, _mid = client.subscribe(self.topic_filter, qos=1)

        # Log What Happened.
        if result != 0:
            logger.error("MQTT subscribe request failed: %s", result)
        else:
            logger.info("Subscribed to %s", self.topic_filter)
        if self.monitor is not None:
            client.subscribe(bus_heartbeat_filter(self.region_id), qos=1)

    # When the MQTT subscriber disconnects from the MQTT broker.
    def on_disconnect(self, client: object, userdata: object, disconnect_flags: object, reason_code: 
                object, properties: object,) -> None:
        """Report a disconnect; Paho manages automatic reconnect attempts."""
        logger.warning("MQTT disconnected: %s", reason_code)

    def start(self) -> None:
        """Start the real MQTT client (requires Paho and a running broker)."""

       # Import the Paho MQTT Clinet Libuary when u start 
        try:
            import paho.mqtt.client as mqtt
        # Throw error if not installed and tell user to install it before connecting to MQTT
        except ImportError as exc:
            raise RuntimeError("Install paho-mqtt before connecting to MQTT") from exc

        # Define The Actual MQTT Client Object
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2, # Function that runs when a message is received
            client_id=self.edge_id, # Unique identifier for this MQTT client
            protocol=mqtt.MQTTv311, # Communication protocol version to use between Subscriber and Broker
        )

        #The previously defined functions are registered and configured to tell the MQTT client how to react when specific events occur.
        client.on_connect = self.on_connect
        client.on_message = self.on_message
        client.on_disconnect = self.on_disconnect
        client.reconnect_delay_set(min_delay=1, max_delay=10)

        # TODO (Julian, security): broker authentication, ACLs and TLS.
        client.connect(self.broker_host, self.broker_port, keepalive=60)
        client.loop_start()
        self._client = client

    # What happens when you disconect the MQTT Subscriber from the MQTT Broker.
    def stop(self) -> None:
        """Disconnect and stop the network loop, if one was started."""
        # It stops the MQTT client loop, disconects from the broker, 
        # and sets the client reference to None to indicate that it is no longer active.
        if self._client is not None:
            self._client.disconnect()
            self._client.loop_stop()
            self._client = None


# So Here we are loading the edge server config from env variables.
def load_edge_config(*, city_id=None, region_id=None, edge_id=None) -> EdgeConfig:
    """Read this server's city, region and cache settings from the environment."""
    return EdgeConfig(
        city_id=city_id or os.environ["CITY_ID"], region_id=region_id or os.environ["REGION_ID"],
        edge_id=edge_id or os.environ["EDGE_ID"],
        max_buses=int(os.getenv("EDGE_MAX_BUSES", "1000")),
        freshness_seconds=float(os.getenv("EDGE_FRESHNESS_SECONDS", "30")),
        expire_seconds=float(os.getenv("EDGE_EXPIRE_SECONDS", "300")),
        medium_threshold=float(os.getenv("EDGE_MEDIUM_THRESHOLD", "50")),
        high_threshold=float(os.getenv("EDGE_HIGH_THRESHOLD", "80")),
    )


def create_subscriber(config: EdgeConfig, network: NetworkConfig, *, monitor=None) -> TelemetrySubscriber:
    """ This is the function that creates an MQTT SUbscriber object using the provided edge and network configuration. 
        It sets up the subscriber to listen for telemetry messages from buses in the specified region and city, 
        using the provided bus registry for validation. The MQTT broker host and port are also configured based on environment variables, with default values if not specified."""
    return TelemetrySubscriber(
        config.region_id, config.edge_id, city_id=config.city_id,
        registry=network.registry, monitor=monitor, queue_size=int(os.getenv("EDGE_QUEUE_SIZE", "1000")),
        broker_host=os.getenv("MQTT_HOST", "127.0.0.1"),
        broker_port=int(os.getenv("MQTT_PORT", "1883")),
    )


def process_pending_messages(subscriber: TelemetrySubscriber, receiver: EdgeIngress,
                             forwarder: CentralForwarder | None = None, *, stop_event=None) -> None:
    """takes validated messages from the subscriber's queue, sends them through EdgeIngress for processing, 
    and coordinates with CentralForwarder to forward the processed results to the central server.."""

    while stop_event is None or not stop_event.is_set():
        # So basically waits for a second to get a message from the queue
        try:
            telemetry = subscriber.messages.get(timeout=1.0)

        # If centeral server exists, then attempt to send one pending message.
        except Empty:
            if forwarder is not None:
                forwarder.flush_one()
            continue

        # Try to process the telemetry locally.
        started = time.monotonic()
        try:    
            # If There is no fowarder, than process it locally. otherwise 
            # send it to the forwarder to process and send to the central server.
            result = (forwarder.process(telemetry) if forwarder is not None
                      else receiver.process_telemetry(telemetry))
            if subscriber.monitor is not None and result.status.value == "ACCEPTED":
                subscriber.monitor.mark_valid_data(telemetry["bus_id"])
            logging.info("Processing result=%s", result.status.value)

            # # If centeral server exists, then attempt to send one pending message.
            if forwarder is not None:
                forwarder.flush_one()

        # If the telemetry is invalid, log a warning and continue to the next message.
        except ValueError as error:
            logging.warning("Not forwarded: validation failed (%s)", type(error).__name__)

        # Mark the message as processed in the queue, regardless of whether it was successfully processed or not.
        finally:
            subscriber.messages.task_done()
            if subscriber.monitor is not None:
                subscriber.monitor.mark_processed(time.monotonic() - started)

# Run the pipeline that connects the subscriber, receiver, and forwarder to process telemetry messages 
# and forward them to the central server.

def run() -> None:
    """Keep the old single-worker command; main.py now creates the instance."""
    from services.edge.main import run_single_instance
    run_single_instance()

# If this file is run, it will run the run() function,
# which sets up the edge server's main tasks and starts processing telemetry messages.
if __name__ == "__main__":
    run()

# MQTT subscriber script, receives the validated bus states from edge server, and store them in the database
#
# expecting edge servers to publish topics in shape: transit/<region_id>/bus/<bus_id>/state
# with a JSON payload matching the BusStateUpdate method.

import json
import logging
import os

import paho.mqtt.client as mqtt
from pydantic import ValidationError

from services.central.routers.bus_states import BusStateUpdate, apply_bus_state
from services.edge.auth_edge_server import configure_mqtt_client
from services.messaging import alerts, commands, topics
from services.messaging.lamport import LamportClock
from services.messaging.topics import ALERT_FILTER, EDGE_HEARTBEAT_FILTER, assignment

logger = logging.getLogger(__name__)

CLIENT_ID = "central-server"

# Central's Lamport clock (MQTT-03). Advanced by every valid bus-state message.
clock = LamportClock()

# Alerts received from buses and edges (MQTT-05), newest kept, duplicates dropped.
alert_store = alerts.AlertStore()

client: mqtt.Client | None = None
topic: str = ""


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        logger.error("MQTT connect failed: %s", reason_code)
        return
    # start subscription
    client.subscribe(topic, qos=1)
    client.subscribe(EDGE_HEARTBEAT_FILTER, qos=1)
    client.subscribe(ALERT_FILTER, qos=1)
    logger.info("MQTT is connected, Server subscribe to %s", topic)
    # publish every region's ownership so the retained assignments always match central,
    # including after a central restart or a missed publish while disconnected
    from services.central.ownership import bus_ownership

    for region_id in bus_ownership.regions():
        publish_assignment(region_id, bus_ownership.assignment(region_id))


# tell the edges in a region which edge owns each bus; retained so a restarting edge gets it immediately
def publish_assignment(region_id: str, payload: dict) -> None:
    if client is None:
        return
    result = client.publish(
        assignment(region_id), json.dumps(payload), qos=1, retain=True
    )
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        logger.warning(
            "Assignment for %s not sent yet (rc=%s); it is republished on reconnect",
            region_id,
            result.rc,
        )


def on_disconnection(client, userdata, flags, reason_code, properties):
    logger.warning(
        "Disconnected from MQTT (%s), paho will try reconnection", reason_code
    )


def on_message(client, userdata, msg):
    try:
        if msg.topic.startswith("transit/heartbeats/edge/"):
            from services.central.health import monitor

            if not monitor.receive(msg.topic, msg.payload):
                logger.warning("Rejected edge heartbeat")
            return
        if msg.topic.startswith("transit/alerts/"):
            handle_alert(msg.topic, msg.payload)
            return
        parts = msg.topic.split("/")
        if (
            len(parts) != 5
            or parts[0] != "transit"
            or parts[2] != "bus"
            or parts[4] != "state"
            or not parts[1]
            or not parts[3]
        ):
            logger.warning("Ignoring unexpected topic %s", msg.topic)
            return
        region_id = parts[1]
        bus_id = parts[3]

        bus_state = BusStateUpdate.model_validate_json(msg.payload)
        # Receive rule, applied on delivery whether or not the update is then
        # stored. Use the sender's (edge's) stamp; senders without one only
        # relay the bus's stamp.
        metadata = bus_state.edge_metadata
        sent_at = (
            metadata.edge_lamport
            if metadata is not None and metadata.edge_lamport is not None
            else bus_state.lamport
        )
        central_clock = clock.receive(sent_at)
        logger.debug("Lamport %s -> central %s for %s", sent_at, central_clock, bus_id)
        applied, reason = apply_bus_state(bus_id, region_id, bus_state)

        if not applied:  # bus state update was not accepted
            if reason == "stale update":
                logger.debug(
                    "Stale update for %s (seq %s) ignored",
                    bus_id,
                    bus_state.sequence_number,
                )
            else:
                logger.warning(
                    "Rejected update for %s on %s: %s", bus_id, msg.topic, reason
                )
        else:
            logger.debug(
                "Applied %s seq=%s from %s",
                bus_id,
                bus_state.sequence_number,
                msg.topic,
            )

    # errpr handling
    except ValidationError as error:
        logger.warning(
            "Invalid payload on %s: %d validation errors",
            msg.topic,
            error.error_count(),
        )
    except Exception:
        logger.exception("Failed to process message on %s", msg.topic)


def handle_alert(topic: str, payload: bytes) -> bool:
    """Validate and store one alert; apply the Lamport receive rule to it."""
    try:
        alert = alerts.parse(topic, payload)
    except ValueError as error:
        logger.warning("Rejected alert on %s: %s", topic, type(error).__name__)
        return False
    central_clock = clock.receive(alert.lamport_clock)
    stored = alert_store.add(alert, central_clock)
    if stored:
        logger.warning(
            "Alert %s from %s/%s: %s",
            alert.kind,
            alert.region_id,
            alert.source_id,
            alert.message,
        )
    return stored


def publish_command(
    region_id: str,
    action: str,
    params: dict | None = None,
    target: str = topics.ALL_BUSES,
) -> commands.Command:
    """Publish one simulation command (QoS 1). Raise ConnectionError if it cannot be queued."""
    if client is None:
        raise ConnectionError("MQTT client is not started")
    command = commands.make_command(
        region_id, action, params, target=target, clock=clock
    )
    topic, payload = commands.encode(command)
    info = client.publish(topic, payload, qos=1, retain=False)
    if info.rc not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
        raise ConnectionError(f"MQTT publish failed: {mqtt.error_string(info.rc)}")
    logger.info(
        "Command %s %s -> %s (lamport %s)",
        command.command_id,
        action,
        topic,
        command.lamport_clock,
    )
    return command


def start() -> None:
    # read config after .env get load inmain.py
    global client, topic
    host = os.getenv("MQTT_HOST", "localhost")
    port = int(os.getenv("MQTT_PORT", "1883"))  # for Mosquitto
    topic = os.getenv("MQTT_TOPIC", "transit/+/bus/+/state")

    # register as new client
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=CLIENT_ID,
        clean_session=False,
    )
    client.on_connect = on_connect
    client.on_disconnect = on_disconnection
    client.on_message = on_message
    configure_mqtt_client(
        client
    )  # broker account "central" when MQTT-07 security is on
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect_async(host, port)
    client.loop_start()


def stop() -> None:
    if client is not None:
        client.loop_stop()
        client.disconnect()

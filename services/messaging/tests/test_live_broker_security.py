"""MQTT-07 live checks: broker-wide authentication and topic authorization.

Opt-in, against the secured broker (infrastructure/mosquitto-secure) loaded with
files from ``python -m services.messaging.broker_security``:

    podman run -d --name mqtt-secure-test -p 18832:1883 `
        -v ${PWD}/infrastructure/mosquitto-secure:/mosquitto/secure:ro `
        docker.io/library/eclipse-mosquitto:2 mosquitto -c /mosquitto/secure/mosquitto.conf
    $env:MQTT_SECURE_HOST = "127.0.0.1"
    $env:MQTT_SECURE_PORT = "18832"
    python -m pytest services/messaging/tests/test_live_broker_security.py -q

Credentials are read from infrastructure/secure.env (or MQTT_SECURE_ENV_FILE)
and never printed. Uses the real Durham accounts and topics, so use a
dedicated broker.
"""

import json
import os
import time
from pathlib import Path
from threading import Event
from uuid import uuid4

import paho.mqtt.client as mqtt
import pytest

from services.edge.auth_edge_server import configure_mqtt_client
from services.messaging import alerts, commands, topics
from services.messaging.lamport import LamportClock
from services.messaging.publisher import ReliablePublisher
from services.messaging.relay import wait_for

HOST = os.getenv("MQTT_SECURE_HOST")
PORT = int(os.getenv("MQTT_SECURE_PORT", "1883"))
ENV_FILE = Path(os.getenv("MQTT_SECURE_ENV_FILE", Path(__file__).resolve().parents[3] / "infrastructure" / "secure.env"))
pytestmark = pytest.mark.skipif(not HOST, reason="set MQTT_SECURE_HOST to run secured-broker checks")


def load_passwords():
    values = {}
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        if line and not line.startswith("#"):
            key, value = line.split("=", 1)
            values[key] = value.strip("'")
    passwords = json.loads(values.pop("MQTT_BUS_PASSWORDS_JSON"))
    for key, value in values.items():
        passwords[key.removesuffix("_MQTT_PASSWORD").lower().replace("_", "-")] = value
    return passwords


@pytest.fixture(scope="module")
def passwords():
    return load_passwords()


@pytest.fixture
def clients():
    opened = []
    yield opened
    for client in opened:
        client.disconnect()
        client.loop_stop()


def connect(clients, username, password):
    """MQTT v5 client (PUBACK carries the ACL decision). Return (client, refused)."""
    ready, refused = Event(), []
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"{username}-{uuid4().hex[:6]}",
                         protocol=mqtt.MQTTv5)
    if username is not None:
        client.username_pw_set(username, password)
    client.on_connect = lambda c, u, f, reason, p: (refused.append(reason.is_failure), ready.set())
    client.connect(HOST, PORT)
    client.loop_start()
    clients.append(client)
    assert ready.wait(5), "broker did not answer CONNECT"
    return client, refused[0]


def publish_denied(client, topic):
    """True if the broker refused this publish (PUBACK reason 'not authorized')."""
    results = {}
    client.on_publish = lambda c, u, mid, reason, p: results.update({mid: reason.is_failure})
    info = client.publish(topic, "{}", qos=1)
    wait_for(lambda: info.mid in results, timeout=5, what=f"PUBACK for {topic}")
    return results[info.mid]


def listen(client, topic_filter):
    received, subscribed = [], Event()
    client.on_message = lambda c, u, m: received.append(m.topic)
    client.on_subscribe = lambda *args: subscribed.set()
    client.subscribe(topic_filter, qos=1)
    assert subscribed.wait(5)
    return received


# ==================== AUTHENTICATION ====================
def test_invalid_credentials_are_rejected(clients, passwords):
    assert connect(clients, None, None)[1]                                   # anonymous
    assert connect(clients, "bus-302-1", "not-the-password")[1]              # wrong password
    assert connect(clients, "bus-302-1", passwords["bus-302-2"])[1]          # another account's password
    assert connect(clients, "intruder", passwords["central"])[1]             # unknown account
    assert not connect(clients, "bus-302-1", passwords["bus-302-1"])[1]      # the real one works


# ==================== AUTHORIZATION ====================
def test_bus_cannot_publish_on_protected_topics_it_does_not_own(clients, passwords):
    bus, _ = connect(clients, "bus-302-1", passwords["bus-302-1"])
    for forbidden in (topics.telemetry("west", "bus-302-2"),          # another bus's telemetry
                      topics.telemetry("east", "bus-302-1"),          # its own ID in another region
                      topics.state("west", "bus-302-1"),              # forging edge output
                      topics.command("west", "bus-302-2"),            # issuing commands
                      topics.alert("west", "edge-west-1"),            # impersonating an edge
                      topics.edge_heartbeat("west", "edge-west-1")):
        assert publish_denied(bus, forbidden), forbidden
    for allowed in (topics.telemetry("west", "bus-302-1"), topics.bus_heartbeat("west", "bus-302-1"),
                    topics.alert("west", "bus-302-1")):
        assert not publish_denied(bus, allowed), allowed


def test_edge_may_publish_only_states_of_buses_it_owns(clients, passwords):
    edge, _ = connect(clients, "edge-west-1", passwords["edge-west-1"])
    assert not publish_denied(edge, topics.state("west", "bus-302-1"))      # assigned to edge-west-1
    assert publish_denied(edge, topics.state("west", "bus-302-2"))          # assigned to edge-west-2
    assert publish_denied(edge, topics.telemetry("west", "bus-302-1"))
    assert publish_denied(edge, topics.command("west"))


def test_bus_reads_only_its_own_and_its_regions_commands(clients, passwords):
    bus, _ = connect(clients, "bus-302-1", passwords["bus-302-1"])
    received = listen(bus, "transit/commands/#")         # asks for everything; ACL filters delivery
    central, _ = connect(clients, "central", passwords["central"])
    for topic in (topics.command("west", "bus-302-2"), topics.command("east"),
                  topics.command("west", "bus-302-1"), topics.command("west")):
        assert not publish_denied(central, topic)
    wait_for(lambda: len(received) >= 2, what="permitted commands")
    time.sleep(0.5)
    assert sorted(received) == sorted([topics.command("west", "bus-302-1"), topics.command("west")])


def test_bus_cannot_read_other_buses_telemetry(clients, passwords):
    bus, _ = connect(clients, "bus-302-1", passwords["bus-302-1"])
    received = listen(bus, "transit/buses/#")
    other, _ = connect(clients, "bus-302-2", passwords["bus-302-2"])
    assert not publish_denied(other, topics.telemetry("west", "bus-302-2"))
    time.sleep(0.5)
    assert received == []


# ==================== AUTHORIZED SERVICES STILL COMMUNICATE ====================
def test_authorized_services_communicate_normally(monkeypatch, passwords):
    """Bus -> edge -> central, central -> bus command, bus -> central alert, all authenticated."""
    from services.data_simulation.generate_data import generate_messages
    from services.edge.communication.data_to_edge import TelemetrySubscriber
    from services.edge.config import NetworkConfig
    from shared.schema.network import DEFAULT_NETWORK_PATH
    monkeypatch.setenv("EDGE_ALLOW_INSECURE_MQTT", "true")
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/live-security")
    registry = NetworkConfig.load(DEFAULT_NETWORK_PATH).registry

    # Edge subscriber with its own account (read from the environment like in compose).
    monkeypatch.setenv("MQTT_USERNAME", "edge-west-1")
    monkeypatch.setenv("MQTT_PASSWORD", passwords["edge-west-1"])
    edge = TelemetrySubscriber("west", "edge-west-1", city_id="DURHAM", registry=registry,
                               broker_host=HOST, broker_port=PORT)
    edge.start()

    # Central with the central account.
    from services.messaging import mqtt_client
    monkeypatch.setenv("MQTT_USERNAME", "central")
    monkeypatch.setenv("MQTT_PASSWORD", passwords["central"])
    monkeypatch.setenv("MQTT_HOST", HOST)
    monkeypatch.setenv("MQTT_PORT", str(PORT))
    monkeypatch.setattr(mqtt_client, "CLIENT_ID", "central-secure-" + uuid4().hex[:6])
    monkeypatch.setattr(mqtt_client, "alert_store", alerts.AlertStore())
    applied = []
    monkeypatch.setattr(mqtt_client, "apply_bus_state",
                        lambda bus, region, state: (applied.append((bus, state.sequence_number)), (True, None))[1])
    mqtt_client.start()

    # Bus with its per-bus account, as run_buses selects it from MQTT_BUS_PASSWORDS_JSON.
    monkeypatch.delenv("MQTT_USERNAME")
    monkeypatch.delenv("MQTT_PASSWORD")
    from services.data_simulation.run_buses import SensorAlerts, bus_credentials
    monkeypatch.setenv("MQTT_BUS_PASSWORDS_JSON", json.dumps({"bus-302-1": passwords["bus-302-1"]}))
    credentials = bus_credentials("bus-302-1")
    bus = ReliablePublisher("bus-302-1-secure-" + uuid4().hex[:6], HOST, PORT,
                            configure=lambda c: configure_mqtt_client(c, *credentials))
    controls, clock = commands.BusControls(), LamportClock()
    consumer = commands.CommandConsumer("west", "bus-302-1", controls, clock)
    for topic_filter in commands.bus_subscriptions("west", "bus-302-1"):
        bus.subscribe(topic_filter, consumer.handle)
    bus.connect()
    sender = ReliablePublisher("edge-west-1-to-central-" + uuid4().hex[:6], HOST, PORT,
                               configure=lambda c: configure_mqtt_client(c, "edge-west-1", passwords["edge-west-1"]))
    sender.connect()
    try:
        wait_for(lambda: edge._client.is_connected() and mqtt_client.client.is_connected(), what="services")
        reading = next(generate_messages("302", 0, "bus-302-1", max_updates=1, update_seconds=0, clock=clock))
        topic = topics.telemetry("west", "bus-302-1")
        wait_for(lambda: bus.publish(topic, json.dumps(reading)) and edge.messages.qsize() > 0,
                 what="telemetry at the authenticated edge", interval=0.5)
        assert edge.messages.get_nowait()["bus_id"] == "bus-302-1"

        state = {"lat": 43.9, "lon": -78.9, "speed": 30.0, "direction_id": 0, "bus_status": "moving", "occupancy": 10,
                     "sequence_number": reading["sequence_number"], "lamport": reading["lamport_clock"]}
        wait_for(lambda: sender.publish(topics.state("west", "bus-302-1"), json.dumps(state)) and applied,
                 what="state at authenticated central", interval=0.5)
        assert applied[0][0] == "bus-302-1"

        wait_for(lambda: mqtt_client.publish_command("west", "pause", target="bus-302-1") and controls.paused,
                 what="command at the bus", interval=0.5)

        SensorAlerts({"bus_id": "bus-302-1", "region_id": "west"}, clock, bus.publish).update({"gps": "faulty"})
        wait_for(lambda: mqtt_client.alert_store.list(region_id="west"), what="alert at central")
    finally:
        bus.close(flush_timeout=1)
        sender.close(flush_timeout=1)
        edge.stop()
        mqtt_client.stop()

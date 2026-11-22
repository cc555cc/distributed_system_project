"""MQTT-10 live checks: the broker itself stops and restarts.

Opt-in. Needs a dedicated broker container the test may stop and start, running
the project's configuration (infrastructure/mosquitto/mosquitto.conf, which
enables persistence):

    podman run -d --name mqtt-restart-test -p 18831:1883 `
        -v ${PWD}/infrastructure/mosquitto:/mosquitto/config:ro docker.io/library/eclipse-mosquitto:2
    $env:MQTT_RESTART_HOST = "127.0.0.1"
    $env:MQTT_RESTART_PORT = "18831"
    $env:MQTT_RESTART_CONTAINER = "mqtt-restart-test"
    $env:MQTT_RESTART_RUNTIME = "podman"          # or docker
    python -m pytest services/messaging/tests/test_live_broker_restart.py -q

Unlike the MQTT-04 relay tests, every client loses the broker at once and the
broker loses its in-memory state; only what it persisted to disk survives.
"""

import json
import os
import subprocess
import time
from uuid import uuid4

import paho.mqtt.client as mqtt
import pytest

from services.edge.communication.data_to_edge import TelemetrySubscriber
from services.edge.validation.bus_registry import BusAssignment, BusRegistry
from services.messaging import commands, topics
from services.messaging.delivery import SequenceTracker
from services.messaging.lamport import LamportClock
from services.messaging.publisher import ReliablePublisher
from services.messaging.relay import wait_for

HOST = os.getenv("MQTT_RESTART_HOST")
PORT = int(os.getenv("MQTT_RESTART_PORT", "1883"))
CONTAINER = os.getenv("MQTT_RESTART_CONTAINER")
RUNTIME = os.getenv("MQTT_RESTART_RUNTIME", "podman")
pytestmark = pytest.mark.skipif(
    not (HOST and CONTAINER),
    reason="set MQTT_RESTART_HOST and MQTT_RESTART_CONTAINER to run broker-restart checks",
)


def broker(action):
    subprocess.run(
        [RUNTIME, action, *(["-t", "5"] if action == "stop" else []), CONTAINER],
        check=True,
        capture_output=True,
        timeout=60,
    )


def broker_accepts_connections():
    probe = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2, client_id="probe-" + uuid4().hex[:6]
    )
    try:
        probe.connect(HOST, PORT, keepalive=5)
        probe.disconnect()
        return True
    except OSError:
        return False


@pytest.fixture(autouse=True)
def broker_running():
    yield
    # Leave the broker up for the next test even if this one failed mid-outage.
    try:
        broker("start")
    except subprocess.CalledProcessError:
        pass
    wait_for(broker_accepts_connections, timeout=30, what="broker after test")


def reading(region, sequence):
    return {
        "bus_id": "R1",
        "city_id": "CITY-R",
        "region_id": region,
        "route_id": "RT",
        "latitude": 43.9,
        "longitude": -78.9,
        "speed_kmh": 30.0,
        "passengers": 10,
        "capacity": 50,
        "sequence_number": sequence,
        "timestamp": "2026-11-22T12:00:00Z",
        "bus_status": "moving",
        "lamport_clock": sequence,
        "direction_id": 0,
        "current_stop_id": "S1",
        "next_stop_id": "S2",
        "sensor_status": "healthy",
    }


class CountingSubscriber(TelemetrySubscriber):
    def handle_raw_message(self, topic, raw_payload):
        self.arrivals = getattr(self, "arrivals", 0) + 1
        return super().handle_raw_message(topic, raw_payload)


def test_clients_reconnect_resubscribe_and_report_what_was_lost():
    region = "restart" + uuid4().hex[:8]
    edge_id = "edge-" + region
    registry = BusRegistry({"R1": BusAssignment(region, "RT", "CITY-R", edge_id)})
    edge = CountingSubscriber(
        region,
        edge_id,
        city_id="CITY-R",
        registry=registry,
        broker_host=HOST,
        broker_port=PORT,
    )
    bus = ReliablePublisher("R1-" + uuid4().hex[:6], HOST, PORT)
    # The bus also listens for commands, so its resubscription is checked too.
    controls = commands.BusControls()
    consumer = commands.CommandConsumer(region, "R1", controls, LamportClock())
    for topic_filter in commands.bus_subscriptions(region, "R1"):
        bus.subscribe(topic_filter, consumer.handle)
    operator = ReliablePublisher("operator-" + uuid4().hex[:6], HOST, PORT)
    tracker, sent = SequenceTracker(), []
    topic = topics.telemetry(region, "R1")

    def publish(sequence):
        outcome = bus.publish(topic, json.dumps(reading(region, sequence)))
        sent.append(sequence)
        return outcome

    def drain():
        while not edge.messages.empty():
            tracker.record("R1", edge.messages.get_nowait()["sequence_number"])

    def edge_receiving():
        before = getattr(edge, "arrivals", 0)
        operator.publish(
            topics.telemetry(region, "probe"), "probe"
        )  # rejected by validation
        time.sleep(0.2)
        return getattr(edge, "arrivals", 0) > before

    edge.start()
    bus.connect()
    operator.connect()
    try:
        wait_for(edge_receiving, what="edge subscription")
        assert [publish(n) for n in (1, 2, 3)] == ["sent"] * 3
        wait_for(lambda: edge.messages.qsize() == 3, what="readings before restart")
        drain()

        broker("stop")
        wait_for(
            lambda: not bus.connected and not edge._client.is_connected(),
            what="clients to notice",
        )
        # The bus keeps generating while the broker is down.
        assert [publish(n) for n in (4, 5, 6)] == ["queued"] * 3
        assert bus.unacknowledged == 3
        broker("start")

        wait_for(
            lambda: bus.connected and edge._client.is_connected(),
            timeout=30,
            what="clients to reconnect",
        )
        assert bus.stats["reconnects"] == 1
        wait_for(edge_receiving, what="edge resubscribed after restart")
        assert bus.flush(15) == 0  # broker acknowledged the queued readings
        assert publish(7) == "sent"
        wait_for(
            lambda: (
                7 in tracker.received("R1") or (drain() or 7 in tracker.received("R1"))
            ),
            what="reading after restart",
        )
        time.sleep(0.5)
        drain()

        # Communication resumed, and every reading is accounted for: received or listed as lost.
        report = tracker.report({"R1": sent})
        received = tracker.received("R1")
        assert {1, 2, 3, 7} <= received
        assert received | set(tracker.missing("R1", sent)) == set(sent)
        assert set(report["missing"].get("R1", [])) <= {
            4,
            5,
            6,
        }  # only the outage can lose readings
        print(
            f"\nqueued during restart: 4-6; received after restart: {sorted(received & {4, 5, 6})}; "
            f"identified as lost: {report['missing'].get('R1', [])}"
        )

        # Command subscriptions came back too (clean session: restored by on_connect).
        command = commands.make_command(
            region, "pause", target="R1", clock=LamportClock()
        )
        operator.publish(*commands.encode(command))
        wait_for(lambda: controls.paused, what="command after restart")
    finally:
        edge.stop()
        bus.close(flush_timeout=1)
        operator.close(flush_timeout=1)


def test_persistent_session_queue_survives_a_broker_restart(monkeypatch):
    """Central's persistent session: states published while it is away survive the restart."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/live-restart")
    from services.messaging import mqtt_client

    region = "restart" + uuid4().hex[:8]
    client_id = "central-restart-" + uuid4().hex[:8]
    applied = []
    monkeypatch.setattr(mqtt_client, "CLIENT_ID", client_id)
    monkeypatch.setattr(
        mqtt_client,
        "apply_bus_state",
        lambda bus, region_id, state: (
            applied.append(state.sequence_number),
            (True, None),
        )[1],
    )
    monkeypatch.setenv("MQTT_HOST", HOST)
    monkeypatch.setenv("MQTT_PORT", str(PORT))
    monkeypatch.setenv("MQTT_TOPIC", f"transit/{region}/bus/+/state")
    edge = ReliablePublisher("edge-sender-" + uuid4().hex[:6], HOST, PORT)
    topic = topics.state(region, "R1")
    state = lambda sequence: json.dumps(
        {
            "lat": 43.9,
            "lon": -78.9,
            "speed": 30.0,
            "direction_id": 0,
            "bus_status": "moving",
            "occupancy": 10,
            "sequence_number": sequence,
            "lamport": sequence,
        }
    )
    edge.connect()
    mqtt_client.start()
    try:

        def central_receiving():
            edge.publish(topic, state(0))
            time.sleep(0.2)
            return 0 in applied

        wait_for(central_receiving, what="central subscription")
        time.sleep(0.5)
        applied.clear()
        edge.publish(topic, state(1))
        wait_for(lambda: applied == [1], what="state before restart")

        mqtt_client.stop()  # central away; its session stays on the broker
        for sequence in (2, 3):
            edge.publish(topic, state(sequence))
        assert edge.flush(5) == 0  # broker holds 2 and 3 for central

        broker("stop")  # persistence writes the queue to disk
        broker("start")
        wait_for(broker_accepts_connections, timeout=30, what="broker restart")
        wait_for(lambda: edge.connected, timeout=30, what="edge sender reconnect")

        mqtt_client.start()
        wait_for(
            lambda: applied == [1, 2, 3], timeout=15, what="queued states after restart"
        )
        edge.publish(topic, state(4))
        wait_for(lambda: applied == [1, 2, 3, 4], what="state after restart")
    finally:
        mqtt_client.stop()
        edge.close(flush_timeout=1)
        cleaner = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=True
        )
        cleaner.connect(HOST, PORT)
        cleaner.loop_start()
        time.sleep(0.5)
        cleaner.disconnect()
        cleaner.loop_stop()

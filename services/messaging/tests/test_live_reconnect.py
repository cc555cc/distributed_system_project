"""MQTT-04 live checks: real clients, real broker, real connection loss.

Opt-in. Point these at a dedicated Mosquitto broker:

    $env:EDGE_LIVE_MQTT_HOST = "127.0.0.1"
    $env:EDGE_LIVE_MQTT_PORT = "1883"
    python -m pytest services/messaging/tests/test_live_reconnect.py -q

Each client under test connects through a small TCP relay that the test can cut
and restore, so the client sees a genuine dropped connection while the broker
keeps running. Broker restarts are MQTT-10.
"""

import json
import os
import socket
from threading import Thread
import time
from types import SimpleNamespace
from uuid import uuid4

import paho.mqtt.client as mqtt
import pytest

from services.edge.communication.data_to_edge import TelemetrySubscriber
from services.edge.config import EdgeConfig, NetworkConfig, RouteDefinition
from services.edge.edge_server import EdgeIngress
from services.edge.processing.processor import EdgeProcessor, ProcessingStatus
from services.edge.validation.bus_registry import BusAssignment, BusRegistry
from services.messaging import topics
from services.messaging.publisher import ReliablePublisher

HOST = os.getenv("EDGE_LIVE_MQTT_HOST")
PORT = int(os.getenv("EDGE_LIVE_MQTT_PORT", "1883"))
pytestmark = pytest.mark.skipif(not HOST, reason="set EDGE_LIVE_MQTT_HOST to run live MQTT checks")


def wait_for(condition, timeout=20.0, what="condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {what}")


class Link:
    """TCP relay to the broker. cut() drops every connection and refuses new ones."""

    def __init__(self):
        self.server = socket.create_server(("127.0.0.1", 0))
        self.port = self.server.getsockname()[1]
        self.up, self.open, self.sockets = True, True, []
        Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while self.open:
            try:
                near, _ = self.server.accept()
            except OSError:
                return
            if not self.up:
                near.close()
                continue
            try:
                far = socket.create_connection((HOST, PORT), timeout=5)
            except OSError:
                near.close()
                continue
            far.settimeout(None)
            self.sockets += [near, far]
            Thread(target=self._pump, args=(near, far), daemon=True).start()
            Thread(target=self._pump, args=(far, near), daemon=True).start()

    @staticmethod
    def _pump(source, target):
        try:
            while data := source.recv(65536):
                target.sendall(data)
        except OSError:
            pass
        finally:
            for sock in (source, target):
                try:
                    sock.close()
                except OSError:
                    pass

    def cut(self):
        self.up = False
        sockets, self.sockets = self.sockets, []
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

    def restore(self):
        self.up = True

    def close(self):
        self.cut()
        self.open = False
        self.server.close()


@pytest.fixture
def link():
    link = Link()
    yield link
    link.close()


@pytest.fixture
def region():
    return "live" + uuid4().hex[:10]     # unique topics: no interference between runs


@pytest.fixture
def direct():
    """Publisher connected straight to the broker, unaffected by the relay."""
    publisher = ReliablePublisher("direct-" + uuid4().hex[:8], HOST, PORT)
    publisher.connect()
    yield publisher
    publisher.close(flush_timeout=1)


def reading(region, sequence):
    return dict(bus_id="L1", city_id="CITY-L", region_id=region, route_id="R1",
                latitude=43.9, longitude=-78.9, speed_kmh=30.0, passengers=10, capacity=50,
                sequence_number=sequence, timestamp="2026-11-11T12:00:00Z", bus_status="travelling",
                lamport_clock=sequence, direction_id=0, current_stop_id="S1", next_stop_id="S2",
                sensor_status="healthy")


# ==================== PUBLISHER LOSES ITS CONNECTION ====================
def test_publisher_reconnects_and_delivers_queued_readings_in_order(link, region):
    received = []
    listener = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="listener-" + uuid4().hex[:8])
    listener.on_message = lambda c, u, m: received.append(json.loads(m.payload)["sequence_number"])
    subscribed = []
    listener.on_subscribe = lambda *args: subscribed.append(True)
    listener.connect(HOST, PORT)
    listener.subscribe(topics.telemetry_filter(region), qos=1)
    listener.loop_start()
    publisher = ReliablePublisher("bus-" + uuid4().hex[:8], "127.0.0.1", link.port)
    try:
        wait_for(lambda: subscribed, what="listener subscription")
        publisher.connect()
        topic = topics.telemetry(region, "L1")
        send = lambda sequence: publisher.publish(topic, json.dumps(reading(region, sequence)))

        assert [send(n) for n in (1, 2, 3)] == ["sent"] * 3
        wait_for(lambda: received == [1, 2, 3], what="readings before the outage")

        link.cut()
        wait_for(lambda: not publisher.connected, what="publisher to notice the lost connection")
        assert [send(n) for n in (4, 5, 6)] == ["queued"] * 3     # bus keeps generating
        assert publisher.unacknowledged == 3 and received == [1, 2, 3]

        link.restore()
        wait_for(lambda: received == [1, 2, 3, 4, 5, 6], what="queued readings after reconnect")
        assert publisher.flush(5) == 0
        assert publisher.stats["reconnects"] == 1 and publisher.stats["disconnects"] == 1
        assert send(7) == "sent"
        wait_for(lambda: received[-1] == 7, what="reading after recovery")
        assert received == sorted(set(received))                   # in order, none repeated
    finally:
        publisher.close(flush_timeout=1)
        listener.disconnect()
        listener.loop_stop()


# ==================== EDGE SUBSCRIBER LOSES ITS CONNECTION ====================
class RecordingSubscriber(TelemetrySubscriber):
    def handle_raw_message(self, topic, raw_payload):
        self.arrivals = getattr(self, "arrivals", 0) + 1
        return super().handle_raw_message(topic, raw_payload)


def wait_subscribed(subscriber, publisher, region):
    """Probe until the edge really receives on its topic filter again."""
    probe = topics.telemetry(region, "probe")
    def arrived():
        before = getattr(subscriber, "arrivals", 0)
        publisher.publish(probe, "not telemetry")       # rejected by validation, never queued
        time.sleep(0.2)
        return getattr(subscriber, "arrivals", 0) > before
    wait_for(arrived, what="edge subscription")
    time.sleep(0.5)                                     # let remaining probes drain
    assert subscriber.messages.empty()


def test_edge_resubscribes_after_reconnect_and_late_readings_do_not_move_state_backward(link, region, direct):
    edge_id = "edge-" + region                           # also the MQTT client ID
    registry = BusRegistry({"L1": BusAssignment(region, "R1", "CITY-L", edge_id)})
    config = EdgeConfig("CITY-L", region, edge_id)
    network = NetworkConfig(registry, (RouteDefinition("CITY-L", region, "R1", ("S1", "S2")),))
    receiver = EdgeIngress(EdgeProcessor(config, network))
    subscriber = RecordingSubscriber(region, edge_id, city_id="CITY-L", registry=registry,
                                     broker_host="127.0.0.1", broker_port=link.port)
    topic = topics.telemetry(region, "L1")
    statuses = []

    def send_and_process(*sequences):
        for sequence in sequences:
            direct.publish(topic, json.dumps(reading(region, sequence)))
        wait_for(lambda: subscriber.messages.qsize() == len(sequences), what=f"readings {sequences} at the edge")
        results = [result.status for result in receiver.drain_pending(subscriber)]
        statuses.extend(zip(sequences, results))
        return results

    current = lambda: receiver.accepted[0]["sequence_number"]
    subscriber.start()
    try:
        wait_subscribed(subscriber, direct, region)
        assert send_and_process(1, 2) == [ProcessingStatus.ACCEPTED] * 2

        link.cut()
        wait_for(lambda: not subscriber._client.is_connected(), what="edge to notice the lost connection")
        for sequence in (3, 4):                          # published while the edge is away
            direct.publish(topic, json.dumps(reading(region, sequence)))
        assert direct.flush(5) == 0                      # the broker did accept them
        link.restore()
        # No subscribe call here: on_connect must restore the subscription itself.
        wait_subscribed(subscriber, direct, region)

        assert send_and_process(5) == [ProcessingStatus.ACCEPTED] and current() == 5
        # 3 arrives late and 5 is delivered twice: both identified, state stays at 5.
        assert send_and_process(3, 5) == [ProcessingStatus.STALE, ProcessingStatus.DUPLICATE]
        assert current() == 5
        # Readings lost during the outage are visible as a sequence gap, not assumed delivered.
        accepted = [sequence for sequence, status in statuses if status == ProcessingStatus.ACCEPTED]
        assert accepted == [1, 2, 5]
        assert set(range(1, 6)) - set(accepted) == {3, 4}
    finally:
        subscriber.stop()


# ==================== CENTRAL SUBSCRIBER LOSES ITS CONNECTION ====================
def test_central_reconnects_and_receives_states_queued_by_the_broker(link, region, direct, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/live-mqtt-only")
    from services.messaging import mqtt_client
    client_id = "central-test-" + uuid4().hex[:8]
    applied = []
    monkeypatch.setattr(mqtt_client, "CLIENT_ID", client_id)
    monkeypatch.setattr(mqtt_client, "apply_bus_state",
                        lambda bus, region_id, state: (applied.append(state.sequence_number), (True, None))[1])
    monkeypatch.setenv("MQTT_HOST", "127.0.0.1")
    monkeypatch.setenv("MQTT_PORT", str(link.port))
    monkeypatch.setenv("MQTT_TOPIC", f"transit/{region}/bus/+/state")
    topic = topics.state(region, "L1")
    state = lambda sequence: json.dumps(dict(lat=43.9, lon=-78.9, speed=30.0, direction_id=0,
                                             bus_status="travelling", occupancy=10,
                                             sequence_number=sequence, lamport=sequence))
    mqtt_client.start()
    try:
        def subscribed():
            direct.publish(topic, state(0))
            time.sleep(0.2)
            return bool(applied)
        wait_for(subscribed, what="central subscription")
        time.sleep(0.5)
        applied.clear()
        direct.publish(topic, state(1))
        wait_for(lambda: applied == [1], what="state before the outage")

        link.cut()
        wait_for(lambda: not mqtt_client.client.is_connected(), what="central to notice the lost connection")
        for sequence in (2, 3):
            direct.publish(topic, state(sequence))
        assert direct.flush(5) == 0
        assert applied == [1]
        link.restore()
        # Central keeps a persistent session, so the broker held 2 and 3 for it.
        wait_for(lambda: applied == [1, 2, 3], what="states queued during the outage")
        direct.publish(topic, state(4))
        wait_for(lambda: applied == [1, 2, 3, 4], what="state after recovery")
    finally:
        mqtt_client.stop()
        # Remove the persistent test session from the broker.
        cleaner = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=True)
        cleaner.connect(HOST, PORT)
        cleaner.loop_start()
        time.sleep(0.5)
        cleaner.disconnect()
        cleaner.loop_stop()

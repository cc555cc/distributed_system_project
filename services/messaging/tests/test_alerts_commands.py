"""MQTT-05: alerts reach central, commands reach only their intended buses.

Offline: real message models, consumers, central callback and API, with the
MQTT transport replaced. The broker round trip is test_live_alerts_commands.py.
"""

import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from services.data_simulation.generate_data import generate_messages
from services.data_simulation.run_buses import SensorAlerts
from services.messaging import alerts, commands, topics
from services.messaging.lamport import LamportClock


def command_for(region, action, params=None, target=topics.ALL_BUSES, stamp=None):
    clock = LamportClock(stamp - 1) if stamp else LamportClock()
    return commands.make_command(region, action, params, target=target, clock=clock)


def deliver(consumer, command, topic=None):
    encoded_topic, payload = commands.encode(command)
    return consumer.handle(topic or encoded_topic, payload.encode())


@pytest.fixture
def fleet():
    """Two buses in west, one in east, each with its own controls and clock."""

    def bus(region, bus_id):
        controls = commands.BusControls()
        return commands.CommandConsumer(region, bus_id, controls, LamportClock())

    return {"W1": bus("west", "W1"), "W2": bus("west", "W2"), "E1": bus("east", "E1")}


def broadcast(fleet, command):
    """Deliver to every bus subscribed to the command's topic (what the broker does)."""
    topic, _ = commands.encode(command)
    reached = {}
    for bus_id, consumer in fleet.items():
        if topic in commands.bus_subscriptions(consumer.region_id, bus_id):
            reached[bus_id] = deliver(consumer, command)
    return reached


# ==================== COMMANDS ====================
def test_each_bus_subscribes_only_to_its_own_and_its_region_topic():
    assert commands.bus_subscriptions("west", "W1") == (
        "transit/commands/west/W1",
        "transit/commands/west/all",
    )


def test_command_to_one_bus_changes_only_that_bus(fleet):
    reached = broadcast(fleet, command_for("west", "pause", target="W1"))
    assert reached == {"W1": commands.APPLIED}
    assert fleet["W1"].controls.paused
    assert not fleet["W2"].controls.paused and not fleet["E1"].controls.paused


def test_region_command_reaches_every_bus_in_that_region_only(fleet):
    reached = broadcast(
        fleet, command_for("west", "set_traffic", {"traffic_factor": 0.5})
    )
    assert reached == {"W1": commands.APPLIED, "W2": commands.APPLIED}
    assert (
        fleet["W1"].controls.traffic_factor
        == fleet["W2"].controls.traffic_factor
        == 0.5
    )
    assert fleet["E1"].controls.traffic_factor == 1.0


def test_misdelivered_command_is_ignored_by_an_unrelated_bus(fleet):
    # Even if the broker (or a wrong subscription) delivered it, E1 does not act.
    command = command_for("west", "pause", target="W1")
    assert deliver(fleet["E1"], command) == commands.IGNORED
    assert deliver(fleet["W2"], command) == commands.IGNORED
    assert not fleet["E1"].controls.paused and not fleet["W2"].controls.paused


def test_command_on_a_topic_other_than_its_own_target_is_rejected(fleet):
    command = command_for("west", "pause", target="W2")
    assert (
        deliver(fleet["W1"], command, topic=topics.command("west", "W1"))
        == commands.REJECTED
    )
    assert not fleet["W1"].controls.paused


def test_repeated_delivery_applies_once(fleet):
    command = command_for("west", "set_demand", {"demand": "peak"}, target="W1")
    assert deliver(fleet["W1"], command) == commands.APPLIED
    fleet["W1"].controls.apply("set_demand", {"demand": "off_peak"})
    assert deliver(fleet["W1"], command) == commands.DUPLICATE  # QoS 1 redelivery
    assert fleet["W1"].controls.demand == "off_peak"


@pytest.mark.parametrize(
    "action, params",
    [
        ("set_traffic", {"traffic_factor": 0}),
        ("set_traffic", {"traffic_factor": 1.5}),
        ("set_traffic", {}),
        ("pause", {"extra": 1}),
        ("set_demand", {"demand": "rush"}),
        ("set_sensor_failure_rate", {"sensor_failure_rate": -0.1}),
        ("set_traffic", {"traffic_factor": True}),
    ],
)
def test_invalid_commands_cannot_be_built(action, params):
    with pytest.raises(ValueError):
        command_for("west", action, params)


def test_malformed_payload_is_rejected_without_acting(fleet):
    consumer = fleet["W1"]
    for payload in (
        b"not json",
        b"{}",
        json.dumps({"action": "pause"}).encode(),
        b"x" * 20000,
    ):
        assert (
            consumer.handle(topics.command("west", "W1"), payload) == commands.REJECTED
        )
    assert not consumer.controls.paused


def test_bus_applies_receive_rule_so_next_reading_is_stamped_later(fleet):
    consumer = fleet["W1"]
    deliver(consumer, command_for("west", "resume", target="W1", stamp=40))
    assert consumer.clock.value == 41
    # Even an ignored command advances the clock: it was still received.
    deliver(consumer, command_for("west", "resume", target="W2", stamp=60))
    assert consumer.clock.value == 61
    assert consumer.clock.tick() == 62  # next telemetry stamp


# ==================== COMMANDS CHANGE THE RUNNING SIMULATOR ====================
def test_traffic_and_failure_commands_change_generated_telemetry():
    controls = commands.BusControls()
    messages = generate_messages(
        "302", 0, "bus-302-1", max_updates=200, update_seconds=0, controls=controls
    )
    first = next(messages)
    moving = next(m for m in messages if m["speed_kmh"])
    assert moving["speed_kmh"] == 30
    controls.apply("set_traffic", {"traffic_factor": 0.5})
    later = next(
        m
        for m in messages
        if m["speed_kmh"] and m["current_stop_id"] != moving["current_stop_id"]
    )
    assert later["speed_kmh"] == 15  # from the next segment
    controls.apply("set_sensor_failure_rate", {"sensor_failure_rate": 1.0})
    assert list(messages) == []  # every reading fails: none invented
    assert first["sequence_number"] == 1


def test_pause_holds_the_bus_until_resume():
    from threading import Thread

    controls = commands.BusControls()
    messages = generate_messages(
        "302", 0, "bus-302-1", max_updates=5, update_seconds=0, controls=controls
    )
    assert next(messages)["sequence_number"] == 1
    controls.apply("pause", {})
    produced = []
    worker = Thread(target=lambda: produced.extend(messages), daemon=True)
    worker.start()
    worker.join(0.3)
    assert worker.is_alive() and produced == []  # paused: nothing generated
    controls.apply("resume", {})
    worker.join(2)
    assert [m["sequence_number"] for m in produced] == [
        2,
        3,
        4,
        5,
    ]  # resumes where it stopped


def test_stop_releases_a_paused_bus():
    controls = commands.BusControls()
    controls.apply("pause", {})
    controls.stop()
    assert controls.wait_while_paused(1) is False


# ==================== ALERTS ====================
def alert_from(region="west", source="bus-302-1", stamp=None, **changes):
    clock = LamportClock(stamp - 1) if stamp else LamportClock()
    alert = alerts.make_alert(
        region,
        source,
        "bus",
        "sensor_failure",
        "warning",
        "sensor failed",
        clock=clock,
        bus_id=source,
    )
    return alert.model_copy(update=changes)


def test_alert_must_arrive_on_its_own_source_topic():
    alert = alert_from()
    topic, payload = alerts.encode(alert)
    assert topic == "transit/alerts/west/bus-302-1"
    assert alerts.parse(topic, payload.encode()) == alert
    with pytest.raises(ValueError):
        alerts.parse("transit/alerts/west/bus-302-2", payload.encode())  # impersonation
    with pytest.raises(ValueError):
        alerts.parse(topic, b"{}")


def test_store_keeps_one_copy_newest_first_and_stays_bounded():
    store = alerts.AlertStore(limit=3)
    first = alert_from()
    assert store.add(first) and not store.add(first)
    assert store.duplicates == 1
    for _ in range(3):
        store.add(alert_from(region="east", source="bus-407-1"))
    listed = store.list()
    assert len(listed) == 3 and first.alert_id not in {a["alert_id"] for a in listed}
    assert store.list(region_id="west") == []


def test_failure_alert_reaches_central_through_its_mqtt_callback(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/alerts")
    from services.messaging import mqtt_client

    monkeypatch.setattr(mqtt_client, "alert_store", alerts.AlertStore())
    monkeypatch.setattr(mqtt_client, "clock", LamportClock())
    alert = alert_from(stamp=17)
    topic, payload = alerts.encode(alert)
    message = SimpleNamespace(topic=topic, payload=payload.encode())
    mqtt_client.on_message(None, None, message)
    mqtt_client.on_message(None, None, message)  # QoS 1 duplicate
    stored = mqtt_client.alert_store.list()
    assert [a["alert_id"] for a in stored] == [alert.alert_id]
    assert stored[0]["central_lamport"] == 18  # receive rule
    mqtt_client.on_message(
        None, None, SimpleNamespace(topic="transit/alerts/west/x", payload=b"junk")
    )
    assert len(mqtt_client.alert_store.list()) == 1


def test_simulated_sensor_failure_publishes_one_alert_per_change():
    sent = []
    bus = {"bus_id": "bus-302-1", "region_id": "west"}
    monitor = SensorAlerts(
        bus,
        LamportClock(),
        lambda topic, payload: sent.append((topic, json.loads(payload))) or "sent",
    )
    healthy = dict.fromkeys(("gps", "speed", "stop", "occupancy"), "healthy")
    faulty = dict.fromkeys(healthy, "faulty")
    for state in (healthy, faulty, faulty, faulty, healthy, healthy):
        monitor.update(state)
    assert [payload["kind"] for _, payload in sent] == [
        "sensor_failure",
        "sensor_recovered",
    ]
    assert {topic for topic, _ in sent} == {"transit/alerts/west/bus-302-1"}
    assert [payload["lamport_clock"] for _, payload in sent] == [1, 2]


def test_edge_overload_alert_is_rate_limited_and_scoped_to_its_edge():
    from services.edge.edge_server import EdgeServerInstance

    edge = EdgeServerInstance.__new__(EdgeServerInstance)
    sent = []
    edge.config = SimpleNamespace(region_id="west", edge_id="edge-west-1")
    edge.subscriber = SimpleNamespace(messages=SimpleNamespace(maxsize=50))
    edge.forwarder = SimpleNamespace(clock=LamportClock())
    edge.sender = SimpleNamespace(publish_alert=sent.append)
    edge._last_overload_alert = None
    assert edge.report_overload(now=100.0)
    assert not edge.report_overload(now=110.0)
    assert edge.report_overload(now=131.0)
    assert [(a.kind, a.source_id, a.severity) for a in sent] == [
        ("edge_overload", "edge-west-1", "critical")
    ] * 2


def test_edge_sender_publishes_only_its_own_alerts():
    from services.edge.communication.edge_to_central import EdgeToCentral
    from services.edge.config import EdgeConfig, NetworkConfig
    from shared.schema.bus_state import BusStateUpdate
    from shared.schema.network import DEFAULT_NETWORK_PATH

    sender = EdgeToCentral(
        EdgeConfig("DURHAM", "west", "edge-west-1"),
        NetworkConfig.load(DEFAULT_NETWORK_PATH).registry,
        BusStateUpdate,
    )
    published = []
    sender._client = SimpleNamespace(
        publish=lambda *a, **k: published.append(a) or SimpleNamespace(rc=0)
    )
    own = alerts.make_alert(
        "west",
        "edge-west-1",
        "edge",
        "edge_overload",
        "critical",
        "full",
        clock=LamportClock(),
    )
    sender.publish_alert(own)
    assert published[0][0] == "transit/alerts/west/edge-west-1"
    other = own.model_copy(update={"source_id": "edge-west-2"})
    with pytest.raises(ValueError):
        sender.publish_alert(other)


# ==================== CENTRAL API ====================
@pytest.fixture
def api(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/commands")
    from services.central.main import app
    from services.messaging import mqtt_client

    published = []
    fake = SimpleNamespace(
        publish=lambda topic, payload, qos, retain: (
            published.append((topic, json.loads(payload))) or SimpleNamespace(rc=0)
        )
    )
    monkeypatch.setattr(mqtt_client, "client", fake)
    monkeypatch.setattr(mqtt_client, "alert_store", alerts.AlertStore())
    return TestClient(app), published, mqtt_client


def test_command_endpoint_publishes_to_the_bus_or_region_topic(api):
    client, published, _ = api
    response = client.post(
        "/commands",
        json={"region_id": "west", "bus_id": "bus-302-1", "action": "pause"},
    )
    assert response.status_code == 202
    assert published[-1][0] == "transit/commands/west/bus-302-1"
    response = client.post(
        "/commands",
        json={
            "region_id": "east",
            "action": "set_demand",
            "params": {"demand": "peak"},
        },
    )
    assert (
        response.status_code == 202 and published[-1][0] == "transit/commands/east/all"
    )
    assert published[-1][1]["lamport_clock"] > published[0][1]["lamport_clock"]


@pytest.mark.parametrize(
    "body, status",
    [
        ({"region_id": "north", "action": "pause"}, 404),
        (
            {"region_id": "east", "bus_id": "bus-302-1", "action": "pause"},
            404,
        ),  # bus is in west
        ({"region_id": "west", "action": "explode"}, 422),
        (
            {
                "region_id": "west",
                "action": "set_traffic",
                "params": {"traffic_factor": 9},
            },
            422,
        ),
    ],
)
def test_invalid_command_requests_publish_nothing(api, body, status):
    client, published, _ = api
    assert client.post("/commands", json=body).status_code == status
    assert published == []


def test_alerts_endpoint_lists_received_alerts(api):
    client, _, mqtt_client = api
    topic, payload = alerts.encode(alert_from())
    mqtt_client.handle_alert(topic, payload.encode())
    response = client.get("/alerts", params={"region_id": "west"})
    assert response.status_code == 200
    assert [a["kind"] for a in response.json()] == ["sensor_failure"]
    assert client.get("/alerts", params={"region_id": "east"}).json() == []

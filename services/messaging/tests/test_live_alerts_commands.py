"""MQTT-05 live check: alerts reach central and commands reach only their recipients.

Opt-in, against a dedicated broker:

    $env:EDGE_LIVE_MQTT_HOST = "127.0.0.1"
    $env:EDGE_LIVE_MQTT_PORT = "1883"
    python -m pytest services/messaging/tests/test_live_alerts_commands.py -q

Real clients: central's MQTT module, and three bus processes' publishers
(two in one region, one in another) each with the bus command consumer.
"""

import os
from uuid import uuid4

import pytest

from services.data_simulation.run_buses import SensorAlerts
from services.messaging import commands
from services.messaging.lamport import LamportClock
from services.messaging.publisher import ReliablePublisher
from services.messaging.relay import wait_for

HOST = os.getenv("EDGE_LIVE_MQTT_HOST")
PORT = int(os.getenv("EDGE_LIVE_MQTT_PORT", "1883"))
pytestmark = pytest.mark.skipif(not HOST, reason="set EDGE_LIVE_MQTT_HOST to run live MQTT checks")


@pytest.fixture
def central(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/live-mqtt-only")
    from services.messaging import alerts, mqtt_client
    monkeypatch.setattr(mqtt_client, "CLIENT_ID", "central-test-" + uuid4().hex[:8])
    monkeypatch.setattr(mqtt_client, "alert_store", alerts.AlertStore())
    monkeypatch.setattr(mqtt_client, "clock", LamportClock())
    monkeypatch.setenv("MQTT_HOST", HOST)
    monkeypatch.setenv("MQTT_PORT", str(PORT))
    monkeypatch.setenv("MQTT_TOPIC", "transit/unused-" + uuid4().hex[:6] + "/bus/+/state")
    mqtt_client.start()
    wait_for(lambda: mqtt_client.client.is_connected(), what="central connection")
    yield mqtt_client
    mqtt_client.stop()


@pytest.fixture
def buses():
    suffix = uuid4().hex[:8]
    north, south = "north" + suffix, "south" + suffix
    started = {}
    for bus_id, region in (("N1", north), ("N2", north), ("S1", south)):
        controls, clock = commands.BusControls(), LamportClock()
        consumer = commands.CommandConsumer(region, bus_id, controls, clock)
        publisher = ReliablePublisher(f"{bus_id}-{suffix}-publisher", HOST, PORT)
        for topic_filter in commands.bus_subscriptions(region, bus_id):
            publisher.subscribe(topic_filter, consumer.handle)
        publisher.connect()
        started[bus_id] = (region, consumer, publisher)
    yield started
    for _, _, publisher in started.values():
        publisher.close(flush_timeout=1)


def outcomes(buses, sent):
    """What each bus did with the commands this test sent (probes excluded)."""
    ids = {command.command_id for command in sent}
    return {bus_id: [outcome for command_id, outcome in consumer.history if command_id in ids]
            for bus_id, (_, consumer, _) in buses.items()}


def test_commands_reach_only_their_intended_buses(central, buses):
    north, south = buses["N1"][0], buses["S1"][0]
    # Wait until every bus's subscription is live (a harmless resume per region).
    def all_subscribed():
        for region in (north, south):
            central.publish_command(region, "resume")
        return all(consumer.history for _, consumer, _ in buses.values())
    wait_for(all_subscribed, what="bus command subscriptions", interval=0.3)

    sent = [central.publish_command(north, "pause", target="N1")]
    wait_for(lambda: outcomes(buses, sent)["N1"] == ["applied"], what="command at N1")
    sent.append(central.publish_command(north, "set_traffic", {"traffic_factor": 0.4}))
    wait_for(lambda: outcomes(buses, sent)["N2"] == ["applied"], what="region command at N2")
    wait_for(lambda: outcomes(buses, sent)["N1"] == ["applied", "applied"], what="region command at N1")
    sent.append(central.publish_command(south, "set_demand", {"demand": "peak"}, target="S1"))
    wait_for(lambda: outcomes(buses, sent)["S1"] == ["applied"], what="command at S1")

    controls = {bus_id: consumer.controls for bus_id, (_, consumer, _) in buses.items()}
    assert controls["N1"].paused and not controls["N2"].paused and not controls["S1"].paused
    assert controls["N1"].traffic_factor == controls["N2"].traffic_factor == 0.4
    assert controls["S1"].traffic_factor == 1.0
    assert controls["S1"].demand == "peak" and controls["N1"].demand == controls["N2"].demand == "off_peak"
    # No bus received (let alone acted on) a command not addressed to it.
    assert outcomes(buses, sent) == {"N1": ["applied", "applied"], "N2": ["applied"], "S1": ["applied"]}
    # Receive rule: S1's clock is now past central's stamp on its command.
    assert buses["S1"][1].clock.value > sent[-1].lamport_clock


def test_failure_alert_from_a_bus_reaches_central(central, buses):
    region, consumer, publisher = buses["S1"]
    monitor = SensorAlerts({"bus_id": "S1", "region_id": region}, consumer.clock, publisher.publish)
    faulty = dict.fromkeys(("gps", "speed", "stop", "occupancy"), "faulty")
    monitor.update(faulty)
    wait_for(lambda: central.alert_store.list(region_id=region), what="alert at central")
    [alert] = central.alert_store.list(region_id=region)
    assert (alert["kind"], alert["source_id"], alert["severity"]) == ("sensor_failure", "S1", "warning")
    assert alert["central_lamport"] > alert["lamport_clock"]
    assert publisher.flush(5) == 0

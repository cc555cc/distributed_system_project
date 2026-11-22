"""Edges follow central's retained ownership assignment (live failover / failback)."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from services.data_simulation.generate_data import generate_messages
from services.edge.communication.data_to_edge import TelemetrySubscriber
from services.edge.communication.edge_to_central import CentralForwarder
from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.edge_server import EdgeIngress
from services.edge.edge_server_health import EdgeMonitor
from services.edge.processing.processor import EdgeProcessor, ProcessingStatus
from services.edge.validation.validator import parse_telemetry
from shared.schema.health import HealthPolicy
from shared.schema.network import DEFAULT_NETWORK_PATH

WEST_1_HOME = {"bus-302-1", "bus-302-4", "bus-900-1", "bus-900-4"}
WEST_2_HOME = {"bus-302-2", "bus-302-3", "bus-900-2", "bus-900-3"}


@pytest.fixture
def backup():
    """edge-west-2, the peer that should take over edge-west-1's buses."""
    network = NetworkConfig.load(DEFAULT_NETWORK_PATH)
    config = EdgeConfig("DURHAM", "west", "edge-west-2")
    monitor = EdgeMonitor(config, network.registry, HealthPolicy())
    subscriber = TelemetrySubscriber(
        "west",
        "edge-west-2",
        city_id="DURHAM",
        registry=network.registry,
        monitor=monitor,
    )
    return SimpleNamespace(
        network=network, config=config, monitor=monitor, subscriber=subscriber
    )


def assignment(owners, epoch=100, region="west"):
    return json.dumps({"region_id": region, "epoch": epoch, "owners": owners}).encode()


def failover_owners():
    return {bus: "edge-west-2" for bus in WEST_1_HOME | WEST_2_HOME}


def telemetry_for(bus_id):
    return next(generate_messages("302", 0, bus_id, max_updates=1, update_seconds=0))


def test_backup_takes_over_buses_and_processes_their_telemetry(backup):
    b = backup
    assert set(b.monitor.bus_ids) == WEST_2_HOME
    assert sorted(
        b.subscriber.apply_assignment(assignment(failover_owners()))
    ) == sorted(WEST_1_HOME)
    assert (
        set(b.network.registry.assigned_bus_ids("DURHAM", "west", "edge-west-2"))
        == WEST_1_HOME | WEST_2_HOME
    )
    assert (
        set(b.monitor.bus_ids) == WEST_1_HOME | WEST_2_HOME
    )  # heartbeat now reports them too

    source = telemetry_for("bus-302-1")
    topic = "transit/buses/west/bus-302-1/telemetry"
    assert b.subscriber.handle_raw_message(
        topic, json.dumps(source).encode()
    )  # queued, not discarded
    valid, errors = parse_telemetry(
        source,
        expected_region="west",
        expected_bus="bus-302-1",
        registry=b.network.registry,
    )
    assert not errors
    assert (
        EdgeProcessor(b.config, b.network).process(valid).status
        == ProcessingStatus.ACCEPTED
    )


def test_failback_hands_buses_back_and_stops_processing_them(backup):
    b = backup
    b.subscriber.apply_assignment(assignment(failover_owners(), epoch=100))
    home = {
        **{bus: "edge-west-1" for bus in WEST_1_HOME},
        **{bus: "edge-west-2" for bus in WEST_2_HOME},
    }
    assert sorted(b.subscriber.apply_assignment(assignment(home, epoch=101))) == sorted(
        WEST_1_HOME
    )
    assert set(b.monitor.bus_ids) == WEST_2_HOME
    source = telemetry_for("bus-302-1")
    assert not b.subscriber.handle_raw_message(
        "transit/buses/west/bus-302-1/telemetry", json.dumps(source).encode()
    )


def test_older_assignment_arriving_late_is_ignored(backup):
    b = backup
    b.subscriber.apply_assignment(assignment(failover_owners(), epoch=200))
    home = {bus: "edge-west-1" for bus in WEST_1_HOME}
    assert b.subscriber.apply_assignment(assignment(home, epoch=150)) == []
    assert b.network.registry.get("bus-302-1").edge_id == "edge-west-2"


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        json.dumps(["list"]).encode(),
        assignment({"bus-302-1": "edge-west-2"}, region="east"),
        json.dumps({"region_id": "west", "epoch": "1", "owners": {}}).encode(),
        json.dumps(
            {"region_id": "west", "epoch": 1, "owners": {"bus-302-1": ""}}
        ).encode(),
    ],
)
def test_malformed_assignment_is_rejected_without_changing_ownership(backup, raw):
    b = backup
    message = SimpleNamespace(topic="transit/assignments/west", payload=raw)
    b.subscriber.on_message(None, None, message)  # logged and ignored, never raises
    assert (
        set(b.network.registry.assigned_bus_ids("DURHAM", "west", "edge-west-2"))
        == WEST_2_HOME
    )


def test_assignment_cannot_pull_buses_from_another_region(backup):
    b = backup
    assert (
        b.subscriber.apply_assignment(
            assignment({"bus-407-1": "edge-west-2", "unknown-bus": "edge-west-2"})
        )
        == []
    )
    assert b.network.registry.get("bus-407-1").edge_id == "edge-east-1"


def test_pending_update_for_a_bus_handed_away_is_dropped_not_crashing(backup):
    b = backup
    sender = Mock()
    sender.send.side_effect = ValueError("Cannot send a bus assigned to another edge")
    receiver = EdgeIngress(EdgeProcessor(b.config, b.network))
    forwarder = CentralForwarder(receiver, sender, "passengers")
    forwarder.pending["bus-302-2"] = ({"payload": 1}, receiver.processor.clock())
    assert forwarder.flush_one() is False
    assert not forwarder.pending

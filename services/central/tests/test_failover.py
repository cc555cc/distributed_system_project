"""Central-driven failover: failed edge -> peer takes its buses -> recovered edge gets them back."""

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from services.central import ingestion_check, ownership as ownership_module
from services.central.failover import run_round
from services.central.health import CentralHealthMonitor
from services.central.ownership import BusOwnership
from shared.schema.health import HealthPolicy
from shared.schema.network import load_network

from services.central.tests.test_health import report

WEST_1_HOME = {"bus-302-1", "bus-302-4", "bus-900-1", "bus-900-4"}
WEST_2_HOME = {"bus-302-2", "bus-302-3", "bus-900-2", "bus-900-3"}


@pytest.fixture
def central():
    now = [0.0]
    policy = HealthPolicy(interval_seconds=1, sensor_timeout_seconds=5, edge_timeout_seconds=10,
                          recovery_seconds=2, sustained_seconds=3)
    ownership = BusOwnership(load_network())
    monitor = CentralHealthMonitor(load_network(), policy, clock=lambda: now[0], ownership=ownership)
    published = []
    sequence = {}

    def beat(edge_id, region="west", session="first"):
        key = (edge_id, session)
        sequence[key] = sequence.get(key, 0) + 1
        value = report(edge_id, region, sequence=sequence[key], session=session)
        assert monitor.receive(f"transit/heartbeats/edge/{region}/{edge_id}", json.dumps(value).encode())

    def round_():
        return run_round(monitor, ownership, lambda region, payload: published.append((region, payload)))

    return SimpleNamespace(now=now, ownership=ownership, monitor=monitor, published=published, beat=beat, round=round_)


def all_edges_beat(c, session="first", skip=()):
    for edge_id, region in (("edge-west-1", "west"), ("edge-west-2", "west"),
                            ("edge-east-1", "east"), ("edge-east-2", "east")):
        if edge_id not in skip:
            c.beat(edge_id, region, session=session)


def test_failed_edge_buses_move_to_peer_and_return_after_recovery(central):
    c = central
    all_edges_beat(c)
    assert c.round() == set()  # everyone healthy: nothing moves, nothing published
    assert c.published == []

    # edge-west-1 goes silent past the timeout while the others keep reporting
    for t in range(1, 12):
        c.now[0] = t
        all_edges_beat(c, skip=("edge-west-1",))
    assert c.round() == {"west"}
    assert c.ownership.buses_of("edge-west-2") == WEST_1_HOME | WEST_2_HOME
    region, payload = c.published[-1]
    assert region == "west" and set(payload["owners"].values()) == {"edge-west-2"}
    moved = [e for e in c.monitor.events.recent(50) if e["type"] == "bus_reassigned"]
    assert {e["bus_id"] for e in moved} == WEST_1_HOME
    assert all(e["from_edge"] == "edge-west-1" and e["to_edge"] == "edge-west-2" for e in moved)

    # restarted edge-west-1 is only "recovering" at first: no failback yet
    c.now[0] = 12
    all_edges_beat(c, session="restart", skip=("edge-west-2", "edge-east-1", "edge-east-2"))
    all_edges_beat(c, skip=("edge-west-1",))
    assert c.round() == set()
    assert c.ownership.buses_of("edge-west-1") == set()

    # after the recovery window it is healthy again and gets exactly its home buses back
    for t in (13, 14, 15):
        c.now[0] = t
        all_edges_beat(c, session="restart", skip=("edge-west-2", "edge-east-1", "edge-east-2"))
        all_edges_beat(c, skip=("edge-west-1",))
    assert c.round() == {"west"}
    assert c.ownership.buses_of("edge-west-1") == WEST_1_HOME
    assert c.ownership.buses_of("edge-west-2") == WEST_2_HOME
    assert c.published[-1][1]["owners"]["bus-302-1"] == "edge-west-1"


def test_backup_heartbeat_listing_taken_over_buses_is_accepted(central):
    c = central
    value = report("edge-west-2", "west")
    extra = report("edge-west-1", "west")["buses"]
    value["buses"] += extra
    value["workload"]["assigned_buses"] = len(value["buses"])
    assert c.monitor.receive("transit/heartbeats/edge/west/edge-west-2", json.dumps(value).encode())


@pytest.mark.parametrize("problem", ["other_region_bus", "duplicate_bus", "count_mismatch"])
def test_inconsistent_heartbeat_bus_lists_are_still_rejected(central, problem):
    c = central
    value = report("edge-west-2", "west")
    if problem == "other_region_bus":
        value["buses"].append(dict(report("edge-east-1", "east")["buses"][0]))
        value["workload"]["assigned_buses"] = len(value["buses"])
    elif problem == "duplicate_bus":
        value["buses"].append(dict(value["buses"][0]))
        value["workload"]["assigned_buses"] = len(value["buses"])
    else:
        value["workload"]["assigned_buses"] += 1
    assert not c.monitor.receive("transit/heartbeats/edge/west/edge-west-2", json.dumps(value).encode())


def test_snapshot_lists_each_bus_once_under_its_current_owner(central):
    c = central
    all_edges_beat(c)
    c.ownership.reconcile({"edge-west-1": "unavailable", "edge-west-2": "healthy"})
    buses = c.monitor.snapshot()["buses"]
    assert len(buses) == len({b["bus_id"] for b in buses}) == 20
    assert {b["bus_id"] for b in buses if b["edge_id"] == "edge-west-2"} == WEST_1_HOME | WEST_2_HOME
    assert not [b for b in buses if b["edge_id"] == "edge-west-1"]


def test_ingestion_fences_updates_from_an_edge_that_is_not_the_owner(monkeypatch):
    live = BusOwnership(load_network())
    monkeypatch.setattr(ownership_module, "bus_ownership", live)

    def check(edge_id):
        connection = Mock()
        responses = [Mock(), Mock()]
        responses[0].fetchone.return_value = (1,)            # bus is in the region
        responses[1].fetchone.return_value = ("302", None)   # route, stored next stop
        connection.execute.side_effect = responses
        state = SimpleNamespace(next_stop_id=None, direction_id=0,
                                edge_metadata=SimpleNamespace(route_id="302", edge_id=edge_id))
        return ingestion_check.check_update(connection, "bus-302-1", "west", state)

    assert check("edge-west-1") is None                       # home owner accepted
    assert "not the current owner" in check("edge-west-2")    # peer fenced while west-1 owns it
    live.reconcile({"edge-west-1": "unavailable", "edge-west-2": "healthy"})
    assert check("edge-west-2") is None                       # after failover the peer is accepted
    assert "not the current owner" in check("edge-west-1")    # the failed edge is fenced

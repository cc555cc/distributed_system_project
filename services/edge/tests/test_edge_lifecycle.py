"""Instance lifecycle: creation, configuration, registration, recovery and scaling."""

from unittest.mock import Mock

import pytest

from services.edge.edge_server import EdgeServerInstance
from services.edge.main import configured_instances, run_single_instance
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network


def slow_simulation():
    from services.edge.main import EdgeLoadSimulation
    simulation = EdgeLoadSimulation()
    for edge_id in ("edge-west-1", "edge-west-2"):
        simulation.configure(edge_id, processing_ms=60000)
    return simulation


@pytest.fixture
def cluster():
    from services.edge.main import EdgeClusterSimulation
    return EdgeClusterSimulation()


def test_main_uses_four_configured_instances():
    assert set(configured_instances(DEFAULT_NETWORK_PATH)) == {
        "edge-west-1", "edge-west-2", "edge-east-1", "edge-east-2"}


def test_single_instance_checks_environment(monkeypatch):
    monkeypatch.setenv("EDGE_ID", "edge-west-1")
    monkeypatch.setenv("REGION_ID", "east")
    with pytest.raises(ValueError, match="does not match"):
        run_single_instance()


def test_main_deployment_has_two_disjoint_owners_per_region():
    from services.edge.main import deployment_plan
    plan = deployment_plan(DEFAULT_NETWORK_PATH)
    all_buses = []
    for region, routes, count in (("west", {"302", "900"}, 4), ("east", {"407", "409", "901"}, 6)):
        workers = [worker for worker in plan if worker["region_id"] == region]
        assert len(workers) == 2
        assert all(set(worker["route_ids"]) == routes for worker in workers)
        assert all(len(worker["bus_ids"]) == count for worker in workers)
        assert set(workers[0]["bus_ids"]).isdisjoint(workers[1]["bus_ids"])
        all_buses.extend(bus for worker in workers for bus in worker["bus_ids"])
    assert set(all_buses) == {bus["bus_id"] for bus in load_network()["buses"]}
    assert len(all_buses) == len(set(all_buses))


def test_main_constructs_independent_workers(monkeypatch):
    import services.edge.main as launcher
    monkeypatch.setenv("CENTRAL_OCCUPANCY_UNIT", "passengers")
    workers = []
    real_worker = EdgeServerInstance
    def capture(*args):
        worker = real_worker(*args)
        worker.run = Mock()
        workers.append(worker)
        return worker
    monkeypatch.setattr(launcher, "EdgeServerInstance", capture)
    stop = Mock()
    for identity in launcher.configured_instances(DEFAULT_NETWORK_PATH).values():
        launcher.run_instance(DEFAULT_NETWORK_PATH, identity, stop)
    assert len(workers) == 4
    assert len({id(worker.subscriber.messages) for worker in workers}) == 4
    assert len({id(worker.receiver.processor.cache) for worker in workers}) == 4
    for worker in workers:
        worker.run.assert_called_once_with(stop)


def test_launcher_rejects_unknown_instance_before_starting_process(monkeypatch):
    import services.edge.main as launcher
    process = Mock()
    monkeypatch.setattr(launcher, "Process", process)
    with pytest.raises(ValueError, match="unregistered"):
        launcher.launch_instances(DEFAULT_NETWORK_PATH, ["unknown-edge"])
    process.assert_not_called()


def test_sustained_load_starts_third_and_preserves_every_synthetic_request():
    simulation = slow_simulation()
    simulation.submit("bus-302-1", 80)
    before = {job[0] for w in simulation.workers.values() for job in w.queue}
    simulation.advance(5)
    assert not simulation.workers["edge-west-3"].active
    simulation.advance(1)
    third = simulation.workers["edge-west-3"]
    assert third.active and third.queue
    assert not simulation.workers["edge-east-3"].active
    after = [job[0] for w in simulation.workers.values() for job in w.queue]
    assert len(after) == len(set(after)) == 80
    assert set(after) == before
    assert simulation.snapshot()["live_workers_changed"] is False
    simulation.advance(1)
    assert third.completed > 0
    assert sum(len(w.queue) + w.completed for w in simulation.workers.values()) == 80


def test_short_burst_drains_without_starting_third_instance():
    from services.edge.main import EdgeLoadSimulation
    simulation = EdgeLoadSimulation()
    simulation.submit("bus-302-1", 80)
    report = simulation.advance(1)
    assert sum(w["completed"] for w in report["workers"]) == 80
    assert not simulation.workers["edge-west-3"].active
    assert simulation.workers["edge-west-1"].snapshot()["average_latency_ms"] == pytest.approx(102.5)


def test_disabled_autoscale_keeps_third_stopped():
    simulation = slow_simulation()
    simulation.auto_scale = False
    simulation.submit("bus-302-1", 80)
    simulation.advance(10)
    assert not simulation.workers["edge-west-3"].active


def test_11_sustained_real_telemetry_scales_up_then_safely_scales_down(cluster):
    for edge_id in ("edge-west-1", "edge-west-2"):
        cluster.configure(edge_id, processing_ms=60000)
    cluster.generate("bus-302-1", 40)
    cluster.generate("bus-302-2", 40)
    cluster.advance(5)
    assert not cluster.workers["edge-west-3"].active
    cluster.advance(1)
    assert cluster.workers["edge-west-3"].active
    assert cluster._assigned("edge-west-3")
    assert any(len(job) > 3 for job in cluster.workers["edge-west-3"].queue)
    for edge_id in ("edge-west-1", "edge-west-2"):
        cluster.configure(edge_id, processing_ms=5)
    cluster.advance(1)
    assert not cluster.journal
    assert cluster.outcomes["ACCEPTED"] == 80
    cluster.advance(10)
    assert not cluster.workers["edge-west-3"].active
    assert not cluster._assigned("edge-west-3")
    cluster.generate("bus-302-1", 1)
    cluster.advance(1)
    assert cluster.central_states["bus-302-1"]["sequence_number"] == 41


def test_scale_down_rejects_insufficient_capacity_atomically(cluster):
    cluster.generate("bus-302-1", 10)
    cluster.generate("bus-302-2", 50)
    before = cluster.snapshot()["ownership"]
    with pytest.raises(ValueError, match="capacity"):
        cluster.scale_down("edge-west-1")
    assert cluster.snapshot()["ownership"] == before
    assert cluster.workers["edge-west-1"].active


def test_new_instance_registration_and_snapshot_are_isolated(cluster):
    with pytest.raises(ValueError, match="unique"):
        cluster.register("edge-west-1", "west")
    with pytest.raises(ValueError, match="Unknown region"):
        cluster.register("edge-new", "unknown")
    with pytest.raises(ValueError):
        cluster.register("edge-new", "west", capacity=51)
    assert "edge-new" not in cluster.workers
    cluster.generate("bus-302-1", 1)
    cluster.advance(1)
    snapshot = cluster.snapshot()
    snapshot["central_states"]["bus-302-1"]["sequence_number"] = -1
    assert cluster.central_states["bus-302-1"]["sequence_number"] == 1


def test_advance_chunking_does_not_change_recovery_or_processing():
    from services.edge.main import EdgeClusterSimulation
    large, small = EdgeClusterSimulation(), EdgeClusterSimulation()
    for cluster in (large, small):
        cluster.generate("bus-302-1", 1)
        cluster.advance(1)
        cluster.fail("edge-west-1")
        cluster.recover("edge-west-1")
        cluster.generate("bus-302-1", 4)
    large.advance(5)
    for _ in range(5):
        small.advance(1)
    assert large.snapshot()["ownership"] == small.snapshot()["ownership"]
    assert large.outcomes == small.outcomes
    assert [w.state for w in large.workers.values()] == [w.state for w in small.workers.values()]


def test_autoscaling_cannot_resurrect_a_failed_third_instance(cluster):
    cluster.fail("edge-west-3")
    for edge_id in ("edge-west-1", "edge-west-2"):
        cluster.configure(edge_id, processing_ms=60000)
    cluster.generate("bus-302-1", 40)
    cluster.generate("bus-302-2", 40)
    cluster.advance(6)
    assert cluster.workers["edge-west-3"].state == "failed"
    assert not cluster.workers["edge-west-3"].active

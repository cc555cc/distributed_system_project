"""Pipeline behaviour: telemetry, state replication, handoffs, ordering and throughput."""

import json

import pytest

from services.data_simulation.run_buses import SimulatorHeartbeats
from services.edge.communication.data_to_edge import TelemetrySubscriber
from services.edge.config import NetworkConfig
from shared.schema.health import SENSORS
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network


@pytest.fixture
def cluster():
    from services.edge.main import EdgeClusterSimulation

    return EdgeClusterSimulation()


def test_simulator_four_logical_channels_and_shared_sequence():
    source = SimulatorHeartbeats(load_network()["buses"][0])
    source.update({sensor: "faulty" for sensor in SENSORS})
    first, second = source.snapshot(), source.snapshot()
    assert set(first.sensors) == set(SENSORS)
    assert first.session_id == second.session_id
    assert second.sequence_number == first.sequence_number + 1


def test_other_workers_telemetry_never_fills_our_queue():
    from services.data_simulation.generate_data import generate_messages

    network = NetworkConfig.load(DEFAULT_NETWORK_PATH)
    source = next(
        generate_messages("302", 1, "bus-302-2", max_updates=1, update_seconds=0)
    )
    subscriber = TelemetrySubscriber(
        "west", "edge-west-1", registry=network.registry, city_id="DURHAM", monitor=None
    )
    assert not subscriber.handle_raw_message(
        "transit/buses/west/bus-302-2/telemetry", json.dumps(source).encode()
    )
    assert subscriber.messages.empty()


def test_generator_reports_fault_without_inventing_telemetry():
    from services.data_simulation.generate_data import generate_messages

    health = []
    assert (
        list(
            generate_messages(
                "302",
                0,
                "bus-302-1",
                max_updates=2,
                update_seconds=0,
                sensor_failure_rate=1.0,
                sensor_health=health.append,
            )
        )
        == []
    )
    assert len(health) == 2
    assert all(
        set(states) == set(SENSORS) and set(states.values()) == {"faulty"}
        for states in health
    )


def test_05_replication_takeover_pending_fifo_and_old_owner_fencing(cluster):
    bus = "bus-302-1"
    assert cluster.generate(bus, 1)["accepted"] == 1
    cluster.advance(1)
    owner = cluster.registry.get(bus).edge_id
    assert cluster.workers["edge-west-2"].replicas[bus]["floor"] == 1
    assert cluster.generate(bus, 5)["accepted"] == 5
    old_job = next(job for job in cluster.workers[owner].queue if job[1] == bus)
    old_epoch = cluster.epochs[bus]
    cluster.fail(owner)
    assert not cluster.workers[owner].processors  # Actual local state is lost.
    assert cluster.registry.get(bus).edge_id == "edge-west-2"
    assert cluster.epochs[bus] > old_epoch
    target = cluster.workers["edge-west-2"]
    assert [job[3]["sequence_number"] for job in target.queue if job[1] == bus] == [
        2,
        3,
        4,
        5,
        6,
    ]
    cluster._complete(cluster.workers[owner], old_job)
    assert cluster.central_states[bus]["sequence_number"] == 1
    assert cluster.outcomes["FENCED"] == 1
    cluster.advance(1)
    assert cluster.central_states[bus]["sequence_number"] == 6
    assert cluster.central_states[bus]["edge_metadata"]["edge_id"] == "edge-west-2"
    assert cluster.outcomes["ACCEPTED"] == 6
    assert not cluster.journal


def test_06_replays_conflicts_invalid_data_after_takeover_are_not_accepted(cluster):
    from copy import deepcopy

    bus = "bus-302-1"
    cluster.generate(bus, 2)
    cluster.advance(1)
    cluster.fail("edge-west-1")
    source = deepcopy(cluster.message_templates[bus])
    source["sequence_number"] = source["lamport_clock"] = 2
    assert cluster.submit_telemetry(source)["accepted"]
    cluster.advance(1)
    assert cluster.outcomes["DUPLICATE"] == 1
    source["passengers"] += 1
    assert cluster.submit_telemetry(source)["accepted"]
    cluster.advance(1)
    assert cluster.outcomes["CONFLICT"] == 1
    source["sequence_number"] = 1
    assert cluster.submit_telemetry(source)["accepted"]
    cluster.advance(1)
    assert cluster.outcomes["STALE"] == 1
    source["latitude"] = 999
    assert not cluster.submit_telemetry(source)["accepted"]
    assert cluster.central_states[bus]["sequence_number"] == 2


def test_08_assignment_balancing_and_12_new_instance_gets_synchronized_state(cluster):
    for bus in cluster.bus_regions:
        cluster.generate(bus, 1)
    cluster.advance(1)
    cluster.register("edge-west-dynamic", "west")
    assert cluster.workers["edge-west-dynamic"].replicas["bus-302-1"]["floor"] == 1
    cluster.advance(1)
    counts = [len(cluster._assigned(w.edge_id)) for w in cluster._eligible("west")]
    assert max(counts) - min(counts) <= 1
    moved = cluster._assigned("edge-west-dynamic")
    assert moved
    for bus in moved:
        cluster.generate(bus, 1)
    cluster.advance(1)
    for bus in moved:
        assert cluster.central_states[bus]["sequence_number"] == 2
        assert (
            cluster.central_states[bus]["edge_metadata"]["edge_id"]
            == "edge-west-dynamic"
        )


def test_09_cross_region_takeover_preserves_home_route_then_synchronized_recovery(
    cluster,
):
    bus = "bus-302-1"
    cluster.generate(bus, 1)
    cluster.advance(1)
    cluster.fail("edge-west-1")
    cluster.fail("edge-west-2")
    remote = cluster.registry.get(bus).edge_id
    assert remote.startswith("edge-east-")
    cluster.generate(bus, 1)
    cluster.advance(1)
    state = cluster.central_states[bus]
    assert state["edge_metadata"]["region_id"] == "west"
    assert state["edge_metadata"]["route_id"] == "302"
    assert state["edge_metadata"]["edge_id"] == remote
    cluster.recover("edge-west-1")
    assert cluster.workers["edge-west-1"].state == "recovering"
    assert not cluster.workers["edge-west-1"].active
    cluster.advance(1)
    assert cluster.workers["edge-west-1"].state == "recovering"
    cluster.advance(1)
    assert cluster.workers["edge-west-1"].state == "healthy"
    assert cluster.workers["edge-west-1"].replicas[bus]["floor"] == 2
    cluster.transfer(bus, "edge-west-1")
    cluster.generate(bus, 1)
    cluster.advance(1)
    assert cluster.central_states[bus]["sequence_number"] == 3


def test_failed_full_target_keeps_pending_updates_until_capacity_returns(cluster):
    cluster.configure("edge-west-2", processing_ms=60000)
    cluster.generate("bus-302-2", 50)
    cluster.generate("bus-302-1", 5)
    cluster.fail("edge-west-1")
    assert cluster.registry.get("bus-302-1").edge_id == "edge-west-1"
    assert len(cluster.journal) == 55
    cluster.configure("edge-west-2", processing_ms=5)
    cluster.advance(1)
    assert cluster.registry.get("bus-302-1").edge_id == "edge-west-2"
    cluster.advance(1)
    assert not cluster.journal
    assert cluster.outcomes["ACCEPTED"] == 55


def test_loss_of_all_replicas_does_not_reset_sequence_protection(cluster):
    cluster.generate("bus-302-1", 1)
    cluster.advance(1)
    for edge_id in ("edge-west-1", "edge-west-2", "edge-east-1", "edge-east-2"):
        cluster.fail(edge_id)
    assert not cluster.submit_telemetry(cluster.message_templates["bus-302-1"])[
        "accepted"
    ]
    with pytest.raises(ValueError, match="replica"):
        cluster.recover("edge-west-1")
    assert cluster.workers["edge-west-1"].state == "failed"


def test_transfer_refuses_full_or_unhealthy_target_without_changing_owner(cluster):
    cluster.generate("bus-302-1", 5)
    cluster.generate("bus-302-2", 50)
    before = cluster.snapshot()["ownership"]["bus-302-1"]
    with pytest.raises(ValueError, match="capacity"):
        cluster.transfer("bus-302-1", "edge-west-2")
    assert cluster.snapshot()["ownership"]["bus-302-1"] == before
    with pytest.raises(ValueError, match="healthy"):
        cluster.transfer("bus-302-1", "edge-west-3")


def test_replica_includes_sequence_floor_after_state_expiry(cluster):
    bus = "bus-302-1"
    cluster.generate(bus, 1)
    cluster.advance(1)
    owner = cluster.registry.get(bus).edge_id
    processor = cluster._processor(owner, bus)
    processor.cache.release_bus(cluster._key(bus))
    cluster._replicate(owner, bus)
    cluster.transfer(bus, "edge-west-2")
    cluster.submit_telemetry(cluster.message_templates[bus])
    cluster.advance(1)
    assert cluster.outcomes["STALE"] == 1
    assert cluster.central_states[bus]["sequence_number"] == 1


@pytest.mark.parametrize("size", [5, 10, 25, 50])
def test_10_fleet_performance_and_failover_have_no_lost_accepted_updates(size):
    from services.edge.main import EdgeClusterSimulation

    cluster = EdgeClusterSimulation(fleet_size=size)
    for bus in cluster.bus_regions:
        assert cluster.generate(bus, 2)["accepted"] == 2
    cluster.advance(1)
    cluster.fail("edge-west-1")
    for bus in cluster.bus_regions:
        assert cluster.generate(bus, 1)["accepted"] == 1
    cluster.advance(1)
    assert len(cluster.central_states) == size
    assert all(
        state["sequence_number"] == 3 for state in cluster.central_states.values()
    )
    assert cluster.outcomes["ACCEPTED"] == size * 3
    assert not cluster.journal
    assert sum(w.completed for w in cluster.workers.values()) == size * 3
    assert all(len(w.queue) <= w.capacity <= 50 for w in cluster.workers.values())


def test_08_queue_pressure_moves_whole_bus_without_ownership_oscillation(cluster):
    for edge_id in ("edge-west-1", "edge-west-2"):
        cluster.configure(edge_id, processing_ms=60000)
    cluster.generate("bus-302-1", 25)
    cluster.generate("bus-900-1", 20)
    cluster.advance(1)
    workers = cluster._eligible("west")
    assert sorted(len(worker.queue) for worker in workers) == [20, 25]
    assert (
        max(len(cluster._assigned(w.edge_id)) for w in workers)
        - min(len(cluster._assigned(w.edge_id)) for w in workers)
        <= 1
    )
    assert len(cluster.journal) == 45
    assert not cluster.workers["edge-west-3"].active


def test_failure_preserves_synthetic_jobs_too(cluster):
    cluster.submit("bus-302-1", 20)
    cluster.fail("edge-west-1")
    cluster.advance(1)
    assert sum(w.completed for w in cluster.workers.values()) == 20
    assert not cluster.orphaned_requests

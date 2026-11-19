"""EDGE-04/07 health and EDGE-05/06 instance checks; takeover tests await implementation."""

import json
from queue import Queue
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from services.data_simulation.run_buses import SimulatorHeartbeats
from services.edge.communication.data_to_edge import TelemetrySubscriber
from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.edge_server import EdgeServerInstance
from services.edge.main import configured_instances, run_single_instance
from services.edge.edge_server_health import EdgeMonitor
from shared.schema.health import BusHeartbeat, HealthPolicy, HeartbeatTracker, SENSORS, load_band
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network


@pytest.fixture
def setup():
    now = [0.0]
    policy = HealthPolicy(interval_seconds=1, sensor_timeout_seconds=5,
                          edge_timeout_seconds=8, recovery_seconds=2, sustained_seconds=3)
    network = NetworkConfig.load(DEFAULT_NETWORK_PATH)
    config = EdgeConfig("DURHAM", "west", "edge-west-1")
    monitor = EdgeMonitor(config, network.registry, policy, clock=lambda: now[0])
    return SimpleNamespace(now=now, policy=policy, network=network, config=config, monitor=monitor)


def bus_heartbeat(sequence=1, session="first", sensors=None, bus_id="bus-302-1"):
    return BusHeartbeat(city_id="DURHAM", region_id="west", bus_id=bus_id,
                        session_id=session, sequence_number=sequence,
                        timestamp="2026-01-01T00:00:00Z",
                        sensors={sensor: "healthy" for sensor in SENSORS} if sensors is None else sensors)


def receive(setup, heartbeat):
    return setup.monitor.receive(f"transit/heartbeats/bus/west/{heartbeat.bus_id}", heartbeat.model_dump_json().encode())


def sensor(setup, name="gps"):
    return next(bus for bus in setup.monitor.sensor_reports() if bus["bus_id"] == "bus-302-1")["sensors"][name]


# ==================== RECEIPT TIME, TIMEOUT AND RECOVERY ====================
def test_never_seen_sources_time_out_without_any_new_message(setup):
    assert sensor(setup)["status"] == "unknown"
    setup.now[0] = 5
    assert sensor(setup)["status"] == "unavailable"


def test_healthy_heartbeat_timeout_and_recovery(setup):
    assert receive(setup, bus_heartbeat())
    assert sensor(setup)["status"] == "healthy"
    setup.now[0] = 5
    assert sensor(setup)["status"] == "unavailable"
    assert receive(setup, bus_heartbeat(2))
    assert sensor(setup)["status"] == "recovering"
    setup.now[0] = 7
    assert receive(setup, bus_heartbeat(3))
    assert sensor(setup)["status"] == "healthy"


def test_duplicate_and_old_heartbeats_do_not_refresh_liveness(setup):
    receive(setup, bus_heartbeat(2))
    setup.now[0] = 4
    assert not receive(setup, bus_heartbeat(2))
    assert not receive(setup, bus_heartbeat(1))
    setup.now[0] = 5
    assert sensor(setup)["status"] == "unavailable"


def test_old_session_cannot_resurrect_after_restart(setup):
    receive(setup, bus_heartbeat())
    setup.now[0] = 1
    receive(setup, bus_heartbeat(1, "restart"))
    assert sensor(setup)["status"] == "recovering"
    assert not receive(setup, bus_heartbeat(100, "first"))


def test_sensor_channels_timeout_independently(setup):
    receive(setup, bus_heartbeat())
    setup.now[0] = 4
    receive(setup, bus_heartbeat(2, sensors={"speed": "healthy"}))
    setup.now[0] = 5
    assert sensor(setup, "gps")["status"] == "unavailable"
    assert sensor(setup, "speed")["status"] == "healthy"


def test_faulty_sensor_can_be_alive_without_a_valid_reading(setup):
    receive(setup, bus_heartbeat(sensors={"gps": "faulty"}))
    assert sensor(setup)["status"] == "healthy"
    assert sensor(setup)["reported_state"] == "faulty"
    assert setup.monitor.sensor_reports()[0]["valid_telemetry_age_seconds"] is None


def test_unknown_or_other_workers_bus_cannot_refresh_health(setup):
    assert not receive(setup, bus_heartbeat(bus_id="unknown"))
    assert not receive(setup, bus_heartbeat(bus_id="bus-302-2"))
    assert not setup.monitor.receive("wrong/topic", bus_heartbeat().model_dump_json().encode())
    assert not setup.monitor.receive("transit/heartbeats/bus/west/bus-302-1", b"broken json")


# ==================== QUEUE UTILIZATION IS NOT PASSENGER OCCUPANCY ====================
@pytest.mark.parametrize("percent,expected", [(0,"normal"),(50,"notice"),(70,"prepare"),
                                             (75,"ready"),(79,"near_capacity"),(80,"overloaded")])
def test_load_bands(percent, expected):
    assert load_band(percent) == expected


def test_queue_metrics_are_real_queue_counts(setup):
    queue = Queue(maxsize=5)
    for value in range(4):
        queue.put(value)
    setup.monitor.record_received()
    setup.monitor.mark_valid_data("bus-302-1")
    setup.monitor.mark_processed(0.02)
    setup.now[0] = 2
    report = setup.monitor.workload(queue, active_buses=1, pending_updates=0)
    assert report["queue_utilization_percent"] == 80
    assert report["load_band"] == "overloaded"
    assert report["messages_per_second"] == 0.5
    assert report["average_processing_ms"] == 20


def test_subscriber_handles_heartbeat_outside_the_telemetry_queue(setup):
    subscriber = TelemetrySubscriber("west", "edge-west-1", registry=setup.network.registry,
                                     city_id="DURHAM", monitor=setup.monitor)
    message = SimpleNamespace(topic="transit/heartbeats/bus/west/bus-302-1",
                              payload=bus_heartbeat().model_dump_json().encode())
    subscriber.on_message(None, None, message)
    assert subscriber.messages.empty()
    assert sensor(setup)["status"] == "healthy"


def test_worker_heartbeat_and_outgoing_health_connection(setup, monkeypatch):
    monkeypatch.setenv("EDGE_QUEUE_SIZE", "5")
    instance = EdgeServerInstance(setup.config, setup.network, setup.policy)
    for value in range(4):
        instance.subscriber.messages.put(value)
    report = instance.heartbeat()
    assert report.edge_id == "edge-west-1"
    assert report.workload.queue_utilization_percent == 80
    assert {bus.bus_id for bus in report.buses} == {"bus-302-1", "bus-900-1"}
    client = Mock()
    client.is_connected.return_value = True
    client.publish.return_value.rc = 0
    instance.sender._client = client
    instance.sender.publish_heartbeat(report)
    assert client.publish.call_args.args[0] == "transit/heartbeats/edge/west/edge-west-1"
    assert json.loads(client.publish.call_args.args[1])["workload"]["queue_size"] == 4


# ==================== SOURCE / LAUNCHER CONFIGURATION ====================
def test_simulator_four_logical_channels_and_shared_sequence():
    source = SimulatorHeartbeats(load_network()["buses"][0])
    source.update({sensor: "faulty" for sensor in SENSORS})
    first, second = source.snapshot(), source.snapshot()
    assert set(first.sensors) == set(SENSORS)
    assert first.session_id == second.session_id
    assert second.sequence_number == first.sequence_number + 1


def test_main_uses_four_configured_instances():
    assert set(configured_instances(DEFAULT_NETWORK_PATH)) == {
        "edge-west-1", "edge-west-2", "edge-east-1", "edge-east-2"}


def test_single_instance_checks_environment(monkeypatch):
    monkeypatch.setenv("EDGE_ID", "edge-west-1")
    monkeypatch.setenv("REGION_ID", "east")
    with pytest.raises(ValueError, match="does not match"):
        run_single_instance()


@pytest.mark.parametrize("values", [dict(interval_seconds=0),dict(sensor_timeout_seconds=1),
                                    dict(overload_percent=101),dict(recovery_seconds=-1)])
def test_bad_thresholds_are_rejected(values):
    with pytest.raises(ValueError):
        HealthPolicy(**values)


def test_heartbeat_subscription_is_registered(setup):
    subscriber = TelemetrySubscriber("west", "edge-west-1", registry=setup.network.registry,
                                     city_id="DURHAM", monitor=setup.monitor)
    client = Mock()
    client.subscribe.return_value = (0, 1)
    subscriber.on_connect(client, None, None, SimpleNamespace(is_failure=False), None)
    assert client.subscribe.call_count == 2
    assert client.subscribe.call_args.args[0] == "transit/heartbeats/bus/west/+"


def test_other_workers_telemetry_never_fills_our_queue(setup):
    from services.data_simulation.generate_data import generate_messages
    source = next(generate_messages("302", 1, "bus-302-2", max_updates=1, update_seconds=0))
    subscriber = TelemetrySubscriber("west", "edge-west-1", registry=setup.network.registry,
                                     city_id="DURHAM", monitor=setup.monitor)
    assert not subscriber.handle_raw_message("transit/buses/west/bus-302-2/telemetry", json.dumps(source).encode())
    assert subscriber.messages.empty()


def test_generator_reports_fault_without_inventing_telemetry():
    from services.data_simulation.generate_data import generate_messages
    health = []
    assert list(generate_messages("302", 0, "bus-302-1", max_updates=2, update_seconds=0,
                                  sensor_failure_rate=1.0, sensor_health=health.append)) == []
    assert len(health) == 2
    assert all(set(states) == set(SENSORS) and set(states.values()) == {"faulty"} for states in health)


# EDGE-05/06: use the launcher's real configuration and worker construction.
def test_main_deployment_has_two_disjoint_owners_per_region():
    from services.edge.main import deployment_plan
    plan = deployment_plan(DEFAULT_NETWORK_PATH)
    all_buses = []
    for region, routes, count in (("west", {"302", "900"}, 2), ("east", {"407", "409", "901"}, 3)):
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


# Bounded synthetic load experiments: queue limits, region isolation and scale-up.
def test_live_queue_defaults_to_fifty_and_rejects_larger_capacity(setup, monkeypatch):
    from services.edge.communication.data_to_edge import create_subscriber
    monkeypatch.delenv("EDGE_QUEUE_SIZE", raising=False)
    assert create_subscriber(setup.config, setup.network).messages.maxsize == 50
    with pytest.raises(ValueError, match="between 1 and 50"):
        TelemetrySubscriber("west", "edge-west-1", queue_size=51)


def slow_simulation():
    from services.edge.main import EdgeLoadSimulation
    simulation = EdgeLoadSimulation()
    for edge_id in ("edge-west-1", "edge-west-2"):
        simulation.configure(edge_id, processing_ms=60000)
    return simulation


def test_full_region_rejects_excess_requests_without_spilling_to_east():
    simulation = slow_simulation()
    result = simulation.submit("bus-302-1", 105)
    assert (result["accepted"], result["rejected"]) == (100, 5)
    assert all(len(w.queue) <= w.capacity == 50 for w in simulation.workers.values())
    assert all(not w.queue for w in simulation.workers.values() if w.region_id == "east")


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


def test_capacity_changes_cannot_discard_queued_requests():
    simulation = slow_simulation()
    simulation.submit("bus-302-1", 80)
    with pytest.raises(ValueError, match="Drain"):
        simulation.configure("edge-west-1", capacity=10)
    with pytest.raises(ValueError, match="Drain"):
        simulation.configure("edge-west-1", active=False)
    assert len(simulation.workers["edge-west-1"].queue) == 40


def test_balancer_accounts_for_processing_speed_within_region():
    simulation = slow_simulation()
    simulation.configure("edge-west-1", processing_ms=5)
    simulation.configure("edge-west-2", processing_ms=50)
    simulation.submit("bus-900-1", 30)
    assert len(simulation.workers["edge-west-1"].queue) > len(simulation.workers["edge-west-2"].queue)
    assert all(not w.queue for w in simulation.workers.values() if w.region_id == "east")


@pytest.mark.parametrize("changes", [dict(capacity=51), dict(capacity=0), dict(capacity=True),
                                    dict(processing_ms=float("nan")), dict(processing_ms=0)])
def test_simulated_characteristics_reject_invalid_values(changes):
    simulation = slow_simulation()
    with pytest.raises(ValueError):
        simulation.configure("edge-west-1", **changes)



def test_only_accepted_readings_supply_bus_speed_and_reading_age(setup):
    setup.monitor.mark_valid_data("bus-302-1", 0.0)
    setup.now[0] = 2
    bus = next(bus for bus in setup.monitor.sensor_reports() if bus["bus_id"] == "bus-302-1")
    assert bus["last_speed_kmh"] == 0.0
    assert bus["valid_telemetry_age_seconds"] == 2
    # A new heartbeat says the sensor is alive; it cannot refresh the reading.
    receive(setup, bus_heartbeat())
    setup.now[0] = 3
    assert next(bus for bus in setup.monitor.sensor_reports() if bus["bus_id"] == "bus-302-1")["valid_telemetry_age_seconds"] == 3



@pytest.fixture
def cluster():
    from services.edge.main import EdgeClusterSimulation
    return EdgeClusterSimulation()


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
    assert [job[3]["sequence_number"] for job in target.queue if job[1] == bus] == [2, 3, 4, 5, 6]
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
        assert cluster.central_states[bus]["edge_metadata"]["edge_id"] == "edge-west-dynamic"


def test_09_cross_region_takeover_preserves_home_route_then_synchronized_recovery(cluster):
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
    assert not cluster.submit_telemetry(cluster.message_templates["bus-302-1"])["accepted"]
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
    assert all(state["sequence_number"] == 3 for state in cluster.central_states.values())
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
    assert max(len(cluster._assigned(w.edge_id)) for w in workers) - min(len(cluster._assigned(w.edge_id)) for w in workers) <= 1
    assert len(cluster.journal) == 45
    assert not cluster.workers["edge-west-3"].active


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


def test_failure_preserves_synthetic_jobs_too(cluster):
    cluster.submit("bus-302-1", 20)
    cluster.fail("edge-west-1")
    cluster.advance(1)
    assert sum(w.completed for w in cluster.workers.values()) == 20
    assert not cluster.orphaned_requests



def test_autoscaling_cannot_resurrect_a_failed_third_instance(cluster):
    cluster.fail("edge-west-3")
    for edge_id in ("edge-west-1", "edge-west-2"):
        cluster.configure(edge_id, processing_ms=60000)
    cluster.generate("bus-302-1", 40)
    cluster.generate("bus-302-2", 40)
    cluster.advance(6)
    assert cluster.workers["edge-west-3"].state == "failed"
    assert not cluster.workers["edge-west-3"].active

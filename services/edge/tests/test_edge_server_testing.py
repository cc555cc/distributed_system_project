"""Edge object behaviour: heartbeat health, measurements and queue capacity."""

import json
from queue import Queue
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from services.edge.communication.data_to_edge import TelemetrySubscriber
from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.edge_server import EdgeServerInstance
from services.edge.edge_server_health import EdgeMonitor
from shared.schema.health import SENSORS, BusHeartbeat, HealthPolicy, load_band
from shared.schema.network import DEFAULT_NETWORK_PATH


@pytest.fixture
def setup():
    now = [0.0]
    policy = HealthPolicy(
        interval_seconds=1,
        sensor_timeout_seconds=5,
        edge_timeout_seconds=8,
        recovery_seconds=2,
        sustained_seconds=3,
    )
    network = NetworkConfig.load(DEFAULT_NETWORK_PATH)
    config = EdgeConfig("DURHAM", "west", "edge-west-1")
    monitor = EdgeMonitor(config, network.registry, policy, clock=lambda: now[0])
    return SimpleNamespace(
        now=now, policy=policy, network=network, config=config, monitor=monitor
    )


def bus_heartbeat(sequence=1, session="first", sensors=None, bus_id="bus-302-1"):
    return BusHeartbeat(
        city_id="DURHAM",
        region_id="west",
        bus_id=bus_id,
        session_id=session,
        sequence_number=sequence,
        timestamp="2026-01-01T00:00:00Z",
        sensors={sensor: "healthy" for sensor in SENSORS}
        if sensors is None
        else sensors,
    )


def receive(setup, heartbeat):
    return setup.monitor.receive(
        f"transit/heartbeats/bus/west/{heartbeat.bus_id}",
        heartbeat.model_dump_json().encode(),
    )


def sensor(setup, name="gps"):
    return next(
        bus for bus in setup.monitor.sensor_reports() if bus["bus_id"] == "bus-302-1"
    )["sensors"][name]


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
    assert not setup.monitor.receive(
        "wrong/topic", bus_heartbeat().model_dump_json().encode()
    )
    assert not setup.monitor.receive(
        "transit/heartbeats/bus/west/bus-302-1", b"broken json"
    )


@pytest.mark.parametrize(
    "percent,expected",
    [
        (0, "normal"),
        (50, "notice"),
        (70, "prepare"),
        (75, "ready"),
        (79, "near_capacity"),
        (80, "overloaded"),
    ],
)
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
    subscriber = TelemetrySubscriber(
        "west",
        "edge-west-1",
        registry=setup.network.registry,
        city_id="DURHAM",
        monitor=setup.monitor,
    )
    message = SimpleNamespace(
        topic="transit/heartbeats/bus/west/bus-302-1",
        payload=bus_heartbeat().model_dump_json().encode(),
    )
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
    assert {bus.bus_id for bus in report.buses} == {
        "bus-302-1",
        "bus-302-4",
        "bus-900-1",
        "bus-900-4",
    }
    client = Mock()
    client.is_connected.return_value = True
    client.publish.return_value.rc = 0
    instance.sender._client = client
    instance.sender.publish_heartbeat(report)
    assert (
        client.publish.call_args.args[0] == "transit/heartbeats/edge/west/edge-west-1"
    )
    assert json.loads(client.publish.call_args.args[1])["workload"]["queue_size"] == 4


@pytest.mark.parametrize(
    "values",
    [
        {"interval_seconds": 0},
        {"sensor_timeout_seconds": 1},
        {"overload_percent": 101},
        {"recovery_seconds": -1},
    ],
)
def test_bad_thresholds_are_rejected(values):
    with pytest.raises(ValueError):
        HealthPolicy(**values)


def test_heartbeat_subscription_is_registered(setup):
    subscriber = TelemetrySubscriber(
        "west",
        "edge-west-1",
        registry=setup.network.registry,
        city_id="DURHAM",
        monitor=setup.monitor,
    )
    client = Mock()
    client.subscribe.return_value = (0, 1)
    subscriber.on_connect(client, None, None, SimpleNamespace(is_failure=False), None)
    assert client.subscribe.call_count == 3  # assignments, telemetry, sensor heartbeats
    assert client.subscribe.call_args_list[0].args[0] == "transit/assignments/west"
    assert client.subscribe.call_args.args[0] == "transit/heartbeats/bus/west/+"


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
    assert all(
        not w.queue for w in simulation.workers.values() if w.region_id == "east"
    )


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
    assert len(simulation.workers["edge-west-1"].queue) > len(
        simulation.workers["edge-west-2"].queue
    )
    assert all(
        not w.queue for w in simulation.workers.values() if w.region_id == "east"
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"capacity": 51},
        {"capacity": 0},
        {"capacity": True},
        {"processing_ms": float("nan")},
        {"processing_ms": 0},
    ],
)
def test_simulated_characteristics_reject_invalid_values(changes):
    simulation = slow_simulation()
    with pytest.raises(ValueError):
        simulation.configure("edge-west-1", **changes)


def test_only_accepted_readings_supply_bus_speed_and_reading_age(setup):
    setup.monitor.mark_valid_data("bus-302-1", 0.0)
    setup.now[0] = 2
    bus = next(
        bus for bus in setup.monitor.sensor_reports() if bus["bus_id"] == "bus-302-1"
    )
    assert bus["last_speed_kmh"] == 0.0
    assert bus["valid_telemetry_age_seconds"] == 2
    # A new heartbeat says the sensor is alive; it cannot refresh the reading.
    receive(setup, bus_heartbeat())
    setup.now[0] = 3
    assert (
        next(
            bus
            for bus in setup.monitor.sensor_reports()
            if bus["bus_id"] == "bus-302-1"
        )["valid_telemetry_age_seconds"]
        == 3
    )

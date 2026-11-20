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
from shared.schema.health import BusHeartbeat, HealthPolicy, SENSORS, load_band
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network


def make_setup():
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


def slow_simulation():
    from services.edge.main import EdgeLoadSimulation
    simulation = EdgeLoadSimulation()
    for edge_id in ("edge-west-1", "edge-west-2"):
        simulation.configure(edge_id, processing_ms=60000)
    return simulation


def test_sensor_health_timeout_and_recovery():
    # Never seen sources time out without any new message.
    setup = make_setup()
    assert sensor(setup)["status"] == "unknown"
    setup.now[0] = 5
    assert sensor(setup)["status"] == "unavailable"

    # Healthy heartbeat timeout and recovery.
    setup = make_setup()
    assert receive(setup, bus_heartbeat())
    assert sensor(setup)["status"] == "healthy"
    setup.now[0] = 5
    assert sensor(setup)["status"] == "unavailable"
    assert receive(setup, bus_heartbeat(2))
    assert sensor(setup)["status"] == "recovering"
    setup.now[0] = 7
    assert receive(setup, bus_heartbeat(3))
    assert sensor(setup)["status"] == "healthy"

    # Duplicate and old heartbeats do not refresh liveness.
    setup = make_setup()
    receive(setup, bus_heartbeat(2))
    setup.now[0] = 4
    assert not receive(setup, bus_heartbeat(2))
    assert not receive(setup, bus_heartbeat(1))
    setup.now[0] = 5
    assert sensor(setup)["status"] == "unavailable"

    # Old session cannot resurrect after restart.
    setup = make_setup()
    receive(setup, bus_heartbeat())
    setup.now[0] = 1
    receive(setup, bus_heartbeat(1, "restart"))
    assert sensor(setup)["status"] == "recovering"
    assert not receive(setup, bus_heartbeat(100, "first"))

    # Sensor channels timeout independently.
    setup = make_setup()
    receive(setup, bus_heartbeat())
    setup.now[0] = 4
    receive(setup, bus_heartbeat(2, sensors={"speed": "healthy"}))
    setup.now[0] = 5
    assert sensor(setup, "gps")["status"] == "unavailable"
    assert sensor(setup, "speed")["status"] == "healthy"

    # Faulty sensor can be alive without a valid reading.
    setup = make_setup()
    receive(setup, bus_heartbeat(sensors={"gps": "faulty"}))
    assert sensor(setup)["status"] == "healthy"
    assert sensor(setup)["reported_state"] == "faulty"
    assert setup.monitor.sensor_reports()[0]["valid_telemetry_age_seconds"] is None

    # Unknown or other workers bus cannot refresh health.
    setup = make_setup()
    assert not receive(setup, bus_heartbeat(bus_id="unknown"))
    assert not receive(setup, bus_heartbeat(bus_id="bus-302-2"))
    assert not setup.monitor.receive("wrong/topic", bus_heartbeat().model_dump_json().encode())
    assert not setup.monitor.receive("transit/heartbeats/bus/west/bus-302-1", b"broken json")

    # Only accepted readings supply bus speed and reading age.
    setup = make_setup()
    setup.monitor.mark_valid_data("bus-302-1", 0.0)
    setup.now[0] = 2
    bus = next(bus for bus in setup.monitor.sensor_reports() if bus["bus_id"] == "bus-302-1")
    assert bus["last_speed_kmh"] == 0.0
    assert bus["valid_telemetry_age_seconds"] == 2
    # A new heartbeat says the sensor is alive; it cannot refresh the reading.
    receive(setup, bus_heartbeat())
    setup.now[0] = 3
    assert next(bus for bus in setup.monitor.sensor_reports() if bus["bus_id"] == "bus-302-1")["valid_telemetry_age_seconds"] == 3


def test_queue_capacity_metrics_and_distribution():
    # Load bands.
    for percent,expected in [(0, 'normal'), (50, 'notice'), (70, 'prepare'), (75, 'ready'), (79, 'near_capacity'), (80, 'overloaded')]:
        assert load_band(percent) == expected

    # Queue metrics are real queue counts.
    setup = make_setup()
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

    # Live queue defaults to fifty and rejects larger capacity.
    with pytest.MonkeyPatch.context() as monkeypatch:
        setup = make_setup()
        from services.edge.communication.data_to_edge import create_subscriber
        monkeypatch.delenv("EDGE_QUEUE_SIZE", raising=False)
        assert create_subscriber(setup.config, setup.network).messages.maxsize == 50
        with pytest.raises(ValueError, match="between 1 and 50"):
            TelemetrySubscriber("west", "edge-west-1", queue_size=51)

    # Full region rejects excess requests without spilling to east.
    simulation = slow_simulation()
    result = simulation.submit("bus-302-1", 105)
    assert (result["accepted"], result["rejected"]) == (100, 5)
    assert all(len(w.queue) <= w.capacity == 50 for w in simulation.workers.values())
    assert all(not w.queue for w in simulation.workers.values() if w.region_id == "east")

    # Capacity changes cannot discard queued requests.
    simulation = slow_simulation()
    simulation.submit("bus-302-1", 80)
    with pytest.raises(ValueError, match="Drain"):
        simulation.configure("edge-west-1", capacity=10)
    with pytest.raises(ValueError, match="Drain"):
        simulation.configure("edge-west-1", active=False)
    assert len(simulation.workers["edge-west-1"].queue) == 40

    # Balancer accounts for processing speed within region.
    simulation = slow_simulation()
    simulation.configure("edge-west-1", processing_ms=5)
    simulation.configure("edge-west-2", processing_ms=50)
    simulation.submit("bus-900-1", 30)
    assert len(simulation.workers["edge-west-1"].queue) > len(simulation.workers["edge-west-2"].queue)
    assert all(not w.queue for w in simulation.workers.values() if w.region_id == "east")


def test_heartbeat_transport_and_instance_report():
    # Subscriber handles heartbeat outside the telemetry queue.
    setup = make_setup()
    subscriber = TelemetrySubscriber("west", "edge-west-1", registry=setup.network.registry,
                                     city_id="DURHAM", monitor=setup.monitor)
    message = SimpleNamespace(topic="transit/heartbeats/bus/west/bus-302-1",
                              payload=bus_heartbeat().model_dump_json().encode())
    subscriber.on_message(None, None, message)
    assert subscriber.messages.empty()
    assert sensor(setup)["status"] == "healthy"

    # Worker heartbeat and outgoing health connection.
    with pytest.MonkeyPatch.context() as monkeypatch:
        setup = make_setup()
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

    # Heartbeat subscription is registered.
    setup = make_setup()
    subscriber = TelemetrySubscriber("west", "edge-west-1", registry=setup.network.registry,
                                     city_id="DURHAM", monitor=setup.monitor)
    client = Mock()
    client.subscribe.return_value = (0, 1)
    subscriber.on_connect(client, None, None, SimpleNamespace(is_failure=False), None)
    assert client.subscribe.call_count == 2
    assert client.subscribe.call_args.args[0] == "transit/heartbeats/bus/west/+"


def test_invalid_health_and_instance_settings_are_rejected():
    # Bad thresholds are rejected.
    for values in [dict(interval_seconds=0), dict(sensor_timeout_seconds=1), dict(overload_percent=101), dict(recovery_seconds=-1)]:
        with pytest.raises(ValueError):
            HealthPolicy(**values)

    # Simulated characteristics reject invalid values.
    for changes in [dict(capacity=51), dict(capacity=0), dict(capacity=True), dict(processing_ms=float('nan')), dict(processing_ms=0)]:
        simulation = slow_simulation()
        with pytest.raises(ValueError):
            simulation.configure("edge-west-1", **changes)

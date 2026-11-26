"""Central detects failed edges without waiting for the failed worker to report."""

import json

import pytest
from services.central.tests.client import OperatorClient as TestClient

from services.central.health import CentralHealthMonitor
from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.edge_server import EdgeServerInstance
from shared.schema.health import HealthPolicy
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network


@pytest.fixture
def monitor():
    now = [0.0]
    policy = HealthPolicy(
        interval_seconds=1,
        sensor_timeout_seconds=5,
        edge_timeout_seconds=10,
        recovery_seconds=2,
        sustained_seconds=3,
    )
    return (
        CentralHealthMonitor(load_network(), policy, clock=lambda: now[0]),
        now,
        policy,
    )


def report(
    edge_id="edge-west-1", region="west", percent=0, sequence=1, session="first"
):
    instance = EdgeServerInstance(
        EdgeConfig("DURHAM", region, edge_id),
        NetworkConfig.load(DEFAULT_NETWORK_PATH),
        HealthPolicy(),
    )
    value = instance.heartbeat().model_dump(mode="json")
    value["session_id"] = session
    value["sequence_number"] = sequence
    value["workload"].update(
        queue_size=percent, queue_capacity=100, queue_utilization_percent=float(percent)
    )
    return value


def receive(monitor, value):
    return monitor.receive(
        f"transit/heartbeats/edge/{value['region_id']}/{value['edge_id']}",
        json.dumps(value).encode(),
    )


def edge(monitor, edge_id="edge-west-1"):
    return next(
        item for item in monitor.snapshot()["edges"] if item["edge_id"] == edge_id
    )


def test_timeout_is_detected_without_new_packets(monitor):
    store, now, _ = monitor
    assert receive(store, report())
    assert edge(store)["status"] == "healthy"
    now[0] = 10
    assert edge(store)["status"] == "unavailable"


def test_missing_workers_become_unavailable(monitor):
    store, now, _ = monitor
    assert all(item["status"] == "unknown" for item in store.snapshot()["edges"])
    now[0] = 10
    assert all(item["status"] == "unavailable" for item in store.snapshot()["edges"])


def test_duplicates_do_not_keep_a_failed_edge_alive(monitor):
    store, now, _ = monitor
    receive(store, report(sequence=2))
    now[0] = 9
    assert not receive(store, report(sequence=2))
    assert not receive(store, report(sequence=1))
    now[0] = 10
    assert edge(store)["status"] == "unavailable"


def test_returning_edge_is_recovering_before_healthy(monitor):
    store, now, _ = monitor
    receive(store, report())
    now[0] = 10
    receive(store, report(sequence=2))
    assert edge(store)["status"] == "recovering"
    now[0] = 12
    receive(store, report(sequence=3))
    assert edge(store)["status"] == "healthy"


def test_threshold_uses_least_loaded_healthy_same_region_peer(monitor):
    store, now, _ = monitor
    receive(store, report(percent=80))
    receive(store, report("edge-west-2", percent=30))
    receive(store, report("edge-east-1", region="east", percent=0))
    assert not store.snapshot()["recommendations"]
    now[0] = 3
    receive(store, report(percent=80, sequence=2))
    choice = store.snapshot()["recommendations"][0]
    assert choice["to_edge_id"] == "edge-west-2"
    assert choice["algorithm"] == "least_queue_utilization"
    assert choice["migration_performed"] is False


def test_both_overloaded_recommend_extra_capacity_without_starting_it(monitor):
    store, now, _ = monitor
    receive(store, report(percent=90))
    receive(store, report("edge-west-2", percent=80))
    now[0] = 3
    choices = store.snapshot()["recommendations"]
    assert len(choices) == 2
    assert all(
        item["action"] == "additional_capacity_needed" and item["to_edge_id"] is None
        for item in choices
    )


def test_bad_identity_or_metrics_cannot_refresh_health(monitor):
    store, _, _ = monitor
    value = report()
    value["city_id"] = "wrong"
    assert not receive(store, value)
    value = report()
    value["workload"]["queue_size"] = 1000
    assert not receive(store, value)


def test_health_callback_and_api_use_the_same_central_monitor(monitor, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/health_test")
    from types import SimpleNamespace

    from services.central import health
    from services.central.main import app
    from services.messaging import mqtt_client

    store, _, _ = monitor
    monkeypatch.setattr(health, "monitor", store)
    value = report()
    mqtt_client.on_message(
        None,
        None,
        SimpleNamespace(
            topic="transit/heartbeats/edge/west/edge-west-1",
            payload=json.dumps(value).encode(),
        ),
    )
    response = TestClient(app).get("/health/edges")
    assert response.status_code == 200
    west = next(
        item for item in response.json()["edges"] if item["edge_id"] == "edge-west-1"
    )
    assert west["status"] == "healthy"
    assert west["last_report"]["workload"]["queue_capacity"] == 100


def test_sensor_publisher_to_edge_to_central_health_api(monitor, monkeypatch):
    from types import SimpleNamespace

    from services.central import health
    from services.central.main import app
    from services.data_simulation.run_buses import SimulatorHeartbeats
    from services.edge.edge_server_health import EdgeMonitor
    from services.messaging import mqtt_client
    from shared.schema.health import SENSORS

    store, now, policy = monitor
    monkeypatch.setattr(health, "monitor", store)
    config = EdgeConfig("DURHAM", "west", "edge-west-1")
    network = NetworkConfig.load(DEFAULT_NETWORK_PATH)
    instance = EdgeServerInstance(config, network, policy)
    instance.monitor = EdgeMonitor(
        config, network.registry, policy, clock=lambda: now[0]
    )
    instance.subscriber.monitor = instance.monitor
    bus = next(bus for bus in load_network()["buses"] if bus["bus_id"] == "bus-302-1")
    source = SimulatorHeartbeats(bus)
    source.update({sensor: "healthy" for sensor in SENSORS})
    instance.subscriber.on_message(
        None,
        None,
        SimpleNamespace(
            topic="transit/heartbeats/bus/west/bus-302-1",
            payload=source.snapshot().model_dump_json().encode(),
        ),
    )
    assert instance.subscriber.messages.empty()
    outgoing = instance.heartbeat()
    mqtt_client.on_message(
        None,
        None,
        SimpleNamespace(
            topic="transit/heartbeats/edge/west/edge-west-1",
            payload=outgoing.model_dump_json().encode(),
        ),
    )
    result = TestClient(app).get("/health/edges").json()
    west = next(item for item in result["edges"] if item["edge_id"] == "edge-west-1")
    bus_health = next(
        bus for bus in west["last_report"]["buses"] if bus["bus_id"] == "bus-302-1"
    )
    assert all(
        sensor["status"] == "healthy" for sensor in bus_health["sensors"].values()
    )
    assert bus_health["valid_telemetry_age_seconds"] is None
    now[0] = 10
    result = TestClient(app).get("/health/edges").json()
    west = next(item for item in result["edges"] if item["edge_id"] == "edge-west-1")
    assert west["status"] == "unavailable"
    assert all(
        sensor["status"] == "unknown"
        for sensor in west["last_report"]["buses"][0]["sensors"].values()
    )


def test_restart_resets_sustained_overload(monitor):
    store, now, _ = monitor
    receive(store, report(percent=90))
    now[0] = 2
    receive(store, report(percent=90, session="restart"))
    now[0] = 4
    receive(store, report(percent=90, session="restart", sequence=2))
    assert edge(store)["status"] == "healthy"
    assert not store.snapshot()["recommendations"]


@pytest.mark.parametrize(
    "bad_report", ["missing_bus", "missing_sensor", "negative_age"]
)
def test_incomplete_or_invalid_sensor_summary_is_rejected(monitor, bad_report):
    store, _, _ = monitor
    value = report()
    if bad_report == "missing_bus":
        value["buses"].pop()
    elif bad_report == "missing_sensor":
        value["buses"][0]["sensors"].pop("gps")
    else:
        value["buses"][0]["sensors"]["gps"]["age_seconds"] = -1.0
    assert not receive(store, value)


def test_simulation_api_controls_virtual_capacity_and_third_instance(monkeypatch):
    from services.central import health
    from services.central.main import app
    from services.edge.main import EdgeLoadSimulation

    monkeypatch.setattr(health, "simulation", EdgeLoadSimulation())
    client = TestClient(app)
    for edge_id in ("edge-west-1", "edge-west-2"):
        response = client.patch(
            f"/health/simulation/instances/{edge_id}", json={"processing_ms": 60000}
        )
        assert response.status_code == 200
    response = client.post(
        "/health/simulation/requests", json={"bus_id": "bus-302-1", "count": 80}
    )
    assert response.json()["accepted"] == 80
    response = client.post("/health/simulation/advance", json={"seconds": 6})
    third = next(w for w in response.json()["workers"] if w["edge_id"] == "edge-west-3")
    assert third["active"] and third["queue_size"] > 0
    assert response.json()["live_workers_changed"] is False
    assert (
        client.patch(
            "/health/simulation/instances/edge-west-1", json={"capacity": 51}
        ).status_code
        == 422
    )
    assert (
        client.patch(
            "/health/simulation/instances/unknown", json={"active": True}
        ).status_code
        == 422
    )
    assert (
        client.post(
            "/health/simulation/requests", json={"bus_id": "unknown", "count": 1}
        ).status_code
        == 422
    )
    assert (
        client.post(
            "/health/simulation/requests", json={"bus_id": "bus-302-1", "count": True}
        ).status_code
        == 422
    )
    assert (
        client.post("/health/simulation/advance", json={"seconds": -1}).status_code
        == 422
    )
    assert (
        client.patch("/health/simulation/policy", json={"auto_scale": False}).json()[
            "auto_scale"
        ]
        is False
    )
    reset = client.post("/health/simulation/reset").json()
    assert reset["accepted"] == 0 and reset["virtual_seconds"] == 0
    assert all(w["queue_size"] == 0 for w in reset["workers"])
    assert (
        sum(w["active"] for w in client.get("/health/simulation").json()["workers"])
        == 4
    )


def healthy_bus_report():
    value = report()
    for bus in value["buses"]:
        bus["valid_telemetry_age_seconds"] = 0.0
        bus["last_speed_kmh"] = 0.0
        for sensor in bus["sensors"].values():
            sensor.update(status="healthy", reported_state="healthy", age_seconds=0.0)
    return value


@pytest.mark.parametrize(
    "pattern,expected",
    [
        ("stationary", "stationary"),
        ("moving", "healthy"),
        ("faulty", "degraded"),
        ("one_missing", "degraded"),
        ("all_missing", "telemetry_unavailable"),
        ("stale_data", "data_stale"),
        ("no_data", "data_stale"),
        ("unknown_sensor", "unknown"),
        ("recovering_sensor", "recovering"),
    ],
)
def test_central_classifies_bus_patterns_from_report_evidence(
    monitor, pattern, expected
):
    store, _, _ = monitor
    value = healthy_bus_report()
    bus = value["buses"][0]
    if pattern == "moving":
        bus["last_speed_kmh"] = 25.0
    elif pattern == "faulty":
        bus["sensors"]["gps"]["reported_state"] = "faulty"
    elif pattern == "one_missing":
        bus["sensors"]["gps"].update(status="unavailable", age_seconds=5.0)
    elif pattern == "all_missing":
        for sensor in bus["sensors"].values():
            sensor.update(status="unavailable", age_seconds=5.0)
    elif pattern == "stale_data":
        bus["valid_telemetry_age_seconds"] = 5.0
    elif pattern == "no_data":
        bus["valid_telemetry_age_seconds"] = None
    elif pattern == "unknown_sensor":
        bus["sensors"]["gps"].update(
            status="unknown", reported_state="unknown", age_seconds=None
        )
    elif pattern == "recovering_sensor":
        bus["sensors"]["gps"]["status"] = "recovering"
    assert receive(store, value)
    classified = next(
        item for item in store.snapshot()["buses"] if item["bus_id"] == bus["bus_id"]
    )
    assert classified["health"]["status"] == expected
    assert classified["health"]["reasons"]
    if pattern == "faulty":
        assert classified["sensors"]["gps"]["status"] == "healthy"
        assert classified["sensors"]["gps"]["health_status"] == "faulty"


def test_missing_edge_overrides_old_bus_faults_with_unknown(monitor):
    store, now, _ = monitor
    value = healthy_bus_report()
    value["buses"][0]["sensors"]["gps"]["reported_state"] = "faulty"
    receive(store, value)
    now[0] = 10
    bus = next(
        item
        for item in store.snapshot()["buses"]
        if item["bus_id"] == value["buses"][0]["bus_id"]
    )
    assert bus["health"] == {"status": "unknown", "reasons": ["edge_unavailable"]}
    assert all(
        sensor["health_status"] == "unknown" for sensor in bus["sensors"].values()
    )


def test_unseen_buses_are_listed_as_unknown_without_inventing_breakdowns(monitor):
    store, _, _ = monitor
    buses = store.snapshot()["buses"]
    assert len(buses) == 20
    assert all(bus["health"]["status"] == "unknown" for bus in buses)
    assert all(bus["health"]["reasons"] == ["edge_unknown"] for bus in buses)


def test_stationary_expires_to_missing_telemetry_then_unknown_edge(monitor):
    store, now, _ = monitor
    value = healthy_bus_report()
    receive(store, value)
    bus_id = value["buses"][0]["bus_id"]
    now[0] = 5
    bus = next(bus for bus in store.snapshot()["buses"] if bus["bus_id"] == bus_id)
    assert bus["health"]["status"] == "telemetry_unavailable"
    now[0] = 10
    assert (
        next(bus for bus in store.snapshot()["buses"] if bus["bus_id"] == bus_id)[
            "health"
        ]["status"]
        == "unknown"
    )


def test_edge_overload_is_separate_from_heartbeat_liveness(monitor):
    store, now, _ = monitor
    receive(store, report(percent=80))
    assert edge(store)["status"] == "healthy"
    assert edge(store)["workload_status"] == "high_load"
    now[0] = 3
    assert edge(store)["workload_status"] == "overloaded"
    now[0] = 10
    assert edge(store)["workload_status"] == "unknown"


def test_zero_speed_is_not_stationary_during_edge_recovery(monitor):
    store, now, _ = monitor
    value = healthy_bus_report()
    receive(store, value)
    now[0] = 10
    value["sequence_number"] = 2
    receive(store, value)
    bus = next(
        bus
        for bus in store.snapshot()["buses"]
        if bus["bus_id"] == value["buses"][0]["bus_id"]
    )
    assert bus["health"]["status"] == "recovering"


def test_cluster_ui_api_runs_telemetry_takeover_registration_and_recovery(monkeypatch):
    from services.central import health
    from services.central.main import app
    from services.edge.main import EdgeClusterSimulation

    monkeypatch.setattr(health, "simulation", EdgeClusterSimulation())
    client = TestClient(app)
    assert (
        client.post(
            "/health/simulation/telemetry/generate",
            json={"bus_id": "bus-302-1", "count": 2},
        ).json()["accepted"]
        == 2
    )
    assert (
        client.post("/health/simulation/advance", json={"seconds": 1}).status_code
        == 200
    )
    failed = client.post("/health/simulation/instances/edge-west-1/fail").json()
    assert failed["ownership"]["bus-302-1"]["edge_id"] == "edge-west-2"
    client.post(
        "/health/simulation/telemetry/generate",
        json={"bus_id": "bus-302-1", "count": 1},
    )
    result = client.post("/health/simulation/advance", json={"seconds": 1}).json()
    assert result["central_states"]["bus-302-1"]["sequence_number"] == 3
    assert (
        client.post(
            "/health/simulation/instances",
            json={"edge_id": "edge-west-4", "region_id": "west"},
        ).status_code
        == 200
    )
    transferred = client.post(
        "/health/simulation/transfer",
        json={"bus_id": "bus-302-1", "target_id": "edge-west-4"},
    ).json()
    assert transferred["ownership"]["bus-302-1"]["edge_id"] == "edge-west-4"
    assert (
        client.post("/health/simulation/instances/edge-west-4/scale-down").status_code
        == 200
    )
    assert (
        client.post("/health/simulation/instances/edge-west-1/recover").status_code
        == 200
    )
    client.post("/health/simulation/advance", json={"seconds": 2})
    assert health.simulation.workers["edge-west-1"].state == "healthy"
    assert client.post("/health/simulation/instances/unknown/fail").status_code == 422
    assert (
        client.post(
            "/health/simulation/transfer",
            json={"bus_id": "unknown", "target_id": "unknown"},
        ).status_code
        == 422
    )
    assert (
        client.post("/health/simulation/reset", json={"fleet_size": 50}).status_code
        == 200
    )
    assert len(health.simulation.bus_regions) == 50
    assert (
        client.post("/health/simulation/reset", json={"fleet_size": 51}).status_code
        == 422
    )


def test_recovery_clock_runs_without_browser_steps_and_supports_pause_speed(monkeypatch):
    from services.central import health
    from services.central.main import app
    from services.edge.main import EdgeClusterSimulation

    now = [0.0]
    monkeypatch.setattr(health, "simulation", EdgeClusterSimulation())
    clock = health.SimulationClock(clock=lambda: now[0])
    monkeypatch.setattr(health, "simulation_clock", clock)
    client = TestClient(app)
    client.post("/health/simulation/instances/edge-west-1/fail")
    client.post("/health/simulation/instances/edge-west-1/recover")
    # Polling from multiple pages must not advance time or accelerate recovery.
    assert client.get("/health/simulation").json()["virtual_seconds"] == 0
    assert client.get("/health/simulation").json()["virtual_seconds"] == 0
    client.patch("/health/simulation/clock", json={"running": False, "speed": 2.0})
    now[0] = 20
    clock.tick()
    assert health.simulation.workers["edge-west-1"].state == "recovering"
    client.patch("/health/simulation/clock", json={"running": True, "speed": 2.0})
    now[0] = 21
    clock.tick()  # central's background task calls this even with no browser open
    result = client.get("/health/simulation").json()
    assert result["virtual_seconds"] == 2
    assert health.simulation.workers["edge-west-1"].state == "healthy"
    assert result["clock"] == {"running": True, "speed": 2.0, "movement_multiplier": 1.0}
    assert client.patch("/health/simulation/clock", json={"running": True, "speed": 0.0}).status_code == 422
    assert client.post("/health/simulation/reset").json()["virtual_seconds"] == 0
    now[0] = 21.5
    clock.tick()
    assert health.simulation.time == 1


@pytest.fixture
def demo_client(monkeypatch):
    # These endpoints are in memory; importing the app still requires a DB URL.
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/controlled-demo")
    from services.central import health
    from services.central.main import app
    from services.edge.main import EdgeClusterSimulation

    monkeypatch.setattr(health, "simulation", EdgeClusterSimulation())
    monkeypatch.setattr(health, "simulation_clock", health.SimulationClock(clock=lambda: 0.0))
    client = TestClient(app)
    assert client.patch("/health/simulation/demo", json={"running": True}).status_code == 200
    return client


def demo_step(client, seconds=2):
    response = client.post("/health/simulation/advance", json={"seconds": float(seconds)})
    assert response.status_code == 200
    return response.json()


def demo_condition(client, condition):
    response = client.patch("/health/simulation/demo", json={"bus_id": "bus-302-1", "condition": condition})
    assert response.status_code == 200


def test_demo_movement_stops_traffic_demand_and_runtime_controls(demo_client):
    client = demo_client
    state = demo_step(client)
    assert len(state["central_states"]) == 20
    original = state["demo_buses"]["bus-302-1"]["distance_m"]
    demo_condition(client, "stopped")
    state = demo_step(client, 3)
    assert state["demo_buses"]["bus-302-1"]["distance_m"] == original
    assert state["central_states"]["bus-302-1"]["speed"] == 0
    assert state["demo_buses"]["bus-302-1"]["status"] == "healthy"
    demo_condition(client, "traffic")
    state = demo_step(client, 3)
    assert state["central_states"]["bus-302-1"]["speed"] == 9
    assert state["demo_buses"]["bus-302-1"]["distance_m"] - original == pytest.approx(7.5)
    demo_condition(client, "peak")
    state = demo_step(client)
    assert 43 <= state["central_states"]["bus-302-1"]["occupancy"] <= 50
    demo_condition(client, "out_of_service")
    state = demo_step(client)
    assert state["central_states"]["bus-302-1"]["occupancy"] == 0
    assert state["central_states"]["bus-302-1"]["bus_status"] == "out_of_service"
    demo_condition(client, "normal")
    from services.central import health
    from services.data_simulation.generate_data import distance_metres
    bus = health.simulation.demo.buses["bus-302-1"]
    a, b = bus["stops"][bus["segment"]:bus["segment"] + 2]
    bus["metres"] = max(0.0, distance_metres(a, b) - 20)
    phases = [demo_step(client, 1)["demo_buses"]["bus-302-1"]["phase"] for _ in range(7)]
    assert all(phase in phases for phase in ("arriving", "stopped", "boarding", "departing", "travelling"))
    state = client.get("/health/simulation").json()
    client.patch("/health/simulation/demo", json={"running": False})
    distance = state["demo_buses"]["bus-302-1"]["distance_m"]
    assert demo_step(client)["demo_buses"]["bus-302-1"]["distance_m"] == distance
    client.patch("/health/simulation/demo", json={"running": True})
    assert demo_step(client)["demo_buses"]["bus-302-1"]["distance_m"] > distance


def test_demo_sensor_fault_invalid_reading_silence_and_recovery(demo_client):
    client = demo_client
    state = demo_step(client)
    accepted = state["central_states"]["bus-302-1"]["sequence_number"]
    demo_condition(client, "sensor_fault")
    state = demo_step(client, 3)
    bus = state["demo_buses"]["bus-302-1"]
    assert bus["sensors"]["occupancy"] == "faulty" and bus["sensors"]["gps"] == "healthy"
    assert bus["status"] == "degraded"
    assert state["central_states"]["bus-302-1"]["sequence_number"] == accepted
    demo_condition(client, "invalid_reading")
    state = demo_step(client, 3)
    assert state["demo_buses"]["bus-302-1"]["rejected_readings"] > 0
    assert "unconfirmed" in state["demo_buses"]["bus-302-1"]["evidence"]
    assert state["central_states"]["bus-302-1"]["sequence_number"] == accepted
    for condition in ("sensors_stopped", "silence"):
        demo_condition(client, condition)
        state = demo_step(client, 7)
        assert state["demo_buses"]["bus-302-1"]["status"] == "unavailable"
    assert "cause unknown" in state["demo_buses"]["bus-302-1"]["evidence"]
    demo_condition(client, "normal")
    assert client.get("/health/simulation").json()["demo_buses"]["bus-302-1"]["status"] != "healthy"
    state = demo_step(client, 3)
    assert state["demo_buses"]["bus-302-1"]["status"] == "healthy"
    assert state["central_states"]["bus-302-1"]["sequence_number"] > accepted


def test_demo_transport_process_outage_retries_takeover_and_recovery(demo_client):
    client = demo_client
    demo_step(client)
    demo_condition(client, "connection_loss")
    state = demo_step(client, 7)
    bus = state["demo_buses"]["bus-302-1"]
    assert bus["status"] == "unavailable" and bus["delivery_pending"]
    assert bus["local"]["sequence_number"] > state["central_states"]["bus-302-1"]["sequence_number"]
    attempts = [e["time"] for e in state["events"] if e["action"] == "delivery_attempt" and e.get("bus_id") == "bus-302-1"]
    assert all(b - a >= 2 for a, b in zip(attempts, attempts[1:]))
    demo_condition(client, "process_failure")
    distance = bus["distance_m"]
    assert demo_step(client, 7)["demo_buses"]["bus-302-1"]["distance_m"] == distance
    demo_condition(client, "normal")
    demo_step(client, 3)
    client.patch("/health/simulation/demo", json={"central_available": False})
    state = demo_step(client, 3)
    assert state["central_pending"] == 20
    delivered = state["central_states"]["bus-302-1"]["sequence_number"]
    client.post("/health/simulation/instances/edge-west-1/fail")
    state = demo_step(client, 3)
    assert state["ownership"]["bus-302-1"]["edge_id"] == "edge-west-2"
    client.patch("/health/simulation/demo", json={"central_available": True})
    client.post("/health/simulation/instances/edge-west-1/recover")
    state = demo_step(client, 3)
    assert state["central_pending"] == 0
    assert state["central_states"]["bus-302-1"]["sequence_number"] > delivered
    assert next(w for w in state["workers"] if w["edge_id"] == "edge-west-1")["state"] == "healthy"
    client.patch("/health/simulation/demo", json={"central_available": False})
    demo_step(client, 3)
    demo_condition(client, "process_failure")
    state = demo_step(client, 35)
    delivered = state["central_states"]["bus-302-1"]["sequence_number"]
    client.patch("/health/simulation/demo", json={"central_available": True})
    state = demo_step(client, 3)
    assert state["central_states"]["bus-302-1"]["sequence_number"] == delivered
    assert any(e["action"] == "central_expired" for e in state["events"])
    # A delayed payload from the former owner must not bypass central's fence.
    from services.edge.main import EdgeClusterSimulation
    cluster = EdgeClusterSimulation()
    cluster.configure_demo(central_available=False)
    cluster.generate("bus-302-1", 1)
    cluster.advance(1)
    cluster.fail("edge-west-1")
    cluster.configure_demo(central_available=True)
    result = cluster.advance(3)
    assert "bus-302-1" not in result["central_states"]
    assert any(e["action"] == "central_fenced" for e in result["events"])


def test_demo_api_rejects_invalid_controls_and_preserves_other_region(demo_client):
    client = demo_client
    routes = client.get("/health/simulation/routes").json()
    assert {route["region_id"] for route in routes} == {"west", "east"}
    assert client.get("/health/simulation/routes/302/stops").json()[0]["direction_id"] == 0
    assert client.get("/health/simulation/routes/unknown/stops").status_code == 404
    for body in ({}, {"bus_id": "bus-302-1"}, {"bus_id": "unknown", "condition": "normal"},
                 {"bus_id": "bus-302-1", "condition": "guess_crash"}, {"running": "yes"}):
        assert client.patch("/health/simulation/demo", json=body).status_code == 422
    demo_step(client)
    demo_condition(client, "process_failure")
    state = demo_step(client, 7)
    assert state["demo_buses"]["bus-302-1"]["status"] == "unavailable"
    assert state["demo_buses"]["bus-407-1"]["status"] == "healthy"
    from shared.schema.bus_state import BusStateUpdate
    for payload in state["central_states"].values():
        BusStateUpdate.model_validate(payload, strict=True)


def test_fleet_presets_generate_exact_valid_stream_counts(demo_client):
    client = demo_client
    for size in (5, 10, 25, 50):
        assert client.post("/health/simulation/reset", json={"fleet_size": size}).status_code == 200
        client.patch("/health/simulation/demo", json={"running": True})
        state = demo_step(client)
        ids = {bus["bus_id"] for bus in state["registered_buses"]}
        assert len(ids) == size
        assert set(state["central_states"]) == ids
        assert set(state["demo_buses"]) == ids
        assert all(bus["route_id"] in {"302", "407", "409", "900", "901"} for bus in state["registered_buses"])
    for size in (0, 51, True):
        assert client.post("/health/simulation/reset", json={"fleet_size": size}).status_code == 422
        assert len(client.get("/health/simulation").json()["registered_buses"]) == 50


def register_demo_bus(client, name):
    return client.post("/health/simulation/buses", json={
        "schema_version": "bus-config-v1", "bus_id": name, "route_id": "302", "capacity": 60, "direction_id": 0})


def test_versioned_bus_registration_preserves_existing_streams_and_rejects_bad_config(demo_client):
    client = demo_client
    before = demo_step(client)
    assert register_demo_bus(client, "bus-extra-1").status_code == 201
    state = demo_step(client)
    assert len(state["registered_buses"]) == 21
    assert state["central_states"]["bus-extra-1"]["bus_status"] == "out_of_service"
    assert state["central_states"]["bus-407-1"]["sequence_number"] > before["central_states"]["bus-407-1"]["sequence_number"]
    assert register_demo_bus(client, "bus-extra-1").status_code == 422
    base = dict(schema_version="bus-config-v1", bus_id="bad-bus", route_id="302")
    for change in ({"schema_version": "v99"}, {"route_id": "unknown"}, {"capacity": 0},
                   {"bus_id": "bad/id"}, {"direction_id": True}, {"sensors": ["new-sensor"]}):
        assert client.post("/health/simulation/buses", json={**base, **change}).status_code == 422
    assert len(client.get("/health/simulation").json()["registered_buses"]) == 21


def test_scheduler_dispatch_and_changed_headway_appear_in_central_telemetry(demo_client):
    client = demo_client
    for name in ("reserve-1", "reserve-2"):
        assert register_demo_bus(client, name).status_code == 201
    demo_step(client)
    assert client.post("/health/simulation/schedule", json={"route_id": "302", "action": "set_headway", "headway_seconds": 5.0}).status_code == 200
    for name in ("reserve-1", "reserve-2"):
        assert client.post("/health/simulation/schedule", json={"route_id": "302", "action": "dispatch", "bus_id": name}).status_code == 200
    state = demo_step(client, 4)
    assert state["central_states"]["reserve-1"]["speed"] == 0
    state = demo_step(client, 1)
    assert state["central_states"]["reserve-1"]["speed"] > 0
    assert state["central_states"]["reserve-2"]["speed"] == 0
    # Changing the interval also changes an already queued departure.
    client.post("/health/simulation/schedule", json={"route_id": "302", "action": "set_headway", "headway_seconds": 2.0})
    state = demo_step(client, 2)
    assert state["central_states"]["reserve-2"]["speed"] > 0
    assert state["schedules"]["302"]["pending"] == []
    events = [e for e in state["events"] if e["action"] == "bus_dispatched"]
    assert events[-1]["time"] - events[-2]["time"] == 2


def test_scheduler_rejects_unavailable_wrong_route_and_duplicate_requests(demo_client):
    client = demo_client
    assert register_demo_bus(client, "reserve-1").status_code == 201
    request = {"route_id": "302", "action": "dispatch", "bus_id": "reserve-1"}
    for body in ({**request, "route_id": "407"}, {**request, "bus_id": "unknown"},
                 {**request, "bus_id": "bus-302-1"}, {**request, "headway_seconds": 2.0},
                 {"route_id": "302", "action": "set_headway", "headway_seconds": 0.0}):
        assert client.post("/health/simulation/schedule", json=body).status_code == 422
    assert client.post("/health/simulation/schedule", json=request).status_code == 200
    assert client.post("/health/simulation/schedule", json=request).status_code == 422
    assert client.get("/health/simulation").json()["schedules"]["302"]["pending"] == ["reserve-1"]


def test_health_routes_separate_bus_sensors_faults_and_recovery(demo_client):
    client = demo_client
    demo_step(client)
    assert len(client.get("/buses/health?source=demo").json()) == 20
    bus = client.get("/buses/bus-302-1/health?source=demo").json()
    assert bus["health"]["status"] == "healthy" and "sensors" not in bus
    assert len(client.get("/sensors").json()["sensors"]) == 4
    demo_condition(client, "sensor_fault")
    demo_step(client)
    sensor = client.get("/buses/bus-302-1/sensors/occupancy?source=demo").json()
    assert sensor["health_status"] == "faulty"
    assert client.get("/buses/bus-302-1/sensors/gps?source=demo").json()["health_status"] == "healthy"
    assert client.get("/buses/bus-302-1/health?source=demo").json()["health"]["status"] == "degraded"
    demo_condition(client, "silence")
    demo_step(client, 7)
    bus = client.get("/buses/bus-302-1/health?source=demo").json()
    assert bus["health"]["status"] == "unavailable" and "cause unknown" in bus["health"]["reasons"][0]
    demo_condition(client, "normal")
    demo_step(client, 3)
    assert client.get("/buses/bus-302-1/health?source=demo").json()["health"]["status"] == "healthy"


def test_live_health_routes_keep_cold_stale_and_failed_edge_reports_honest(demo_client, monitor, monkeypatch):
    from services.central import health
    from services.central.routers import buses
    from datetime import UTC, datetime

    client = demo_client
    store, now, _ = monitor
    monkeypatch.setattr(health, "monitor", store)
    monkeypatch.setattr(buses, "get_all_buses", lambda **kwargs: [dict(bus_id="bus-302-1", bus_status="stopped",
        last_updated=datetime.now(UTC), edge_metadata={"passengers": 10, "capacity": 50})])
    initial = client.get("/buses/bus-302-1/sensors").json()
    assert all(sensor["health_status"] == "unknown" for sensor in initial["sensors"].values())
    assert receive(store, healthy_bus_report())
    assert client.get("/buses/bus-302-1/health").json()["health"]["status"] == "stationary"
    route = client.get("/routes/302/health").json()
    assert route["passengers"] == 10 and route["occupancy_samples"] == 1
    now[0] = 5
    assert client.get("/buses/bus-302-1/health").json()["health"]["status"] == "telemetry_unavailable"
    now[0] = 10
    assert client.get("/buses/bus-302-1/health").json()["health"]["status"] == "unknown"
    assert all(sensor["health_status"] == "unknown" for sensor in client.get("/buses/bus-302-1/sensors").json()["sensors"].values())
    assert client.get("/routes/302/health").json()["passengers"] is None


def test_route_capacity_health_alerts_and_stale_central_coverage(demo_client):
    client = demo_client
    state = demo_step(client)
    route = client.get("/routes/302/health?source=demo").json()
    assert route["configured_capacity"] == route["verified_service_capacity"] == 200
    assert route["available_buses"] == route["occupancy_samples"] == 4
    total = sum(state["central_states"][f"bus-302-{index}"]["occupancy"] for index in range(1, 5))
    assert route["passengers"] == total and route["available_seats_in_samples"] == 200 - total
    assert all(edge["queue_capacity"] == 50 for edge in route["edge_workload"])
    demo_condition(client, "sensor_fault")
    state = demo_step(client)
    route = client.get("/routes/302/health?source=demo").json()
    expected = sum(state["central_states"][f"bus-302-{index}"]["occupancy"] for index in range(2, 5))
    assert route["occupancy_samples"] == 3 and route["passengers"] == expected
    alerts = client.get("/alerts?source=demo&bus_id=bus-302-1").json()
    assert any("degraded" in alert["message"] for alert in alerts)
    assert sum(alert["message"].startswith("delivery_attempt:") for alert in alerts) == 1
    assert all(alert.get("bus_id") in (None, "bus-302-1") for alert in alerts)
    client.patch("/health/simulation/demo", json={"central_available": False})
    demo_step(client, 31)
    route = client.get("/routes/302/health?source=demo").json()
    assert route["passengers"] is None and route["occupancy_samples"] == 0
    assert any(alert["severity"] == "critical" for alert in client.get("/alerts?source=demo").json())
    bus = client.get("/buses/bus-302-2/health?source=demo").json()
    assert bus["central_delivery_pending"] and not bus["central_available"]
    assert bus["valid_telemetry_age_seconds"] < 2 and bus["central_update_age_seconds"] >= 30


def test_health_api_filters_invalid_ids_and_database_unavailability(demo_client, monkeypatch):
    from services.central.routers import buses
    import psycopg

    client = demo_client
    demo_step(client)
    filtered = client.get("/buses/health?source=demo&region_id=east&route_id=407").json()
    assert len(filtered) == 4 and all(bus["region_id"] == "east" and bus["route_id"] == "407" for bus in filtered)
    for path in ("/buses/unknown/health", "/buses/unknown/sensors", "/buses/bus-302-1/sensors/unknown", "/routes/unknown/health"):
        assert client.get(path + "?source=demo").status_code == 404
    assert client.get("/buses/health?source=invalid").status_code == 422
    def unavailable(**kwargs):
        raise psycopg.OperationalError("test database unavailable")
    monkeypatch.setattr(buses, "get_all_buses", unavailable)
    route = client.get("/routes/302/health").json()
    assert route["notes"] and route["passengers"] is None and route["occupancy_samples"] == 0

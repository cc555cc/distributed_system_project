"""Central detects failed edges without waiting for the failed worker to report."""

import json

from fastapi.testclient import TestClient
import pytest

from services.central.health import CentralHealthMonitor
from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.edge_server import EdgeServerInstance
from shared.schema.health import HealthPolicy
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network


@pytest.fixture
def monitor():
    now = [0.0]
    policy = HealthPolicy(interval_seconds=1, sensor_timeout_seconds=5, edge_timeout_seconds=10,
                          recovery_seconds=2, sustained_seconds=3)
    return CentralHealthMonitor(load_network(), policy, clock=lambda: now[0]), now, policy


def report(edge_id="edge-west-1", region="west", percent=0, sequence=1, session="first"):
    instance = EdgeServerInstance(EdgeConfig("DURHAM", region, edge_id), NetworkConfig.load(DEFAULT_NETWORK_PATH), HealthPolicy())
    value = instance.heartbeat().model_dump(mode="json")
    value["session_id"] = session
    value["sequence_number"] = sequence
    value["workload"].update(queue_size=percent, queue_capacity=100,
                              queue_utilization_percent=float(percent))
    return value


def receive(monitor, value):
    return monitor.receive(f"transit/heartbeats/edge/{value['region_id']}/{value['edge_id']}", json.dumps(value).encode())


def edge(monitor, edge_id="edge-west-1"):
    return next(item for item in monitor.snapshot()["edges"] if item["edge_id"] == edge_id)


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
    assert all(item["action"] == "additional_capacity_needed" and item["to_edge_id"] is None for item in choices)


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
    from services.central import health
    from services.central.main import app
    from services.messaging import mqtt_client
    from types import SimpleNamespace
    store, _, _ = monitor
    monkeypatch.setattr(health, "monitor", store)
    value = report()
    mqtt_client.on_message(None, None, SimpleNamespace(topic="transit/heartbeats/edge/west/edge-west-1", payload=json.dumps(value).encode()))
    response = TestClient(app).get("/health/edges")
    assert response.status_code == 200
    west = next(item for item in response.json()["edges"] if item["edge_id"] == "edge-west-1")
    assert west["status"] == "healthy"
    assert west["last_report"]["workload"]["queue_capacity"] == 100


def test_sensor_publisher_to_edge_to_central_health_api(monitor, monkeypatch):
    from services.data_simulation.run_buses import SimulatorHeartbeats
    from services.edge.edge_server_health import EdgeMonitor
    from services.edge.communication.data_to_edge import TelemetrySubscriber
    from services.messaging import mqtt_client
    from services.central import health
    from services.central.main import app
    from shared.schema.health import SENSORS
    from types import SimpleNamespace
    from unittest.mock import Mock
    store, now, policy = monitor
    monkeypatch.setattr(health, "monitor", store)
    config = EdgeConfig("DURHAM", "west", "edge-west-1")
    network = NetworkConfig.load(DEFAULT_NETWORK_PATH)
    instance = EdgeServerInstance(config, network, policy)
    instance.monitor = EdgeMonitor(config, network.registry, policy, clock=lambda: now[0])
    instance.subscriber.monitor = instance.monitor
    bus = next(bus for bus in load_network()["buses"] if bus["bus_id"] == "bus-302-1")
    source = SimulatorHeartbeats(bus)
    source.update({sensor: "healthy" for sensor in SENSORS})
    instance.subscriber.on_message(None, None, SimpleNamespace(
        topic="transit/heartbeats/bus/west/bus-302-1", payload=source.snapshot().model_dump_json().encode()))
    assert instance.subscriber.messages.empty()
    outgoing = instance.heartbeat()
    mqtt_client.on_message(None, None, SimpleNamespace(
        topic="transit/heartbeats/edge/west/edge-west-1", payload=outgoing.model_dump_json().encode()))
    result = TestClient(app).get("/health/edges").json()
    west = next(item for item in result["edges"] if item["edge_id"] == "edge-west-1")
    bus_health = next(bus for bus in west["last_report"]["buses"] if bus["bus_id"] == "bus-302-1")
    assert all(sensor["status"] == "healthy" for sensor in bus_health["sensors"].values())
    assert bus_health["valid_telemetry_age_seconds"] is None
    now[0] = 10
    result = TestClient(app).get("/health/edges").json()
    west = next(item for item in result["edges"] if item["edge_id"] == "edge-west-1")
    assert west["status"] == "unavailable"
    assert all(sensor["status"] == "unknown" for sensor in west["last_report"]["buses"][0]["sensors"].values())


def test_restart_resets_sustained_overload(monitor):
    store, now, _ = monitor
    receive(store, report(percent=90))
    now[0] = 2
    receive(store, report(percent=90, session="restart"))
    now[0] = 4
    receive(store, report(percent=90, session="restart", sequence=2))
    assert edge(store)["status"] == "healthy"
    assert not store.snapshot()["recommendations"]


@pytest.mark.parametrize("bad_report", ["missing_bus", "missing_sensor", "negative_age"])
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
        response = client.patch(f"/health/simulation/instances/{edge_id}", json={"processing_ms": 60000})
        assert response.status_code == 200
    response = client.post("/health/simulation/requests", json={"bus_id": "bus-302-1", "count": 80})
    assert response.json()["accepted"] == 80
    response = client.post("/health/simulation/advance", json={"seconds": 6})
    third = next(w for w in response.json()["workers"] if w["edge_id"] == "edge-west-3")
    assert third["active"] and third["queue_size"] > 0
    assert response.json()["live_workers_changed"] is False
    assert client.patch("/health/simulation/instances/edge-west-1", json={"capacity": 51}).status_code == 422
    assert client.patch("/health/simulation/instances/unknown", json={"active": True}).status_code == 422
    assert client.post("/health/simulation/requests", json={"bus_id": "unknown", "count": 1}).status_code == 422
    assert client.post("/health/simulation/requests", json={"bus_id": "bus-302-1", "count": True}).status_code == 422
    assert client.post("/health/simulation/advance", json={"seconds": -1}).status_code == 422
    assert client.patch("/health/simulation/policy", json={"auto_scale": False}).json()["auto_scale"] is False
    reset = client.post("/health/simulation/reset").json()
    assert reset["accepted"] == 0 and reset["virtual_seconds"] == 0
    assert all(w["queue_size"] == 0 for w in reset["workers"])
    assert sum(w["active"] for w in client.get("/health/simulation").json()["workers"]) == 4

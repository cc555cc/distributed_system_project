"""Opt-in real MQTT/PostgreSQL test using a temporary, isolated database schema.

Set EDGE_LIVE_DATABASE_URL and optionally EDGE_LIVE_MQTT_HOST/PORT. Use dedicated
test services. This is skipped by default and never runs the destructive project
schema against the database's public schema.
"""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
from queue import Empty
from threading import Event
import time
from uuid import uuid4

import pytest


@pytest.mark.skipif(not os.getenv("EDGE_LIVE_DATABASE_URL"), reason="Set EDGE_LIVE_DATABASE_URL for live MQTT/PostgreSQL test")
def test_live_telemetry_reaches_central_database_and_api(monkeypatch):
    import paho.mqtt.client as mqtt
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    from fastapi.testclient import TestClient

    from services.edge.communication.edge_to_central import CentralForwarder, EdgeToCentral
    from services.edge.communication.data_to_edge import TelemetrySubscriber
    from services.edge.config import EdgeConfig, NetworkConfig, RouteDefinition
    from services.edge.edge_server import EdgeIngress
    from services.edge.processing.processor import EdgeProcessor
    from services.edge.validation.bus_registry import BusRegistry, BusAssignment
    from shared.schema.bus_state import BusStateUpdate

    url = os.environ["EDGE_LIVE_DATABASE_URL"]
    monkeypatch.setenv("DATABASE_URL", url)
    from services.central.routers import bus_states
    from services.central.main import app
    from services.messaging import mqtt_client

    identifier = uuid4().hex
    schema = "edge_test_" + identifier
    region, bus_id = "region-" + identifier, "bus-" + identifier
    host = os.getenv("EDGE_LIVE_MQTT_HOST", "127.0.0.1")
    port = int(os.getenv("EDGE_LIVE_MQTT_PORT", "1883"))
    scoped_url = make_conninfo(url, options=f"-csearch_path={schema}")
    config = EdgeConfig("TEST-CITY", region, "test-edge-" + identifier)
    registry = BusRegistry({bus_id: BusAssignment(region, "R1", "TEST-CITY", config.edge_id)})
    network = NetworkConfig(registry, (RouteDefinition("TEST-CITY", region, "R1", ("S1", "S2")),))
    receiver = EdgeIngress(EdgeProcessor(config, network))
    subscriber = TelemetrySubscriber(region, config.edge_id, city_id=config.city_id, registry=registry)
    sender = EdgeToCentral(config, registry, BusStateUpdate)
    forwarder = CentralForwarder(receiver, sender, "passengers")
    clients = []
    created = False
    try:
        with psycopg.connect(url, autocommit=True) as admin:
            admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        created = True
        with psycopg.connect(scoped_url) as connection:
            ddl = Path(__file__).resolve().parents[3] / "shared/schema/schema.sql"
            connection.execute(ddl.read_text())
            connection.execute("INSERT INTO regions(region_id,name,primary_edge,backup_edge) VALUES (%s,'test',%s,'backup')", (region, config.edge_id))
            connection.execute("INSERT INTO routes(route_id,name,region_id) VALUES ('R1','test',%s)", (region,))
            connection.execute("INSERT INTO buses(bus_id,route_id) VALUES (%s,'R1')", (bus_id,))
            connection.execute("INSERT INTO stops(stop_id,name,lat,lon) VALUES ('S1','one',43.9,-78.9),('S2','two',43.91,-78.91)")
            connection.execute("INSERT INTO route_stops VALUES ('R1',0,'S1',0),('R1',0,'S2',1)")
        monkeypatch.setattr(bus_states, "db_url", scoped_url)

        def subscribe(topic, callback, on_connect=None):
            ready = Event()
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="test-" + uuid4().hex)
            client.on_message = callback
            client.on_connect = on_connect or (lambda c, u, f, reason, p: c.subscribe(topic, qos=1))
            client.on_subscribe = lambda *args: ready.set()
            clients.append(client)
            client.connect(host, port)
            client.loop_start()
            assert ready.wait(10), "MQTT subscription did not become ready"
            return client

        central_received = Event()

        def central_callback(client, userdata, message):
            mqtt_client.on_message(client, userdata, message)
            central_received.set()

        subscribe(f"transit/{region}/bus/{bus_id}/state", central_callback)
        subscribe(subscriber.topic_filter, subscriber.on_message, subscriber.on_connect)
        sender.connect(host, port)
        deadline = time.monotonic() + 10
        while not sender._client.is_connected() and time.monotonic() < deadline:
            Event().wait(0.02)
        assert sender._client.is_connected()
        source = dict(bus_id=bus_id, city_id=config.city_id, region_id=region, route_id="R1",
                      latitude=43.9, longitude=-78.9, speed_kmh=30.0, passengers=40, capacity=50,
                      timestamp=datetime.now(timezone.utc).isoformat(), sequence_number=1,
                      bus_status="travelling", lamport_clock=10, direction_id=0,
                      current_stop_id="S1", next_stop_id="S2", sensor_status="healthy")
        sender._client.publish(f"transit/buses/{region}/{bus_id}/telemetry", json.dumps(source), qos=1).wait_for_publish(10)
        try:
            queued = subscriber.messages.get(timeout=10)
        except Empty:
            pytest.fail("Real MQTT telemetry never reached the edge queue")
        try:
            forwarder.process(queued)
            assert forwarder.flush_one()
        finally:
            subscriber.messages.task_done()
        assert central_received.wait(10), "Central did not receive the forwarded message"
        response = TestClient(app).get(f"/bus_states/{bus_id}")
        assert response.status_code == 200
        state = response.json()
        assert state["sequence_number"] == 1 and state["occupancy"] == 40
        assert state["lat"] == source["latitude"] and state["lamport"] == 10
        assert state["edge_metadata"]["city_id"] == config.city_id
        assert state["edge_metadata"]["source_timestamp"] == source["timestamp"]
        assert state["edge_metadata"]["statistics"]["active_buses"] == 1
    finally:
        sender.close()
        for client in clients:
            client.disconnect()
            client.loop_stop()
        if created:
            # Drop only the generated test schema, never application data.
            with psycopg.connect(url, autocommit=True) as admin:
                admin.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))



@pytest.mark.skipif(not os.getenv("EDGE_LIVE_DATABASE_URL"), reason="Set EDGE_LIVE_DATABASE_URL for live multi-instance test")
def test_live_four_instances_forward_and_report_failure_recovery(monkeypatch):
    """Real launcher children, MQTT, PostgreSQL and central API; isolated SQL schema.

    Test the fixed MQTT topology. Stateful handoff has separate simulation
    tests; this test does not certify an interprocess replication protocol.
    """
    from multiprocessing import get_context
    import paho.mqtt.client as mqtt
    import psycopg
    from psycopg import sql
    from psycopg.conninfo import make_conninfo
    from fastapi.testclient import TestClient
    from services.central import health
    from services.central.routers import bus_states
    from services.central.main import app
    from services.messaging import mqtt_client
    from services.messaging.topics import EDGE_HEARTBEAT_FILTER, bus_heartbeat
    from services.edge.main import configured_instances, run_instance
    from services.data_simulation.export_routes import seed_sql
    from services.data_simulation.generate_data import generate_messages
    from shared.schema.network import DEFAULT_NETWORK_PATH, load_network
    from shared.schema.health import BusHeartbeat, HealthPolicy, SENSORS

    url = os.environ["EDGE_LIVE_DATABASE_URL"]
    host = os.getenv("EDGE_LIVE_MQTT_HOST", "127.0.0.1")
    port = int(os.getenv("EDGE_LIVE_MQTT_PORT", "1883"))
    schema = "edge_instances_" + uuid4().hex
    scoped_url = make_conninfo(url, options=f"-csearch_path={schema}")
    network = load_network()
    policy = HealthPolicy(interval_seconds=0.2, sensor_timeout_seconds=2,
                          edge_timeout_seconds=2, recovery_seconds=0.8, sustained_seconds=1)
    store = health.CentralHealthMonitor(network, policy)
    monkeypatch.setattr(health, "monitor", store)
    monkeypatch.setattr(bus_states, "db_url", scoped_url)
    for key, value in dict(MQTT_HOST=host, MQTT_PORT=str(port), CENTRAL_MQTT_HOST=host,
                           CENTRAL_MQTT_PORT=str(port), CENTRAL_OCCUPANCY_UNIT="passengers",
                           EDGE_QUEUE_SIZE="50", HEARTBEAT_INTERVAL_SECONDS="0.2",
                           SENSOR_HEARTBEAT_TIMEOUT_SECONDS="2", EDGE_HEARTBEAT_TIMEOUT_SECONDS="2",
                           HEARTBEAT_RECOVERY_SECONDS="0.8", EDGE_OVERLOAD_SUSTAIN_SECONDS="1").items():
        monkeypatch.setenv(key, value)
    api = TestClient(app)
    context = get_context("spawn")
    processes, stops = [], []
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="edge-instances-" + uuid4().hex)
    subscribed = Event()
    client.on_connect = lambda c, u, f, reason, p: c.subscribe([
        ("transit/+/bus/+/state", 1), (EDGE_HEARTBEAT_FILTER, 1)])
    client.on_subscribe = lambda *args: subscribed.set()
    client.on_message = mqtt_client.on_message
    created = False

    def wait_for(predicate, description, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = predicate()
            if result:
                return result
            Event().wait(0.05)
        pytest.fail(description)

    def start(identity):
        stop = context.Event()
        process = context.Process(target=run_instance, args=(str(DEFAULT_NETWORK_PATH), identity, stop))
        process.start()
        processes.append(process)
        stops.append(stop)
        return process, stop

    def publish(topic, payload):
        receipt = client.publish(topic, payload, qos=1, retain=False)
        receipt.wait_for_publish(5)
        assert receipt.is_published()

    def edge_status(edge_id):
        return next(edge["status"] for edge in store.snapshot()["edges"] if edge["edge_id"] == edge_id)

    try:
        with psycopg.connect(url, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        created = True
        with psycopg.connect(scoped_url) as connection:
            ddl = Path(__file__).resolve().parents[3] / "shared/schema/schema.sql"
            connection.execute(ddl.read_text(encoding="utf-8"))
            connection.execute(seed_sql(network))
        client.connect(host, port)
        client.loop_start()
        assert subscribed.wait(10)
        identities = configured_instances(DEFAULT_NETWORK_PATH)
        for identity in identities.values():
            start(identity)
        wait_for(lambda: all(edge_status(edge_id) == "healthy" for edge_id in identities),
                 "Four launcher-created processes never became healthy")
        sources = []
        heartbeat_session = uuid4().hex
        for bus in network["buses"]:
            source = next(generate_messages(bus["route_id"], bus.get("direction_id", 0), bus["bus_id"],
                                            max_updates=1, update_seconds=0))
            sources.append(source)
            heartbeat = BusHeartbeat(city_id=bus["city_id"], region_id=bus["region_id"], bus_id=bus["bus_id"],
                session_id=heartbeat_session, sequence_number=1, timestamp=datetime.now(timezone.utc).isoformat(),
                sensors={sensor: "healthy" for sensor in SENSORS})
            publish(bus_heartbeat(bus["region_id"], bus["bus_id"]), heartbeat.model_dump_json())

        def all_states_arrive():
            # Repeated identical QoS updates also exercise duplicate handling.
            for source in sources:
                publish(f"transit/buses/{source['region_id']}/{source['bus_id']}/telemetry", json.dumps(source))
            return all(api.get(f"/bus_states/{source['bus_id']}").status_code == 200 for source in sources)
        wait_for(all_states_arrive, "Not all ten buses reached the real database/API")
        for source in sources:
            state = api.get(f"/bus_states/{source['bus_id']}").json()
            assert (state["lat"], state["lon"], state["speed"], state["occupancy"]) == (
                source["latitude"], source["longitude"], source["speed_kmh"], source["passengers"])
            assert state["sequence_number"] == source["sequence_number"]
            assert state["lamport"] == source["lamport_clock"]
            assert state["edge_metadata"]["region_id"] == source["region_id"]
            owner = next(bus["edge_id"] for bus in network["buses"] if bus["bus_id"] == source["bus_id"])
            assert state["edge_metadata"]["edge_id"] == owner
            assert (state["direction_id"], state["next_stop_id"], state["bus_status"]) == (
                source["direction_id"], source["next_stop_id"], source["bus_status"])
            for field in ("city_id", "route_id", "current_stop_id", "capacity", "passengers"):
                assert state["edge_metadata"][field] == source[field]
            assert state["edge_metadata"]["source_timestamp"] == source["timestamp"]

        def metrics_verified():
            for edge in store.snapshot()["edges"]:
                workload = edge["last_report"]["workload"]
                if workload["active_buses"] != workload["assigned_buses"] or workload["processed_messages"] < 1:
                    return False
                assert workload["queue_capacity"] == 50
                assert 0 <= workload["queue_size"] <= 50
                assert workload["messages_per_second"] > 0
                assert workload["average_processing_ms"] >= 0
            return True
        wait_for(metrics_verified, "Live instance metrics did not reflect their assigned buses")
        # Exercise sensor evidence through the real publisher/edge/central path.
        bus = next(bus for bus in network["buses"] if bus["bus_id"] == "bus-302-1")
        sensor_report = BusHeartbeat(city_id=bus["city_id"], region_id=bus["region_id"], bus_id=bus["bus_id"],
            session_id=heartbeat_session, sequence_number=2, timestamp=datetime.now(timezone.utc).isoformat(),
            sensors={sensor: "faulty" if sensor == "gps" else "healthy" for sensor in SENSORS})
        publish(bus_heartbeat(bus["region_id"], bus["bus_id"]), sensor_report.model_dump_json())
        def gps_status():
            response = api.get("/health/edges").json()
            return next(item for item in response["buses"] if item["bus_id"] == bus["bus_id"])["sensors"]["gps"]["health_status"]
        wait_for(lambda: gps_status() == "faulty", "GPS fault never reached central health API")
        sensor_report.sequence_number = 3
        sensor_report.sensors["gps"] = "healthy"
        publish(bus_heartbeat(bus["region_id"], bus["bus_id"]), sensor_report.model_dump_json())
        wait_for(lambda: gps_status() == "healthy", "Restored sensor never reached central health API")
        # Stop one actual child. Its partner continues sending independent health.
        stopped_id = "edge-west-1"
        index = list(identities).index(stopped_id)
        stops[index].set()
        processes[index].join(10)
        assert not processes[index].is_alive()
        wait_for(lambda: edge_status(stopped_id) == "unavailable", "Stopped process did not time out")
        assert edge_status("edge-west-2") == "healthy"
        affected = [bus for bus in store.snapshot()["buses"] if bus["edge_id"] == stopped_id]
        assert affected and all(bus["health"]["status"] == "unknown" for bus in affected)
        start(identities[stopped_id])
        wait_for(lambda: edge_status(stopped_id) == "recovering", "Restarted process did not enter recovery")
        wait_for(lambda: edge_status(stopped_id) == "healthy", "Restarted process did not recover")
        assert all(process.is_alive() for process in processes if process is not processes[index])
    finally:
        for stop in stops:
            stop.set()
        for process in processes:
            process.join(10)
            if process.is_alive():
                process.terminate()
                process.join(5)
        client.disconnect()
        client.loop_stop()
        if created:
            with psycopg.connect(url, autocommit=True) as connection:
                connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))

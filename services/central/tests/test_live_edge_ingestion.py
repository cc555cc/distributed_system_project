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
    from services.edge.communication.mqtt_subscriber import TelemetrySubscriber
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

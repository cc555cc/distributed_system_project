"""Publisher authentication, topic authorization, replay defense and MQTT credentials."""

import json
import os
import time
from queue import Empty
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from services.data_simulation.generate_data import generate_messages
from services.edge.auth_edge_server import (
    EdgeAuth,
    configure_mqtt_client,
    encode_bus_message,
    mosquitto_acl,
    sign_message,
)
from services.edge.communication.data_to_edge import TelemetrySubscriber
from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.edge_server_health import EdgeMonitor
from shared.schema.health import SENSORS, BusHeartbeat, HealthPolicy
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network

SECRET = "test-key-only-" + "a" * 32
TOPIC = "transit/buses/west/bus-302-1/telemetry"


@pytest.fixture
def security():
    registry = NetworkConfig.load(DEFAULT_NETWORK_PATH).registry
    now = [1000.0]
    auth = EdgeAuth("edge-west-1", "west", registry, city_id="DURHAM",
                    keys={"bus:bus-302-1": SECRET, "bus:bus-302-2": SECRET}, clock=lambda: now[0])
    source = next(generate_messages("302", 0, "bus-302-1", max_updates=1, update_seconds=0))
    return auth, now, registry, source


def signed(security, **options):
    _, now, _, source = security
    return sign_message(TOPIC, source, "bus:bus-302-1", SECRET, timestamp=now[0], **options)


def test_valid_signature_and_owned_topic_are_required_before_queueing(security):
    auth, _, registry, source = security
    subscriber = TelemetrySubscriber("west", "edge-west-1", city_id="DURHAM", registry=registry, auth=auth)
    assert not subscriber.handle_raw_message(TOPIC, json.dumps(source).encode())
    assert subscriber.messages.empty()
    assert subscriber.handle_raw_message(TOPIC, json.dumps(signed(security)).encode())
    assert subscriber.messages.get_nowait() == source


@pytest.mark.parametrize("change", ["payload", "principal", "signature", "extra", "timestamp"])
def test_tampering_is_rejected(security, change):
    auth, _, _, _ = security
    message = signed(security)
    if change == "payload": message["payload"]["passengers"] += 1
    if change == "principal": message["auth"]["principal"] = "bus:unknown"
    if change == "signature": message["auth"]["signature"] = "0" * 64
    if change == "extra": message["auth"]["extra"] = "operator"
    if change == "timestamp": message["auth"]["timestamp"] += 1
    with pytest.raises(PermissionError):
        auth.verify(TOPIC, message)


def test_signature_is_bound_to_topic_and_cannot_impersonate_another_owner(security):
    auth, now, _, source = security
    with pytest.raises(PermissionError):
        auth.verify("transit/buses/west/bus-302-2/telemetry", signed(security))
    message = sign_message("transit/buses/west/bus-302-2/telemetry", source,
                           "bus:bus-302-2", SECRET, timestamp=now[0])
    with pytest.raises(PermissionError):
        auth.verify("transit/buses/west/bus-302-2/telemetry", message)


def test_replay_and_timestamp_boundaries_are_rejected(security):
    auth, now, _, _ = security
    message = signed(security)
    assert auth.verify(TOPIC, message)
    with pytest.raises(PermissionError, match="Replayed"):
        auth.verify(TOPIC, message)
    now[0] += 30
    with pytest.raises(PermissionError, match="timestamp"):
        auth.verify(TOPIC, message)
    future = signed(security)
    future["auth"]["timestamp"] += 6
    with pytest.raises(PermissionError, match="timestamp"):
        auth.verify(TOPIC, future)


def test_replay_memory_is_bounded_and_expires_without_reaccepting_old_packets(security):
    auth, now, _, _ = security
    auth._seen = {("bus:bus-302-1", str(i)): now[0] + 30 for i in range(4096)}
    message = signed(security)
    with pytest.raises(PermissionError, match="full"):
        auth.verify(TOPIC, message)
    now[0] += 30
    assert auth.verify(TOPIC, signed(security))
    assert len(auth._seen) == 1


def test_heartbeat_authentication_cannot_be_bypassed(security):
    auth, now, registry, _ = security
    monitor = EdgeMonitor(EdgeConfig("DURHAM", "west", "edge-west-1"), registry, HealthPolicy())
    subscriber = TelemetrySubscriber("west", "edge-west-1", city_id="DURHAM", registry=registry, auth=auth, monitor=monitor)
    topic = "transit/heartbeats/bus/west/bus-302-1"
    heartbeat = BusHeartbeat(city_id="DURHAM", region_id="west", bus_id="bus-302-1", session_id="first",
        sequence_number=1, timestamp="2026-11-19T12:00:00Z", sensors={sensor: "healthy" for sensor in SENSORS})
    subscriber.on_message(None, None, SimpleNamespace(topic=topic, payload=heartbeat.model_dump_json().encode()))
    assert monitor.sensor_reports()[0]["sensors"]["gps"]["status"] == "unknown"
    message = sign_message(topic, heartbeat.model_dump(mode="json"), "bus:bus-302-1", SECRET, timestamp=now[0])
    subscriber.on_message(None, None, SimpleNamespace(topic=topic, payload=json.dumps(message).encode()))
    assert monitor.sensor_reports()[0]["sensors"]["gps"]["status"] == "healthy"
    assert subscriber.messages.empty()


def test_outgoing_and_subscription_permissions_have_no_wildcard_bypass(security):
    auth, _, _, _ = security
    auth.authorize_publish("transit/west/bus/bus-302-1/state")
    auth.authorize_publish("transit/heartbeats/edge/west/edge-west-1")
    auth.authorize_subscribe("transit/buses/west/+/telemetry")
    for topic in ("transit/west/bus/bus-302-2/state", "transit/east/bus/bus-407-1/state", "transit/#"):
        with pytest.raises(PermissionError): auth.authorize_publish(topic)
    with pytest.raises(PermissionError): auth.authorize_subscribe("#")


def test_environment_signer_and_secure_default_when_keys_are_provided(security, monkeypatch):
    _, _, registry, source = security
    monkeypatch.setenv("EDGE_PUBLISHER_KEYS_JSON", json.dumps({"bus:bus-302-1": SECRET}))
    monkeypatch.delenv("EDGE_AUTH_REQUIRED", raising=False)
    auth = EdgeAuth.from_environment("edge-west-1", "west", registry, "DURHAM")
    assert auth.required
    encoded = encode_bus_message(TOPIC, source, "bus-302-1")
    assert auth.verify(TOPIC, json.loads(encoded)) == source
    with pytest.raises(ValueError, match="signing key"):
        encode_bus_message(TOPIC, source, "unknown")


@pytest.mark.parametrize("keys,required", [("{}", "true"), ('{"bus:bus-302-1":"short"}', "true"), ("{}", "guess")])
def test_missing_or_invalid_auth_configuration_fails_closed(security, monkeypatch, keys, required):
    monkeypatch.setenv("EDGE_PUBLISHER_KEYS_JSON", keys)
    monkeypatch.setenv("EDGE_AUTH_REQUIRED", required)
    with pytest.raises(ValueError):
        EdgeAuth.from_environment("edge-west-1", "west", security[2], "DURHAM")


def test_broker_credentials_require_tls_unless_local_test_explicitly_allows_plaintext(monkeypatch):
    monkeypatch.setenv("MQTT_USERNAME", "edge-west-1")
    monkeypatch.setenv("MQTT_PASSWORD", "temporary-test-password")
    monkeypatch.delenv("EDGE_ALLOW_INSECURE_MQTT", raising=False)
    monkeypatch.setenv("MQTT_TLS", "false")
    client = Mock()
    with pytest.raises(ValueError, match="TLS"):
        configure_mqtt_client(client)
    client.username_pw_set.assert_not_called()
    monkeypatch.setenv("MQTT_TLS", "true")
    configure_mqtt_client(client)
    client.username_pw_set.assert_called_once_with("edge-west-1", "temporary-test-password")
    context = client.tls_set_context.call_args.args[0]
    assert context.check_hostname
    import ssl
    assert context.verify_mode == ssl.CERT_REQUIRED


def test_acl_generator_grants_each_bus_only_its_own_topics():
    acl = mosquitto_acl(load_network())
    bus_rules = acl.split("user bus-302-1\n", 1)[1].split("\n\n", 1)[0]
    assert "topic write transit/buses/west/bus-302-1/telemetry" in bus_rules
    assert "bus-302-2" not in bus_rules and "+" not in bus_rules and "#" not in bus_rules
    with pytest.raises(ValueError):
        EdgeAuth("edge/#", "west", NetworkConfig.load(DEFAULT_NETWORK_PATH).registry, required=False)


def test_signed_external_csv_converts_before_processing_and_payload_identity_must_match(security):
    import csv
    import io
    auth, now, registry, source = security
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(source))
    writer.writeheader()
    writer.writerow(source)
    body = {"format": "csv-v1", "data": output.getvalue()}
    subscriber = TelemetrySubscriber("west", "edge-west-1", city_id="DURHAM", registry=registry, auth=auth)
    message = sign_message(TOPIC, body, "bus:bus-302-1", SECRET, timestamp=now[0])
    assert subscriber.handle_raw_message(TOPIC, json.dumps(message).encode())
    assert subscriber.messages.get_nowait() == source
    other_bus = {**source, "bus_id": "bus-302-2"}
    message = sign_message(TOPIC, other_bus, "bus:bus-302-1", SECRET, timestamp=now[0])
    assert not subscriber.handle_raw_message(TOPIC, json.dumps(message).encode())
    assert subscriber.messages.empty()


@pytest.mark.skipif(not os.getenv("EDGE_AUTH_LIVE_MQTT_PORT"), reason="Set dedicated authenticated MQTT test settings")
def test_live_broker_authentication_acl_and_signed_ingress(monkeypatch):
    import paho.mqtt.client as mqtt
    registry = NetworkConfig.load(DEFAULT_NETWORK_PATH).registry
    auth = EdgeAuth("edge-west-1", "west", registry, city_id="DURHAM", keys={"bus:bus-302-1": SECRET})
    subscriber = TelemetrySubscriber("west", "edge-west-1", city_id="DURHAM", registry=registry, auth=auth)
    clients = []
    host = os.getenv("EDGE_AUTH_LIVE_MQTT_HOST", "127.0.0.1")
    port = int(os.environ["EDGE_AUTH_LIVE_MQTT_PORT"])
    password = os.environ["EDGE_AUTH_LIVE_PASSWORD"]
    def connect(username, supplied_password):
        ready = Event()
        results = []
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        if username is not None:
            client.username_pw_set(username, supplied_password)
        client.on_connect = lambda c, u, f, reason, p: (results.append(reason.is_failure), ready.set())
        client.connect(host, port)
        client.loop_start()
        clients.append(client)
        assert ready.wait(5)
        return client, results[0]
    try:
        _anonymous, rejected = connect(None, None)
        assert rejected
        _wrong, rejected = connect("bus-302-1", "incorrect-password")
        assert rejected
        edge, rejected = connect("edge-west-1", password)
        assert not rejected
        subscribed = Event()
        edge.on_message = subscriber.on_message
        edge.on_subscribe = lambda *args: subscribed.set()
        edge.subscribe(subscriber.topic_filter, qos=1)
        assert subscribed.wait(5)
        bus, rejected = connect("bus-302-1", password)
        assert not rejected
        acknowledgements = {}
        bus.on_publish = lambda c, u, mid, reason, p: acknowledgements.update({mid: reason.is_failure})
        source = next(generate_messages("302", 0, "bus-302-1", max_updates=1, update_seconds=0))
        receipt = bus.publish(TOPIC, json.dumps(sign_message(TOPIC, source, "bus:bus-302-1", SECRET)), qos=1)
        receipt.wait_for_publish(5)
        assert subscriber.messages.get(timeout=5) == source
        for forbidden in ("transit/buses/west/bus-302-2/telemetry", "transit/west/bus/bus-302-1/state",
                          "transit/heartbeats/edge/west/edge-west-1"):
            denied = bus.publish(forbidden, "{}", qos=1)
            denied.wait_for_publish(5)
            deadline = time.monotonic() + 5
            while denied.mid not in acknowledgements and time.monotonic() < deadline:
                Event().wait(0.01)
            assert acknowledgements[denied.mid] is True
        denied_subscription = Event()
        subscription_reasons = []
        bus.on_subscribe = lambda c, u, mid, reasons, p: (subscription_reasons.extend(reasons), denied_subscription.set())
        bus.subscribe(TOPIC, qos=1)
        assert denied_subscription.wait(5)
        # Mosquitto can acknowledge SUBSCRIBE but enforce read ACLs on delivery.
        leaked = Event()
        bus.on_message = lambda *args: leaked.set()
        receipt = bus.publish(TOPIC, json.dumps(sign_message(TOPIC, source, "bus:bus-302-1", SECRET)), qos=1)
        receipt.wait_for_publish(5)
        assert subscriber.messages.get(timeout=5) == source
        assert not leaked.wait(0.3)
        with pytest.raises(Empty): subscriber.messages.get_nowait()
    finally:
        for client in clients:
            client.disconnect()
            client.loop_stop()

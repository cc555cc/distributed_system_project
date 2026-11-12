"""MQTT-01 smoke test: a publisher and a subscriber exchange one message per agreed topic.

Run from the repository root while the broker is up:

    python -m services.messaging.tests.smoke_broker [--host HOST] [--port PORT]

Messages use the region "smoke", so regional edge services ignore them and the
central subscriber rejects them as invalid payloads.
"""

import argparse
import json
import os
import sys
import threading
import uuid

import paho.mqtt.client as mqtt

from services.messaging import topics

TIMEOUT_SECONDS = 5
REGION = "smoke"


def connect(client_id: str, host: str, port: int) -> mqtt.Client:
    accepted = threading.Event()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    client.on_connect = lambda c, u, f, reason, p: None if reason.is_failure else accepted.set()
    client.connect(host, port, keepalive=30)
    client.loop_start()
    if not accepted.wait(TIMEOUT_SECONDS):
        client.loop_stop()
        raise TimeoutError(f"broker did not accept client {client_id}")
    return client


def main() -> int:
    parser = argparse.ArgumentParser(description="Check the broker with the agreed MQTT topics.")
    parser.add_argument("--host", default=os.getenv("MQTT_HOST", "localhost"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    args = parser.parse_args()

    run_id = uuid.uuid4().hex[:8]
    bus_id = f"SMOKE-{run_id}"
    # Each topic is paired with the filter its real subscriber uses.
    cases = [
        ("telemetry", topics.telemetry(REGION, bus_id), topics.telemetry_filter(REGION)),
        ("state", topics.state(REGION, bus_id), topics.STATE_FILTER),
        ("alert", topics.alert(REGION, bus_id), topics.ALERT_FILTER),
        ("command to one bus", topics.command(REGION, bus_id), topics.command(REGION, bus_id)),
        ("command to region", topics.command(REGION), topics.command(REGION)),
    ]
    expected = {topic for _name, topic, _filter in cases}
    received: set[str] = set()
    all_received = threading.Event()
    subscribed = threading.Event()

    def on_message(client, userdata, msg):
        try:
            if json.loads(msg.payload).get("run_id") != run_id:
                return
        except (ValueError, AttributeError):
            return
        received.add(msg.topic)
        if received >= expected:
            all_received.set()

    print(f"Broker {args.host}:{args.port}, run {run_id}")
    try:
        subscriber = connect(f"smoke-sub-{run_id}", args.host, args.port)
        publisher = connect(f"smoke-pub-{run_id}", args.host, args.port)
    except (OSError, TimeoutError) as error:
        print(f"FAIL  could not connect: {error}")
        return 1

    try:
        subscriber.on_message = on_message
        subscriber.on_subscribe = lambda c, u, mid, reasons, p: subscribed.set()
        subscriber.subscribe([(topic_filter, 1) for _name, _topic, topic_filter in cases])
        if not subscribed.wait(TIMEOUT_SECONDS):
            print("FAIL  broker did not confirm the subscriptions")
            return 1

        for _name, topic, _filter in cases:
            publisher.publish(topic, json.dumps({"run_id": run_id}), qos=1)
        all_received.wait(TIMEOUT_SECONDS)
    finally:
        for client in (publisher, subscriber):
            client.disconnect()
            client.loop_stop()

    for name, topic, _filter in cases:
        print(f"{'PASS' if topic in received else 'FAIL'}  {name:<19} {topic}")
    return 0 if received >= expected else 1


if __name__ == "__main__":
    sys.exit(main())

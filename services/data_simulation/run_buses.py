"""Run configured buses as independent processes, with optional MQTT publishing."""

import argparse
import json
import logging
import os
import urllib.error
import urllib.request
from datetime import UTC, datetime
from multiprocessing import Process
from threading import Event, RLock, Thread
from uuid import uuid4

from services.data_simulation import generate_data
from services.edge.auth_edge_server import configure_mqtt_client, encode_bus_message
from services.messaging import alerts, commands
from services.messaging.lamport import LamportClock
from services.messaging.publisher import ReliablePublisher
from services.messaging.topics import bus_heartbeat
from shared.schema.health import BusHeartbeat, policy_from_environment
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network

logger = logging.getLogger(__name__)

SEQUENCE_MARGIN = 1000

class SimulatorHeartbeats:
    """Four logical sensor channels share the simulated bus's MQTT client."""

    def __init__(self, bus):
        self.bus = bus
        self.session_id = str(uuid4())
        self.sequence = 0
        self.sensors = {}
        self.lock = RLock()

    def update(self, sensors):
        with self.lock:
            self.sensors = dict(sensors)

    def snapshot(self):
        with self.lock:
            self.sequence += 1
            return BusHeartbeat(
                city_id=self.bus["city_id"],
                region_id=self.bus["region_id"],
                bus_id=self.bus["bus_id"],
                session_id=self.session_id,
                sequence_number=self.sequence,
                timestamp=datetime.now(UTC).isoformat(),
                sensors=self.sensors,
            )

    def publish_until_stopped(self, client, stop, interval):
        while not stop.is_set():
            # A heartbeat means "alive now". Do not queue one while offline:
            # delivered late it would only take space from telemetry.
            if not client.is_connected():
                stop.wait(interval)
                continue
            heartbeat = self.snapshot()
            try:
                topic = bus_heartbeat(heartbeat.region_id, heartbeat.bus_id)
                receipt = client.publish(
                    topic,
                    encode_bus_message(
                        topic, heartbeat.model_dump(mode="json"), heartbeat.bus_id
                    ),
                    qos=1,
                    retain=False,
                )
                if receipt.rc != 0:
                    logger.warning(
                        "Sensor heartbeat delivery pending for bus=%s", heartbeat.bus_id
                    )
            except (OSError, RuntimeError):
                logger.warning(
                    "Sensor heartbeat delivery pending for bus=%s", heartbeat.bus_id
                )
            stop.wait(interval)


def resume_state(bus_id, central_url, margin=SEQUENCE_MARGIN):
    """continue above central's last stored state so a revived bus process is not stale"""
    try:
        with urllib.request.urlopen(
            f"{central_url}/bus_states/{bus_id}", timeout=3
        ) as response:
            state = json.load(response)
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return 0, None, None  # central has never seen this bus
        raise
    return state["sequence_number"] + margin, state["next_stop_id"], state["direction_id"]

def bus_credentials(bus_id):
    """Per-bus broker account from MQTT_BUS_PASSWORDS_JSON (MQTT-07), if configured.

    The broker ACL names each bus account after its bus ID, so one launcher
    process can start every bus with the account that may use its topics.
    """
    raw = os.getenv("MQTT_BUS_PASSWORDS_JSON")
    if not raw:
        return None
    passwords = json.loads(raw)
    if (
        not isinstance(passwords, dict)
        or not isinstance(passwords.get(bus_id), str)
        or not passwords[bus_id]
    ):
        raise ValueError(f"MQTT_BUS_PASSWORDS_JSON has no password for {bus_id}")
    return bus_id, passwords[bus_id]


class SensorAlerts:
    """Publish one alert when a bus's sensors fail and one when they recover (MQTT-05)."""

    def __init__(self, bus, clock, publish):
        self.bus, self.clock, self.publish = bus, clock, publish
        self.faulty = False

    def update(self, sensors):
        faulty = any(status != "healthy" for status in sensors.values())
        if faulty == self.faulty:
            return
        self.faulty = faulty
        kind, severity, text = (
            (
                "sensor_failure",
                "warning",
                "Sensor reading failed; no telemetry was produced",
            )
            if faulty
            else ("sensor_recovered", "info", "Sensor readings recovered")
        )
        alert = alerts.make_alert(
            self.bus["region_id"],
            self.bus["bus_id"],
            "bus",
            kind,
            severity,
            text,
            clock=self.clock,
            bus_id=self.bus["bus_id"],
        )
        topic, payload = alerts.encode(alert)
        outcome = self.publish(topic, payload)
        logger.warning("Alert %s for bus=%s: %s", kind, self.bus["bus_id"], outcome)


def run_bus(bus, network_path, updates, interval, publish, host, port):
    """Keep each bus's random state, counters and connection independent."""
    start_sequence = 0
    resume_stop = None
    client = publisher = None
    heartbeat_source = SimulatorHeartbeats(bus)
    scenario = dict(bus.get("scenario", {}))
    # Commands change these while the bus runs; the clock is shared by sending
    # (telemetry, alerts) and receiving (commands) in this bus process.
    controls = commands.BusControls(
        demand=scenario.pop("demand", "off_peak"),
        traffic_factor=scenario.pop("traffic_factor", 1.0),
        sensor_failure_rate=scenario.pop("sensor_failure_rate", 0.0),
    )
    clock = LamportClock()
    sensor_health = heartbeat_source.update
    heartbeat_stop = Event()
    heartbeat_thread = None
    try:
        if publish:
            # Reconnects by itself; readings generated while offline are queued
            # and arrive late with their original sequence/clock (MQTT-04).
            credentials = bus_credentials(bus["bus_id"])
            configure = (
                (lambda c: configure_mqtt_client(c, *credentials))
                if credentials
                else configure_mqtt_client
            )
            central_url = os.getenv("CENTRAL_URL")
            if central_url:
                start_sequence, next_stop, direction = resume_state(
                    bus["bus_id"], central_url
                )
                # Only resume the position on the same directional route.
                if direction == bus.get("direction_id", 0):
                    resume_stop = next_stop
                logger.info(
                    "bus=%s resumes at sequence %s before stop %s",
                    bus["bus_id"], start_sequence, resume_stop,
                )
            publisher = ReliablePublisher(
                bus["bus_id"] + "-publisher", host, port, configure=configure
            )
            consumer = commands.CommandConsumer(
                bus["region_id"], bus["bus_id"], controls, clock
            )
            for topic_filter in commands.bus_subscriptions(
                bus["region_id"], bus["bus_id"]
            ):
                publisher.subscribe(topic_filter, consumer.handle)
            publisher.connect()
            client = publisher.client
            sensor_alerts = SensorAlerts(bus, clock, publisher.publish)
            sensor_health = lambda sensors: (
                heartbeat_source.update(sensors),
                sensor_alerts.update(sensors),
            )
            heartbeat_thread = Thread(
                target=heartbeat_source.publish_until_stopped,
                args=(
                    client,
                    heartbeat_stop,
                    policy_from_environment().interval_seconds,
                ),
                daemon=True,
            )
            heartbeat_thread.start()

        def emit(message):
            encoded = json.dumps(message, allow_nan=False)
            if client is not None:
                topic = f"transit/buses/{message['region_id']}/{message['bus_id']}/telemetry"
                outcome = publisher.publish(
                    topic, encode_bus_message(topic, message, bus["bus_id"])
                )
                if outcome != "sent":
                    logger.warning(
                        "Telemetry %s for bus=%s sequence=%s",
                        outcome,
                        message["bus_id"],
                        message["sequence_number"],
                    )
            print(encoded, flush=True)

        generate_data.main(
            bus["route_id"],
            bus.get("direction_id", 0),
            bus["bus_id"],
            network_path=network_path,
            max_updates=updates,
            update_seconds=interval,
            emit=emit,
            sensor_health=sensor_health,
            clock=clock,
            controls=controls,
            start_sequence=start_sequence,
            resume_next_stop_id=resume_stop,
            **scenario,
        )
    finally:
        controls.stop()
        heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=3)
        if publisher is not None:
            publisher.close()
            logger.info(
                "Publisher totals for bus=%s: %s", bus["bus_id"], publisher.stats
            )


def main(
    network_path=DEFAULT_NETWORK_PATH,
    *,
    updates=100,
    interval=1.0,
    publish=False,
    host="127.0.0.1",
    port=1883,
    bus_ids=None,
):
    network = load_network(network_path)
    selected = network["buses"]
    if bus_ids is not None:
        wanted = set(bus_ids)
        selected = [bus for bus in selected if bus["bus_id"] in wanted]
        if wanted != {bus["bus_id"] for bus in selected}:
            raise ValueError("Unknown bus selected")
    processes = []
    try:
        for bus in selected:
            process = Process(
                target=run_bus,
                args=(bus, str(network_path), updates, interval, publish, host, port),
                name=bus["bus_id"],
            )
            process.start()
            processes.append(process)
        for process in processes:
            process.join()
    except KeyboardInterrupt:
        pass
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
        for process in processes:
            process.join()
    if any(process.exitcode != 0 for process in processes):
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run buses from the shared network.")
    parser.add_argument("--network", default=str(DEFAULT_NETWORK_PATH))
    parser.add_argument("--updates", type=int, default=100)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--bus", action="append", dest="bus_ids")
    parser.add_argument(
        "--mqtt", action="store_true", help="Also publish to Julian's broker"
    )
    parser.add_argument("--host", default=os.getenv("MQTT_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    args = parser.parse_args()
    main(
        args.network,
        updates=args.updates,
        interval=args.interval,
        publish=args.mqtt,
        host=args.host,
        port=args.port,
        bus_ids=args.bus_ids,
    )

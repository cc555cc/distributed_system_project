"""Run configured buses as independent processes, with optional MQTT publishing."""

import argparse
import json
from multiprocessing import Process
from threading import Event, Thread, RLock
from datetime import datetime, timezone
from uuid import uuid4
import logging
import os

from services.data_simulation import generate_data
from services.edge.auth_edge_server import encode_bus_message, configure_mqtt_client
from shared.schema.health import BusHeartbeat, policy_from_environment
from services.messaging.publisher import ReliablePublisher
from services.messaging.topics import bus_heartbeat
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network


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
            return BusHeartbeat(city_id=self.bus["city_id"], region_id=self.bus["region_id"],
                                bus_id=self.bus["bus_id"], session_id=self.session_id,
                                sequence_number=self.sequence, timestamp=datetime.now(timezone.utc).isoformat(),
                                sensors=self.sensors)

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
                receipt = client.publish(topic, encode_bus_message(topic, heartbeat.model_dump(mode="json"), heartbeat.bus_id),
                                         qos=1, retain=False)
                if receipt.rc != 0:
                    logging.warning("Sensor heartbeat delivery pending for bus=%s", heartbeat.bus_id)
            except (OSError, RuntimeError):
                logging.warning("Sensor heartbeat delivery pending for bus=%s", heartbeat.bus_id)
            stop.wait(interval)


def run_bus(bus, network_path, updates, interval, publish, host, port):
    """Keep each bus's random state, counters and connection independent."""
    client = publisher = None
    heartbeat_source = SimulatorHeartbeats(bus)
    heartbeat_stop = Event()
    heartbeat_thread = None
    try:
        if publish:
            # Reconnects by itself; readings generated while offline are queued
            # and arrive late with their original sequence/clock (MQTT-04).
            publisher = ReliablePublisher(bus["bus_id"] + "-publisher", host, port,
                                          configure=configure_mqtt_client)
            publisher.connect()
            client = publisher.client
            heartbeat_thread = Thread(target=heartbeat_source.publish_until_stopped,
                                      args=(client, heartbeat_stop, policy_from_environment().interval_seconds), daemon=True)
            heartbeat_thread.start()
        def emit(message):
            encoded = json.dumps(message, allow_nan=False)
            if client is not None:
                topic = f"transit/buses/{message['region_id']}/{message['bus_id']}/telemetry"
                outcome = publisher.publish(topic, encode_bus_message(topic, message, bus["bus_id"]))
                if outcome != "sent":
                    logging.warning("Telemetry %s for bus=%s sequence=%s", outcome,
                                    message["bus_id"], message["sequence_number"])
            print(encoded, flush=True)
        scenario = bus.get("scenario", {})
        generate_data.main(bus["route_id"], bus.get("direction_id", 0), bus["bus_id"],
                           network_path=network_path, max_updates=updates,
                           update_seconds=interval, emit=emit, sensor_health=heartbeat_source.update, **scenario)
    finally:
        heartbeat_stop.set()
        if heartbeat_thread is not None:
            heartbeat_thread.join(timeout=3)
        if publisher is not None:
            publisher.close()
            logging.info("Publisher totals for bus=%s: %s", bus["bus_id"], publisher.stats)


def main(network_path=DEFAULT_NETWORK_PATH, *, updates=100, interval=1.0,
         publish=False, host="127.0.0.1", port=1883, bus_ids=None):
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
            process = Process(target=run_bus,
                              args=(bus, str(network_path), updates, interval, publish, host, port),
                              name=bus["bus_id"])
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
    parser.add_argument("--mqtt", action="store_true", help="Also publish to Julian's broker")
    parser.add_argument("--host", default=os.getenv("MQTT_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    args = parser.parse_args()
    main(args.network, updates=args.updates, interval=args.interval, publish=args.mqtt,
         host=args.host, port=args.port, bus_ids=args.bus_ids)

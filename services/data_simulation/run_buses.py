"""Run configured buses as independent processes, with optional MQTT publishing."""

import argparse
import json
from multiprocessing import Process
from threading import Event
import os

from services.data_simulation import generate_data
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network


def run_bus(bus, network_path, updates, interval, publish, host, port):
    """Keep each bus's random state, counters and connection independent."""
    client = None
    try:
        if publish:
            import paho.mqtt.client as mqtt
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=bus["bus_id"] + "-publisher")
            ready = Event()
            client.on_connect = lambda c, u, f, reason, p: ready.set() if not reason.is_failure else None
            client.connect(host, port, keepalive=60)
            client.loop_start()
            if not ready.wait(10):
                raise TimeoutError("MQTT publisher connection did not become ready")
        def emit(message):
            encoded = json.dumps(message, allow_nan=False)
            if client is not None:
                topic = f"transit/buses/{message['region_id']}/{message['bus_id']}/telemetry"
                receipt = client.publish(topic, encoded, qos=1, retain=False)
                if receipt.rc != 0:
                    raise ConnectionError("Broker rejected bus telemetry")
                receipt.wait_for_publish(timeout=5)
                if not receipt.is_published():
                    raise TimeoutError("Bus telemetry delivery timed out")
            print(encoded, flush=True)
        scenario = bus.get("scenario", {})
        generate_data.main(bus["route_id"], bus.get("direction_id", 0), bus["bus_id"],
                           network_path=network_path, max_updates=updates,
                           update_seconds=interval, emit=emit, **scenario)
    finally:
        if client is not None:
            client.disconnect()
            client.loop_stop()


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

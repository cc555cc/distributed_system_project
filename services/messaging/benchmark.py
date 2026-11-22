"""Measure MQTT delivery with simulated buses (MQTT-06, MQTT-11).

Each simulated bus is a separate MQTT client (ReliablePublisher, QoS 1) that
publishes telemetry-shaped messages at a fixed rate on its own topic. One
subscriber, like a regional edge, receives the region's filter directly from
the broker. Every bus connects through its own TCP relay (relay.py), so the
scenarios can disturb the bus-to-broker path while the broker keeps running:

* normal   no disturbance
* delayed  every forwarded packet is held for --delay-ms during the middle third
* outage   every bus connection is cut for --outage-s, starting a third of the way in

Recorded per run: messages sent, received, duplicates, lost (listed), late
(out-of-order) arrivals, per-bus sequence/Lamport ordering, delivery delay
(generation to arrival, including time in the client's offline queue) and
throughput.

    python -m services.messaging.benchmark --host 127.0.0.1 --port 1883 \\
        --buses 5 10 25 50 --scenarios normal --output results.json

Use a dedicated broker: the benchmark creates up to hundreds of clients.
"""

import argparse
import json
import math
import os
import platform
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Lock, Thread
from uuid import uuid4

import paho.mqtt.client as mqtt

from services.messaging import topics
from services.messaging.delivery import NEW, SequenceTracker
from services.messaging.lamport import LamportClock
from services.messaging.publisher import ReliablePublisher
from services.messaging.relay import Link, wait_for

SCENARIOS = ("normal", "delayed", "outage")


def reading(region, bus_id, sequence, lamport):
    # Same size and fields as real telemetry, plus the generation time in ns.
    return {"schema_version": "bus-telemetry-v1", "bus_id": bus_id, "city_id": "BENCH", "region_id": region,
                "route_id": "R1", "latitude": 43.9, "longitude": -78.9, "speed_kmh": 30.0, "direction_id": 0,
                "bus_status": "moving", "passengers": 10, "capacity": 50, "sequence_number": sequence,
                "lamport_clock": lamport, "timestamp": datetime.now(UTC).isoformat(),
                "sensor_status": "healthy", "current_stop_id": "S1", "next_stop_id": "S2",
                "generated_ns": time.time_ns()}


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[index]


class Receiver:
    """Region subscriber: records every arrival with its receive time."""

    def __init__(self, host, port, region):
        self.region = region
        self.tracker = SequenceTracker()
        self.delays_ms, self.lamport = [], {}
        self.lamport_violations = 0
        self.first_ns = self.last_ns = None
        self._lock = Lock()
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="bench-sub-" + uuid4().hex[:8])
        self.client.max_inflight_messages_set(100)
        self.client.on_message = self._on_message
        self._ready = Event()
        self.client.on_subscribe = lambda *args: self._ready.set()
        self.client.connect(host, port, keepalive=60)
        self.client.subscribe(topics.telemetry_filter(region), qos=1)
        self.client.loop_start()
        if not self._ready.wait(10):
            raise TimeoutError("benchmark subscriber not subscribed")

    def _on_message(self, client, userdata, message):
        now = time.time_ns()
        body = json.loads(message.payload)
        bus, sequence = body["bus_id"], body["sequence_number"]
        outcome = self.tracker.record(bus, sequence)
        with self._lock:
            self.first_ns = self.first_ns or now
            self.last_ns = now
            if outcome == NEW:
                self.delays_ms.append((now - body["generated_ns"]) / 1e6)
                # Lamport stamps must rise with each bus's in-order sequence.
                previous = self.lamport.get(bus)
                if previous is not None and body["lamport_clock"] <= previous:
                    self.lamport_violations += 1
                self.lamport[bus] = body["lamport_clock"]

    def close(self):
        self.client.disconnect()
        self.client.loop_stop()


class Bus:
    def __init__(self, host, port, region, bus_id, link):
        self.region, self.bus_id, self.link = region, bus_id, link
        self.clock = LamportClock()
        self.sent, self.outcomes = [], {"sent": 0, "queued": 0, "dropped": 0}
        self.publisher = ReliablePublisher(f"bench-{bus_id}-{uuid4().hex[:6]}", "127.0.0.1", link.port)
        self.publisher.connect()

    def run(self, count, interval, start_at):
        topic = topics.telemetry(self.region, self.bus_id)
        for sequence in range(1, count + 1):
            target = start_at + (sequence - 1) * interval
            pause = target - time.monotonic()
            if pause > 0:
                time.sleep(pause)
            outcome = self.publisher.publish(topic, json.dumps(reading(self.region, self.bus_id, sequence,
                                                                       self.clock.tick())))
            self.outcomes[outcome] += 1
            if outcome != "dropped":
                self.sent.append(sequence)


def run_once(host, port, buses, *, scenario="normal", rate=5.0, duration=10.0, delay_ms=200, outage_s=3.0,
             drain_s=30.0):
    if scenario not in SCENARIOS:
        raise ValueError(f"scenario must be one of {SCENARIOS}")
    region = "bench" + uuid4().hex[:8]
    count = max(1, round(rate * duration))
    interval = 1.0 / rate
    receiver = Receiver(host, port, region)
    links = [Link(host, port) for _ in range(buses)]
    fleet = [Bus(host, port, region, f"B{i:03d}", links[i]) for i in range(buses)]
    events = []
    try:
        start_at = time.monotonic() + 0.5
        # Stagger buses across one interval so they do not all publish at once.
        threads = [Thread(target=bus.run, args=(count, interval, start_at + interval * i / buses), daemon=True)
                   for i, bus in enumerate(fleet)]
        started_ns = time.time_ns()
        for thread in threads:
            thread.start()

        def disturb():
            third = start_at + duration / 3
            time.sleep(max(0.0, third - time.monotonic()))
            if scenario == "delayed":
                for link in links:
                    link.delay = delay_ms / 1000
                events.append(("delay on", time.monotonic() - start_at))
                time.sleep(duration / 3)
                for link in links:
                    link.delay = 0.0
                events.append(("delay off", time.monotonic() - start_at))
            elif scenario == "outage":
                for link in links:
                    link.cut()
                events.append(("cut", time.monotonic() - start_at))
                time.sleep(outage_s)
                for link in links:
                    link.restore()
                events.append(("restore", time.monotonic() - start_at))

        disturber = Thread(target=disturb, daemon=True)
        disturber.start()
        for thread in threads:
            thread.join()
        disturber.join()
        publish_end_ns = time.time_ns()
        # Wait for resends after an outage, then for arrivals to stop.
        expected = sum(len(bus.sent) for bus in fleet)
        for bus in fleet:
            bus.publisher.flush(drain_s)
        try:
            wait_for(lambda: receiver.tracker.report({}).get("unique", 0) >= expected, timeout=drain_s,
                     what="all deliveries")
        except AssertionError:
            pass
        time.sleep(1.0)                      # late duplicates
    finally:
        unacknowledged = sum(bus.publisher.close(flush_timeout=1) for bus in fleet)
        receiver.close()
        for link in links:
            link.close()

    sent = {bus.bus_id: bus.sent for bus in fleet}
    report = receiver.tracker.report(sent)
    delays = receiver.delays_ms
    elapsed = ((receiver.last_ns or publish_end_ns) - started_ns) / 1e9
    stats = [bus.publisher.stats for bus in fleet]
    return {
        "scenario": scenario, "buses": buses, "rate_per_bus_hz": rate, "duration_s": duration,
        "offered_msgs_per_s": round(buses * rate, 1),
        "published": sum(bus.outcomes["sent"] + bus.outcomes["queued"] for bus in fleet),
        "queued_while_offline": sum(bus.outcomes["queued"] for bus in fleet),
        "dropped_by_publisher": sum(bus.outcomes["dropped"] for bus in fleet),
        "received": report["arrivals"], "unique": report["unique"], "duplicates": report["duplicates"],
        "lost": report["lost"], "late": report["late"], "unacknowledged_at_close": unacknowledged,
        "lost_messages": {bus: seqs for bus, seqs in report["missing"].items()},
        "lamport_violations": receiver.lamport_violations,
        "per_bus_order_preserved": report["late"] == 0,
        "delay_ms": {"p50": _round(percentile(delays, 0.50)), "p95": _round(percentile(delays, 0.95)),
                      "p99": _round(percentile(delays, 0.99)), "max": _round(max(delays) if delays else None),
                      "mean": _round(statistics.fmean(delays) if delays else None)},
        "throughput_msgs_per_s": round(report["unique"] / elapsed, 1) if elapsed > 0 else None,
        "reconnects": sum(s["reconnects"] for s in stats), "disconnects": sum(s["disconnects"] for s in stats),
        "events": [(name, round(at, 2)) for name, at in events],
    }


def _round(value):
    return None if value is None else round(value, 2)


def table(results):
    header = ("| Scenario | Buses | Offered msg/s | Sent | Received | Duplicates | Lost | Late | "
              "Lamport errors | p50 ms | p95 ms | p99 ms | Max ms | Throughput msg/s | Reconnects |")
    lines = [header, "|" + "---|" * (header.count("|") - 1)]
    for r in results:
        d = r["delay_ms"]
        lines.append(f"| {r['scenario']} | {r['buses']} | {r['offered_msgs_per_s']} | {r['published']} | "
                     f"{r['received']} | {r['duplicates']} | {r['lost']} | {r['late']} | {r['lamport_violations']} | "
                     f"{d['p50']} | {d['p95']} | {d['p99']} | {d['max']} | {r['throughput_msgs_per_s']} | "
                     f"{r['reconnects']} |")
    return "\n".join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--host", default=os.getenv("MQTT_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    parser.add_argument("--buses", type=int, nargs="+", default=[5, 10, 25, 50])
    parser.add_argument("--scenarios", nargs="+", choices=SCENARIOS, default=["normal"])
    parser.add_argument("--rate", type=float, default=5.0, help="messages per second per bus")
    parser.add_argument("--duration", type=float, default=10.0, help="seconds of publishing per run")
    parser.add_argument("--delay-ms", type=float, default=200.0)
    parser.add_argument("--outage-s", type=float, default=3.0)
    parser.add_argument("--output", help="write JSON results here")
    args = parser.parse_args(argv)
    results = []
    for scenario in args.scenarios:
        for buses in args.buses:
            print(f"running {scenario} with {buses} buses ...", flush=True)
            results.append(run_once(args.host, args.port, buses, scenario=scenario, rate=args.rate,
                                    duration=args.duration, delay_ms=args.delay_ms, outage_s=args.outage_s))
    print(table(results))
    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps({
            "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "environment": {"python": platform.python_version(), "platform": platform.platform(),
                             "paho": getattr(mqtt, "__version__", None) or "2.x", "broker": f"{args.host}:{args.port}"},
            "settings": vars(args), "results": results}, indent=2) + "\n", encoding="utf-8")
    return results


if __name__ == "__main__":
    main()

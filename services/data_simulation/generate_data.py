"""Generate Zach's movement/scenarios using the shared telemetry field names."""

import argparse
from datetime import datetime, timezone
import json
import math
import random
import sys
import time

from services.data_simulation import export_routes
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network

SPEED_KMH = 30
UPDATE_SECONDS = 1
MAX_UPDATES = 100
CAPACITY = 50


def load_stops(route_id, direction, *, network=None, network_path=DEFAULT_NETWORK_PATH):
    return export_routes.main(route_id, direction, network=network, network_path=network_path)


def distance_metres(start, end):
    # Keep the existing movement calculation; the new network supplies the points.
    north = math.radians(end["lat"] - start["lat"]) * 6371000
    east = math.radians(end["lon"] - start["lon"]) * 6371000
    east *= math.cos(math.radians((start["lat"] + end["lat"]) / 2))
    return math.hypot(north, east)


def generate_messages(route_id, direction, bus_id=None, demand="off_peak",
                      traffic_factor=1.0, sensor_failure_rate=0.0, *,
                      network_path=DEFAULT_NETWORK_PATH, max_updates=MAX_UPDATES,
                      update_seconds=UPDATE_SECONDS, utc_now=None):
    """Yield standard messages so stdout, MQTT and tests use identical data."""
    if demand not in ("peak", "off_peak"):
        raise ValueError("Demand must be peak or off_peak")
    if not 0 < traffic_factor <= 1 or not math.isfinite(traffic_factor):
        raise ValueError("Traffic factor must be greater than 0 and at most 1")
    if not 0 <= sensor_failure_rate <= 1:
        raise ValueError("Sensor failure rate must be between 0 and 1")
    if isinstance(max_updates, bool) or not isinstance(max_updates, int) or max_updates < 1:
        raise ValueError("max_updates must be a positive integer")
    if not math.isfinite(update_seconds) or update_seconds < 0:
        raise ValueError("update_seconds must be finite and nonnegative")
    network = load_network(network_path)
    buses = [bus for bus in network["buses"] if bus["route_id"] == route_id
             and (bus_id is None or bus["bus_id"] == bus_id)]
    if not buses:
        raise ValueError("Bus is not registered on the requested route")
    bus = buses[0]
    route = next(route for route in network["routes"] if route["route_id"] == route_id)
    stops = load_stops(route_id, direction, network=network)
    utc_now = utc_now or (lambda: datetime.now(timezone.utc))
    passengers = random.Random(42)
    sensors = random.Random(43)
    speed = SPEED_KMH * traffic_factor
    occupancy = 0
    sequence = 0
    logical_clock = 0
    for index in range(len(stops) - 1):
        start, end = stops[index:index + 2]
        occupancy -= passengers.randint(0, occupancy)
        boarding = passengers.randint(10, 20) if demand == "peak" else passengers.randint(0, 5)
        occupancy = min(bus["capacity"], occupancy + boarding)
        metres_per_update = speed / 3.6 * (update_seconds or UPDATE_SECONDS)
        steps = max(1, math.ceil(distance_metres(start, end) / metres_per_update))
        for step in range(steps + 1):
            if sequence >= max_updates:
                return
            progress = step / steps
            at_stop = step == 0 or step == steps
            sequence += 1
            # This bus generates a real local event. Julian can extend clock
            # receive/update rules when simulator commands are integrated.
            logical_clock += 1
            current = end if step == steps else start
            following = (stops[index + 2] if index + 2 < len(stops) else None) if step == steps else end
            message = {
                "schema_version": "bus-telemetry-v1",
                "bus_id": bus["bus_id"], "city_id": bus["city_id"],
                "region_id": bus["region_id"], "route_id": route_id,
                "latitude": round(start["lat"] + (end["lat"] - start["lat"]) * progress, 6),
                "longitude": round(start["lon"] + (end["lon"] - start["lon"]) * progress, 6),
                "speed_kmh": 0 if at_stop else speed,
                "direction_id": direction, "bus_status": "at_stop" if at_stop else "moving",
                "passengers": occupancy, "capacity": bus["capacity"],
                "sequence_number": sequence, "lamport_clock": logical_clock,
                "timestamp": utc_now().isoformat(), "sensor_status": "healthy",
                "current_stop_id": current["stop_id"], "current_stop_city_id": current["city_id"],
            }
            if following is not None:
                message["next_stop_id"] = following["stop_id"]
                message["next_stop_city_id"] = following["city_id"]
            # Keep Zach's fault behaviour: movement continues but a failed sensor
            # produces no reading. Do not publish invented replacement values.
            if sensors.random() < sensor_failure_rate:
                print(f"{bus['bus_id']}: sensor failure at update {sequence}", file=sys.stderr, flush=True)
            else:
                yield message
            if update_seconds:
                time.sleep(update_seconds)


def main(route_id, direction, bus_id=None, demand="off_peak", traffic_factor=1.0,
         sensor_failure_rate=0.0, *, emit=None, **settings):
    """Keep printing JSON by default; an MQTT publisher can use the same messages."""
    for message in generate_messages(route_id, direction, bus_id, demand, traffic_factor,
                                     sensor_failure_rate, **settings):
        if emit is None:
            print(json.dumps(message, allow_nan=False), flush=True)
        else:
            emit(message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run one configured bus.")
    parser.add_argument("bus_id")
    parser.add_argument("--network", default=str(DEFAULT_NETWORK_PATH))
    parser.add_argument("--updates", type=int, default=MAX_UPDATES)
    parser.add_argument("--interval", type=float, default=UPDATE_SECONDS)
    args = parser.parse_args()
    network = load_network(args.network)
    bus = next((bus for bus in network["buses"] if bus["bus_id"] == args.bus_id), None)
    if bus is None:
        parser.error("Unknown bus ID")
    try:
        main(bus["route_id"], bus.get("direction_id", 0), bus["bus_id"],
             network_path=args.network, max_updates=args.updates, update_seconds=args.interval)
    except KeyboardInterrupt:
        pass

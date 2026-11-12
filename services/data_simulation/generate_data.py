"""Run with: python3 services/data_simulation/generate_data.py"""

import json
import math
import random
import sys
import time

if __package__:
    from . import export_routes
else:
    import export_routes

SPEED_KMH = 30
UPDATE_SECONDS = 1
MAX_UPDATES = 100
CAPACITY = 50


def load_stops(route_id, direction):
    return export_routes.main(route_id, direction)


def distance_metres(start, end):
    north = math.radians(end["lat"] - start["lat"]) * 6371000
    east = math.radians(end["lon"] - start["lon"]) * 6371000
    east *= math.cos(math.radians((start["lat"] + end["lat"]) / 2))
    return math.hypot(north, east)


def main(route_id, direction, bus_id=None, demand="off_peak",
         traffic_factor=1.0, sensor_failure_rate=0.0):
    if demand not in ("peak", "off_peak"):
        raise ValueError("Demand must be peak or off_peak")
    if not 0 < traffic_factor <= 1:
        raise ValueError("Traffic factor must be greater than 0 and at most 1")
    if not 0 <= sensor_failure_rate <= 1:
        raise ValueError("Sensor failure rate must be between 0 and 1")

    stops = load_stops(route_id, direction)
    if len(stops) < 2:
        raise ValueError("Choose a route and direction with at least two stops")

    # Separate random sources keep failures from changing passenger demand.
    passengers = random.Random(42)
    sensors = random.Random(43)
    speed = SPEED_KMH * traffic_factor
    occupancy = 0
    sequence = 0

    for index in range(len(stops) - 1):
        start = stops[index]
        end = stops[index + 1]

        occupancy -= passengers.randint(0, occupancy)
        if demand == "peak":
            boarding = passengers.randint(10, 20)
        else:
            boarding = passengers.randint(0, 5)
        occupancy = min(CAPACITY, occupancy + boarding)

        metres_per_update = speed / 3.6 * UPDATE_SECONDS
        steps = max(1, math.ceil(distance_metres(start, end) / metres_per_update))

        for step in range(steps + 1):
            if sequence >= MAX_UPDATES:
                return

            progress = step / steps
            at_stop = step == 0 or step == steps
            sequence += 1
            message = {
                "lat": round(start["lat"] + (end["lat"] - start["lat"]) * progress, 6),
                "lon": round(start["lon"] + (end["lon"] - start["lon"]) * progress, 6),
                "speed": 0 if at_stop else speed,
                "direction_id": direction,
                "next_stop_id": end["stop_id"],
                "bus_status": "at_stop" if at_stop else "moving",
                "occupancy": occupancy,
                "sequence_number": sequence,
            }
            if bus_id is not None:
                message["bus_id"] = bus_id
            # A failed sensor produces no reading; movement and time continue.
            if sensors.random() < sensor_failure_rate:
                print(f"{bus_id or 'bus'}: sensor failure at update {sequence}",
                      file=sys.stderr, flush=True)
            else:
                print(json.dumps(message), flush=True)
            time.sleep(UPDATE_SECONDS)


if __name__ == "__main__":
    try:
        main("302", 0)
    except KeyboardInterrupt:
        pass  

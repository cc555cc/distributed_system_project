"""Run with: python3 services/data_simulation/generate_data.py"""

import json
import math
import random
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


def main(route_id, direction, bus_id=None):
    stops = load_stops(route_id, direction)
    if len(stops) < 2:
        raise ValueError("Choose a route and direction with at least two stops")

    random.seed(42)
    occupancy = 0
    sequence = 0

    for index in range(len(stops) - 1):
        start = stops[index]
        end = stops[index + 1]

        occupancy -= random.randint(0, occupancy)
        occupancy = min(CAPACITY, occupancy + random.randint(0, 10))

        metres_per_update = SPEED_KMH / 3.6 * UPDATE_SECONDS
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
                "speed": 0 if at_stop else SPEED_KMH,
                "direction_id": direction,
                "next_stop_id": end["stop_id"],
                "bus_status": "at_stop" if at_stop else "moving",
                "occupancy": occupancy,
                "sequence_number": sequence,
            }
            if bus_id is not None:
                message["bus_id"] = bus_id
            print(json.dumps(message), flush=True)
            time.sleep(UPDATE_SECONDS)


if __name__ == "__main__":
    try:
        main("302", 0)
    except KeyboardInterrupt:
        pass  

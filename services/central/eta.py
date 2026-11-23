import math

# functions for calculating eta.

def route_arrival(stops, state, target_stop, *, cruise_speed=None, dwell_seconds=3.0, dwell_remaining=0.0):
    """Follow ordered route segments; missing speed/position never becomes a guessed ETA."""
    ids = [stop["stop_id"] for stop in stops]
    metadata = state.get("edge_metadata") or {}
    current = metadata.get("current_stop_id", state.get("current_stop_id"))
    if current not in ids or target_stop not in ids:
        return None
    index, target = ids.index(current), ids.index(target_stop)
    if target < index:
        return None
    offset = distance_km(state["lat"], state["lon"], stops[index]["lat"], stops[index]["lon"])
    speed = state.get("speed", 0)
    if target == index:
        return 0.0 if offset <= 0.005 and speed == 0 else None
    speed = speed if isinstance(speed, (int, float)) and speed > 0 else cruise_speed
    if speed is None or not math.isfinite(speed) or speed <= 0:
        return None
    distance = distance_km(state["lat"], state["lon"], stops[index + 1]["lat"], stops[index + 1]["lon"])
    distance += sum(distance_km(a["lat"], a["lon"], b["lat"], b["lon"]) for a, b in zip(stops[index + 1:target], stops[index + 2:target + 1]))
    return distance / speed * 60 + (dwell_remaining + max(0, target - index - 1) * dwell_seconds) / 60


def riding_minutes(stops, origin_index, destination_index, speed, dwell_seconds=3.0):
    distance = sum(distance_km(a["lat"], a["lon"], b["lat"], b["lon"]) for a, b in zip(stops[origin_index:destination_index], stops[origin_index + 1:destination_index + 1]))
    return distance / speed * 60 + (destination_index - origin_index) * dwell_seconds / 60


class ArrivalEvaluation:
    """Compare issued next-stop estimates with observed arrivals, using source time."""
    def __init__(self):
        from collections import deque
        self.forecasts = {}
        self.errors = deque(maxlen=100)

    def observe(self, context):
        from services.central.journeys import vehicles, arrival
        fleet = {bus["bus_id"]: bus for bus in vehicles(context, None)}
        for bus_id in list(self.forecasts):
            if bus_id not in fleet:
                del self.forecasts[bus_id]  # a data outage invalidates the comparison
        for bus_id, bus in fleet.items():
            old = self.forecasts.get(bus_id)
            current = (bus["state"].get("edge_metadata") or {}).get("current_stop_id")
            if old and current == old["stop_id"] and bus["state"]["speed"] == 0:
                self.errors.append(dict(bus_id=bus_id, stop_id=current,
                    error_seconds=context["time"] - old["predicted_at"], observed_at=context["time"]))
                del self.forecasts[bus_id]
            target = bus["state"].get("next_stop_id")
            if target and bus_id not in self.forecasts:
                eta = arrival(bus, target)
                if eta is not None:
                    self.forecasts[bus_id] = dict(stop_id=target, predicted_at=context["time"] + eta*60)

    def view(self):
        return dict(sample_count=len(self.errors), mean_absolute_error_seconds=
            sum(abs(item["error_seconds"]) for item in self.errors)/len(self.errors) if self.errors else None,
            observation_resolution_seconds=2, recent=list(self.errors)[-10:])


# get the location of a bus in a route, use its lat and lon to estimate the eta of the rest of the
# stops in that direction
# haversine helper function to give real distance between two GPS points
EARTH_RADIUS_KM = 6371


def distance_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    x = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(x))

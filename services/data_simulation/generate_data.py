"""Generate Zach's movement/scenarios using the shared telemetry field names."""

import argparse
import json
import math
import random
import sys
import time
from datetime import UTC, datetime

from services.data_simulation import export_routes
from services.messaging.lamport import LamportClock
from shared.schema.health import SENSORS
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network

SPEED_KMH = 30
UPDATE_SECONDS = 1
MAX_UPDATES = 100
CAPACITY = 50


def load_stops(route_id, direction, *, network=None, network_path=DEFAULT_NETWORK_PATH):
    return export_routes.main(
        route_id, direction, network=network, network_path=network_path
    )


def distance_metres(start, end):
    # Keep the existing movement calculation; the new network supplies the points.
    north = math.radians(end["lat"] - start["lat"]) * 6371000
    east = math.radians(end["lon"] - start["lon"]) * 6371000
    east *= math.cos(math.radians((start["lat"] + end["lat"]) / 2))
    return math.hypot(north, east)


def generate_messages(
    route_id,
    direction,
    bus_id=None,
    demand="off_peak",
    traffic_factor=1.0,
    sensor_failure_rate=0.0,
    *,
    network_path=DEFAULT_NETWORK_PATH,
    max_updates=MAX_UPDATES,
    update_seconds=UPDATE_SECONDS,
    utc_now=None,
    sensor_health=None,
    clock=None,
    controls=None,
    start_stop_index=0,
):
    """Yield standard messages so stdout, MQTT and tests use identical data.

    start_stop_index lets a second bus in the same direction begin partway along
    the route instead of on top of the first one.

    ``controls`` (a services.messaging.commands.BusControls) lets simulation
    commands change a running bus: pause/resume take effect before the next
    reading; demand and traffic apply from the next stop segment; the sensor
    failure rate applies from the next reading.
    """
    if demand not in ("peak", "off_peak"):
        raise ValueError("Demand must be peak or off_peak")
    if not 0 < traffic_factor <= 1 or not math.isfinite(traffic_factor):
        raise ValueError("Traffic factor must be greater than 0 and at most 1")
    if not 0 <= sensor_failure_rate <= 1:
        raise ValueError("Sensor failure rate must be between 0 and 1")
    if (
        isinstance(max_updates, bool)
        or not isinstance(max_updates, int)
        or max_updates < 1
    ):
        raise ValueError("max_updates must be a positive integer")
    if not math.isfinite(update_seconds) or update_seconds < 0:
        raise ValueError("update_seconds must be finite and nonnegative")
    network = load_network(network_path)
    buses = [
        bus
        for bus in network["buses"]
        if bus["route_id"] == route_id and (bus_id is None or bus["bus_id"] == bus_id)
    ]
    if not buses:
        raise ValueError("Bus is not registered on the requested route")
    bus = buses[0]
    stops = load_stops(route_id, direction, network=network)
    if (
        isinstance(start_stop_index, bool)
        or not isinstance(start_stop_index, int)
        or not 0 <= start_stop_index < len(stops) - 1
    ):
        raise ValueError(
            "start_stop_index must leave at least one stop ahead on the route"
        )
    utc_now = utc_now or (lambda: datetime.now(UTC))
    passengers = random.Random(42)
    sensors = random.Random(43)
    speed = SPEED_KMH * traffic_factor
    occupancy = 0
    sequence = 0
    # Pass a clock in when something else in this bus process also receives
    # messages (commands): its receive() calls then raise later send stamps.
    clock = clock if clock is not None else LamportClock()
    for index in range(start_stop_index, len(stops) - 1):
        start, end = stops[index : index + 2]
        if controls is not None:
            demand, speed = controls.demand, SPEED_KMH * controls.traffic_factor
        occupancy -= passengers.randint(0, occupancy)
        boarding = (
            passengers.randint(10, 20) if demand == "peak" else passengers.randint(0, 5)
        )
        occupancy = min(bus["capacity"], occupancy + boarding)
        metres_per_update = speed / 3.6 * (update_seconds or UPDATE_SECONDS)
        steps = max(1, math.ceil(distance_metres(start, end) / metres_per_update))
        for step in range(steps + 1):
            if sequence >= max_updates:
                return
            # A paused bus generates nothing; it resumes from the same point.
            if controls is not None and not controls.wait_while_paused():
                return
            progress = step / steps
            at_stop = step == 0 or step == steps
            sequence += 1
            # Send rule: every reading is a new local event on this bus.
            logical_clock = clock.tick()
            current = end if step == steps else start
            following = (
                (stops[index + 2] if index + 2 < len(stops) else None)
                if step == steps
                else end
            )
            message = {
                "schema_version": "bus-telemetry-v1",
                "bus_id": bus["bus_id"],
                "city_id": bus["city_id"],
                "region_id": bus["region_id"],
                "route_id": route_id,
                "latitude": round(
                    start["lat"] + (end["lat"] - start["lat"]) * progress, 6
                ),
                "longitude": round(
                    start["lon"] + (end["lon"] - start["lon"]) * progress, 6
                ),
                "speed_kmh": 0 if at_stop else speed,
                "direction_id": direction,
                "bus_status": "at_stop" if at_stop else "moving",
                "passengers": occupancy,
                "capacity": bus["capacity"],
                "sequence_number": sequence,
                "lamport_clock": logical_clock,
                "timestamp": utc_now().isoformat(),
                "sensor_status": "healthy",
                "current_stop_id": current["stop_id"],
                "current_stop_city_id": current["city_id"],
            }
            if following is not None:
                message["next_stop_id"] = following["stop_id"]
                message["next_stop_city_id"] = following["city_id"]
            # Keep Zach's fault behaviour: movement continues but a failed sensor
            # produces no reading. Do not publish invented replacement values.
            if controls is not None:
                sensor_failure_rate = controls.sensor_failure_rate
            failed = sensors.random() < sensor_failure_rate
            if sensor_health is not None:
                sensor_health(
                    {sensor: "faulty" if failed else "healthy" for sensor in SENSORS}
                )
            if failed:
                print(
                    f"{bus['bus_id']}: sensor failure at update {sequence}",
                    file=sys.stderr,
                    flush=True,
                )
            else:
                yield message
            if update_seconds:
                time.sleep(update_seconds)


class DemoFleet:
    """Repeatable UI conditions, using the coordinator's simulation seconds.

    This is a controlled demonstration, not evidence of a real OS process crash.
    Local GPS, received heartbeats and accepted telemetry are tracked separately.
    """

    CONDITIONS = frozenset({"normal", "stopped", "traffic", "peak", "sensor_fault",
                            "invalid_reading", "sensors_stopped", "connection_loss",
                            "process_failure", "silence", "out_of_service"})
    TIMEOUT = 6.0
    RETRY = 2.0

    def __init__(self, network_path, definitions, now, event):
        self.event = event
        self.buses = {}
        for bus in definitions:
            stops = load_stops(bus["route_id"], bus.get("direction_id", 0), network_path=network_path)
            # Keep configured starting positions so a fleet does not stack at a terminal.
            start = min(bus.get("scenario", {}).get("start_stop_index", 0), len(stops) - 2)
            template = next(generate_messages(bus["route_id"], bus.get("direction_id", 0),
                network_path=network_path, max_updates=1, update_seconds=0))
            template.update(bus_id=bus["bus_id"], capacity=bus["capacity"])
            self.buses[bus["bus_id"]] = dict(definition=bus, stops=stops, segment=start,
                metres=0.0, distance_m=0.0, condition="normal", speed=0.0, dwell=0.0,
                sequence=0, template=template, local=None, heartbeat=None, valid=None,
                report_at=now, pending=None, retry_at=now, attempts=0, rejected=0,
                recovery_until=now, started=now)

    def configure(self, bus_id, condition, now):
        if bus_id not in self.buses or condition not in self.CONDITIONS:
            raise ValueError("Choose a registered bus and supported condition")
        bus = self.buses[bus_id]
        previous = bus["condition"]
        bus["condition"] = condition
        if condition == "normal" and previous != "normal":
            bus["recovery_until"] = now + 2
        self.event(dict(time=now, action="condition", bus_id=bus_id,
                        condition=condition, evidence="operator_injected"))

    def _move(self, bus, seconds):
        if bus["condition"] in {"stopped", "process_failure", "out_of_service"}:
            bus["speed"] = 0.0
            return
        remaining = max(0.0, seconds - bus["dwell"])
        bus["dwell"] = max(0.0, bus["dwell"] - seconds)
        speed = SPEED_KMH * (0.3 if bus["condition"] == "traffic" else 1.0)
        bus["speed"] = speed if remaining else 0.0
        while remaining > 1e-9 and bus["segment"] < len(bus["stops"]) - 1:
            start, end = bus["stops"][bus["segment"]:bus["segment"] + 2]
            length = distance_metres(start, end)
            travel = min(speed / 3.6 * remaining, max(0.0, length - bus["metres"]))
            bus["metres"] += travel
            bus["distance_m"] += travel
            remaining -= travel / (speed / 3.6)
            if bus["metres"] + 1e-8 < length:
                break
            bus["segment"] += 1
            bus["metres"] = 0.0
            # Three modeled seconds at each stop, even at accelerated clock speeds.
            wait = min(3.0, remaining)
            remaining -= wait
            bus["dwell"] = 3.0 - wait
            bus["speed"] = 0.0 if bus["dwell"] else speed
        if bus["segment"] == len(bus["stops"]) - 1:
            bus["speed"] = 0.0

    def phase(self, bus):
        if bus["condition"] == "process_failure":
            return "unknown"
        if bus["condition"] == "out_of_service":
            return "out_of_service"
        if bus["condition"] == "stopped" or bus["segment"] == len(bus["stops"]) - 1:
            return "stopped"
        if bus["dwell"] > 0:
            return "stopped" if bus["dwell"] > 2 else "boarding" if bus["dwell"] > 1 else "departing"
        start, end = bus["stops"][bus["segment"]:bus["segment"] + 2]
        return "arriving" if distance_metres(start, end) - bus["metres"] <= 15 else "travelling"

    def _reading(self, bus):
        stops, index = bus["stops"], bus["segment"]
        current = stops[index]
        following = stops[min(index + 1, len(stops) - 1)]
        length = distance_metres(current, following)
        progress = bus["metres"] / length if length else 0.0
        bus["sequence"] += 1
        source = dict(bus["template"], sequence_number=bus["sequence"],
            lamport_clock=bus["sequence"], timestamp=datetime.now(UTC).isoformat(),
            latitude=round(current["lat"] + (following["lat"] - current["lat"]) * progress, 6),
            longitude=round(current["lon"] + (following["lon"] - current["lon"]) * progress, 6),
            speed_kmh=bus["speed"], bus_status=self.phase(bus),
            current_stop_id=current["stop_id"], current_stop_city_id=current["city_id"],
            passengers=min(bus["definition"]["capacity"], 0 if bus["condition"] == "out_of_service" else 40 if bus["condition"] == "peak" else 10))
        source.pop("next_stop_id", None)
        source.pop("next_stop_city_id", None)
        if index + 1 < len(stops):
            source.update(next_stop_id=following["stop_id"], next_stop_city_id=following["city_id"])
        return source

    def step(self, seconds, now, submit):
        for bus_id, bus in self.buses.items():
            self._move(bus, seconds)
            if now + 1e-9 < bus["report_at"]:
                continue
            bus["report_at"] = now + 1
            condition = bus["condition"]
            if condition == "process_failure":
                continue
            source = self._reading(bus)
            if condition != "sensors_stopped":
                bus["local"] = source
            if condition not in {"connection_loss", "sensors_stopped", "silence"}:
                bus["heartbeat"] = now
            if condition in {"sensors_stopped", "silence"}:
                continue
            if condition == "sensor_fault":
                source = dict(source, sensor_status="faulty")
            if condition == "invalid_reading":
                source = dict(source, latitude=999.0)
            bus["pending"] = source  # latest reading only, not an unbounded history
            if now + 1e-9 < bus["retry_at"]:
                continue
            bus["attempts"] += 1
            result = {"accepted": False, "reason": "simulated connection unavailable"}
            if condition != "connection_loss":
                result = submit(source)
            if result["accepted"]:
                bus["pending"] = None
                if condition != "sensor_fault":
                    bus["valid"] = now
                bus["retry_at"] = now
            else:
                bus["rejected"] += condition == "invalid_reading"
                bus["retry_at"] = now + self.RETRY
            if not result["accepted"] or condition in {"invalid_reading", "sensor_fault"}:
                self.event(dict(time=now, action="delivery_attempt", bus_id=bus_id,
                    accepted=result["accepted"], reason=result.get("reason", "faulty_sensor_not_forwardable")))

    def snapshot(self, now):
        result = {}
        for bus_id, bus in self.buses.items():
            age = now - (bus["heartbeat"] if bus["heartbeat"] is not None else bus["started"])
            condition = bus["condition"]
            status = "unavailable" if age >= self.TIMEOUT else "unknown" if bus["heartbeat"] is None else "healthy"
            if status == "healthy":
                if condition in {"sensor_fault", "invalid_reading"}:
                    status = "degraded"
                elif now < bus["recovery_until"]:
                    status = "recovering"
            sensors = {name: "healthy" for name in SENSORS}
            if condition == "sensor_fault":
                sensors["occupancy"] = "faulty"
            if condition == "sensors_stopped":
                sensors = dict.fromkeys(SENSORS, "unavailable" if age >= self.TIMEOUT else "unknown")
            evidence = {
                "connection_loss": "injected_connection_loss; local GPS continues",
                "process_failure": "injected_process_failure; no local updates",
                "sensor_fault": "occupancy reports faulty; GPS and heartbeat continue",
                "invalid_reading": "reading rejected by validator; sensor failure unconfirmed",
                "sensors_stopped": "all sensor channels silent; no crash inferred",
                "silence": "cause unknown; no received heartbeat",
            }.get(condition, "valid readings and heartbeat" if bus["heartbeat"] is not None else "awaiting first heartbeat")
            result[bus_id] = dict(condition=condition, phase=self.phase(bus), status=status, evidence=evidence,
                sensors=sensors, heartbeat_age_seconds=age, distance_m=bus["distance_m"],
                local=bus["local"], attempts=bus["attempts"], rejected_readings=bus["rejected"],
                delivery_pending=bus["pending"] is not None, retry_at=bus["retry_at"],
                region_id=bus["definition"]["region_id"], route_id=bus["definition"]["route_id"])
        return result


def main(
    route_id,
    direction,
    bus_id=None,
    demand="off_peak",
    traffic_factor=1.0,
    sensor_failure_rate=0.0,
    *,
    emit=None,
    **settings,
):
    """Keep printing JSON by default; an MQTT publisher can use the same messages."""
    for message in generate_messages(
        route_id,
        direction,
        bus_id,
        demand,
        traffic_factor,
        sensor_failure_rate,
        **settings,
    ):
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
        main(
            bus["route_id"],
            bus.get("direction_id", 0),
            bus["bus_id"],
            network_path=args.network,
            max_updates=args.updates,
            update_seconds=args.interval,
        )
    except KeyboardInterrupt:
        pass

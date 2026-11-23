"""Central watches edge heartbeats independently; failed edges cannot report themselves."""

import time
import psycopg
from collections import Counter, deque
from copy import deepcopy
from datetime import UTC, datetime
from threading import RLock

from fastapi import APIRouter, Query

from services.messaging.topics import edge_heartbeat
from shared.schema.health import (
    SENSORS,
    EdgeHeartbeat,
    HealthPolicy,
    HeartbeatTracker,
    classify_bus_health,
    edge_workload_status,
    policy_from_environment,
    sensor_health_status,
)
from shared.schema.network import load_network


class EventLog:
    """Recent central events, newest last. In memory; a central restart clears it."""

    def __init__(self, maxlen=200):
        self.events = deque(
            maxlen=maxlen
        )  # oldest events drop off automatically once full
        self.lock = RLock()

    def append(self, event_type: str, **fields) -> None:
        # wall-clock UTC is for people reading the log; timeout logic keeps using the monotonic clock
        event = {"time": datetime.now(UTC).isoformat(), "type": event_type, **fields}
        with self.lock:
            self.events.append(event)

    def recent(self, limit: int = 50) -> list[dict]:
        with self.lock:
            return list(reversed(self.events))[:limit]  # newest first


class CentralHealthMonitor:
    def __init__(
        self,
        network,
        policy: HealthPolicy,
        *,
        clock=time.monotonic,
        events=None,
        ownership=None,
    ):
        self.policy, self.clock = policy, clock
        self.events = events or EventLog()
        self.ownership = ownership  # live BusOwnership; None = use the static owners from the network file
        self.last_status = {}  # edge_id -> status seen by the previous poll()
        self.expected = {}
        self.owners = {
            bus["bus_id"]: (bus["city_id"], bus["region_id"], bus["edge_id"])
            for bus in network["buses"]
        }
        for region in network["regions"]:
            for field in ("primary_edge", "backup_edge"):
                key = (region["city_id"], region["region_id"], region[field])
                self.expected[region[field]] = key
        self.tracker = HeartbeatTracker(
            self.expected,
            policy.edge_timeout_seconds,
            policy.recovery_seconds,
            clock=clock,
        )
        self.reports, self.received_at, self.overloaded_since = {}, {}, {}
        self.lock = RLock()

    def receive(self, topic, payload) -> bool:
        if len(payload) > 1024 * 1024:
            return False
        try:
            heartbeat = EdgeHeartbeat.model_validate_json(payload, strict=True)
        except ValueError:
            return False
        key = (heartbeat.city_id, heartbeat.region_id, heartbeat.edge_id)
        if self.expected.get(heartbeat.edge_id) != key or topic != edge_heartbeat(
            heartbeat.region_id, heartbeat.edge_id
        ):
            return False
        # Liveness must not depend on ownership being in sync: during a handoff the edge may
        # report its old or new bus set for a moment. Accept any set of buses from the edge's
        # own region, as long as the report is internally consistent.
        region_buses = {
            bus_id for bus_id, owner in self.owners.items() if owner[:2] == key[:2]
        }
        supplied = [bus.bus_id for bus in heartbeat.buses]
        if (
            len(set(supplied)) != len(supplied)
            or not set(supplied) <= region_buses
            or heartbeat.workload.assigned_buses != len(supplied)
        ):
            return False
        if any(set(bus.sensors) != set(SENSORS) for bus in heartbeat.buses):
            return False
        with self.lock:
            if not self.tracker.record(
                heartbeat.edge_id, heartbeat.session_id, heartbeat.sequence_number
            ):
                return False
            now = self.clock()
            # A gap/restart cannot count toward sustained overload.
            previous = self.received_at.get(heartbeat.edge_id)
            old_report = self.reports.get(heartbeat.edge_id)
            restarted = (
                old_report is not None
                and old_report["session_id"] != heartbeat.session_id
            )
            if (
                previous is None
                or restarted
                or now - previous >= self.policy.edge_timeout_seconds
            ):
                self.overloaded_since.pop(heartbeat.edge_id, None)
            if (
                heartbeat.workload.queue_utilization_percent
                >= self.policy.overload_percent
            ):
                self.overloaded_since.setdefault(heartbeat.edge_id, now)
            else:
                self.overloaded_since.pop(heartbeat.edge_id, None)
            self.received_at[heartbeat.edge_id] = now
            self.reports[heartbeat.edge_id] = heartbeat.model_dump(mode="json")
            return True

    def snapshot(self) -> dict:
        with self.lock:
            now = self.clock()
            edges = []
            buses = []
            for edge_id, (city, region, _) in sorted(self.expected.items()):
                status = self.tracker.report(edge_id)
                report = deepcopy(self.reports.get(edge_id))
                if report is not None:
                    # Sensor reports age while travelling to / waiting at central.
                    elapsed = max(0.0, now - self.received_at[edge_id])
                    for bus in report["buses"]:
                        bus["source_edge_status"] = status["status"]
                        if bus["valid_telemetry_age_seconds"] is not None:
                            bus["valid_telemetry_age_seconds"] += elapsed
                        for sensor in bus["sensors"].values():
                            if sensor["age_seconds"] is not None:
                                sensor["age_seconds"] += elapsed
                                if (
                                    sensor["age_seconds"]
                                    >= self.policy.sensor_timeout_seconds
                                ):
                                    sensor["status"] = "unavailable"
                            if status["status"] == "unavailable":
                                sensor["status"] = "unknown"
                # List each bus once, under the edge that owns it now. A bus the owner has not
                # reported yet (e.g. just handed over) appears as unknown instead of disappearing.
                reported = (
                    {bus["bus_id"]: bus for bus in report["buses"]}
                    if report is not None
                    else {}
                )
                owned_now = sorted(
                    bus_id
                    for bus_id, owner in self.owners.items()
                    if owner[:2] == (city, region) and self._owner(bus_id) == edge_id
                )
                reports = [
                    reported.get(bus_id)
                    or {
                        "bus_id": bus_id,
                        "valid_telemetry_age_seconds": None,
                        "last_speed_kmh": None,
                        "sensors": {
                            name: {
                                "status": "unknown",
                                "reported_state": "unknown",
                                "age_seconds": None,
                            }
                            for name in SENSORS
                        },
                    }
                    for bus_id in owned_now
                ]
                for bus in reports:
                    bus["health"] = classify_bus_health(
                        bus, status["status"], self.policy.sensor_timeout_seconds
                    )
                    for sensor in bus["sensors"].values():
                        sensor["health_status"] = sensor_health_status(
                            sensor, status["status"], self.policy.sensor_timeout_seconds
                        )
                    buses.append(
                        dict(
                            edge_id=edge_id,
                            city_id=city,
                            region_id=region,
                            **deepcopy(bus),
                        )
                    )
                since = self.overloaded_since.get(edge_id)
                utilization = (
                    report["workload"]["queue_utilization_percent"] if report else None
                )
                workload_status = edge_workload_status(
                    status["status"],
                    utilization,
                    since is not None and now - since >= self.policy.sustained_seconds,
                    self.policy.overload_percent,
                )
                edges.append(
                    dict(
                        edge_id=edge_id,
                        city_id=city,
                        region_id=region,
                        **status,
                        workload_status=workload_status,
                        last_report=report,
                    )
                )
            return {
                "edges": edges,
                "buses": buses,
                "recommendations": self.recommendations(edges, now),
            }

    def recommendations(self, edges, now):
        """Least-loaded healthy peer selection; no migration or instance creation."""
        choices = []
        for source in edges:
            since = self.overloaded_since.get(source["edge_id"])
            if (
                source["status"] != "healthy"
                or since is None
                or now - since < self.policy.sustained_seconds
            ):
                continue
            peers = [
                edge
                for edge in edges
                if edge["edge_id"] != source["edge_id"]
                and (edge["city_id"], edge["region_id"])
                == (source["city_id"], source["region_id"])
                and edge["status"] == "healthy"
                and edge["last_report"] is not None
                and edge["last_report"]["workload"]["queue_utilization_percent"]
                < self.policy.overload_percent
            ]
            target = (
                min(
                    peers,
                    key=lambda edge: (
                        edge["last_report"]["workload"]["queue_utilization_percent"],
                        edge["edge_id"],
                    ),
                )
                if peers
                else None
            )
            choices.append(
                {
                    "algorithm": "least_queue_utilization",
                    "from_edge_id": source["edge_id"],
                    "to_edge_id": target["edge_id"] if target else None,
                    "action": "consider_migration"
                    if target
                    else "additional_capacity_needed",
                    "migration_performed": False,
                }
            )
        return choices

    def _owner(self, bus_id):
        if self.ownership is not None:
            return self.ownership.owner(bus_id)
        return self.owners[bus_id][2]

    def statuses(self) -> dict[str, str]:
        """Current liveness status of every expected edge (healthy/recovering/unavailable/unknown)."""
        return {
            edge_id: self.tracker.report(edge_id)["status"] for edge_id in self.expected
        }

    def poll(self) -> None:
        """Called every second by central. An edge going silent produces no message,
        so status changes can only be noticed by checking regularly."""
        with self.lock:
            for edge_id in sorted(self.expected):
                status = self.tracker.report(edge_id)["status"]
                previous = self.last_status.get(edge_id)
                if previous is not None and previous != status:
                    city, region, _ = self.expected[edge_id]
                    self.events.append(
                        "edge_status",
                        edge_id=edge_id,
                        city_id=city,
                        region_id=region,
                        from_status=previous,
                        to_status=status,
                    )
                self.last_status[edge_id] = status


router = APIRouter(prefix="/health", tags=["health"])
# Built once per central process. A central restart intentionally forgets old liveness.
from services.central.ownership import bus_ownership

monitor = CentralHealthMonitor(
    load_network(), policy_from_environment(), ownership=bus_ownership
)


@router.get("/edges")
def get_edge_health():
    return monitor.snapshot()


# recent edge status changes, newest first
@router.get("/events")
def get_events(limit: int = Query(default=50, ge=1, le=200)):
    return monitor.events.recent(limit)


# UI experiments have their own queues and virtual clock. They do not publish
# telemetry, start OS processes or alter the live ownership registry.
from fastapi import HTTPException
from typing import Literal

from services.edge.main import EdgeClusterSimulation

simulation = EdgeClusterSimulation()


class SimulationClock:
    """Use elapsed real seconds once in central, regardless of open browser tabs."""

    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.last = clock()
        self.running = True
        self.speed = 1.0
        self.lock = RLock()

    def tick(self):
        with self.lock:
            now = self.clock()
            elapsed = max(0.0, now - self.last)
            self.last = now
            remaining = elapsed * self.speed if self.running else 0.0
            while remaining > 1e-9:
                step = min(60.0, remaining)
                simulation.advance(step)
                if getattr(simulation, "demo_running", False):
                    from services.central.scheduling import observe_demo
                    observe_demo()
                remaining -= step

    def snapshot(self):
        return {"running": self.running, "speed": self.speed}


simulation_clock = SimulationClock()


def bus_health_snapshot(source="live"):
    """One backend view for bus, sensor and route health; missing data stays unknown."""
    if source == "live":
        definitions = load_network()["buses"]
        reports = {bus["bus_id"]: bus for bus in monitor.snapshot()["buses"]}
        result = []
        for definition in definitions:
            report = reports.get(definition["bus_id"], {})
            result.append(dict(
                bus_id=definition["bus_id"], route_id=definition["route_id"],
                city_id=definition["city_id"], region_id=definition["region_id"],
                edge_id=report.get("edge_id", definition["edge_id"]), source=source,
                health=report.get("health", {"status": "unknown", "reasons": ["no_health_report"]}),
                source_edge_status=report.get("source_edge_status", "unknown"),
                valid_telemetry_age_seconds=report.get("valid_telemetry_age_seconds"),
                sensors={name: deepcopy(report.get("sensors", {}).get(name,
                    {"health_status": "unknown", "age_seconds": None})) for name in SENSORS},
            ))
        return result
    if source != "demo":
        raise ValueError("Choose live or demo")
    with simulation_clock.lock:
        state = simulation.snapshot()
        pending_ids = set(simulation.central_pending)
    workers = {worker["edge_id"]: worker for worker in state["workers"]}
    result = []
    for definition in state["registered_buses"]:
        bus_id = definition["bus_id"]
        bus = state["demo_buses"].get(bus_id)
        owner = state["ownership"][bus_id]["edge_id"]
        edge_status = workers[owner]["state"]
        status = bus["status"] if bus else "unknown"
        reasons = [bus["evidence"]] if bus else ["no_demo_reading"]
        if bus and not state["demo_running"]:
            status, reasons = "paused", ["demonstration_stopped_intentionally"]
        elif edge_status not in ("healthy", "recovering"):
            status, reasons = "unknown", ["source_edge_unavailable; sensor_failure_unconfirmed"]
        if bus and bus.get("reserved"):
            status, reasons = "standby", ["registered_reserve; awaiting_dispatch"]
        age = bus["heartbeat_age_seconds"] if bus else None
        sensors = {}
        for name in SENSORS:
            sensor_status = bus["sensors"][name] if bus else "unknown"
            if status in ("unknown", "paused", "standby"):
                sensor_status = "standby" if status == "standby" else "unknown"
            elif status == "unavailable":
                sensor_status = "unavailable"
            elif status == "recovering" and sensor_status == "healthy":
                sensor_status = "recovering"
            sensors[name] = dict(health_status=sensor_status, age_seconds=age)
        received = state["central_received_at"].get(bus_id)
        result.append(dict(bus_id=bus_id, route_id=definition["route_id"],
            city_id=definition["city_id"], region_id=definition["region_id"], edge_id=owner,
            source=source, health={"status": status, "reasons": reasons},
            source_edge_status=edge_status,
            valid_telemetry_age_seconds=bus["valid_telemetry_age_seconds"] if bus else None,
            central_update_age_seconds=(state["virtual_seconds"] - received if received is not None else None),
            central_available=state["central_available"], central_delivery_pending=bus_id in pending_ids,
            pipeline={"bus_delivery_attempts": bus["attempts"] if bus else 0,
                "bus_rejected_readings": bus["rejected_readings"] if bus else 0,
                "global_edge_accepted": state["accepted"], "global_edge_rejected": state["rejected"],
                "global_processing_outcomes": state["outcomes"], "global_central_acknowledged": state["central_ack_count"],
                "global_central_pending_buses": state["central_pending"], "mapping_contract": "edge-central-v1"},
            sensors=sensors))
    return result


def find_bus_health(bus_id, source="live"):
    bus = next((bus for bus in bus_health_snapshot(source) if bus["bus_id"] == bus_id), None)
    if bus is None:
        raise HTTPException(404, "Bus is not registered in this data source")
    return bus




@router.get("/overview")
def system_overview(source: Literal["demo", "live"] = "demo"):
    """One health response for both operator pages, including unselected faulty buses."""
    with simulation_clock.lock:
        buses = bus_health_snapshot(source)
        if source == "demo":
            state = simulation.snapshot()
            workers = state["workers"]
            events = list(reversed(state["events"]))[:12]
            clock = dict(seconds=state["virtual_seconds"], **simulation_clock.snapshot())
        else:
            workers = []
            for edge in monitor.snapshot()["edges"]:
                report = edge.get("last_report") or {}
                workers.append(dict(edge_id=edge["edge_id"], region_id=edge["region_id"], state=edge["status"],
                                    **report.get("workload", {})))
            events, clock = monitor.events.recent(12), None
        issues = []
        for bus in buses:
            for name, sensor in bus["sensors"].items():
                status = sensor.get("health_status", "unknown")
                if status not in {"healthy", "standby"}:
                    issues.append(dict(bus_id=bus["bus_id"], route_id=bus["route_id"], sensor=name, status=status))
        # Operator ETA uses the same calculation as passenger recommendations.
        from services.central.journeys import transit_context, vehicles, arrival
        arrivals = []
        try:
            for bus in vehicles(transit_context(source), None):
                target = bus["state"].get("next_stop_id")
                estimate = arrival(bus, target) if target else None
                arrivals.append(dict(bus_id=bus["bus_id"], route_id=bus["route_id"],
                    passengers=bus["passengers"], capacity=bus["capacity"], occupancy_percent=bus["occupancy_percent"],
                    next_stop_id=target, eta_minutes=round(estimate, 2) if estimate is not None else None))
        except psycopg.Error:
            pass  # health remains available; no database means no arrival predictions
        return dict(source=source, clock=clock, workers=workers, sensor_issues=issues, arrivals=arrivals,
                    bus_count=len(buses), healthy_buses=sum(b["health"]["status"] in {"healthy", "stationary"} for b in buses),
                    events=events)


def route_health(route_id: str, source: Literal["live", "demo"] = "live"):
    network = load_network()
    if route_id not in {route["route_id"] for route in network["routes"]}:
        raise HTTPException(404, "Unknown route")
    buses = [bus for bus in bus_health_snapshot(source) if bus["route_id"] == route_id]
    notes = []
    if source == "demo":
        with simulation_clock.lock:
            state = simulation.snapshot()
        definitions = state["registered_buses"]
        states = state["central_states"]
        ages = {bus: state["virtual_seconds"] - received for bus, received in state["central_received_at"].items()}
        edges = [dict(edge_id=w["edge_id"], status=w["state"], queue_size=w["queue_size"],
                      queue_capacity=w["queue_capacity"], queue_utilization_percent=w["queue_utilization_percent"],
                      assigned_buses=len(w["assigned_buses"])) for w in state["workers"]]
    else:
        from services.central.routers.buses import get_all_buses

        definitions = network["buses"]
        try:
            rows = get_all_buses(route_id=route_id)
        except psycopg.Error:
            rows = []
            notes.append("Database unavailable; occupancy is not known")
        states = {row["bus_id"]: row for row in rows}
        ages = {row["bus_id"]: (datetime.now(UTC) - row["last_updated"]).total_seconds()
                for row in rows if row.get("last_updated") is not None}
        edges = []
        for edge in monitor.snapshot()["edges"]:
            workload = edge["last_report"]["workload"] if edge["last_report"] else {}
            edges.append(dict(edge_id=edge["edge_id"], status=edge["status"],
                              **{key: workload.get(key) for key in ("queue_size", "queue_capacity", "queue_utilization_percent", "assigned_buses")}))
    configured = {bus["bus_id"]: bus for bus in definitions if bus["route_id"] == route_id}
    samples, available = [], []
    for bus in buses:
        current = states.get(bus["bus_id"], {})
        if (bus["health"]["status"] not in ("healthy", "stationary")
                or ages.get(bus["bus_id"], 30) >= 30
                or current.get("bus_status") in (None, "out_of_service", "idle")):
            continue
        available.append(bus["bus_id"])
        metadata = current.get("edge_metadata") or {}
        passengers, capacity = metadata.get("passengers"), metadata.get("capacity")
        if isinstance(passengers, int) and isinstance(capacity, int) and 0 <= passengers <= capacity and capacity > 0:
            samples.append((passengers, capacity))
    passengers = sum(count for count, _ in samples)
    sample_capacity = sum(capacity for _, capacity in samples)
    owners = {bus["edge_id"] for bus in buses}
    return {"route_id": route_id, "source": source, "bus_count": len(buses),
            "status_counts": dict(Counter(bus["health"]["status"] for bus in buses)),
            "availability": "available" if available else "unknown" if any(bus["health"]["status"] == "unknown" for bus in buses) else "unavailable",
            "available_buses": len(available), "configured_capacity": sum(bus["capacity"] for bus in configured.values()),
            "verified_service_capacity": sum(configured[bus]["capacity"] for bus in available),
            "occupancy_samples": len(samples), "passengers": passengers if samples else None,
            "available_seats_in_samples": sample_capacity - passengers if samples else None,
            "occupancy_percent": passengers / sample_capacity * 100 if sample_capacity else None,
            "edge_workload": [edge for edge in edges if edge["edge_id"] in owners],
            "notes": notes,
            "buses": [{"bus_id": bus["bus_id"], "health": bus["health"]} for bus in buses]}

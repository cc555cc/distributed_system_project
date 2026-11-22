"""Central watches edge heartbeats independently; failed edges cannot report themselves."""

import time
from collections import deque
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
from pydantic import BaseModel, ConfigDict, Field

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
                remaining -= step

    def snapshot(self):
        return {"running": self.running, "speed": self.speed}


simulation_clock = SimulationClock()


class SimulationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class SimulationRequests(SimulationInput):
    bus_id: str = Field(min_length=1)
    count: int = Field(ge=1, le=10000)


class SimulationStep(SimulationInput):
    seconds: float = Field(gt=0, le=60)


class SimulationTiming(SimulationInput):
    running: bool
    speed: float = Field(gt=0, le=10)


class SimulationCharacteristics(SimulationInput):
    capacity: int | None = Field(default=None, ge=1, le=50)
    processing_ms: float | None = Field(default=None, ge=1, le=60000)
    active: bool | None = None


class SimulationPolicy(SimulationInput):
    auto_scale: bool


@router.get("/simulation")
def get_simulation():
    with simulation_clock.lock:
        return {**simulation.snapshot(), "clock": simulation_clock.snapshot()}


@router.patch("/simulation/clock")
def configure_simulation_clock(body: SimulationTiming):
    with simulation_clock.lock:
        simulation_clock.tick()
        simulation_clock.running, simulation_clock.speed = body.running, body.speed
        return get_simulation()


@router.post("/simulation/requests")
def submit_simulation_requests(body: SimulationRequests):
    try:
        return simulation.submit(body.bus_id, body.count)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@router.post("/simulation/advance")
def advance_simulation(body: SimulationStep):
    return simulation.advance(body.seconds)


@router.patch("/simulation/instances/{edge_id}")
def configure_simulation_instance(edge_id: str, body: SimulationCharacteristics):
    try:
        return simulation.configure(edge_id, **body.model_dump(exclude_none=True))
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@router.patch("/simulation/policy")
def configure_simulation_policy(body: SimulationPolicy):
    with simulation.lock:
        simulation.auto_scale = body.auto_scale
        return simulation.snapshot()


class SimulationReset(SimulationInput):
    fleet_size: int | None = Field(default=None, ge=1, le=50)


class SimulationInstance(SimulationInput):
    edge_id: str = Field(min_length=1, max_length=80)
    region_id: str = Field(min_length=1)
    capacity: int = Field(default=50, ge=1, le=50)
    processing_ms: float = Field(default=5.0, ge=1, le=60000)


class SimulationTransfer(SimulationInput):
    bus_id: str
    target_id: str


@router.post("/simulation/reset")
def reset_simulation(body: SimulationReset | None = None):
    global simulation
    with simulation_clock.lock:
        simulation = EdgeClusterSimulation(fleet_size=body.fleet_size if body else None)
        simulation_clock.last = simulation_clock.clock()
        return get_simulation()


def simulation_action(action, *args, **kwargs):
    try:
        return action(*args, **kwargs)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@router.post("/simulation/telemetry")
def submit_telemetry(body: dict):
    return simulation_action(simulation.submit_telemetry, body)


@router.post("/simulation/telemetry/generate")
def generate_telemetry(body: SimulationRequests):
    return simulation_action(simulation.generate, body.bus_id, body.count)


@router.post("/simulation/instances")
def register_instance(body: SimulationInstance):
    return simulation_action(simulation.register, **body.model_dump())


@router.post("/simulation/instances/{edge_id}/fail")
def fail_instance(edge_id: str):
    return simulation_action(simulation.fail, edge_id)


@router.post("/simulation/instances/{edge_id}/recover")
def recover_instance(edge_id: str):
    return simulation_action(simulation.recover, edge_id)


@router.post("/simulation/instances/{edge_id}/scale-down")
def scale_down_instance(edge_id: str):
    return simulation_action(simulation.scale_down, edge_id)


@router.post("/simulation/transfer")
def transfer_bus(body: SimulationTransfer):
    simulation_action(simulation.transfer, body.bus_id, body.target_id)
    return simulation.snapshot()

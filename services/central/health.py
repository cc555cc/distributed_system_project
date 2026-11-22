"""Central watches edge heartbeats independently; failed edges cannot report themselves."""

import time
from copy import deepcopy
from threading import RLock

from fastapi import APIRouter

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


class CentralHealthMonitor:
    def __init__(self, network, policy: HealthPolicy, *, clock=time.monotonic):
        self.policy, self.clock = policy, clock
        self.expected = {}
        self.owners = {bus["bus_id"]: (bus["city_id"], bus["region_id"], bus["edge_id"])
                       for bus in network["buses"]}
        for region in network["regions"]:
            for field in ("primary_edge", "backup_edge"):
                key = (region["city_id"], region["region_id"], region[field])
                self.expected[region[field]] = key
        self.tracker = HeartbeatTracker(self.expected, policy.edge_timeout_seconds, policy.recovery_seconds, clock=clock)
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
        if self.expected.get(heartbeat.edge_id) != key or topic != edge_heartbeat(heartbeat.region_id, heartbeat.edge_id):
            return False
        owned = {bus_id for bus_id, owner in self.owners.items() if owner == key}
        supplied = [bus.bus_id for bus in heartbeat.buses]
        if set(supplied) != owned or len(supplied) != len(owned) or heartbeat.workload.assigned_buses != len(owned):
            return False
        if any(set(bus.sensors) != set(SENSORS) for bus in heartbeat.buses):
            return False
        with self.lock:
            if not self.tracker.record(heartbeat.edge_id, heartbeat.session_id, heartbeat.sequence_number):
                return False
            now = self.clock()
            # A gap/restart cannot count toward sustained overload.
            previous = self.received_at.get(heartbeat.edge_id)
            old_report = self.reports.get(heartbeat.edge_id)
            restarted = old_report is not None and old_report["session_id"] != heartbeat.session_id
            if previous is None or restarted or now - previous >= self.policy.edge_timeout_seconds:
                self.overloaded_since.pop(heartbeat.edge_id, None)
            if heartbeat.workload.queue_utilization_percent >= self.policy.overload_percent:
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
                                if sensor["age_seconds"] >= self.policy.sensor_timeout_seconds:
                                    sensor["status"] = "unavailable"
                            if status["status"] == "unavailable":
                                sensor["status"] = "unknown"
                reports = report["buses"] if report is not None else [
                    {"bus_id": bus_id, "valid_telemetry_age_seconds": None, "last_speed_kmh": None,
                         "sensors": {name: {"status": "unknown", "reported_state": "unknown", "age_seconds": None}
                                  for name in SENSORS}}
                    for bus_id, owner in self.owners.items() if owner == (city, region, edge_id)]
                for bus in reports:
                    bus["health"] = classify_bus_health(bus, status["status"], self.policy.sensor_timeout_seconds)
                    for sensor in bus["sensors"].values():
                        sensor["health_status"] = sensor_health_status(sensor, status["status"], self.policy.sensor_timeout_seconds)
                    buses.append(dict(edge_id=edge_id, city_id=city, region_id=region, **deepcopy(bus)))
                since = self.overloaded_since.get(edge_id)
                utilization = report["workload"]["queue_utilization_percent"] if report else None
                workload_status = edge_workload_status(status["status"], utilization,
                    since is not None and now - since >= self.policy.sustained_seconds, self.policy.overload_percent)
                edges.append(dict(edge_id=edge_id, city_id=city, region_id=region, **status,
                                  workload_status=workload_status, last_report=report))
            return {"edges": edges, "buses": buses, "recommendations": self.recommendations(edges, now)}

    def recommendations(self, edges, now):
        """Least-loaded healthy peer selection; no migration or instance creation."""
        choices = []
        for source in edges:
            since = self.overloaded_since.get(source["edge_id"])
            if source["status"] != "healthy" or since is None or now - since < self.policy.sustained_seconds:
                continue
            peers = [edge for edge in edges if edge["edge_id"] != source["edge_id"]
                     and (edge["city_id"], edge["region_id"]) == (source["city_id"], source["region_id"])
                     and edge["status"] == "healthy" and edge["last_report"] is not None
                     and edge["last_report"]["workload"]["queue_utilization_percent"] < self.policy.overload_percent]
            target = min(peers, key=lambda edge: (edge["last_report"]["workload"]["queue_utilization_percent"], edge["edge_id"])) if peers else None
            choices.append({"algorithm": "least_queue_utilization", "from_edge_id": source["edge_id"],
                                "to_edge_id": target["edge_id"] if target else None,
                                "action": "consider_migration" if target else "additional_capacity_needed",
                                "migration_performed": False})
        return choices


router = APIRouter(prefix="/health", tags=["health"])
# Built once per central process. A central restart intentionally forgets old liveness.
monitor = CentralHealthMonitor(load_network(), policy_from_environment())


@router.get("/edges")
def get_edge_health():
    return monitor.snapshot()


# UI experiments have their own queues and virtual clock. They do not publish
# telemetry, start OS processes or alter the live ownership registry.
from fastapi import HTTPException
from pydantic import BaseModel, ConfigDict, Field

from services.edge.main import EdgeClusterSimulation

simulation = EdgeClusterSimulation()


class SimulationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)


class SimulationRequests(SimulationInput):
    bus_id: str = Field(min_length=1)
    count: int = Field(ge=1, le=10000)


class SimulationStep(SimulationInput):
    seconds: float = Field(gt=0, le=60)


class SimulationCharacteristics(SimulationInput):
    capacity: int | None = Field(default=None, ge=1, le=50)
    processing_ms: float | None = Field(default=None, ge=1, le=60000)
    active: bool | None = None


class SimulationPolicy(SimulationInput):
    auto_scale: bool


@router.get("/simulation")
def get_simulation():
    return simulation.snapshot()


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
    simulation = EdgeClusterSimulation(fleet_size=body.fleet_size if body else None)
    return simulation.snapshot()


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

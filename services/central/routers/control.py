"""Operator simulation commands. Health monitoring remains in central.health."""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from typing import Literal
from services.central import health
from services.edge.main import EdgeClusterSimulation
from shared.schema.network import load_network

# Keep the existing URL so dashboards and saved clients continue working.
router = APIRouter(prefix="/health", tags=["simulation control"])

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


class SimulationDemo(SimulationInput):
    running: bool | None = None
    bus_id: str | None = None
    condition: str | None = None
    central_available: bool | None = None


class BusConfiguration(SimulationInput):
    schema_version: Literal["bus-config-v1"]
    bus_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]+$", min_length=1, max_length=64)
    route_id: str = Field(min_length=1, max_length=64)
    capacity: int = Field(default=50, ge=1, le=200)
    direction_id: int = Field(default=0, ge=0, le=1)


class DispatchRequest(SimulationInput):
    action: Literal["dispatch", "set_headway"]
    route_id: str = Field(min_length=1, max_length=64)
    bus_id: str | None = None
    headway_seconds: float | None = Field(default=None, ge=1, le=3600)


class SimulationCharacteristics(SimulationInput):
    capacity: int | None = Field(default=None, ge=1, le=50)
    processing_ms: float | None = Field(default=None, ge=1, le=60000)
    active: bool | None = None


class SimulationPolicy(SimulationInput):
    auto_scale: bool


@router.get("/simulation")
def get_simulation():
    with health.simulation_clock.lock:
        return {**health.simulation.snapshot(), "clock": health.simulation_clock.snapshot()}


@router.get("/simulation/routes")
def simulation_routes():
    return load_network(health.simulation.network_path)["routes"]


@router.get("/simulation/routes/{route_id}/stops")
def simulation_route_stops(route_id: str):
    from shared.schema.network import route_stops

    network = load_network(health.simulation.network_path)
    try:
        return route_stops(network, route_id, 0) + route_stops(network, route_id, 1)
    except ValueError as error:
        raise HTTPException(404, str(error)) from error


@router.patch("/simulation/clock")
def configure_simulation_clock(body: SimulationTiming):
    with health.simulation_clock.lock:
        health.simulation_clock.tick()
        health.simulation_clock.running, health.simulation_clock.speed = body.running, body.speed
        return get_simulation()


@router.patch("/simulation/demo")
def configure_demo(body: SimulationDemo):
    if (body.bus_id is None) != (body.condition is None):
        raise HTTPException(422, "Provide both bus_id and condition")
    if not body.model_dump(exclude_none=True):
        raise HTTPException(422, "Provide a demo setting")
    with health.simulation_clock.lock:
        health.simulation_clock.tick()
        simulation_action(health.simulation.configure_demo, **body.model_dump(exclude_none=True))
        return get_simulation()


@router.post("/simulation/buses", status_code=201)
def register_simulated_bus(body: BusConfiguration):
    with health.simulation_clock.lock:
        health.simulation_clock.tick()
        simulation_action(health.simulation.register_bus, **body.model_dump(exclude={"schema_version"}))
        return get_simulation()


@router.post("/simulation/schedule")
def schedule_simulated_service(body: DispatchRequest):
    with health.simulation_clock.lock:
        health.simulation_clock.tick()
        simulation_action(health.simulation.schedule, **body.model_dump())
        return get_simulation()


@router.post("/simulation/requests")
def submit_simulation_requests(body: SimulationRequests):
    try:
        return health.simulation.submit(body.bus_id, body.count)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@router.post("/simulation/advance")
def advance_simulation(body: SimulationStep):
    result = health.simulation.advance(body.seconds)
    if getattr(health.simulation, "demo_running", False):
        from services.central.scheduling import observe_demo
        observe_demo()
    return result


@router.patch("/simulation/instances/{edge_id}")
def configure_simulation_instance(edge_id: str, body: SimulationCharacteristics):
    try:
        return health.simulation.configure(edge_id, **body.model_dump(exclude_none=True))
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@router.patch("/simulation/policy")
def configure_simulation_policy(body: SimulationPolicy):
    with health.simulation.lock:
        health.simulation.auto_scale = body.auto_scale
        return health.simulation.snapshot()


class SimulationReset(SimulationInput):
    fleet_size: int | None = Field(default=None, ge=1, le=50)
    active_buses: int | None = Field(default=None, ge=1, le=50)
    processing_ms: float = Field(default=100.0, ge=1, le=60000)


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
    with health.simulation_clock.lock:
        if body and body.active_buses is not None and body.active_buses > (body.fleet_size or 20):
            raise HTTPException(422, "Active buses cannot exceed the fleet limit")
        health.simulation = EdgeClusterSimulation(fleet_size=body.fleet_size if body else None)
        if body:
            health.simulation.fleet_limit = body.fleet_size or 50
            for worker in health.simulation.workers.values():
                worker.processing_ms = body.processing_ms
            if body.active_buses is not None:
                health.simulation.configure_demo()
                for bus in list(health.simulation.demo.buses.values())[body.active_buses:]:
                    health.simulation.demo.configure(bus["definition"]["bus_id"], "out_of_service", health.simulation.time)
                    bus["reserved"] = True
        health.simulation_clock.last = health.simulation_clock.clock()
        return get_simulation()


def simulation_action(operation, *args, **kwargs):
    try:
        return operation(*args, **kwargs)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@router.post("/simulation/telemetry")
def submit_telemetry(body: dict):
    return simulation_action(health.simulation.submit_telemetry, body)


@router.post("/simulation/telemetry/generate")
def generate_telemetry(body: SimulationRequests):
    return simulation_action(health.simulation.generate, body.bus_id, body.count)


@router.post("/simulation/instances")
def register_instance(body: SimulationInstance):
    return simulation_action(health.simulation.register, **body.model_dump())


@router.post("/simulation/instances/{edge_id}/fail")
def fail_instance(edge_id: str):
    return simulation_action(health.simulation.fail, edge_id)


@router.post("/simulation/instances/{edge_id}/recover")
def recover_instance(edge_id: str):
    return simulation_action(health.simulation.recover, edge_id)


@router.post("/simulation/instances/{edge_id}/scale-down")
def scale_down_instance(edge_id: str):
    return simulation_action(health.simulation.scale_down, edge_id)


@router.post("/simulation/transfer")
def transfer_bus(body: SimulationTransfer):
    simulation_action(health.simulation.transfer, body.bus_id, body.target_id)
    return health.simulation.snapshot()


class Workload(SimulationInput):
    region_id: str
    count: int = Field(default=100, ge=1, le=1000)
    processing_ms: float | None = Field(default=None, ge=1, le=60000)


@router.post("/simulation/workload")
def workload(body: Workload):
    """Change service speed and send a region-scoped burst through the real validator."""
    with health.simulation_clock.lock:
        if body.region_id not in health.simulation.regions:
            raise HTTPException(422, "Unknown region")
        if not health.simulation.demo or not health.simulation.demo_running:
            raise HTTPException(422, "Start the demonstration first")
        if body.processing_ms is not None:
            for worker in health.simulation.workers.values():
                if worker.region_id == body.region_id:
                    health.simulation.configure(worker.edge_id, processing_ms=body.processing_ms)
        return simulation_action(health.simulation.request_burst, body.count, body.region_id)

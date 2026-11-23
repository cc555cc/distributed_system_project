"""Create configured edge instances; each runs in its own process and keeps its own state."""

import argparse
import json
import logging
import math
import os
from copy import deepcopy
from multiprocessing import Event, Process

from services.edge.communication.data_to_edge import load_edge_config
from services.edge.config import NetworkConfig
from services.edge.edge_server import EdgeServerInstance
from shared.schema.health import policy_from_environment
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network

logger = logging.getLogger(__name__)


def configured_instances(network_path):
    """The shared map, not hardcoded instance names, controls the initial fleet."""
    instances = {}
    for region in load_network(network_path)["regions"]:
        for field in ("primary_edge", "backup_edge"):
            edge_id = region[field]
            if edge_id in instances:
                raise ValueError("Edge IDs must be globally unique")
            instances[edge_id] = (region["city_id"], region["region_id"], edge_id)
    return instances


def deployment_plan(network_path):
    """EDGE-05: show the map's fixed regional owners before starting workers.

    A region is a configured set of routes, not a GPS polygon. Keep a bus with
    its assigned owner while it travels; crossing a coordinate must not silently
    change ownership. EDGE-08 will add coordinated reassignment later.
    """
    network = load_network(network_path)
    instances = configured_instances(network_path)
    plan = []
    for edge_id, (city, region, _) in instances.items():
        buses = [bus for bus in network["buses"] if bus["edge_id"] == edge_id]
        if any((bus["city_id"], bus["region_id"]) != (city, region) for bus in buses):
            raise ValueError("Bus owner is outside its configured city/region")
        plan.append(
            {
                "edge_id": edge_id,
                "city_id": city,
                "region_id": region,
                "route_ids": sorted({bus["route_id"] for bus in buses}),
                "bus_ids": sorted(bus["bus_id"] for bus in buses),
            }
        )
    return plan


def run_instance(network_path, identity, stop_event):
    """Construct actual worker objects inside the child, never pickle live clients."""
    city_id, region_id, edge_id = identity
    config = load_edge_config(city_id=city_id, region_id=region_id, edge_id=edge_id)
    worker = EdgeServerInstance(
        config,
        NetworkConfig.load(network_path),
        policy_from_environment(),
        os.environ["CENTRAL_OCCUPANCY_UNIT"],
    )
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    worker.run(stop_event)


def run_single_instance(network_path=None):
    """Container/old command compatibility: create only its configured worker."""
    path = network_path or os.getenv("EDGE_NETWORK_CONFIG", str(DEFAULT_NETWORK_PATH))
    instances = configured_instances(path)
    edge_id = os.environ["EDGE_ID"]
    if edge_id not in instances:
        raise ValueError("EDGE_ID is not registered in the shared network")
    identity = instances[edge_id]
    if (
        os.getenv("CITY_ID", identity[0]) != identity[0]
        or os.getenv("REGION_ID", identity[1]) != identity[1]
    ):
        raise ValueError("Edge environment does not match its registered city/region")
    run_instance(path, identity, Event())


def launch_instances(network_path, edge_ids=None):
    """Run all configured workers or a selected subset; do not migrate workloads."""
    deployment_plan(network_path)  # Check regional ownership before spawning any child.
    instances = configured_instances(network_path)
    selected = list(instances) if edge_ids is None else list(dict.fromkeys(edge_ids))
    if any(edge_id not in instances for edge_id in selected):
        raise ValueError("Requested an unregistered edge instance")
    stop = Event()
    processes = []
    reported = set()
    try:
        for edge_id in selected:
            process = Process(
                target=run_instance,
                args=(str(network_path), instances[edge_id], stop),
                name=edge_id,
            )
            process.start()
            processes.append(process)
        while any(process.is_alive() for process in processes):
            for process in processes:
                if process.exitcode is not None and process.name not in reported:
                    logger.warning(
                        "Edge instance exited: %s code=%s",
                        process.name,
                        process.exitcode,
                    )
                    reported.add(process.name)
            stop.wait(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        for process in processes:
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
                process.join()
    # Central's independent heartbeat timeout reports failed workers. This
    # launcher deliberately does not pretend to provide replication/takeover.


class EdgeLoadSimulation:
    """Bounded UI experiments with synthetic requests, not live ownership transfer.

    The coordinator alone changes queues. Real MQTT workers keep using their
    configured owners until synchronized bus-state handoff is implemented.
    """

    def __init__(self, network_path=DEFAULT_NETWORK_PATH):
        from collections import deque
        from threading import RLock

        from services.edge.edge_server import SimulatedEdge

        self.lock = RLock()
        self.time = 0.0
        self.sequence = self.accepted = self.rejected = 0
        self.auto_scale = True
        self.overload_since = {}
        self.events = deque(maxlen=100)
        self.workers = {}
        network = load_network(network_path)
        self.bus_regions = {bus["bus_id"]: bus["region_id"] for bus in network["buses"]}
        self.regions = {region["region_id"] for region in network["regions"]}
        for edge_id, (city, region, _) in configured_instances(network_path).items():
            self.workers[edge_id] = SimulatedEdge(edge_id, region, city)
        for region in network["regions"]:
            edge_id = "edge-" + region["region_id"] + "-3"
            if edge_id in self.workers:
                raise ValueError(
                    "Simulation third-instance ID conflicts with configured worker"
                )
            self.workers[edge_id] = SimulatedEdge(
                edge_id, region["region_id"], region["city_id"], active=False
            )

    def _eligible(self, region):
        return [
            worker
            for worker in self.workers.values()
            if worker.region_id == region and worker.active
        ]

    @staticmethod
    def _waiting_ms(worker, count=None):
        size = len(worker.queue) if count is None else count
        return max(0.0, size * worker.processing_ms - worker.credit_ms)

    def _observe(self):
        for region in self.regions:
            workers = self._eligible(region)
            overloaded = len(workers) >= 2 and all(
                len(w.queue) / w.capacity >= 0.8 for w in workers
            )
            if overloaded:
                self.overload_since.setdefault(region, self.time)
            else:
                self.overload_since.pop(region, None)

    def _balance(self, region):
        workers = self._eligible(region)
        # Move only synthetic waiting jobs. This makes no claim about bus state,
        # sequence floors, replication or authoritative telemetry ownership.
        for _ in range(sum(len(w.queue) for w in workers)):
            if len(workers) < 2:
                return
            source = max(workers, key=self._waiting_ms)
            targets = [
                w for w in workers if w is not source and len(w.queue) < w.capacity
            ]
            if not source.queue or not targets:
                return
            target = min(
                targets,
                key=lambda w: (self._waiting_ms(w, len(w.queue) + 1), w.edge_id),
            )
            before = self._waiting_ms(source)
            after = max(
                self._waiting_ms(source, len(source.queue) - 1),
                self._waiting_ms(target, len(target.queue) + 1),
            )
            if after >= before - 1e-9:
                return
            target.queue.append(source.queue.pop())
            if not source.queue:
                source.credit_ms = 0.0
            self.events.append(
                {
                    "time": self.time,
                    "action": "move_synthetic_request",
                    "source": source.edge_id,
                    "target": target.edge_id,
                }
            )

    def submit(self, bus_id, count):
        with self.lock:
            if bus_id not in self.bus_regions:
                raise ValueError("Unknown bus ID")
            if (
                isinstance(count, bool)
                or not isinstance(count, int)
                or not 1 <= count <= 10000
            ):
                raise ValueError("Request count must be between 1 and 10000")
            accepted = 0
            region = self.bus_regions[bus_id]
            for _ in range(count):
                choices = [
                    w for w in self._eligible(region) if len(w.queue) < w.capacity
                ]
                if not choices:
                    break
                worker = min(
                    choices,
                    key=lambda w: (
                        self._waiting_ms(w, len(w.queue) + 1),
                        len(w.queue) / w.capacity,
                        w.edge_id,
                    ),
                )
                self.sequence += 1
                worker.queue.append((self.sequence, bus_id, self.time))
                accepted += 1
            self.accepted += accepted
            self.rejected += count - accepted
            self._observe()
            return {
                "accepted": accepted,
                "rejected": count - accepted,
                "simulation": self.snapshot(),
            }

    def configure(self, edge_id, *, capacity=None, processing_ms=None, active=None):
        with self.lock:
            if edge_id not in self.workers:
                raise ValueError("Unknown simulated instance")
            worker = self.workers[edge_id]
            if capacity is not None and (
                isinstance(capacity, bool)
                or not isinstance(capacity, int)
                or not 1 <= capacity <= 50
            ):
                raise ValueError("Queue capacity must be between 1 and 50")
            if processing_ms is not None and (
                isinstance(processing_ms, bool)
                or not isinstance(processing_ms, (float, int))
                or not math.isfinite(processing_ms)
                or not 1 <= processing_ms <= 60000
            ):
                raise ValueError("Processing delay must be between 1 and 60000 ms")
            if active is not None and not isinstance(active, bool):
                raise ValueError("Active must be true or false")
            if capacity is not None and capacity < len(worker.queue):
                raise ValueError(
                    "Drain the queue before lowering capacity below its current size"
                )
            if active is False and worker.queue:
                raise ValueError(
                    "Drain the queue before stopping this simulated instance"
                )
            if capacity is not None:
                worker.capacity = capacity
            if processing_ms is not None:
                worker.processing_ms = float(processing_ms)
                worker.credit_ms = 0.0
            if active is not None:
                worker.active = active
            self._observe()
            return self.snapshot()

    def advance(self, seconds):
        import math

        with self.lock:
            if (
                isinstance(seconds, bool)
                or not isinstance(seconds, (float, int))
                or not math.isfinite(seconds)
                or not 0 < seconds <= 60
            ):
                raise ValueError(
                    "Advance virtual time by more than 0 and at most 60 seconds"
                )
            remaining = seconds
            while remaining > 1e-9:
                step = min(1.0, remaining)
                self.time += step
                for worker in self.workers.values():
                    worker.advance(step, self.time)
                self._observe()
                for region in sorted(self.regions):
                    since = self.overload_since.get(region)
                    third = self.workers["edge-" + region + "-3"]
                    if (
                        self.auto_scale
                        and since is not None
                        and self.time - since >= 6
                        and not third.active
                        and third.state == "standby"
                    ):
                        third.active = True
                        self.events.append(
                            {
                                "time": self.time,
                                "action": "start_simulated_instance",
                                "edge_id": third.edge_id,
                            }
                        )
                    self._balance(region)
                self._observe()
                remaining -= step
            return self.snapshot()

    def snapshot(self):
        with self.lock:
            return {
                "mode": "simulation",
                "live_workers_changed": False,
                "virtual_seconds": self.time,
                "auto_scale": self.auto_scale,
                "overload_percent": 80,
                "sustained_seconds": 6,
                "accepted": self.accepted,
                "rejected": self.rejected,
                "workers": [worker.snapshot() for worker in self.workers.values()],
                "events": list(self.events),
            }


class EdgeClusterSimulation(EdgeLoadSimulation):
    """Single-coordinator failure model using real telemetry and processing.

    The lock represents a reliable ownership authority. Replicas and the pending
    journal survive an edge failure, but not failure of this simulation process.
    This models atomic handoff; it is not a network consensus implementation.
    """

    def __init__(self, network_path=DEFAULT_NETWORK_PATH, fleet_size=None):
        from services.edge.validation.bus_registry import BusAssignment, BusRegistry

        super().__init__(network_path)
        self.network_path = network_path
        self.network = NetworkConfig.load(network_path)
        network_definition = load_network(network_path)
        self.bus_definitions = network_definition["buses"]
        if fleet_size is not None:
            if (
                isinstance(fleet_size, bool)
                or not isinstance(fleet_size, int)
                or not 1 <= fleet_size <= 50
            ):
                raise ValueError("Fleet size must be between 1 and 50")
            originals = self.bus_definitions
            self.bus_definitions = [
                {
                    **originals[i % len(originals)],
                    "bus_id": originals[i % len(originals)]["bus_id"]
                    if i < len(originals)
                    else originals[i % len(originals)]["bus_id"] + "-fleet-" + str(i),
                }
                for i in range(fleet_size)
            ]
            from shared.schema.network import route_stops
            for index, bus in enumerate(self.bus_definitions[len(originals):], start=len(originals)):
                # Extra streams start a few stops apart rather than on top of their template.
                stops = route_stops(network_definition, bus["route_id"], bus.get("direction_id", 0))
                scenario = dict(bus.get("scenario", {}))
                scenario["start_stop_index"] = (scenario.get("start_stop_index", 0) + 5 * (index // len(originals))) % (len(stops) - 1)
                bus["scenario"] = scenario
        self.registry = BusRegistry(
            {
                bus["bus_id"]: BusAssignment(
                    bus["region_id"], bus["route_id"], bus["city_id"], bus["edge_id"]
                )
                for bus in self.bus_definitions
            }
        )
        self.bus_regions = {
            bus["bus_id"]: bus["region_id"] for bus in self.bus_definitions
        }
        self.epochs = dict.fromkeys(self.bus_regions, 1)
        self.committed = dict.fromkeys(self.bus_regions, None)
        self.journal, self.central_states, self.outcomes = {}, {}, {}
        self.recover_at, self.low_since = {}, {}
        self.orphaned_requests = []
        self.auto_workers = set()
        self.generated_sequences = {}
        self.message_templates = {}
        self.demo = None
        self.demo_running = False
        self.central_available = True
        self.central_pending, self.central_received_at, self.central_pending_at = {}, {}, {}
        self.central_retry_at = 0.0
        from uuid import uuid4
        self.session_id = uuid4().hex
        self.central_ack_count = 0
        self.headways, self.departures, self.last_departure = {}, {}, {}
        self.dispatch_queues = {}
        self.fleet_limit = 50
        self.assistance_threshold = 70
        self.assistance_armed = dict.fromkeys(self.regions, True)
        self.assistance_grants = set()
        self.assistance_draining = set()
        self.emergency_workers = set()
        for worker in self.workers.values():
            worker.on_telemetry = self._complete

    def request_burst(self, count, region_id):
        """Queue real demo readings at once; later readings keep their sequence order."""
        with self.lock:
            if not self.demo or not self.demo_running:
                raise ValueError("Start the demonstration first")
            buses = [bus for bus in self.demo.buses.values()
                     if bus["definition"]["region_id"] == region_id and bus["condition"] in {"normal", "traffic", "peak", "stopped"}]
            if not buses or not 1 <= count <= 1000:
                raise ValueError("Choose an active region and 1 to 1000 requests")
            accepted = 0
            for index in range(count):
                bus = buses[index % len(buses)]
                reading = self.demo._reading(bus)
                accepted += self.submit_telemetry(reading)["accepted"]
            self.events.append(dict(time=self.time, action="request_burst", region_id=region_id,
                                    requested=count, accepted=accepted, rejected=count-accepted))
            return dict(requested=count, accepted=accepted, rejected=count-accepted)

    def _eligible(self, region):
        return [w for w in super()._eligible(region) if w.state == "healthy"]

    def _assigned(self, edge_id):
        return [
            bus for bus in self.bus_regions if self.registry.get(bus).edge_id == edge_id
        ]

    def _processor(self, edge_id, bus_id):
        from services.edge.config import EdgeConfig
        from services.edge.processing.processor import EdgeProcessor

        assignment = self.registry.get(bus_id)
        worker = self.workers[edge_id]
        scope = (assignment.city_id, assignment.region_id)
        if scope not in worker.processors:
            # Hosting elsewhere never rewrites the route's home region or GPS.
            config = EdgeConfig(*scope, edge_id)
            worker.processors[scope] = EdgeProcessor(
                config,
                NetworkConfig(self.registry, self.network.routes),
                clock=lambda: self.time,
            )
        return worker.processors[scope]

    def _key(self, bus_id):
        assignment = self.registry.get(bus_id)
        return assignment.city_id, assignment.region_id, bus_id

    def _replicate(self, edge_id, bus_id):
        from copy import deepcopy

        state = self._processor(edge_id, bus_id).cache.export_bus(self._key(bus_id))
        city = self.registry.get(bus_id).city_id
        for worker in self.workers.values():
            if worker.city_id == city and worker.state in ("healthy", "recovering"):
                worker.replicas[bus_id] = deepcopy(state)

    def _latest_replica(self, bus_id):
        from copy import deepcopy

        copies = [
            worker.replicas[bus_id]
            for worker in self.workers.values()
            if worker.state in ("healthy", "recovering")
            and bus_id in worker.replicas
            and worker.replicas[bus_id]["floor"] == self.committed[bus_id]
        ]
        if copies:
            return deepcopy(copies[0])
        if self.committed[bus_id] is None:
            return {"entry": None, "floor": None}
        raise ValueError("No synchronized surviving replica; bus remains unavailable")

    def _complete(self, worker, job):
        _, bus_id, _, source, epoch = job
        if job[0] not in self.journal:
            return
        if (
            self.registry.get(bus_id).edge_id != worker.edge_id
            or self.epochs[bus_id] != epoch
            or worker.state != "healthy"
        ):
            self.outcomes["FENCED"] = self.outcomes.get("FENCED", 0) + 1
            return
        from services.edge.transformation.central_mapper import map_bus_update

        if "lamport_clock" in source:
            worker.clock.receive(source["lamport_clock"])
        result = self._processor(worker.edge_id, bus_id).process(source)
        self.outcomes[result.status.value] = (
            self.outcomes.get(result.status.value, 0) + 1
        )
        self.journal.pop(job[0])
        if result.status.value == "ACCEPTED":
            self.committed[bus_id] = source["sequence_number"]
            self._replicate(worker.edge_id, bus_id)
            try:
                payload = map_bus_update(
                    result.output,
                    bus_id,
                    "passengers",
                    edge_lamport=worker.clock.tick(),
                )
            except ValueError:
                self.outcomes["NOT_FORWARDABLE"] = (
                    self.outcomes.get("NOT_FORWARDABLE", 0) + 1
                )
                return
            self.central_pending[bus_id] = payload
            self.central_pending_at[bus_id] = self.time
            self._deliver_central()

    def _deliver_central(self):
        """Model a delivery acknowledgement; an outage retains one latest state per bus."""
        if not self.central_pending or self.time < self.central_retry_at:
            return
        if not self.central_available:
            self.events.append(dict(time=self.time, action="central_retry", pending=len(self.central_pending),
                                    reason="injected central outage; no acknowledgement"))
            self.central_retry_at = self.time + 2
            return
        from shared.schema.bus_state import BusStateUpdate

        for bus_id, payload in list(self.central_pending.items()):
            expired = self.time - self.central_pending_at[bus_id] >= 30
            fenced = payload["edge_metadata"]["edge_id"] != self.registry.get(bus_id).edge_id
            if expired or fenced:
                del self.central_pending[bus_id]
                del self.central_pending_at[bus_id]
                self.events.append(dict(time=self.time, action="central_expired" if expired else "central_fenced", bus_id=bus_id,
                                        reason="pending reading is no longer fresh" if expired else "former owner cannot update central"))
                continue
            BusStateUpdate.model_validate(payload, strict=True)
            # Sequence checks stand in for central's acknowledgement of this state.
            old = self.central_states.get(bus_id)
            if old is None or payload["sequence_number"] > old["sequence_number"]:
                self.central_states[bus_id] = payload
                self.central_received_at[bus_id] = self.time
                self.central_ack_count += 1
            del self.central_pending[bus_id]
            del self.central_pending_at[bus_id]
            if self.central_retry_at > 0 or old is None:
                self.events.append(dict(time=self.time, action="central_ack", bus_id=bus_id,
                                        sequence=payload["sequence_number"]))
        self.central_retry_at = 0.0

    def configure_demo(self, running=None, bus_id=None, condition=None, central_available=None):
        from services.data_simulation.generate_data import DemoFleet

        with self.lock:
            if condition is not None and (bus_id not in self.bus_regions or condition not in DemoFleet.CONDITIONS):
                raise ValueError("Choose a registered bus and supported condition")
            if self.demo is None:
                self.demo = DemoFleet(self.network_path, self.bus_definitions, self.time, self.events.append)
                for name, bus in self.demo.buses.items():
                    bus["sequence"] = max(self.generated_sequences.get(name, 0), self.committed[name] or 0)
            if running is not None:
                self.demo_running = running
                self.events.append(dict(time=self.time, action="demo_started" if running else "demo_stopped"))
            if condition is not None:
                if any(bus_id in queue for queue in self.dispatch_queues.values()):
                    raise ValueError("Bus is awaiting dispatch; use the scheduler or reset first")
                self.demo.configure(bus_id, condition, self.time)
            if central_available is not None:
                self.central_available = central_available
                self.events.append(dict(time=self.time, action="central_available" if central_available else "central_unavailable"))
            return self.snapshot()

    def register_bus(self, bus_id, route_id, capacity=50, direction_id=0):
        """BUS-11: register a configured bus; no sensor/schema extension is needed."""
        from services.edge.validation.bus_registry import BusAssignment
        from shared.schema.network import validate_network

        with self.lock:
            import re
            if not isinstance(bus_id, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", bus_id):
                raise ValueError("Bus ID must be a valid topic segment")
            network = load_network(self.network_path)
            route = next((r for r in network["routes"] if r["route_id"] == route_id), None)
            if route is None or bus_id in self.bus_regions or len(self.bus_regions) >= self.fleet_limit:
                raise ValueError("Choose a known route, unique bus ID and space below the fleet limit")
            workers = self._eligible(route["region_id"])
            if not workers:
                raise ValueError("No healthy regional edge can own the new bus")
            owner = min(workers, key=lambda w: len(self._assigned(w.edge_id))).edge_id
            definition = dict(bus_id=bus_id, route_id=route_id, city_id=route["city_id"],
                              region_id=route["region_id"], edge_id=owner,
                              capacity=capacity, direction_id=direction_id)
            network["buses"] = self.bus_definitions + [definition]
            validate_network(network)
            self.configure_demo()
            self.demo.add_bus(definition, self.time, condition="out_of_service")
            self.registry.register(bus_id, BusAssignment(route["region_id"], route_id, route["city_id"], owner))
            self.bus_definitions.append(definition)
            self.bus_regions[bus_id] = route["region_id"]
            self.epochs[bus_id], self.committed[bus_id] = 1, None
            self.events.append(dict(time=self.time, action="bus_registered", bus_id=bus_id, route_id=route_id))
            return self.snapshot()

    def schedule(self, route_id, action, bus_id=None, headway_seconds=None):
        """Central or an operator requests dispatch; every delay uses simulation time."""
        with self.lock:
            if route_id not in {r["route_id"] for r in load_network(self.network_path)["routes"]}:
                raise ValueError("Unknown route")
            if action == "set_headway":
                if bus_id is not None or isinstance(headway_seconds, bool) or not isinstance(headway_seconds, (int, float)) or not math.isfinite(headway_seconds) or not 1 <= headway_seconds <= 3600:
                    raise ValueError("Headway must be between 1 and 3600 simulation seconds")
                self.headways[route_id] = float(headway_seconds)
                self.departures[route_id] = max(self.time, self.last_departure.get(route_id, self.time) + headway_seconds)
            elif action == "dispatch":
                if headway_seconds is not None:
                    raise ValueError("Dispatch takes a bus ID, not headway_seconds")
                self.configure_demo()
                bus = self.demo.buses.get(bus_id)
                if bus is None or bus["definition"]["route_id"] != route_id or bus["condition"] != "out_of_service":
                    raise ValueError("Dispatch requires an available out-of-service bus assigned to this route")
                if any(bus_id in queue for queue in self.dispatch_queues.values()):
                    raise ValueError("Bus is already queued for dispatch")
                self.dispatch_queues.setdefault(route_id, []).append(bus_id)
                self.departures.setdefault(route_id, self.time)
            else:
                raise ValueError("Choose dispatch or set_headway")
            self.events.append(dict(time=self.time, action=action, bus_id=bus_id, route_id=route_id,
                                    headway_seconds=self.headways.get(route_id, 10)))
            return self.snapshot()

    def _dispatch_due(self, now):
        for route, queue in self.dispatch_queues.items():
            if queue and now >= self.departures[route]:
                bus_id = queue.pop(0)
                self.demo.configure(bus_id, "normal", now)
                self.last_departure[route] = now
                self.departures[route] = now + self.headways.get(route, 10)
                self.events.append(dict(time=now, action="bus_dispatched", bus_id=bus_id, route_id=route))

    def submit_telemetry(self, source):
        from copy import deepcopy

        from services.edge.validation.validator import parse_telemetry

        with self.lock:
            checked, errors = parse_telemetry(
                source,
                expected_region=source.get("region_id", "")
                if isinstance(source, dict)
                else "",
                expected_bus=source.get("bus_id", "")
                if isinstance(source, dict)
                else "",
                registry=self.registry,
            )
            if checked is None:
                self.rejected += 1
                return {"accepted": False, "reason": "; ".join(errors)}
            bus_id = checked["bus_id"]
            owner = self.registry.get(bus_id).edge_id
            worker = self.workers[owner]
            if owner in self.assistance_draining and len(self._primary_workers(worker.region_id)) == 2:
                self.rejected += 1
                return {"accepted": False, "reason": "Third edge draining accepted batch; source retains latest reading for retry"}
            if worker.state != "healthy" or len(worker.queue) >= worker.capacity:
                self.rejected += 1
                return {"accepted": False, "reason": "Owner unavailable or queue full"}
            self.sequence += 1
            job = (
                self.sequence,
                bus_id,
                self.time,
                deepcopy(checked),
                self.epochs[bus_id],
            )
            self.journal[job[0]] = job
            worker.queue.append(job)
            self.accepted += 1
            self._observe()
            return {
                "accepted": True,
                "edge_id": owner,
                "epoch": self.epochs[bus_id],
                "request_id": job[0],
            }

    def generate(self, bus_id, count):
        """Generate repeated valid sensor readings on the actual configured route."""
        from copy import deepcopy

        from services.data_simulation.generate_data import generate_messages

        with self.lock:
            if (
                bus_id not in self.bus_regions
                or isinstance(count, bool)
                or not isinstance(count, int)
                or not 1 <= count <= 10000
            ):
                raise ValueError(
                    "Known bus and request count between 1 and 10000 required"
                )
            if bus_id not in self.message_templates:
                bus = next(
                    bus for bus in self.bus_definitions if bus["bus_id"] == bus_id
                )
                sample = next(
                    generate_messages(
                        bus["route_id"],
                        bus.get("direction_id", 0),
                        network_path=self.network_path,
                        max_updates=1,
                        update_seconds=0,
                    )
                )
                sample["bus_id"] = bus_id
                self.message_templates[bus_id] = sample
            accepted = 0
            for _ in range(count):
                pending_floor = max(
                    (
                        job[3]["sequence_number"]
                        for job in self.journal.values()
                        if job[1] == bus_id
                    ),
                    default=0,
                )
                sequence = (
                    max(
                        self.generated_sequences.get(bus_id, 0),
                        self.committed[bus_id] or 0,
                        pending_floor,
                    )
                    + 1
                )
                self.generated_sequences[bus_id] = sequence
                source = deepcopy(self.message_templates[bus_id])
                source["sequence_number"] = source["lamport_clock"] = sequence
                accepted += self.submit_telemetry(source)["accepted"]
            return {
                "accepted": accepted,
                "rejected": count - accepted,
                "simulation": self.snapshot(),
            }

    def transfer(self, bus_id, target_id):
        """Synchronize state, fence the old owner, then move every pending update."""
        with self.lock:
            if bus_id not in self.bus_regions or target_id not in self.workers:
                raise ValueError("Unknown bus or instance")
            assignment = self.registry.get(bus_id)
            source_id = assignment.edge_id
            target = self.workers[target_id]
            if target_id == source_id:
                return
            pending = sorted(
                (job for job in self.journal.values() if job[1] == bus_id),
                key=lambda job: job[0],
            )
            if (
                target.state != "healthy"
                or not target.active
                or target.city_id != assignment.city_id
            ):
                raise ValueError("Target must be healthy and in the bus's city")
            if len(target.queue) + len(pending) > target.capacity:
                raise ValueError("Target has insufficient queue capacity")
            state = self._latest_replica(bus_id)
            processor = self._processor(target_id, bus_id)
            processor.cache.restore_bus(self._key(bus_id), state)
            self.epochs[bus_id] += 1
            self.registry.transfer_owner(bus_id, source_id, target_id)
            source = self.workers[source_id]
            from collections import deque

            source.queue = deque(
                job for job in source.queue if not (len(job) > 3 and job[1] == bus_id)
            )
            source.credit_ms = 0.0
            for job in pending:
                updated = (*job[:4], self.epochs[bus_id])
                self.journal[job[0]] = updated
                target.queue.append(updated)
            for old_processor in source.processors.values():
                old_processor.cache.release_bus(self._key(bus_id))
            self.events.append(
                {
                    "time": self.time,
                    "action": "transfer_bus",
                    "bus_id": bus_id,
                    "source": source_id,
                    "target": target_id,
                    "epoch": self.epochs[bus_id],
                }
            )

    def _activate_emergency_standby(self, region):
        """A failed owner needs available capacity now; this is separate from load-only assistance."""
        third = self.workers["edge-" + region + "-3"]
        if third.state != "standby":
            return False
        try:
            self._synchronize(third)
        except ValueError:
            return False  # a restart cannot invent a lost sequence floor
        third.active, third.state = True, "healthy"
        self.emergency_workers.add(third.edge_id)
        self.auto_workers.add(third.edge_id)
        self.events.append(dict(time=self.time, action="emergency_standby_takeover", edge_id=third.edge_id))
        return True

    def _takeover(self):
        # Bring standby online when a failed owner's only regional peer is busy or unavailable.
        for region in self.regions:
            affected = any(self.bus_regions[bus] == region and self.workers[self.registry.get(bus).edge_id].state != "healthy"
                           for bus in self.bus_regions)
            local = self._eligible(region)
            if affected and (not local or all(len(w.queue) / w.capacity * 100 >= self.assistance_threshold for w in local)):
                self._activate_emergency_standby(region)
        remaining = []
        for job in self.orphaned_requests:
            region = self.bus_regions[job[1]]
            choices = [w for w in self._eligible(region) if len(w.queue) < w.capacity]
            if choices:
                min(choices, key=lambda w: (len(w.queue), w.edge_id)).queue.append(job)
            else:
                remaining.append(job)
        self.orphaned_requests = remaining
        for bus_id in self.bus_regions:
            assignment = self.registry.get(bus_id)
            if self.workers[assignment.edge_id].state == "healthy":
                continue
            local = self._eligible(assignment.region_id)
            candidates = local or [w for w in self.workers.values()
                                   if w.active and w.state == "healthy" and w.city_id == assignment.city_id]
            moved = False
            for target in sorted(candidates, key=lambda w: (len(w.queue), len(self._assigned(w.edge_id)), w.edge_id)):
                try:
                    self.transfer(bus_id, target.edge_id)
                    moved = True
                    break
                except ValueError:
                    continue  # Keep the journal until a replica and enough capacity are available.
            if not moved and self._activate_emergency_standby(assignment.region_id):
                try:
                    self.transfer(bus_id, "edge-" + assignment.region_id + "-3")
                except ValueError:
                    pass

    #move buses from an edge  to the least-loaded healthy edge in its home region after a recovery
    def _return_home(self):
        for bus_id, home in self.bus_regions.items():
            owner = self.workers[self.registry.get(bus_id).edge_id]
            if owner.region_id == home:
                continue #already in its home region
            for target in sorted(
                self._eligible(home),
                key = lambda w: (len(w.queue), len(self._assigned(w.edge_id)), w.edge_id)
            ):
                try:
                    self.transfer(bus_id, target.edge_id)
                    break
                except ValueError:
                    continue #try next home edge on error, or retry on next tick

    def fail(self, edge_id):
        with self.lock:
            if edge_id not in self.workers:
                raise ValueError("Unknown instance")
            worker = self.workers[edge_id]
            worker.active, worker.state = False, "failed"
            self.orphaned_requests.extend(job for job in worker.queue if len(job) == 3)
            worker.queue.clear()
            worker.processors.clear()
            worker.replicas.clear()
            worker.credit_ms = 0.0
            self.recover_at.pop(edge_id, None)
            self.auto_workers.discard(edge_id)
            self.emergency_workers.discard(edge_id)
            self.assistance_draining.discard(edge_id)
            self.low_since.pop(edge_id, None)
            for bus_id in self._assigned(edge_id):
                self.epochs[bus_id] += 1
            self.events.append(
                {"time": self.time, "action": "fail_instance", "edge_id": edge_id}
            )
            self._takeover()
            return self.snapshot()

    def recover(self, edge_id):
        with self.lock:
            if edge_id not in self.workers or self.workers[edge_id].state != "failed":
                raise ValueError("Only a failed instance can recover")
            worker = self.workers[edge_id]
            # Do not turn a total loss of replicas into an apparently healthy
            # restart with reset sequence floors.
            for bus_id in self.bus_regions:
                if self.registry.get(bus_id).city_id == worker.city_id:
                    self._latest_replica(bus_id)
            pending = sum(job[1] in self._assigned(edge_id) for job in self.journal.values())
            if pending > worker.capacity:
                raise ValueError("Recovery requires capacity for the pending journal")
            worker.state = "recovering"
            self.recover_at[edge_id] = self.time + 2
            self._synchronize(worker)
            return self.snapshot()

    def _synchronize(self, worker):
        # Rebuild accepted work if the failed owner recovered before another node could take it.
        queued = {job[0] for job in worker.queue}
        pending = [job for job in self.journal.values()
                   if self.registry.get(job[1]).edge_id == worker.edge_id and job[0] not in queued]
        if len(worker.queue) + len(pending) > worker.capacity:
            raise ValueError("Insufficient capacity to restore the pending journal")
        for job in sorted(pending):
            restored = (*job[:4], self.epochs[job[1]])
            self.journal[job[0]] = restored
            worker.queue.append(restored)
        for bus_id in self.bus_regions:
            if self.registry.get(bus_id).city_id == worker.city_id:
                try:
                    worker.replicas[bus_id] = self._latest_replica(bus_id)
                except ValueError:
                    continue

    def register(self, edge_id, region_id, capacity=50, processing_ms=5.0):
        from services.edge.edge_server import SimulatedEdge

        with self.lock:
            if (
                not isinstance(edge_id, str)
                or not edge_id.strip()
                or len(edge_id) > 80
                or edge_id in self.workers
            ):
                raise ValueError(
                    "Instance ID must be unique and nonempty (at most 80 characters)"
                )
            if region_id not in self.regions or len(self.workers) >= 20:
                raise ValueError("Unknown region or instance limit reached")
            template = next(
                w for w in self.workers.values() if w.region_id == region_id
            )
            worker = SimulatedEdge(edge_id, region_id, template.city_id)
            self.workers[edge_id] = worker
            try:
                super().configure(
                    edge_id, capacity=capacity, processing_ms=processing_ms
                )
            except ValueError:
                del self.workers[edge_id]
                raise
            worker.on_telemetry = self._complete
            self._synchronize(worker)
            self.events.append(
                {"time": self.time, "action": "register_instance", "edge_id": edge_id}
            )
            return self.snapshot()

    def scale_down(self, edge_id):
        with self.lock:
            if edge_id not in self.workers:
                raise ValueError("Unknown instance")
            worker = self.workers[edge_id]
            if any(len(job) == 3 for job in worker.queue):
                raise ValueError("Drain synthetic requests before scale-down")
            # Preflight all moves so insufficient capacity changes nothing.
            targets = [w for w in self._eligible(worker.region_id) if w is not worker]
            remaining = {w.edge_id: w.capacity - len(w.queue) for w in targets}
            plan = []
            for bus_id in self._assigned(edge_id):
                pending = sum(job[1] == bus_id for job in self.journal.values())
                eligible = [w for w in targets if remaining[w.edge_id] >= pending]
                if not eligible:
                    raise ValueError("No capacity for safe scale-down")
                self._latest_replica(bus_id)
                target = max(eligible, key=lambda w: (remaining[w.edge_id], w.edge_id))
                remaining[target.edge_id] -= pending
                plan.append((bus_id, target.edge_id))
            for bus_id, target_id in plan:
                self.transfer(bus_id, target_id)
            worker.active, worker.state = False, "standby"
            worker.queue.clear()
            self.auto_workers.discard(edge_id)
            self.low_since.pop(edge_id, None)
            self.events.append(
                {"time": self.time, "action": "scale_down", "edge_id": edge_id}
            )
            return self.snapshot()

    def configure(self, edge_id, **changes):
        with self.lock:
            active = changes.get("active")
            if active is False and edge_id in self.workers and self._assigned(edge_id):
                raise ValueError(
                    "Use failure or safe scale-down for an assigned instance"
                )
            if (
                active is True
                and edge_id in self.workers
                and self.workers[edge_id].state in ("failed", "recovering")
            ):
                raise ValueError("Use recovery before activating a failed instance")
            super().configure(edge_id, **changes)
            if active is not None:
                if active:
                    self._synchronize(self.workers[edge_id])
                self.workers[edge_id].state = "healthy" if active else "standby"
            return self.snapshot()

    def _primary_workers(self, region):
        return [w for w in self._eligible(region) if w.edge_id in
                {"edge-" + region + "-1", "edge-" + region + "-2"}]

    def _observe(self):
        # One grant per sustained crossing. Both queues must fall below 70% to rearm.
        if not hasattr(self, "assistance_armed"):
            return super()._observe()
        for region in self.regions:
            primary = self._primary_workers(region)
            loads = [len(w.queue) / w.capacity * 100 for w in primary]
            if len(primary) == 2 and all(load < self.assistance_threshold for load in loads):
                self.assistance_armed[region] = True
            hot = len(primary) == 2 and all(load >= self.assistance_threshold for load in loads)
            if not hot or not self.assistance_armed[region]:
                self.overload_since.pop(region, None)
                continue
            since = self.overload_since.setdefault(region, self.time)
            if self.auto_scale and self.time - since >= 6:
                self.assistance_grants.add(region)
                self.assistance_armed[region] = False
                self.events.append(dict(time=self.time, action="assistance_threshold_crossed", region_id=region))

    def _balance(self, region):
        third = self.workers["edge-" + region + "-3"]
        grant = region in self.assistance_grants
        if grant:
            self.assistance_grants.discard(region)
            if third.state == "standby" and not third.active:
                third.active = True
                self.events.append(dict(time=self.time, action="start_simulated_instance", edge_id=third.edge_id))
        if third.active and third.state == "standby":
            self._synchronize(third)
            third.state = "healthy"
            self.auto_workers.add(third.edge_id)
        # Normal traffic stays with the primaries. Failover is allowed to use any healthy edge.
        workers = [w for w in self._eligible(region) if w is not third or grant or len(self._primary_workers(region)) < 2]
        before = set(self._assigned(third.edge_id))
        self._balance_workers(region, workers)
        if grant:
            self.assistance_draining.add(third.edge_id)
            self.events.append(dict(time=self.time, action="assistance_batch", edge_id=third.edge_id,
                                    buses=sorted(set(self._assigned(third.edge_id)) - before)))

    def _balance_workers(self, region, workers):
        # Keep per-bus FIFO and one owner: move a bus, never individual telemetry.
        if not any(len(job) > 3 for worker in workers for job in worker.queue):
            if not self.assistance_draining:
                super()._balance(region)
        for _ in range(len(self.bus_regions)):
            if len(workers) < 2:
                return
            # Between healthy workers, balance waiting time even below the scale-up threshold.
            # Equal bus counts can strand a backlog on a deliberately slow simulated node.
            under_pressure = any(w.queue for w in workers)
            ordering = (
                (lambda w: (self._waiting_ms(w), len(self._assigned(w.edge_id))))
                if under_pressure
                else (lambda w: (len(self._assigned(w.edge_id)), self._waiting_ms(w)))
            )
            source = max(workers, key=ordering)
            target = min(workers, key=ordering)
            overloaded = len(source.queue) / source.capacity >= self.assistance_threshold / 100
            if source is target or (
                len(self._assigned(source.edge_id))
                - len(self._assigned(target.edge_id))
                <= 1
                and not under_pressure
            ):
                return
            moved = False
            for bus_id in self._assigned(source.edge_id):
                pending = sum(job[1] == bus_id for job in self.journal.values())
                if pending == 0 and under_pressure:
                    continue  # moving a bus without queued work cannot reduce the current backlog
                if pending == 0 and self.demo and self.demo_running:
                    active = sum(self.demo.buses[bus]["condition"] != "out_of_service" for bus in self._assigned(target.edge_id))
                    adding = self.demo.buses[bus_id]["condition"] != "out_of_service"
                    if active + adding > 1000 / target.processing_ms:
                        continue  # don't move a fresh stream onto a worker that cannot keep up
                if overloaded and pending == 0:
                    continue
                if pending and max(
                    self._waiting_ms(source, len(source.queue) - pending),
                    self._waiting_ms(target, len(target.queue) + pending),
                ) >= max(self._waiting_ms(source), self._waiting_ms(target)):
                    continue
                try:
                    self.transfer(bus_id, target.edge_id)
                    moved = True
                    break
                except ValueError:
                    continue
            if not moved:
                return

    def advance(self, seconds):
        with self.lock:
            if (
                isinstance(seconds, bool)
                or not isinstance(seconds, (int, float))
                or not math.isfinite(seconds)
                or not 0 < seconds <= 60
            ):
                raise ValueError(
                    "Advance virtual time by more than 0 and at most 60 seconds"
                )
            remaining = seconds
            while remaining > 1e-9:
                step = min(1.0, remaining)
                if self.demo is not None and self.demo_running:
                    self._dispatch_due(self.time + step)
                    self.demo.step(step, self.time + step, self.submit_telemetry)
                super().advance(step)
                self._deliver_central()
                for edge_id, ready_at in list(self.recover_at.items()):
                    if self.time >= ready_at:
                        worker = self.workers[edge_id]
                        self._synchronize(worker)
                        worker.state, worker.active = "healthy", True
                        del self.recover_at[edge_id]
                        self.events.append(
                            {
                                "time": self.time,
                                "action": "recovery_complete",
                                "edge_id": edge_id,
                            }
                        )
                self._takeover()
                self._return_home() 
                for region in sorted(self.regions):
                    self._balance(region)
                for edge_id in list(self.emergency_workers):
                    worker = self.workers[edge_id]
                    if worker.state == "healthy" and len(self._primary_workers(worker.region_id)) == 2:
                        self.assistance_draining.add(edge_id)
                # An idle recovered standby must not silently become a third permanent primary.
                for worker in self.workers.values():
                    if worker.edge_id.endswith("-3") and worker.state == "healthy" and not worker.queue and not self._assigned(worker.edge_id) and len(self._primary_workers(worker.region_id)) == 2:
                        self.scale_down(worker.edge_id)
                for edge_id in list(self.assistance_draining):
                    worker = self.workers[edge_id]
                    if worker.state == "healthy" and not worker.queue and len(self._primary_workers(worker.region_id)) == 2:
                        try:
                            self.scale_down(edge_id)
                            self.assistance_draining.discard(edge_id)
                        except ValueError:
                            pass  # a full primary queue delays handback; never drop accepted work
                for edge_id in list(self.auto_workers):
                    worker = self.workers[edge_id]
                    active_sources = sum(bus["definition"]["region_id"] == worker.region_id
                        and bus["condition"] != "out_of_service" for bus in self.demo.buses.values()) if self.demo and self.demo_running else 0
                    other_rate = sum(1000 / w.processing_ms for w in self._eligible(worker.region_id) if w is not worker)
                    if active_sources <= other_rate and all(
                        len(w.queue) / w.capacity < 0.2
                        for w in self._eligible(worker.region_id)
                    ):
                        self.low_since.setdefault(edge_id, self.time)
                        if self.time - self.low_since[edge_id] >= 10:
                            try:
                                self.scale_down(edge_id)
                            except ValueError:
                                pass
                    else:
                        self.low_since.pop(edge_id, None)
                remaining -= step
            return self.snapshot()

    def snapshot(self):
        with self.lock:
            result = super().snapshot()
            if not hasattr(self, "registry"):
                return result
            result.update(
                telemetry_mode=True,
                overload_percent=self.assistance_threshold,
                assistance={region: dict(armed=self.assistance_armed[region], threshold_percent=self.assistance_threshold,
                    third_draining="edge-" + region + "-3" in self.assistance_draining) for region in self.regions},
                fleet_limit=self.fleet_limit,
                active_buses=sum(bus["condition"] != "out_of_service" for bus in self.demo.buses.values()) if self.demo else 0,
                demo_running=self.demo_running,
                session_id=self.session_id,
                central_ack_count=self.central_ack_count,
                registered_buses=deepcopy(self.bus_definitions),
                schedules={route: dict(headway_seconds=self.headways.get(route, 10),
                    next_departure=self.departures.get(route), pending=list(self.dispatch_queues.get(route, [])))
                    for route in set(self.headways) | set(self.dispatch_queues)},
                demo_buses=self.demo.snapshot(self.time) if self.demo is not None else {},
                route_demand=dict(self.demo.route_demand) if self.demo else {},
                demand_measurements={route: dict(waiting_passenger_seconds=value,
                    boarded_passengers=self.demo.boarded_passengers[route])
                    for route, value in self.demo.waiting_passenger_seconds.items()} if self.demo else {},
                waiting_passengers={route: sum(stops.values()) for route, stops in self.demo.waiting.items()} if self.demo else {},
                central_available=self.central_available,
                central_pending=len(self.central_pending),
                central_received_at=dict(self.central_received_at),
                pending_telemetry=len(self.journal),
                pending_synthetic=len(self.orphaned_requests),
                outcomes=dict(self.outcomes),
                central_states=deepcopy(self.central_states),
                ownership={
                    bus_id: {
                        "edge_id": self.registry.get(bus_id).edge_id,
                        "epoch": self.epochs[bus_id],
                        "home_region": self.bus_regions[bus_id],
                        "available": self.workers[
                            self.registry.get(bus_id).edge_id
                        ].state
                        == "healthy",
                    }
                    for bus_id in self.bus_regions
                },
            )
            for worker in result["workers"]:
                worker["overload_percent"] = self.assistance_threshold
                worker["assistance_draining"] = worker["edge_id"] in self.assistance_draining
                worker["emergency_takeover"] = worker["edge_id"] in self.emergency_workers
                worker["assigned_buses"] = self._assigned(worker["edge_id"])
                worker["active_assigned_buses"] = sum(self.demo.buses[bus_id]["condition"] != "out_of_service"
                    for bus_id in worker["assigned_buses"]) if self.demo else 0
                worker["replica_sequences"] = {
                    bus_id: state["floor"]
                    for bus_id, state in self.workers[
                        worker["edge_id"]
                    ].replicas.items()
                }
            return result


def benchmark_cluster():
    """Reproducible real-processing workloads; timing uses both virtual and wall clocks."""
    from time import perf_counter

    results = []
    for size in (5, 10, 25, 50):
        cluster = EdgeClusterSimulation(fleet_size=size)
        started = perf_counter()
        normal_distribution = None
        failover_ms = 0.0
        normal_latency = normal_completed = 0
        for iteration in range(20):
            if iteration == 10:
                normal_distribution = {
                    w.edge_id: len(cluster._assigned(w.edge_id))
                    for w in cluster._eligible("west") + cluster._eligible("east")
                }
                normal_latency = sum(
                    w.latency_seconds for w in cluster.workers.values()
                )
                normal_completed = sum(w.completed for w in cluster.workers.values())
                failed_at = perf_counter()
                cluster.fail("edge-west-1")
                failover_ms = (perf_counter() - failed_at) * 1000
            for bus_id in cluster.bus_regions:
                cluster.generate(bus_id, 1)
            cluster.advance(1)
        elapsed = perf_counter() - started
        completed = sum(w.completed for w in cluster.workers.values())
        results.append(
            {
                "buses": size,
                "accepted": cluster.accepted,
                "rejected": cluster.rejected,
                "processed": cluster.outcomes.get("ACCEPTED", 0),
                "pending": len(cluster.journal),
                "wall_seconds": round(elapsed, 4),
                "virtual_throughput_per_second": cluster.accepted / cluster.time,
                "wall_throughput_per_second": round(cluster.accepted / elapsed, 2),
                "average_virtual_latency_ms": round(
                    sum(w.latency_seconds for w in cluster.workers.values())
                    / completed
                    * 1000,
                    3,
                ),
                "normal_virtual_latency_ms": round(
                    normal_latency / normal_completed * 1000, 3
                ),
                "failover_virtual_latency_ms": round(
                    (
                        sum(w.latency_seconds for w in cluster.workers.values())
                        - normal_latency
                    )
                    / (completed - normal_completed)
                    * 1000,
                    3,
                ),
                "failover_decision_wall_ms": round(failover_ms, 3),
                "normal_distribution": normal_distribution,
                "final_distribution": {
                    w.edge_id: len(cluster._assigned(w.edge_id))
                    for w in cluster.workers.values()
                    if w.active
                },
            }
        )
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch registered edge workers.")
    parser.add_argument(
        "--network", default=os.getenv("EDGE_NETWORK_CONFIG", str(DEFAULT_NETWORK_PATH))
    )
    parser.add_argument("--edge", action="append", dest="edge_ids")
    parser.add_argument(
        "--benchmark",
        action="store_true",
        help="Measure 5/10/25/50-bus simulation workloads",
    )
    parser.add_argument(
        "--plan",
        action="store_true",
        help="Show fixed route/bus assignments without starting workers",
    )
    parser.add_argument(
        "--single",
        action="store_true",
        help="Use EDGE_ID/CITY_ID/REGION_ID for one container",
    )
    args = parser.parse_args()
    if args.benchmark:
        print(json.dumps(benchmark_cluster(), indent=2))
    elif args.plan:
        print(json.dumps(deployment_plan(args.network), indent=2))
    elif args.single:
        run_single_instance(args.network)
    else:
        launch_instances(args.network, args.edge_ids)

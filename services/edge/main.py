"""Create configured edge instances; each runs in its own process and keeps its own state."""

import argparse
import json
import logging
import math
from multiprocessing import Event, Process
import os

from services.edge.communication.data_to_edge import load_edge_config
from services.edge.config import NetworkConfig
from services.edge.edge_server import EdgeServerInstance
from shared.schema.health import policy_from_environment
from shared.schema.network import DEFAULT_NETWORK_PATH, load_network


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
        plan.append(dict(edge_id=edge_id, city_id=city, region_id=region,
                         route_ids=sorted({bus["route_id"] for bus in buses}),
                         bus_ids=sorted(bus["bus_id"] for bus in buses)))
    return plan


def run_instance(network_path, identity, stop_event):
    """Construct actual worker objects inside the child, never pickle live clients."""
    city_id, region_id, edge_id = identity
    config = load_edge_config(city_id=city_id, region_id=region_id, edge_id=edge_id)
    worker = EdgeServerInstance(config, NetworkConfig.load(network_path), policy_from_environment(),
                                os.environ["CENTRAL_OCCUPANCY_UNIT"])
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
    if os.getenv("CITY_ID", identity[0]) != identity[0] or os.getenv("REGION_ID", identity[1]) != identity[1]:
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
            process = Process(target=run_instance, args=(str(network_path), instances[edge_id], stop), name=edge_id)
            process.start()
            processes.append(process)
        while any(process.is_alive() for process in processes):
            for process in processes:
                if process.exitcode is not None and process.name not in reported:
                    logging.warning("Edge instance exited: %s code=%s", process.name, process.exitcode)
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
                raise ValueError("Simulation third-instance ID conflicts with configured worker")
            self.workers[edge_id] = SimulatedEdge(edge_id, region["region_id"], region["city_id"], active=False)

    def _eligible(self, region):
        return [worker for worker in self.workers.values()
                if worker.region_id == region and worker.active]

    @staticmethod
    def _waiting_ms(worker, count=None):
        size = len(worker.queue) if count is None else count
        return max(0.0, size * worker.processing_ms - worker.credit_ms)

    def _observe(self):
        for region in self.regions:
            workers = self._eligible(region)
            overloaded = len(workers) >= 2 and all(len(w.queue) / w.capacity >= 0.8 for w in workers)
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
            targets = [w for w in workers if w is not source and len(w.queue) < w.capacity]
            if not source.queue or not targets:
                return
            target = min(targets, key=lambda w: (self._waiting_ms(w, len(w.queue) + 1), w.edge_id))
            before = self._waiting_ms(source)
            after = max(self._waiting_ms(source, len(source.queue) - 1),
                        self._waiting_ms(target, len(target.queue) + 1))
            if after >= before - 1e-9:
                return
            target.queue.append(source.queue.pop())
            if not source.queue:
                source.credit_ms = 0.0
            self.events.append(dict(time=self.time, action="move_synthetic_request",
                                    source=source.edge_id, target=target.edge_id))

    def submit(self, bus_id, count):
        with self.lock:
            if bus_id not in self.bus_regions:
                raise ValueError("Unknown bus ID")
            if isinstance(count, bool) or not isinstance(count, int) or not 1 <= count <= 10000:
                raise ValueError("Request count must be between 1 and 10000")
            accepted = 0
            region = self.bus_regions[bus_id]
            for _ in range(count):
                choices = [w for w in self._eligible(region) if len(w.queue) < w.capacity]
                if not choices:
                    break
                worker = min(choices, key=lambda w: (self._waiting_ms(w, len(w.queue) + 1),
                                                     len(w.queue) / w.capacity, w.edge_id))
                self.sequence += 1
                worker.queue.append((self.sequence, bus_id, self.time))
                accepted += 1
            self.accepted += accepted
            self.rejected += count - accepted
            self._observe()
            return dict(accepted=accepted, rejected=count - accepted, simulation=self.snapshot())

    def configure(self, edge_id, *, capacity=None, processing_ms=None, active=None):
        with self.lock:
            if edge_id not in self.workers:
                raise ValueError("Unknown simulated instance")
            worker = self.workers[edge_id]
            if capacity is not None and (isinstance(capacity, bool) or not isinstance(capacity, int) or not 1 <= capacity <= 50):
                raise ValueError("Queue capacity must be between 1 and 50")
            if processing_ms is not None and (isinstance(processing_ms, bool) or not isinstance(processing_ms, (float, int))
                                              or not math.isfinite(processing_ms) or not 1 <= processing_ms <= 60000):
                raise ValueError("Processing delay must be between 1 and 60000 ms")
            if active is not None and not isinstance(active, bool):
                raise ValueError("Active must be true or false")
            if capacity is not None and capacity < len(worker.queue):
                raise ValueError("Drain the queue before lowering capacity below its current size")
            if active is False and worker.queue:
                raise ValueError("Drain the queue before stopping this simulated instance")
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
            if isinstance(seconds, bool) or not isinstance(seconds, (float, int)) or not math.isfinite(seconds) or not 0 < seconds <= 60:
                raise ValueError("Advance virtual time by more than 0 and at most 60 seconds")
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
                    if self.auto_scale and since is not None and self.time - since >= 6 and not third.active:
                        third.active = True
                        self.events.append(dict(time=self.time, action="start_simulated_instance", edge_id=third.edge_id))
                    self._balance(region)
                self._observe()
                remaining -= step
            return self.snapshot()

    def snapshot(self):
        with self.lock:
            return dict(mode="simulation", live_workers_changed=False, virtual_seconds=self.time,
                        auto_scale=self.auto_scale, overload_percent=80, sustained_seconds=6,
                        accepted=self.accepted, rejected=self.rejected,
                        workers=[worker.snapshot() for worker in self.workers.values()], events=list(self.events))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Launch registered edge workers.")
    parser.add_argument("--network", default=os.getenv("EDGE_NETWORK_CONFIG", str(DEFAULT_NETWORK_PATH)))
    parser.add_argument("--edge", action="append", dest="edge_ids")
    parser.add_argument("--plan", action="store_true", help="Show fixed route/bus assignments without starting workers")
    parser.add_argument("--single", action="store_true", help="Use EDGE_ID/CITY_ID/REGION_ID for one container")
    args = parser.parse_args()
    if args.plan:
        print(json.dumps(deployment_plan(args.network), indent=2))
    elif args.single:
        run_single_instance(args.network)
    else:
        launch_instances(args.network, args.edge_ids)

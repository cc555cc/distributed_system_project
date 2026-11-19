"""Create configured edge instances; each runs in its own process and keeps its own state."""

import argparse
import json
import logging
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

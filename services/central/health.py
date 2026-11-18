"""Central watches edge heartbeats independently; failed edges cannot report themselves."""

from copy import deepcopy
from threading import RLock
import time

from fastapi import APIRouter

from services.messaging.topics import edge_heartbeat
from shared.schema.health import EdgeHeartbeat, HeartbeatTracker, HealthPolicy, SENSORS, policy_from_environment
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
                edges.append(dict(edge_id=edge_id, city_id=city, region_id=region, **status, last_report=report))
            return dict(edges=edges, recommendations=self.recommendations(edges, now))

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
            choices.append(dict(algorithm="least_queue_utilization", from_edge_id=source["edge_id"],
                                to_edge_id=target["edge_id"] if target else None,
                                action="consider_migration" if target else "additional_capacity_needed",
                                migration_performed=False))
        return choices


router = APIRouter(prefix="/health", tags=["health"])
# Built once per central process. A central restart intentionally forgets old liveness.
monitor = CentralHealthMonitor(load_network(), policy_from_environment())


@router.get("/edges")
def get_edge_health():
    return monitor.snapshot()

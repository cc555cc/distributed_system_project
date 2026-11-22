"""Track assigned sensors and this worker's queue without moving any bus ownership."""

from threading import RLock
import time

from services.messaging.topics import bus_heartbeat
from shared.schema.health import BusHeartbeat, HeartbeatTracker, HealthPolicy, SENSORS, load_band


class EdgeMonitor:
    def __init__(self, config, registry, policy: HealthPolicy, *, clock=time.monotonic):
        self.config, self.policy, self.clock = config, policy, clock
        self.bus_ids = registry.assigned_bus_ids(config.city_id, config.region_id, config.edge_id)
        keys = [(bus, sensor) for bus in self.bus_ids for sensor in SENSORS]
        self.tracker = HeartbeatTracker(keys, policy.sensor_timeout_seconds, policy.recovery_seconds, clock=clock)
        self.lock = RLock()
        self.started_at = clock()
        self.valid_data = {}
        self.last_speed = {}
        self.received = self.processed = 0
        self.processing_seconds = 0.0

    def receive(self, topic: str, payload: bytes) -> bool:
        """Heartbeats update health directly, not the bus-processing queue."""
        if len(payload) > 64 * 1024:
            return False
        try:
            heartbeat = BusHeartbeat.model_validate_json(payload, strict=True)
        except ValueError:
            return False
        cfg = self.config
        expected_topic = bus_heartbeat(cfg.region_id, heartbeat.bus_id)
        if topic != expected_topic or heartbeat.bus_id not in self.bus_ids or (
                heartbeat.city_id, heartbeat.region_id) != (cfg.city_id, cfg.region_id):
            return False
        changed = False
        for sensor, state in heartbeat.sensors.items():
            changed = self.tracker.record((heartbeat.bus_id, sensor), heartbeat.session_id,
                                          heartbeat.sequence_number, state) or changed
        return changed

    def mark_valid_data(self, bus_id, speed_kmh=None):
        """An alive sensor and a recent valid reading are different things."""
        with self.lock:
            self.valid_data[bus_id] = self.clock()
            self.last_speed[bus_id] = speed_kmh

    def record_received(self):
        with self.lock:
            self.received += 1

    def mark_processed(self, elapsed_seconds):
        with self.lock:
            self.processed += 1
            self.processing_seconds += max(0.0, elapsed_seconds)

    def sensor_reports(self) -> list[dict]:
        with self.lock:
            now = self.clock()
            return [dict(bus_id=bus, last_speed_kmh=self.last_speed.get(bus), sensors={sensor: self.tracker.report((bus, sensor)) for sensor in SENSORS},
                         valid_telemetry_age_seconds=(max(0.0, now - self.valid_data[bus]) if bus in self.valid_data else None))
                    for bus in self.bus_ids]

    def workload(self, queue, active_buses, pending_updates) -> dict:
        with self.lock:
            size, capacity = queue.qsize(), queue.maxsize
            elapsed = max(self.clock() - self.started_at, 0.001)
            utilization = size / capacity * 100
            return dict(queue_size=size, queue_capacity=capacity, queue_utilization_percent=utilization,
                        received_messages=self.received, processed_messages=self.processed,
                        messages_per_second=self.received / elapsed,
                        average_processing_ms=(self.processing_seconds / self.processed * 1000 if self.processed else 0.0),
                        active_buses=active_buses, assigned_buses=len(self.bus_ids), pending_updates=pending_updates,
                        load_band=load_band(utilization, self.policy.overload_percent))

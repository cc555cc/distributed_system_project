"""Connect the validated-message queue to the local processing pipeline."""

from dataclasses import dataclass
from queue import Empty

from services.edge.communication.data_to_edge import TelemetrySubscriber
from services.edge.processing.processor import EdgeProcessor, ProcessingResult
from services.edge.validation.validator import BusTelemetry


@dataclass
class EdgeIngress:
    """Drain outside the broker callback; retain latest states, not history."""

    processor: EdgeProcessor

    @property
    def region_id(self) -> str:
        return self.processor.config.region_id

    @property
    def accepted(self) -> list[dict]:
        """Latest accepted telemetry, isolated for inspection/tests."""
        return [entry.telemetry for entry in self.processor.cache.snapshot().values()]

    def process_telemetry(self, telemetry: BusTelemetry) -> ProcessingResult:
        return self.processor.process(telemetry)

    def drain_pending(self, subscriber: TelemetrySubscriber) -> list[ProcessingResult]:
        """Return outcomes to Python; never publish processed JSON."""
        if subscriber.region_id != self.region_id:
            raise ValueError("Subscriber and edge must belong to same region")
        if subscriber.city_id is not None and subscriber.city_id != self.processor.config.city_id:
            raise ValueError("Subscriber and edge must belong to same city")
        results = []
        while True:
            try:
                # The subscriber only queues checked messages; processing happens here.
                item = subscriber.messages.get_nowait()
            except Empty:
                return results
            try:
                results.append(self.process_telemetry(item))
            finally:
                subscriber.messages.task_done()


class EdgeServerInstance:
    """One running worker owns its queue, cache, sender and heartbeat reports."""

    def __init__(self, config, network, policy, occupancy_unit="passengers"):
        from services.edge.communication.data_to_edge import create_subscriber
        from services.edge.communication.edge_to_central import CentralForwarder, EdgeToCentral
        from services.edge.edge_server_health import EdgeMonitor
        from shared.schema.bus_state import BusStateUpdate
        from uuid import uuid4
        self.config, self.policy = config, policy
        self.monitor = EdgeMonitor(config, network.registry, policy)
        self.subscriber = create_subscriber(config, network, monitor=self.monitor)
        self.receiver = EdgeIngress(EdgeProcessor(config, network))
        self.sender = EdgeToCentral(config, network.registry, BusStateUpdate)
        self.forwarder = CentralForwarder(self.receiver, self.sender, occupancy_unit)
        self.session_id = str(uuid4())
        self.heartbeat_sequence = 0

    def heartbeat(self):
        """Build a checked health snapshot without touching bus ownership."""
        from datetime import datetime, timezone
        from shared.schema.health import EdgeHeartbeat
        self.heartbeat_sequence += 1
        active = self.receiver.processor.snapshot()["statistics"]["active_buses"]
        return EdgeHeartbeat(
            city_id=self.config.city_id, region_id=self.config.region_id, edge_id=self.config.edge_id,
            session_id=self.session_id, sequence_number=self.heartbeat_sequence,
            timestamp=datetime.now(timezone.utc).isoformat(),
            workload=self.monitor.workload(self.subscriber.messages, active, len(self.forwarder.pending)),
            buses=self.monitor.sensor_reports(),
        )

    def run(self, stop_event):
        """Keep health reporting independent of an idle or busy telemetry queue."""
        import logging
        import os
        from threading import Event, Thread
        from services.edge.communication.data_to_edge import process_pending_messages
        heartbeat_stop = Event()
        def publish_health():
            while not heartbeat_stop.is_set() and not stop_event.is_set():
                try:
                    self.sender.publish_heartbeat(self.heartbeat())
                except (ConnectionError, OSError, RuntimeError):
                    logging.warning("Edge heartbeat pending: edge=%s", self.config.edge_id)
                heartbeat_stop.wait(self.policy.interval_seconds)
        thread = Thread(target=publish_health, name=self.config.edge_id + "-heartbeat", daemon=True)
        try:
            self.sender.connect(os.getenv("CENTRAL_MQTT_HOST", os.getenv("MQTT_HOST", "127.0.0.1")),
                                int(os.getenv("CENTRAL_MQTT_PORT", os.getenv("MQTT_PORT", "1883"))))
            self.subscriber.start()
            thread.start()
            process_pending_messages(self.subscriber, self.receiver, self.forwarder, stop_event=stop_event)
        except KeyboardInterrupt:
            pass
        finally:
            heartbeat_stop.set()
            if thread.is_alive():
                thread.join(timeout=self.policy.interval_seconds + 2)
            self.subscriber.stop()
            self.sender.close()


@dataclass
class SimulatedEdge:
    """UI experiment worker; telemetry uses isolated processor/cache instances."""

    edge_id: str
    region_id: str
    city_id: str
    capacity: int = 50
    processing_ms: float = 5.0
    active: bool = True

    def __post_init__(self):
        from collections import deque
        from services.messaging.lamport import LamportClock
        self.queue = deque()
        self.clock = LamportClock()  # same receive/send rules as a live worker
        self.credit_ms = 0.0
        self.completed = 0
        self.latency_seconds = 0.0
        self.state = "healthy" if self.active else "standby"
        self.replicas = {}
        self.processors = {}
        self.on_telemetry = None

    def advance(self, seconds, now):
        # Virtual time lets the UI repeat a load experiment without sleeping.
        if not self.active or not self.queue:
            self.credit_ms = 0.0
            return
        available = self.credit_ms + seconds * 1000
        offset_ms = -self.credit_ms
        while self.queue and available + 1e-9 >= self.processing_ms:
            available -= self.processing_ms
            offset_ms += self.processing_ms
            job = self.queue.popleft()
            _, _, received_at = job[:3]
            if len(job) > 3:
                self.on_telemetry(self, job)
            self.completed += 1
            self.latency_seconds += max(0.0, now - seconds + offset_ms / 1000 - received_at)
        self.credit_ms = available if self.queue else 0.0

    def snapshot(self):
        from shared.schema.health import load_band
        utilization = len(self.queue) / self.capacity * 100
        return dict(edge_id=self.edge_id, region_id=self.region_id, city_id=self.city_id,
                    active=self.active, state=self.state, queue_capacity=self.capacity, queue_size=len(self.queue),
                    queue_utilization_percent=utilization, load_band=load_band(utilization),
                    processing_ms=self.processing_ms, completed=self.completed,
                    average_latency_ms=(self.latency_seconds / self.completed * 1000 if self.completed else None))

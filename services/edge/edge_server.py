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

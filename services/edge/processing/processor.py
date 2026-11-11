"""The single EDGE-02 entry point; local transactions are serialized by a lock."""

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import time

from services.edge.cache.bus_state_cache import BusStateCache, CacheEntry
from services.edge.config import EdgeConfig, NetworkConfig
from services.edge.processing.occupancy_processor import calculate_occupancy
from services.edge.processing.route_processor import RouteProcessor
from services.edge.transformation.output_builder import build_output
from services.edge.validation.validator import BusTelemetry


class ProcessingStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    CONFLICT = "CONFLICT"
    NOT_ASSIGNED = "NOT_ASSIGNED"
    INVALID_ROUTE = "INVALID_ROUTE"
    CAPACITY_EXCEEDED = "CAPACITY_EXCEEDED"


@dataclass(frozen=True)
class ProcessingResult:
    status: ProcessingStatus
    output: dict | None = None
    reason: str | None = None


class EdgeProcessor:
    """Process validated telemetry without MQTT, databases or cross-server work."""

    def __init__(self, config: EdgeConfig, network: NetworkConfig, *,
                 clock: Callable[[], float] = time.monotonic,
                 utc_now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)) -> None:
        self.config = config
        self.registry = network.registry
        self.routes = RouteProcessor(network.routes)
        self.cache = BusStateCache()
        self.clock = clock
        self.utc_now = utc_now

    def _states(self, now: float) -> list[dict]:
        states = []
        for _key, entry in self.cache.values():
            state = deepcopy(entry.processed)
            state["freshness"] = ("current" if now - entry.accepted_at <
                                  self.config.freshness_seconds else "stale")
            states.append(state)
        return states

    def snapshot(self) -> dict:
        """Read current freshness and statistics even when no new updates arrive."""
        with self.cache.lock:
            return build_output(self.config, self._states(self.clock()))

    def process(self, telemetry: BusTelemetry) -> ProcessingResult:
        """Accept already validated telemetry and atomically return a snapshot.

        Exact duplicates compare all validated fields (normalizing optional city
        and direction defaults). Unknown extension fields do not affect identity.
        No rejected update changes state, receipt time, sequence floors or output.
        """
        cfg = self.config
        assignment = self.registry.get(telemetry["bus_id"])
        if assignment is None or (assignment.city_id, assignment.region_id, assignment.edge_id) != (
                cfg.city_id, cfg.region_id, cfg.edge_id):
            return ProcessingResult(ProcessingStatus.NOT_ASSIGNED)
        if telemetry["region_id"] != cfg.region_id or telemetry.get("city_id", assignment.city_id) != cfg.city_id:
            return ProcessingResult(ProcessingStatus.NOT_ASSIGNED)
        if telemetry["route_id"] != assignment.route_id:
            return ProcessingResult(ProcessingStatus.INVALID_ROUTE)
        fields = BusTelemetry.__required_keys__ | BusTelemetry.__optional_keys__
        source = deepcopy({k: v for k, v in telemetry.items() if k in fields})
        source["city_id"] = cfg.city_id
        source.setdefault("direction_id", 0)
        key = (cfg.city_id, cfg.region_id, telemetry["bus_id"])
        with self.cache.lock:
            now = self.clock()
            existing = self.cache.get(key)
            floor = self.cache.sequence_floor(key)
            sequence = telemetry["sequence_number"]
            if floor is not None:
                if sequence < floor or (existing is None and sequence == floor):
                    return ProcessingResult(ProcessingStatus.STALE)
                if sequence == floor and existing is not None:
                    status = (ProcessingStatus.DUPLICATE if source == existing.telemetry
                              else ProcessingStatus.CONFLICT)
                    return ProcessingResult(status)
            if existing is None and not self.cache.has_capacity(cfg.max_buses, now, cfg.expire_seconds):
                return ProcessingResult(ProcessingStatus.CAPACITY_EXCEEDED)
            try:
                route = self.routes.process(source, cfg.city_id, cfg.region_id)
            except ValueError as error:
                return ProcessingResult(ProcessingStatus.INVALID_ROUTE, reason=str(error))
            state = {**source, **route,
                     **calculate_occupancy(source, cfg.medium_threshold, cfg.high_threshold)}
            state["source_timestamp"] = state.pop("timestamp")
            state["processing_timestamp"] = self.utc_now().isoformat()
            state["edge_id"] = cfg.edge_id
            self.cache.put(key, CacheEntry(source, state, now), cfg.max_buses, now, cfg.expire_seconds)
            return ProcessingResult(ProcessingStatus.ACCEPTED, build_output(cfg, self._states(now)))

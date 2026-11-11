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
            # Work out age when reading too, even if no new messages arrive.
            state["freshness"] = ("current" if now - entry.accepted_at <
                                  self.config.freshness_seconds else "stale")
            states.append(state)
        return states

    def snapshot(self) -> dict:
        """Read current freshness and statistics even when no new updates arrive."""
        with self.cache.lock:
            return build_output(self.config, self._states(self.clock()))

    def _check_assignment(self, telemetry: BusTelemetry) -> ProcessingStatus | None:
        """Check which city, region and edge server should handle this bus."""
        cfg = self.config
        assignment = self.registry.get(telemetry["bus_id"])
        if assignment is None or (assignment.city_id, assignment.region_id, assignment.edge_id) != (
                cfg.city_id, cfg.region_id, cfg.edge_id):
            return ProcessingStatus.NOT_ASSIGNED
        if telemetry["region_id"] != cfg.region_id or telemetry.get("city_id", assignment.city_id) != cfg.city_id:
            return ProcessingStatus.NOT_ASSIGNED
        if telemetry["route_id"] != assignment.route_id:
            return ProcessingStatus.INVALID_ROUTE
        return None

    def _prepare_source(self, telemetry: BusTelemetry) -> dict:
        """Copy the checked fields so the caller cannot change our cached data."""
        fields = BusTelemetry.__required_keys__ | BusTelemetry.__optional_keys__
        source = deepcopy({key: value for key, value in telemetry.items() if key in fields})
        source["city_id"] = self.config.city_id
        source.setdefault("direction_id", 0)
        return source

    def _check_sequence(self, key: tuple[str, str, str], source: dict) -> ProcessingStatus | None:
        """Reject old or repeated readings before doing the calculations."""
        existing = self.cache.get(key)
        floor = self.cache.sequence_floor(key)
        sequence = source["sequence_number"]
        if floor is None:
            return None
        # Keep remembering the sequence even if the old bus state expired.
        if sequence < floor or (existing is None and sequence == floor):
            return ProcessingStatus.STALE
        if sequence == floor and existing is not None:
            if source == existing.telemetry:
                return ProcessingStatus.DUPLICATE
            return ProcessingStatus.CONFLICT
        return None

    def _build_state(self, source: dict) -> dict:
        """Check the route, calculate occupancy and keep the source timestamp."""
        cfg = self.config
        route = self.routes.process(source, cfg.city_id, cfg.region_id)
        occupancy = calculate_occupancy(source, cfg.medium_threshold, cfg.high_threshold)
        state = {**source, **route, **occupancy}
        state["source_timestamp"] = state.pop("timestamp")
        state["processing_timestamp"] = self.utc_now().isoformat()
        state["edge_id"] = cfg.edge_id
        return state

    def process(self, telemetry: BusTelemetry) -> ProcessingResult:
        """Turn one validated reading into the latest state and local totals."""
        # EDGE-01 has already checked the measurements. Check ownership here.
        rejected = self._check_assignment(telemetry)
        if rejected is not None:
            return ProcessingResult(rejected)
        source = self._prepare_source(telemetry)
        cfg = self.config
        key = (cfg.city_id, cfg.region_id, source["bus_id"])

        # Keep this lock until the snapshot is built. Another message must not
        # slip between checking the old sequence and saving the new state.
        with self.cache.lock:
            now = self.clock()
            rejected = self._check_sequence(key, source)
            if rejected is not None:
                return ProcessingResult(rejected)

            # A full cache can still update buses already stored in it.
            is_new_bus = self.cache.get(key) is None
            if is_new_bus and not self.cache.has_capacity(cfg.max_buses, now, cfg.expire_seconds):
                return ProcessingResult(ProcessingStatus.CAPACITY_EXCEEDED)
            try:
                state = self._build_state(source)
            except ValueError as error:
                return ProcessingResult(ProcessingStatus.INVALID_ROUTE, reason=str(error))

            # Nothing above changes accepted state. Save only after all checks pass.
            entry = CacheEntry(source, state, now)
            self.cache.put(key, entry, cfg.max_buses, now, cfg.expire_seconds)
            output = build_output(cfg, self._states(now))
            return ProcessingResult(ProcessingStatus.ACCEPTED, output)

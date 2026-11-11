"""The single EDGE-02 entry point; local transactions are serialized by a lock."""

from collections import Counter
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import time

from services.edge.cache.edge_state_cache import EdgeStateCache, CacheEntry
from services.edge.config import EdgeConfig, NetworkConfig, RouteDefinition
from services.edge.validation.validator import BusTelemetry


# Small processing helpers stay here until they need their own modules.

def calculate_occupancy(telemetry: dict, medium: float, high: float) -> dict:
    """Use reported counts only; conservatively suppress faulty sensor results."""
    # Keep faulty sensor information unavailable rather than guessing a count.
    if telemetry.get("sensor_status", "healthy") != "healthy":
        return {"occupancy_percent": None, "crowding_status": "unavailable"}
    percent = telemetry["passengers"] / telemetry["capacity"] * 100
    status = "high" if percent >= high else "medium" if percent >= medium else "low"
    return {"occupancy_percent": percent, "crowding_status": status}


def aggregate(states: list[dict]) -> dict:
    """Exclude stale/offline readings; keep unknown occupancy out of the mean."""
    # Old or offline readings must not make the region look busier than it is.
    active = [s for s in states if s["freshness"] == "current"
              and s.get("sensor_status", "healthy") != "offline"]
    # Unavailable readings stay out of the average; they are not zero occupancy.
    occupancy = [s["occupancy_percent"] for s in active
                 if s["occupancy_percent"] is not None]
    return {
        "scope": "edge_assigned_buses",
        "active_buses": len(active),
        "crowded_buses": sum(s["crowding_status"] == "high" for s in active),
        "average_occupancy_percent": sum(occupancy) / len(occupancy) if occupancy else None,
        "occupancy_samples": len(occupancy),
        "active_buses_per_route": dict(Counter(s["route_id"] for s in active)),
    }


OUTPUT_SCHEMA_VERSION = "edge-state-proposal-v1"


def build_output(config: EdgeConfig, states: list[dict]) -> dict:
    """Return a JSON-serializable per-bus snapshot and explicitly scoped totals."""
    # These totals cover this edge server, not every server in the region.
    return {"schema_version": OUTPUT_SCHEMA_VERSION,
            "city_id": config.city_id, "region_id": config.region_id,
            "edge_id": config.edge_id, "buses": states, "statistics": aggregate(states)}


class _RouteProcessor:
    def __init__(self, routes: tuple[RouteDefinition, ...]) -> None:
        self.routes = {}
        self.positions = {}
        for route in routes:
            key = (route.city_id, route.region_id, route.route_id, route.direction_id)
            if key in self.routes:
                raise ValueError("Duplicate directional route")
            self.routes[key] = route
            # Build the stop lookup once instead of searching the route every time.
            self.positions[key] = {stop: i for i, stop in enumerate(route.stops)}

    def process(self, telemetry: dict, city: str, region: str) -> dict:
        """Return only progress explicitly supported by reported stop IDs."""
        direction = telemetry.get("direction_id", 0)
        key = (city, region, telemetry["route_id"], direction)
        route = self.routes.get(key)
        if route is None:
            raise ValueError("Route/city/region/direction is not configured")
        positions = self.positions[key]
        current, following = telemetry.get("current_stop_id"), telemetry.get("next_stop_id")
        for stop in (current, following):
            if stop is not None and stop not in positions:
                raise ValueError("Reported stop does not belong to route")
        if current is not None and following is not None:
            if positions[following] != positions[current] + 1:
                raise ValueError("Current and next stop must be adjacent in travel order")
        # GPS proximity does not prove arrival. Use the reported current stop only.
        progress = None
        if current is not None and len(route.stops) > 1:
            progress = positions[current] / (len(route.stops) - 1) * 100
        return {"direction_id": direction, "current_stop_id": current,
                "next_stop_id": following, "route_progress_percent": progress,
                "progress_basis": "reported_current_stop" if progress is not None else "unavailable"}


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
        self.routes = _RouteProcessor(network.routes)
        self.cache = EdgeStateCache()
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

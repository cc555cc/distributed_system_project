"""Local latest-state storage; the coordinator holds the lock for a transaction."""

from copy import deepcopy
from dataclasses import dataclass
from threading import RLock

CacheKey = tuple[str, str, str]


@dataclass
class CacheEntry:
    """Accepted source data and its derived state, with monotonic receipt time."""

    telemetry: dict
    processed: dict
    accepted_at: float


class BusStateCache:
    """Retain sequence floors when expired entries release state capacity.

    Floors are bounded by the static configured registry: unknown/unowned buses
    are rejected before insertion. Expiration never resets a bus's sequence.
    """

    def __init__(self) -> None:
        self.lock = RLock()
        self._entries: dict[CacheKey, CacheEntry] = {}
        self._sequence_floors: dict[CacheKey, int] = {}

    def snapshot(self) -> dict[CacheKey, CacheEntry]:
        """Return isolated state so callers cannot mutate the cache."""
        with self.lock:
            return deepcopy(self._entries)

    def get(self, key: CacheKey) -> CacheEntry | None:
        """Internal access; caller must hold lock throughout processing."""
        return self._entries.get(key)

    def sequence_floor(self, key: CacheKey) -> int | None:
        return self._sequence_floors.get(key)

    def has_capacity(self, maximum: int, now: float, expiry: float) -> bool:
        """Check without mutation, so rejected updates cannot evict state."""
        return len(self._entries) < maximum or any(
            now - entry.accepted_at >= expiry for entry in self._entries.values()
        )

    def put(self, key: CacheKey, entry: CacheEntry, maximum: int,
            now: float, expiry: float) -> None:
        """Commit only after validation/calculations have succeeded, under lock."""
        if key not in self._entries and len(self._entries) >= maximum:
            expired = next(k for k, v in self._entries.items()
                           if now - v.accepted_at >= expiry)
            del self._entries[expired]
        self._entries[key] = entry
        self._sequence_floors[key] = entry.telemetry["sequence_number"]

    def values(self):
        """Internal iterator; use under lock only."""
        return self._entries.items()

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


class EdgeStateCache:
    """Retain sequence floors when expired entries release state capacity.

    Floors are bounded by the static configured registry: unknown/unowned buses
    are rejected before insertion. Expiration never resets a bus's sequence.
    """

    def __init__(self) -> None:
        self.lock = RLock()
        self._entries: dict[CacheKey, CacheEntry] = {}
        # Remember the newest sequence even after an expired state is removed.
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
            # Never remove a bus that is still inside its expiry timeout.
            expired = next(k for k, v in self._entries.items()
                           if now - v.accepted_at >= expiry)
            del self._entries[expired]
        self._entries[key] = entry
        self._sequence_floors[key] = entry.telemetry["sequence_number"]

    def values(self):
        """Internal iterator; use under lock only."""
        return self._entries.items()


    def export_bus(self, key):
        """Copy both state and replay protection; an empty cache still has a floor."""
        with self.lock:
            return {"entry": deepcopy(self._entries.get(key)), "floor": self._sequence_floors.get(key)}

    def restore_bus(self, key, state):
        """Install a synchronized replica without allowing its sequence to go back."""
        with self.lock:
            floor, entry = state["floor"], state["entry"]
            current = self._sequence_floors.get(key)
            if current is not None and (floor is None or floor < current):
                raise ValueError("Replica is older than the local sequence floor")
            if entry is not None and entry.telemetry["sequence_number"] != floor:
                raise ValueError("Replica state and sequence floor disagree")
            if floor is not None:
                self._sequence_floors[key] = floor
            if entry is None:
                self._entries.pop(key, None)
            else:
                self._entries[key] = deepcopy(entry)

    def release_bus(self, key):
        """A former owner can forget data but must keep its replay floor."""
        with self.lock:
            self._entries.pop(key, None)

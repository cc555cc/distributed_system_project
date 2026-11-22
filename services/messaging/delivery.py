"""Receiver-side delivery accounting from per-sender sequence numbers.

QoS 1 is at-least-once, so a receiver can see a message late, twice, or (when a
subscriber was away) never. This tracker classifies every arrival and lists
exactly which sequence numbers never arrived, so a lost message is reported
instead of assumed delivered (MQTT-06, MQTT-10, MQTT-11).
"""

from dataclasses import dataclass, field
from threading import Lock

NEW, DUPLICATE, LATE = "new", "duplicate", "late"


@dataclass
class _Sender:
    seen: set = field(default_factory=set)
    highest: int = 0
    arrivals: int = 0
    duplicates: int = 0
    late: int = 0


class SequenceTracker:
    def __init__(self):
        self._senders: dict[str, _Sender] = {}
        self._lock = Lock()

    def record(self, sender: str, sequence: int) -> str:
        """Classify one arrival: new in order, duplicate, or late (after a higher one)."""
        with self._lock:
            state = self._senders.setdefault(sender, _Sender())
            state.arrivals += 1
            if sequence in state.seen:
                state.duplicates += 1
                return DUPLICATE
            state.seen.add(sequence)
            if sequence < state.highest:
                state.late += 1
                return LATE
            state.highest = sequence
            return NEW

    def received(self, sender: str) -> set[int]:
        with self._lock:
            return set(self._senders.get(sender, _Sender()).seen)

    def missing(self, sender: str, sent) -> list[int]:
        """Sequence numbers the sender sent that never arrived."""
        return sorted(set(sent) - self.received(sender))

    def report(self, sent: dict[str, list[int]]) -> dict:
        """Totals across senders, given what each sender actually sent."""
        with self._lock:
            senders = {name: (state.arrivals, state.duplicates, state.late, len(state.seen))
                       for name, state in self._senders.items()}
        missing = {name: self.missing(name, sequences) for name, sequences in sent.items()}
        return dict(
            sent=sum(len(s) for s in sent.values()),
            arrivals=sum(v[0] for v in senders.values()),
            unique=sum(v[3] for v in senders.values()),
            duplicates=sum(v[1] for v in senders.values()),
            late=sum(v[2] for v in senders.values()),
            lost=sum(len(m) for m in missing.values()),
            missing={name: m for name, m in missing.items() if m},
            unexpected=sorted(set(senders) - set(sent)),
        )

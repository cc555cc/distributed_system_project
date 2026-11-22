"""Causal analysis of message flows: Lamport stamps plus vector clocks (MQTT-09).

Lamport clocks guarantee one direction only: if event a happened before b
(a -> b), then L(a) < L(b). The converse is false. L(a) < L(b) also holds for
many pairs that are *concurrent* (neither could have influenced the other). To
tell those apart this module tracks a vector clock beside each process's real
LamportClock. Vector clocks are an analysis tool for tests and diagrams. The
wire format carries only the Lamport stamp.

Each Process follows the same rules as the running services:

* local event / send:  Lamport tick; own vector entry + 1
* receive:             Lamport max(local, received) + 1; vector element-wise max, then own entry + 1

``python -m services.messaging.causality`` prints the event diagram for a
scenario with two buses, an edge and central, including a delayed,
out-of-order delivery and a command.
"""

from dataclasses import dataclass, field
from itertools import combinations

from services.messaging.lamport import LamportClock

BEFORE, AFTER, CONCURRENT, SAME = "->", "<-", "||", "=="


def compare(a: dict, b: dict) -> str:
    """Relation of vector timestamp a to b."""
    keys = set(a) | set(b)
    less = any(a.get(k, 0) < b.get(k, 0) for k in keys)
    greater = any(a.get(k, 0) > b.get(k, 0) for k in keys)
    if less and not greater:
        return BEFORE
    if greater and not less:
        return AFTER
    return SAME if not less and not greater else CONCURRENT


@dataclass(frozen=True)
class Event:
    process: str
    index: int  # position on its process's timeline, from 1
    kind: str  # "local", "send" or "receive"
    label: str
    lamport: int
    vector: tuple  # sorted (process, count) pairs
    message: str | None = None  # message id sent or received

    @property
    def vclock(self) -> dict:
        return dict(self.vector)

    @property
    def name(self) -> str:
        return f"{self.process}.{self.index}"


@dataclass(frozen=True)
class Message:
    id: str
    sender: str
    lamport: int
    vector: tuple
    label: str


@dataclass
class Process:
    name: str
    log: "EventLog"
    clock: LamportClock = field(default_factory=LamportClock)
    vector: dict = field(default_factory=dict)
    events: list = field(default_factory=list)

    def _record(self, kind, label, lamport, message=None):
        event = Event(
            self.name,
            len(self.events) + 1,
            kind,
            label,
            lamport,
            tuple(sorted(self.vector.items())),
            message,
        )
        self.events.append(event)
        self.log.events.append(event)
        return event

    def local(self, label) -> Event:
        self.vector[self.name] = self.vector.get(self.name, 0) + 1
        return self._record("local", label, self.clock.tick())

    def send(self, label) -> tuple[Event, Message]:
        self.vector[self.name] = self.vector.get(self.name, 0) + 1
        stamp = self.clock.tick()
        message_id = f"m{len(self.log.messages) + 1}"
        message = Message(
            message_id, self.name, stamp, tuple(sorted(self.vector.items())), label
        )
        self.log.messages[message_id] = message
        return self._record("send", label, stamp, message_id), message

    def receive(self, message: Message, label=None) -> Event:
        for process, count in message.vector:
            self.vector[process] = max(self.vector.get(process, 0), count)
        self.vector[self.name] = self.vector.get(self.name, 0) + 1
        stamp = self.clock.receive(message.lamport)
        return self._record(
            "receive", label or f"recv {message.label}", stamp, message.id
        )


class EventLog:
    def __init__(self, *names):
        self.events: list[Event] = []
        self.messages: dict[str, Message] = {}
        self.processes = {name: Process(name, self) for name in names}

    def __getitem__(self, name) -> Process:
        return self.processes[name]

    def relation(self, a: Event, b: Event) -> str:
        return compare(a.vclock, b.vclock)

    def pairs(self):
        """Every unordered pair of events with its causal relation."""
        for a, b in combinations(self.events, 2):
            yield a, b, self.relation(a, b)

    def concurrent_pairs(self):
        return [(a, b) for a, b, relation in self.pairs() if relation == CONCURRENT]

    def total_order(self):
        """Lamport total order: (stamp, process name). Consistent with causality."""
        return sorted(self.events, key=lambda e: (e.lamport, e.process))

    @staticmethod
    def _cell(event):
        text = f"({event.lamport}) {event.kind[0].upper()} {event.label}"
        return text + (f" [{event.message}]" if event.message else "")

    def diagram(self) -> str:
        """Text space-time diagram: one column per process, rows in Lamport total order."""
        names = list(self.processes)
        width = max(len(self._cell(e)) for e in self.events) + 2
        lines = [
            "".join(name.ljust(width) for name in names).rstrip(),
            "".join(("-" * (width - 2)).ljust(width) for _ in names).rstrip(),
        ]
        for event in self.total_order():
            cells = ["|".ljust(width) for _ in names]
            cells[names.index(event.process)] = self._cell(event).ljust(width)
            lines.append("".join(cells).rstrip())
        lines += [
            "",
            "(n) Lamport stamp; S send, R receive, L local event; [mN] links a send to its receive.",
        ]
        return "\n".join(lines)

    def relation_table(self, events) -> str:
        """Matrix of causal relations between the given events (row relation column)."""
        width = max(len(e.name) for e in events) + 2
        rows = [" " * width + "".join(e.name.rjust(width) for e in events)]
        for a in events:
            rows.append(
                a.name.rjust(width)
                + "".join(
                    (self.relation(a, b) if a is not b else "").rjust(width)
                    for b in events
                )
            )
        return "\n".join(rows)


def demo_scenario() -> EventLog:
    """Two buses, one edge, central. Bus B1's reading 1 is delayed and arrives after reading 2."""
    log = EventLog("bus-1", "bus-2", "edge", "central")
    b1, b2, edge, central = (
        log[name] for name in ("bus-1", "bus-2", "edge", "central")
    )
    _, r11 = b1.send("B1 seq 1")
    _, r21 = b2.send("B2 seq 1")  # concurrent with B1 seq 1
    _, r12 = b1.send("B1 seq 2")
    edge.receive(r12, "recv B1 seq 2")  # arrives first: B1 seq 1 is delayed
    _, s12 = edge.send("state B1 seq 2")
    edge.receive(r11, "recv B1 seq 1 (late: STALE)")
    edge.receive(r21, "recv B2 seq 1")
    _, s21 = edge.send("state B2 seq 1")
    central.receive(s21, "recv state B2")  # edge-to-central also reordered
    central.receive(s12, "recv state B1")
    _, cmd = central.send("cmd pause bus-2")
    b1.local("B1 sensor fault")  # concurrent with the command
    b2.receive(cmd, "recv cmd")
    b2.send("B2 seq 2 (after cmd)")
    return log


if __name__ == "__main__":
    log = demo_scenario()
    print(log.diagram())
    print()
    picks = [
        e
        for e in log.events
        if e.label
        in (
            "B1 seq 1",
            "B2 seq 1",
            "B1 sensor fault",
            "cmd pause bus-2",
            "B2 seq 2 (after cmd)",
        )
    ]
    print(log.relation_table(picks))
    print()
    print(
        "Concurrent pairs with different Lamport stamps (Lamport alone would order them):"
    )
    for a, b in log.concurrent_pairs():
        if a.lamport != b.lamport:
            print(
                f"  {a.name} '{a.label}' (L={a.lamport})  ||  {b.name} '{b.label}' (L={b.lamport})"
            )

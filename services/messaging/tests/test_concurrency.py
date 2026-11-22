"""MQTT-09: Lamport clocks with concurrent buses and services, delays and reordering.

Offline. Processes use the real LamportClock; vector clocks recorded beside it
identify which events are causally related and which are genuinely concurrent.
The event diagram is printed by ``python -m services.messaging.causality``.
"""

import random

import pytest

from services.edge.config import EdgeConfig, NetworkConfig, RouteDefinition
from services.edge.processing.processor import EdgeProcessor, ProcessingStatus
from services.edge.validation.bus_registry import BusAssignment, BusRegistry
from services.messaging.causality import (
    AFTER,
    BEFORE,
    CONCURRENT,
    EventLog,
    compare,
    demo_scenario,
)


def event(log, label):
    return next(e for e in log.events if e.label == label)


def check_lamport_rules(log):
    """Every event obeys send/local = previous + 1 and receive = max(previous, message) + 1."""
    for process in log.processes.values():
        previous = 0
        for e in process.events:
            if e.kind == "receive":
                assert e.lamport == max(previous, log.messages[e.message].lamport) + 1, e
            else:
                assert e.lamport == previous + 1, e
            previous = e.lamport


def check_clock_condition(log):
    """a -> b implies L(a) < L(b); equal stamps imply concurrency."""
    for a, b, relation in log.pairs():
        if relation == BEFORE:
            assert a.lamport < b.lamport, (a, b)
        elif relation == AFTER:
            assert b.lamport < a.lamport, (a, b)
        if a.lamport == b.lamport:
            assert relation == CONCURRENT, (a, b)


# ==================== VECTOR COMPARISON ====================
@pytest.mark.parametrize("a, b, expected", [
    ({"p": 1}, {"p": 2}, BEFORE), ({"p": 2, "q": 1}, {"p": 1, "q": 1}, AFTER),
    ({"p": 1}, {"q": 1}, CONCURRENT), ({"p": 2, "q": 0}, {"p": 1, "q": 3}, CONCURRENT),
    ({"p": 1}, {"p": 1}, "=="),
])
def test_vector_comparison(a, b, expected):
    assert compare(a, b) == expected


# ==================== THE DOCUMENTED SCENARIO ====================
@pytest.fixture
def scenario():
    return demo_scenario()


def test_scenario_lamport_values_follow_the_rules(scenario):
    check_lamport_rules(scenario)
    check_clock_condition(scenario)
    stamps = {e.label: e.lamport for e in scenario.events}
    assert stamps["B1 seq 1"] == stamps["B2 seq 1"] == 1          # independent buses, same stamp
    assert stamps["recv B1 seq 2"] == 3                           # max(0, 2) + 1
    assert stamps["recv B1 seq 1 (late: STALE)"] == 5             # late message cannot lower the clock
    assert stamps["recv cmd"] == 11 and stamps["B2 seq 2 (after cmd)"] == 12


def test_causal_relationships_are_identified(scenario):
    rel = scenario.relation
    # Same bus: program order.
    assert rel(event(scenario, "B1 seq 1"), event(scenario, "B1 seq 2")) == BEFORE
    # Through the edge and central to the command and bus-2's next reading.
    assert rel(event(scenario, "B1 seq 2"), event(scenario, "cmd pause bus-2")) == BEFORE
    assert rel(event(scenario, "B1 seq 1"), event(scenario, "B2 seq 2 (after cmd)")) == BEFORE
    assert rel(event(scenario, "cmd pause bus-2"), event(scenario, "recv cmd")) == BEFORE


def test_genuinely_concurrent_events_are_distinguished_from_lamport_order(scenario):
    rel = scenario.relation
    fault, command = event(scenario, "B1 sensor fault"), event(scenario, "cmd pause bus-2")
    # Lamport orders them (3 < 10) but neither could have influenced the other.
    assert fault.lamport < command.lamport and rel(fault, command) == CONCURRENT
    assert rel(event(scenario, "B1 seq 1"), event(scenario, "B2 seq 1")) == CONCURRENT
    # The fault is concurrent with everything after B1's last send outside bus-1.
    assert rel(fault, event(scenario, "B2 seq 2 (after cmd)")) == CONCURRENT


def test_out_of_order_delivery_keeps_causal_order_of_the_sends(scenario):
    late = event(scenario, "recv B1 seq 1 (late: STALE)")
    early = event(scenario, "recv B1 seq 2")
    assert early.index < late.index                               # delivered in the wrong order
    first_send, second_send = event(scenario, "B1 seq 1"), event(scenario, "B1 seq 2")
    assert scenario.relation(first_send, second_send) == BEFORE   # causality of the sends is unchanged
    assert late.lamport > early.lamport > second_send.lamport > first_send.lamport


def test_lamport_total_order_is_consistent_with_causality(scenario):
    order = {e: position for position, e in enumerate(scenario.total_order())}
    for a, b, relation in scenario.pairs():
        if relation == BEFORE:
            assert order[a] < order[b]
        elif relation == AFTER:
            assert order[b] < order[a]


def test_diagram_lists_every_event_once_in_lamport_order(scenario):
    text = scenario.diagram()
    for e in scenario.events:
        assert text.count(f"({e.lamport}) {e.kind[0].upper()} {e.label}") == 1
    assert text.splitlines()[0].split() == ["bus-1", "bus-2", "edge", "central"]


# ==================== MANY CONCURRENT BUSES, RANDOM DELAYS ====================
@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_random_concurrent_fleet_with_delays_and_reordering(seed):
    rng = random.Random(seed)
    buses = [f"bus-{i}" for i in range(6)]
    log = EventLog(*buses, "edge-a", "edge-b", "central")
    in_flight = []                                    # (deliver_at, message, destination)
    for now in range(1, 121):
        actor = rng.choice(buses + ["central"])
        if actor == "central":
            if rng.random() < 0.3:
                _, command = log["central"].send("command")
                in_flight.append((now + rng.randint(1, 15), command, rng.choice(buses)))
        elif rng.random() < 0.2:
            log[actor].local("sensor check")
        else:
            _, reading = log[actor].send("reading")
            edge = "edge-a" if int(actor.split("-")[1]) % 2 == 0 else "edge-b"
            in_flight.append((now + rng.randint(1, 20), reading, edge))   # random delay: reordering
        # Deliver everything due, in a random order among equals.
        due = [item for item in in_flight if item[0] <= now]
        rng.shuffle(due)
        for item in due:
            in_flight.remove(item)
            _, message, destination = item
            log[destination].receive(message)
            if destination.startswith("edge") and rng.random() < 0.5:
                _, state = log[destination].send("state")
                in_flight.append((now + rng.randint(1, 10), state, "central"))
    for _, message, destination in sorted(in_flight, key=lambda item: item[0]):
        log[destination].receive(message)

    check_lamport_rules(log)
    check_clock_condition(log)
    relations = [relation for _, _, relation in log.pairs()]
    assert relations.count(CONCURRENT) > 0 and relations.count(BEFORE) + relations.count(AFTER) > 0
    # Lamport alone would have ordered many of these concurrent pairs.
    assert any(a.lamport != b.lamport for a, b in log.concurrent_pairs())
    # Some deliveries really were out of send order.
    sends = {m.id: m.lamport for m in log.messages.values()}
    for edge in ("edge-a", "edge-b"):
        received = [sends[e.message] for e in log[edge].events if e.kind == "receive"]
        if received != sorted(received):
            break
    else:
        pytest.fail("scenario produced no out-of-order delivery")


# ==================== THE REAL EDGE UNDER REORDERING ====================
def test_edge_processor_rejects_the_late_reading_from_the_scenario():
    """The late B1 seq 1 in the diagram is what the real processor reports as STALE."""
    registry = BusRegistry({"B1": BusAssignment("west", "R1", "C", "edge"),
                            "B2": BusAssignment("west", "R1", "C", "edge")})
    network = NetworkConfig(registry, (RouteDefinition("C", "west", "R1", ("A", "B")),))
    processor = EdgeProcessor(EdgeConfig("C", "west", "edge"), network)
    def reading(bus, sequence, lamport):
        return {"bus_id": bus, "city_id": "C", "region_id": "west", "route_id": "R1", "latitude": 43.9, "longitude": -78.9,
                    "speed_kmh": 30.0, "passengers": 10, "capacity": 50, "sequence_number": sequence,
                    "timestamp": "2026-11-22T12:00:00+00:00", "bus_status": "moving", "lamport_clock": lamport,
                    "direction_id": 0, "current_stop_id": "A", "next_stop_id": "B", "sensor_status": "healthy"}
    log = demo_scenario()
    arrivals = [e for e in log["edge"].events if e.kind == "receive"]
    statuses = []
    for arrival in arrivals:
        message = log.messages[arrival.message]
        bus, sequence = message.label.split()[0], int(message.label.split()[-1])
        statuses.append(processor.process(reading(bus, sequence, message.lamport)).status)
    assert statuses == [ProcessingStatus.ACCEPTED, ProcessingStatus.STALE, ProcessingStatus.ACCEPTED]

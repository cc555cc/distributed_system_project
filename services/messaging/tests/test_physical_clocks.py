"""MQTT-08: physical timestamps under clock offsets, compared with Lamport ordering.

Offline. Simulated wall clocks with fixed offsets and drift feed the real bus
simulator, command consumer and edge processor. The tests show that clock error
moves timestamps (sometimes making an effect look earlier than its cause) while
Lamport stamps and sequence-based acceptance stay correct.
"""

from datetime import timedelta

import pytest

from services.data_simulation.generate_data import generate_messages
from services.edge.auth_edge_server import EdgeAuth, sign_message
from services.edge.config import EdgeConfig, NetworkConfig, RouteDefinition
from services.edge.processing.processor import EdgeProcessor, ProcessingStatus
from services.edge.validation.bus_registry import BusAssignment, BusRegistry
from services.messaging import commands
from services.messaging.clock_sync import (ManualTime, SimulatedClock, classify_timestamp, estimate_offset,
                                           parse_timestamp, timestamp_skew)
from services.messaging.lamport import LamportClock


def readings(bus_id, route_id, clock, count, *, lamport=None, true_time=None, step=1.0, controls=None):
    """Real simulator output, one reading per `step` seconds of true time."""
    source = generate_messages(route_id, 0, bus_id, max_updates=count, update_seconds=0, utc_now=clock,
                               clock=lamport, controls=controls)
    for message in source:
        yield message
        if true_time is not None:
            true_time.advance(step)


# ==================== THE SIMULATED CLOCK ====================
def test_offset_and_drift_change_reported_time():
    true_time = ManualTime()
    fast = SimulatedClock(offset_seconds=5, true_now=true_time)
    drifting = SimulatedClock(drift_ppm=100, true_now=true_time)
    assert fast() - true_time() == timedelta(seconds=5)
    true_time.advance(3600)
    assert drifting.error_seconds() == pytest.approx(0.36)            # 100 ppm for one hour
    assert (drifting() - true_time()).total_seconds() == pytest.approx(0.36)


def test_simulator_timestamps_carry_each_bus_clock_error():
    true_time = ManualTime()
    ahead = SimulatedClock(offset_seconds=4, true_now=true_time)
    behind = SimulatedClock(offset_seconds=-4, true_now=true_time)
    a = next(readings("bus-302-1", "302", ahead, 1))
    b = next(readings("bus-302-2", "302", behind, 1))
    assert timestamp_skew(a["timestamp"], true_time()) == pytest.approx(4)
    assert timestamp_skew(b["timestamp"], true_time()) == pytest.approx(-4)
    assert parse_timestamp(a["timestamp"]).utcoffset() == timedelta(0)  # always UTC


# ==================== TIMESTAMPS MISORDER EVENTS; LAMPORT DOES NOT ====================
def test_skewed_timestamps_misorder_concurrent_buses_by_true_time():
    """Bus A reports first in real time, but its slow clock makes it look later... or earlier."""
    true_time = ManualTime()
    slow = SimulatedClock(offset_seconds=-3, true_now=true_time)
    fast = SimulatedClock(offset_seconds=+3, true_now=true_time)
    first = next(readings("bus-302-1", "302", fast, 1))     # true t = 0
    true_time.advance(1)
    second = next(readings("bus-302-2", "302", slow, 1))    # true t = 1
    # By timestamp, the later reading appears 5 s *earlier*.
    assert parse_timestamp(second["timestamp"]) < parse_timestamp(first["timestamp"])
    gap = (parse_timestamp(first["timestamp"]) - parse_timestamp(second["timestamp"])).total_seconds()
    assert gap == pytest.approx(5)


def test_effect_can_carry_an_earlier_timestamp_than_its_cause_but_never_a_lower_lamport():
    """Central (fast clock) commands a bus (slow clock); the bus's next reading is caused by it."""
    true_time = ManualTime()
    central_wall = SimulatedClock(offset_seconds=+2, true_now=true_time)
    bus_wall = SimulatedClock(offset_seconds=-2, true_now=true_time)
    central_clock, bus_clock = LamportClock(), LamportClock()
    controls = commands.BusControls()
    consumer = commands.CommandConsumer("west", "bus-302-1", controls, bus_clock)
    bus = readings("bus-302-1", "302", bus_wall, 20, lamport=bus_clock, true_time=true_time, controls=controls)
    before = [next(bus) for _ in range(3)]

    command = commands.make_command("west", "set_traffic", {"traffic_factor": 0.5}, target="bus-302-1",
                                    clock=central_clock, utc_now=central_wall)
    for _ in range(40):                                      # central's clock had advanced on other work
        central_clock.tick()
    command = command.model_copy(update={"lamport_clock": central_clock.tick()})
    topic, payload = commands.encode(command)
    true_time.advance(0.5)                                   # network delay
    assert consumer.handle(topic, payload.encode()) == commands.APPLIED
    effect = next(bus)                                       # first reading after the command

    # Physical time says the effect happened before its cause...
    assert parse_timestamp(effect["timestamp"]) < parse_timestamp(command.timestamp)
    # ...Lamport time does not: receive rule put the bus past central's stamp.
    assert effect["lamport_clock"] > command.lamport_clock > before[-1]["lamport_clock"]
    # And the bus's own readings stay ordered by both sequence and Lamport.
    assert [m["sequence_number"] for m in before + [effect]] == [1, 2, 3, 4]


# ==================== ACCEPTANCE IGNORES TIMESTAMPS ====================
@pytest.fixture
def processor():
    registry = BusRegistry({"S1": BusAssignment("west", "R1", "CITY-S", "edge-s")})
    network = NetworkConfig(registry, (RouteDefinition("CITY-S", "west", "R1", ("A", "B")),))
    return EdgeProcessor(EdgeConfig("CITY-S", "west", "edge-s"), network)


def telemetry(sequence, timestamp):
    return dict(bus_id="S1", city_id="CITY-S", region_id="west", route_id="R1", latitude=43.9, longitude=-78.9,
                speed_kmh=30.0, passengers=10, capacity=50, sequence_number=sequence, timestamp=timestamp,
                bus_status="moving", lamport_clock=sequence, direction_id=0, current_stop_id="A",
                next_stop_id="B", sensor_status="healthy")


def test_edge_orders_by_sequence_whatever_the_timestamps_say(processor):
    true_time = ManualTime()
    wall = SimulatedClock(offset_seconds=0, true_now=true_time)
    t0 = wall().isoformat()
    # Bus clock jumps backward an hour (e.g. NTP step) between readings 1 and 2.
    past = (wall() - timedelta(hours=1)).isoformat()
    future = (wall() + timedelta(hours=1)).isoformat()
    assert processor.process(telemetry(1, t0)).status == ProcessingStatus.ACCEPTED
    assert processor.process(telemetry(2, past)).status == ProcessingStatus.ACCEPTED     # older stamp, newer reading
    # A newer-looking timestamp cannot resurrect an old reading.
    assert processor.process(telemetry(1, future)).status == ProcessingStatus.STALE
    cached = processor.cache.get(("CITY-S", "west", "S1")).processed
    assert cached["sequence_number"] == 2 and cached["source_timestamp"] == past


def test_freshness_uses_the_receivers_clock_not_the_senders(processor):
    now = [100.0]
    processor.clock = lambda: now[0]
    long_ago = "2001-01-01T00:00:00+00:00"                   # bus clock badly wrong
    assert processor.process(telemetry(1, long_ago)).status == ProcessingStatus.ACCEPTED
    states = processor._states(processor.clock())
    assert states[0]["freshness"] == "current"               # judged by when the edge accepted it
    now[0] += processor.config.freshness_seconds + 1
    assert processor._states(processor.clock())[0]["freshness"] == "stale"


# ==================== WHERE WALL CLOCKS MUST AGREE ====================
def test_signed_messages_tolerate_small_skew_but_reject_large_skew():
    """Publisher authentication is the one cross-machine wall-clock comparison."""
    secret = "x" * 32
    registry = BusRegistry({"S1": BusAssignment("west", "R1", "CITY-S", "edge-s")})
    edge_time = 1_800_000_000.0
    auth = EdgeAuth("edge-s", "west", registry, keys={"bus:S1": secret}, clock=lambda: edge_time)
    topic = "transit/buses/west/S1/telemetry"
    def accepted(bus_clock_offset):
        message = sign_message(topic, {"n": bus_clock_offset}, "bus:S1", secret, timestamp=edge_time + bus_clock_offset)
        try:
            auth.verify(topic, message)
            return True
        except PermissionError:
            return False
    assert [accepted(offset) for offset in (-2, 0, 2, 4.9)] == [True] * 4
    assert not accepted(6)        # bus clock 6 s fast: "future" message
    assert not accepted(-31)      # bus clock 31 s slow: looks expired


def test_timestamp_classification_is_reporting_only():
    true_time = ManualTime()
    now = true_time()
    assert classify_timestamp((now + timedelta(seconds=1)).isoformat(), now) == "ok"
    assert classify_timestamp((now + timedelta(seconds=10)).isoformat(), now) == "ahead"
    assert classify_timestamp((now - timedelta(seconds=10)).isoformat(), now) == "ok"     # could be delay
    assert classify_timestamp((now - timedelta(minutes=5)).isoformat(), now) == "behind"
    with pytest.raises(ValueError):
        classify_timestamp("2026-11-22T12:00:00", now)                                    # no offset


# ==================== ESTIMATING AN OFFSET ====================
@pytest.mark.parametrize("true_offset, outbound, inbound", [
    (3.0, 0.05, 0.05),       # symmetric delay: exact
    (-1.5, 0.01, 0.20),      # asymmetric: error bounded by RTT/2
    (0.0, 0.30, 0.00),
])
def test_cristian_estimate_is_within_half_the_round_trip(true_offset, outbound, inbound):
    sent = 1000.0                                # client clock
    server_time = sent + outbound + true_offset  # server's clock when it answers
    received = sent + outbound + inbound
    estimate = estimate_offset(sent, server_time, received)
    assert abs(estimate.offset_seconds - true_offset) <= estimate.error_bound_seconds + 1e-9
    assert estimate.round_trip_seconds == pytest.approx(outbound + inbound)
    if outbound == inbound:
        assert estimate.offset_seconds == pytest.approx(true_offset)


def test_estimate_rejects_impossible_round_trip():
    with pytest.raises(ValueError):
        estimate_offset(10.0, 5.0, 9.0)

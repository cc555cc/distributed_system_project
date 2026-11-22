"""Physical (wall-clock) time: how it is handled and simulated (MQTT-08).

Rules the system follows
------------------------
* Every ``timestamp`` is UTC wall-clock time from the machine that generated the
  event, ISO 8601 with an explicit offset. It records *when* the sender believes
  the event happened.
* Timestamps are never used to order, accept or reject telemetry, alerts or
  commands. Ordering uses per-bus sequence numbers (acceptance) and Lamport
  clocks (cross-process ordering), which are unaffected by clock error.
* Freshness at the edge and at central is measured with the receiver's own
  monotonic clock from the moment it accepted a reading, not by comparing the
  sender's timestamp with the receiver's clock.
* The one place wall-clock time is compared across machines is publisher
  authentication: a signed message must be at most 30 s old and at most 5 s in
  the future at the edge (auth_edge_server.py). That is the system's
  synchronisation requirement: machines must agree to within a few seconds,
  as NTP normally provides (typically within tens of milliseconds).

Assumption: every host runs NTP (or equivalent). ``MAX_ASSUMED_SKEW_SECONDS`` is
the bound the design tolerates for diagnostics. A timestamp outside it is
reported, not corrected or rejected.

This module provides a simulated skewed/drifting clock to feed the bus simulator
(``generate_messages(utc_now=...)``), Cristian's offset estimate for a request/
response exchange, and a classifier that reports how far a timestamp is from a
receiver's clock.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

MAX_ASSUMED_SKEW_SECONDS = 2.0
AUTH_MAX_AGE_SECONDS = 30.0
AUTH_MAX_FUTURE_SECONDS = 5.0


class SimulatedClock:
    """A machine's wall clock: true time + fixed offset + linear drift.

    ``drift_ppm`` is parts per million: 100 ppm gains 0.36 s per hour.
    ``true_now`` supplies the reference (real) time, so tests can control it.
    """

    def __init__(self, offset_seconds=0.0, drift_ppm=0.0, *, true_now=None):
        self.offset_seconds, self.drift_ppm = float(offset_seconds), float(drift_ppm)
        self.true_now = true_now or (lambda: datetime.now(UTC))
        self._start = self.true_now()

    def error_seconds(self, at=None):
        """How far this clock is from true time at the given true time."""
        at = at or self.true_now()
        elapsed = (at - self._start).total_seconds()
        return self.offset_seconds + elapsed * self.drift_ppm / 1e6

    def __call__(self):
        now = self.true_now()
        return now + timedelta(seconds=self.error_seconds(now))


class ManualTime:
    """Controllable true time for deterministic tests."""

    def __init__(self, start=datetime(2026, 11, 22, 12, 0, tzinfo=UTC)):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)
        return self.now


@dataclass(frozen=True)
class OffsetEstimate:
    offset_seconds: float     # add to the local clock to get the server's clock
    error_bound_seconds: float
    round_trip_seconds: float


def estimate_offset(request_sent: float, server_time: float, response_received: float) -> OffsetEstimate:
    """Cristian's algorithm. All values are seconds (epoch or any shared origin).

    The server's reading is assumed to be taken halfway through the round trip,
    so the true offset is within +/- round_trip/2 of the estimate whatever the
    split between the two directions.
    """
    round_trip = response_received - request_sent
    if round_trip < 0:
        raise ValueError("response received before the request was sent")
    offset = server_time + round_trip / 2 - response_received
    return OffsetEstimate(offset, round_trip / 2, round_trip)


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise ValueError("timestamp needs a UTC offset")
    return parsed


def timestamp_skew(timestamp: str, receiver_now: datetime | None = None) -> float:
    """Seconds the sender's timestamp is ahead (+) or behind (-) the receiver's clock.

    Includes network/queue delay, so a positive value larger than the delay
    means the sender's clock is fast.
    """
    receiver_now = receiver_now or datetime.now(UTC)
    return (parse_timestamp(timestamp) - receiver_now).total_seconds()


def classify_timestamp(timestamp: str, receiver_now: datetime | None = None, *,
                       tolerance=MAX_ASSUMED_SKEW_SECONDS, max_delay=AUTH_MAX_AGE_SECONDS) -> str:
    """'ok', 'ahead' (sender clock fast) or 'behind' (old or sender clock slow). For reporting only."""
    skew = timestamp_skew(timestamp, receiver_now)
    if skew > tolerance:
        return "ahead"
    if skew < -(max_delay + tolerance):
        return "behind"
    return "ok"


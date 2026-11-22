"""Simulation commands over MQTT (MQTT-05).

Central publishes a command to one bus (``transit/commands/<region>/<bus_id>``)
or to every bus in a region (``transit/commands/<region>/all``). Each bus
subscribes to exactly those two topics, and :class:`CommandConsumer` checks the
target again before acting, so a command never changes an unrelated bus even if
it were delivered to the wrong subscriber.

Commands are QoS 1, so a bus may receive one twice; ``command_id`` makes a
repeat harmless. Each command carries central's Lamport stamp, and the bus
applies the receive rule before acting, so its next telemetry stamp is higher.
"""

import logging
import math
from collections import OrderedDict
from datetime import UTC, datetime
from threading import Condition
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from services.messaging import topics

logger = logging.getLogger(__name__)

SCHEMA_VERSION = "transit-command-v1"
SEGMENT = r"^[A-Za-z0-9_.:-]+$"

Action = Literal[
    "pause", "resume", "set_traffic", "set_demand", "set_sensor_failure_rate"
]

# Required parameters per action; anything else is rejected.
PARAMETERS = {
    "pause": set(),
    "resume": set(),
    "set_traffic": {"traffic_factor"},
    "set_demand": {"demand"},
    "set_sensor_failure_rate": {"sensor_failure_rate"},
}


def _check_traffic(value):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0 < value <= 1
    ):
        raise ValueError("traffic_factor must be greater than 0 and at most 1")
    return float(value)


def _check_rate(value):
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0 <= value <= 1
    ):
        raise ValueError("sensor_failure_rate must be between 0 and 1")
    return float(value)


def _check_demand(value):
    if value not in ("peak", "off_peak"):
        raise ValueError("demand must be peak or off_peak")
    return value


class Command(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["transit-command-v1"] = SCHEMA_VERSION
    command_id: str = Field(pattern=r"^[A-Za-z0-9_-]{8,64}$")
    region_id: str = Field(pattern=SEGMENT, max_length=64)
    target: str = Field(pattern=SEGMENT, max_length=64)  # a bus ID, or "all"
    action: Action
    params: dict[str, Any] = Field(default_factory=dict)
    issued_by: str = Field(default="central", pattern=SEGMENT, max_length=64)
    timestamp: str
    lamport_clock: int = Field(ge=0)

    @model_validator(mode="after")
    def _params_match_action(self):
        if set(self.params) != PARAMETERS[self.action]:
            raise ValueError(
                f"{self.action} takes parameters {sorted(PARAMETERS[self.action])}"
            )
        checks = {
            "traffic_factor": _check_traffic,
            "sensor_failure_rate": _check_rate,
            "demand": _check_demand,
        }
        self.params = {name: checks[name](value) for name, value in self.params.items()}
        return self


def make_command(
    region_id,
    action,
    params=None,
    *,
    target=topics.ALL_BUSES,
    clock,
    issued_by="central",
    utc_now=None,
) -> Command:
    """Build a command, ticking the sender's Lamport clock (send rule)."""
    now = (utc_now or (lambda: datetime.now(UTC)))()
    return Command(
        command_id=uuid4().hex,
        region_id=region_id,
        target=target,
        action=action,
        params=params or {},
        issued_by=issued_by,
        timestamp=now.isoformat(),
        lamport_clock=clock.tick(),
    )


def encode(command: Command) -> tuple[str, str]:
    return topics.command(command.region_id, command.target), command.model_dump_json()


def bus_subscriptions(region_id: str, bus_id: str) -> tuple[str, str]:
    """The only two command topics a bus listens to."""
    return topics.command(region_id, bus_id), topics.command(region_id)


class BusControls:
    """Live settings a running simulated bus reads on every step.

    The simulator thread reads them, the MQTT thread changes them. ``wait_while_paused``
    blocks the simulator without busy-waiting.
    """

    def __init__(
        self, *, demand="off_peak", traffic_factor=1.0, sensor_failure_rate=0.0
    ):
        self._changed = Condition()
        self._paused = False
        self._stopped = False
        self._demand = _check_demand(demand)
        self._traffic = _check_traffic(traffic_factor)
        self._failure_rate = _check_rate(sensor_failure_rate)

    def _read(self, name):
        with self._changed:
            return getattr(self, name)

    paused = property(lambda self: self._read("_paused"))
    demand = property(lambda self: self._read("_demand"))
    traffic_factor = property(lambda self: self._read("_traffic"))
    sensor_failure_rate = property(lambda self: self._read("_failure_rate"))

    def apply(self, action: str, params: dict) -> None:
        with self._changed:
            if action == "pause":
                self._paused = True
            elif action == "resume":
                self._paused = False
            elif action == "set_traffic":
                self._traffic = _check_traffic(params["traffic_factor"])
            elif action == "set_demand":
                self._demand = _check_demand(params["demand"])
            elif action == "set_sensor_failure_rate":
                self._failure_rate = _check_rate(params["sensor_failure_rate"])
            else:
                raise ValueError(f"unknown action {action}")
            self._changed.notify_all()

    def stop(self) -> None:
        """Release a paused simulator so its process can end."""
        with self._changed:
            self._stopped = True
            self._changed.notify_all()

    def wait_while_paused(self, timeout=None) -> bool:
        """Block while paused. Return False if stop() was called."""
        with self._changed:
            self._changed.wait_for(lambda: not self._paused or self._stopped, timeout)
            return not self._stopped


APPLIED, IGNORED, DUPLICATE, REJECTED = "applied", "ignored", "duplicate", "rejected"


class CommandConsumer:
    """Bus-side handler: act only on valid commands addressed to this bus."""

    def __init__(self, region_id, bus_id, controls: BusControls, clock, *, memory=256):
        self.region_id, self.bus_id = region_id, bus_id
        self.controls, self.clock = controls, clock
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._memory = memory
        self.history: list[tuple[str, str]] = []  # (command_id or "?", outcome)

    def handle(self, topic: str, payload: bytes) -> str:
        try:
            if len(payload) > 16 * 1024:
                raise ValueError("command payload too large")
            command = Command.model_validate_json(payload)
        except ValueError as error:
            logger.warning(
                "Rejected command on %s for bus=%s: invalid (%s)",
                topic,
                self.bus_id,
                type(error).__name__,
            )
            self.history.append(("?", REJECTED))
            return REJECTED
        # Receive rule on every valid delivery, even one this bus ignores.
        self.clock.receive(command.lamport_clock)
        if topic != topics.command(command.region_id, command.target):
            outcome = REJECTED
        elif command.region_id != self.region_id or command.target not in (
            self.bus_id,
            topics.ALL_BUSES,
        ):
            outcome = IGNORED
        elif command.command_id in self._seen:
            outcome = DUPLICATE
        else:
            self._seen[command.command_id] = None
            while len(self._seen) > self._memory:
                self._seen.popitem(last=False)
            self.controls.apply(command.action, command.params)
            outcome = APPLIED
        self.history.append((command.command_id, outcome))
        log = logger.info if outcome == APPLIED else logger.warning
        log(
            "Command %s %s for bus=%s: %s",
            command.command_id,
            command.action,
            self.bus_id,
            outcome,
        )
        return outcome

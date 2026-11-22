"""Heartbeat messages and receipt-time tracking shared by edge and central."""

import math
import os
import time
from dataclasses import dataclass
from datetime import datetime
from threading import RLock
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SENSORS = ("gps", "speed", "stop_status", "occupancy")
SensorName = Literal["gps", "speed", "stop_status", "occupancy"]


@dataclass(frozen=True)
class HealthPolicy:
    interval_seconds: float = 2
    sensor_timeout_seconds: float = 10
    edge_timeout_seconds: float = 10
    recovery_seconds: float = 4
    overload_percent: float = 80
    sustained_seconds: float = 6

    def __post_init__(self):
        if not all(
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
            for value in vars(self).values()
        ):
            raise ValueError("Health thresholds must be finite numbers")
        if (
            not 0
            < self.interval_seconds
            < min(self.sensor_timeout_seconds, self.edge_timeout_seconds)
        ):
            raise ValueError(
                "Heartbeat interval must be shorter than both failure timeouts"
            )
        if (
            min(self.recovery_seconds, self.sustained_seconds) < 0
            or not 0 < self.overload_percent <= 100
        ):
            raise ValueError("Invalid recovery or load threshold")


class Heartbeat(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    city_id: str = Field(min_length=1)
    region_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    sequence_number: int = Field(ge=1)
    timestamp: str

    @field_validator("timestamp")
    @classmethod
    def timestamp_has_timezone(cls, value):
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("Heartbeat timestamp needs a timezone")
        return value


class BusHeartbeat(Heartbeat):
    schema_version: Literal["bus-heartbeat-v1"] = "bus-heartbeat-v1"
    bus_id: str = Field(min_length=1)
    # Missing keys do not refresh a sensor. Four virtual channels share one
    # simulated bus process; this does not claim four independent devices.
    sensors: dict[SensorName, Literal["healthy", "faulty"]]


class SensorHealth(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    status: Literal["unknown", "healthy", "unavailable", "recovering"]
    reported_state: Literal["unknown", "healthy", "faulty"]
    age_seconds: float | None = Field(ge=0)


class BusHealth(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    bus_id: str
    sensors: dict[SensorName, SensorHealth]
    valid_telemetry_age_seconds: float | None = Field(ge=0)
    last_speed_kmh: float | None = Field(default=None, ge=0)


class Workload(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    queue_size: int = Field(ge=0)
    queue_capacity: int = Field(gt=0)
    queue_utilization_percent: float = Field(ge=0, le=100)
    received_messages: int = Field(ge=0)
    processed_messages: int = Field(ge=0)
    messages_per_second: float = Field(ge=0)
    average_processing_ms: float = Field(ge=0)
    active_buses: int = Field(ge=0)
    assigned_buses: int = Field(ge=0)
    pending_updates: int = Field(ge=0)
    load_band: Literal[
        "normal", "notice", "prepare", "ready", "near_capacity", "overloaded"
    ]

    @model_validator(mode="after")
    def queue_counts_match(self):
        if (
            self.queue_size > self.queue_capacity
            or abs(
                self.queue_utilization_percent
                - self.queue_size / self.queue_capacity * 100
            )
            > 1e-8
        ):
            raise ValueError("Queue utilization does not match queue counts")
        if (
            self.active_buses > self.assigned_buses
            or self.pending_updates > self.assigned_buses
        ):
            raise ValueError("Worker counts exceed registered assignments")
        return self


class EdgeHeartbeat(Heartbeat):
    schema_version: Literal["edge-heartbeat-v1"] = "edge-heartbeat-v1"
    edge_id: str = Field(min_length=1)
    workload: Workload
    buses: list[BusHealth]


class HeartbeatTracker:
    """Use local monotonic receipt times, never source clocks, for timeouts."""

    def __init__(self, expected, timeout, recovery, *, clock=time.monotonic):
        self.expected = set(expected)
        self.timeout, self.recovery, self.clock = timeout, recovery, clock
        self.started_at = clock()
        self.entries = {}
        self.lock = RLock()

    def record(self, key, session, sequence, state="healthy") -> bool:
        with self.lock:
            if key not in self.expected:
                return False
            now = self.clock()
            old = self.entries.get(key)
            retired = set() if old is None else set(old["retired"])
            restarted = old is not None and session != old["session"]
            if old is not None:
                # Delayed duplicate packets must not make a dead source look alive.
                if session in retired or (
                    not restarted and sequence <= old["sequence"]
                ):
                    return False
                if restarted:
                    retired.add(old["session"])
            gap = old is not None and now - old["received_at"] >= self.timeout
            recovering = (
                now if gap or restarted else (old["recovering_since"] if old else None)
            )
            count = 1 if gap or restarted else (old["recovery_count"] + 1 if old else 0)
            self.entries[key] = {
                "session": session,
                "sequence": sequence,
                "reported_state": state,
                "received_at": now,
                "retired": retired,
                "recovering_since": recovering,
                "recovery_count": count,
            }
            return True

    def report(self, key) -> dict:
        with self.lock:
            now = self.clock()
            item = self.entries.get(key)
            if item is None:
                return {
                    "status": "unknown"
                    if now - self.started_at < self.timeout
                    else "unavailable",
                    "reported_state": "unknown",
                    "age_seconds": None,
                }
            age = max(0.0, now - item["received_at"])
            if age >= self.timeout:
                status = "unavailable"
            elif item["recovering_since"] is not None and (
                item["recovery_count"] < 2
                or now - item["recovering_since"] < self.recovery
            ):
                status = "recovering"
            else:
                status = "healthy"
            return {
                "status": status,
                "reported_state": item["reported_state"],
                "age_seconds": age,
            }


def load_band(utilization: float, overload: float = 80) -> str:
    """Queue utilization is server workload, not passenger occupancy."""
    if utilization >= overload:
        return "overloaded"
    for threshold, label in (
        (79, "near_capacity"),
        (75, "ready"),
        (70, "prepare"),
        (50, "notice"),
    ):
        if utilization >= threshold:
            return label
    return "normal"


def policy_from_environment() -> HealthPolicy:
    """Use the same interval/timeout names in the simulator, edge and central."""
    return HealthPolicy(
        interval_seconds=float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "2")),
        sensor_timeout_seconds=float(
            os.getenv("SENSOR_HEARTBEAT_TIMEOUT_SECONDS", "10")
        ),
        edge_timeout_seconds=float(os.getenv("EDGE_HEARTBEAT_TIMEOUT_SECONDS", "10")),
        recovery_seconds=float(os.getenv("HEARTBEAT_RECOVERY_SECONDS", "4")),
        overload_percent=float(os.getenv("EDGE_OVERLOAD_PERCENT", "80")),
        sustained_seconds=float(os.getenv("EDGE_OVERLOAD_SUSTAIN_SECONDS", "6")),
    )


def sensor_health_status(sensor, edge_status, timeout):
    """A missing edge makes its sensors unknown, not proven faulty or dead."""
    if edge_status in ("unknown", "unavailable"):
        return "unknown"
    age = sensor["age_seconds"]
    if sensor["status"] == "unavailable" or (age is not None and age >= timeout):
        return "unavailable"
    if sensor["status"] == "unknown":
        return "unknown"
    if sensor["reported_state"] == "faulty":
        return "faulty"
    if edge_status == "recovering" or sensor["status"] == "recovering":
        return "recovering"
    return "healthy" if sensor["reported_state"] == "healthy" else "unknown"


def classify_bus_health(bus, edge_status, timeout):
    """Shared evidence rules. No pattern here diagnoses a crash or breakdown.

    Liveness, reported faults and valid readings are different evidence. Keep
    the reason codes so the UI can explain a classification rather than guess.
    """
    if edge_status in ("unknown", "unavailable"):
        return {"status": "unknown", "reasons": ["edge_" + edge_status]}
    states = {
        name: sensor_health_status(bus["sensors"][name], edge_status, timeout)
        for name in SENSORS
    }
    reasons = [
        name + "_" + state for name, state in states.items() if state != "healthy"
    ]
    if all(state == "unavailable" for state in states.values()):
        return {"status": "telemetry_unavailable", "reasons": reasons}
    if any(state in ("faulty", "unavailable") for state in states.values()):
        return {"status": "degraded", "reasons": reasons}
    if edge_status == "recovering" or "recovering" in states.values():
        return {"status": "recovering", "reasons": reasons or ["edge_recovering"]}
    if "unknown" in states.values():
        return {"status": "unknown", "reasons": reasons}
    age = bus["valid_telemetry_age_seconds"]
    if age is None or age >= timeout:
        return {"status": "data_stale", "reasons": ["no_recent_valid_telemetry"]}
    if bus.get("last_speed_kmh") == 0:
        return {"status": "stationary", "reasons": ["fresh_valid_speed_zero"]}
    return {
        "status": "healthy",
        "reasons": ["fresh_valid_telemetry_and_sensor_heartbeats"],
    }


def edge_workload_status(edge_status, utilization, sustained_overload, threshold):
    """Separate load pressure from the edge's heartbeat liveness status."""
    if edge_status != "healthy" or utilization is None:
        return "unknown"
    if utilization < threshold:
        return "normal"
    return "overloaded" if sustained_overload else "high_load"

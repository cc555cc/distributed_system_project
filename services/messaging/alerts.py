"""System alerts over MQTT (MQTT-05).

A service that detects a problem publishes one alert to
``transit/alerts/<region_id>/<source_id>``. Central subscribes to
``transit/alerts/#`` and keeps the most recent alerts in memory for the API.

QoS 1 can deliver an alert twice; ``alert_id`` lets central keep only one copy.
Every alert carries the sender's Lamport stamp, so central applies the receive
rule to alerts exactly as it does to bus states.
"""

from collections import OrderedDict
from datetime import datetime, timezone
from threading import RLock
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from services.messaging import topics

SCHEMA_VERSION = "transit-alert-v1"
SEGMENT = r"^[A-Za-z0-9_.:-]+$"

AlertKind = Literal["sensor_failure", "sensor_recovered", "edge_overload", "edge_unavailable", "other"]
Severity = Literal["info", "warning", "critical"]


class Alert(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["transit-alert-v1"] = SCHEMA_VERSION
    alert_id: str = Field(pattern=r"^[A-Za-z0-9_-]{8,64}$")
    region_id: str = Field(pattern=SEGMENT, max_length=64)
    source_id: str = Field(pattern=SEGMENT, max_length=64)
    source_type: Literal["bus", "edge", "central"]
    kind: AlertKind
    severity: Severity
    message: str = Field(max_length=500)
    timestamp: str
    lamport_clock: int = Field(ge=0)
    bus_id: str | None = Field(default=None, pattern=SEGMENT, max_length=64)

    @field_validator("timestamp")
    @classmethod
    def _has_offset(cls, value: str) -> str:
        if datetime.fromisoformat(value.replace("Z", "+00:00")).utcoffset() is None:
            raise ValueError("timestamp needs a UTC offset")
        return value


def make_alert(region_id, source_id, source_type, kind, severity, message, *, clock,
               bus_id=None, utc_now=None) -> Alert:
    """Build an alert, ticking the sender's Lamport clock (send rule)."""
    now = (utc_now or (lambda: datetime.now(timezone.utc)))()
    return Alert(alert_id=uuid4().hex, region_id=region_id, source_id=source_id,
                 source_type=source_type, kind=kind, severity=severity, message=message,
                 timestamp=now.isoformat(), lamport_clock=clock.tick(), bus_id=bus_id)


def encode(alert: Alert) -> tuple[str, str]:
    """Return (topic, payload) for publishing."""
    return topics.alert(alert.region_id, alert.source_id), alert.model_dump_json(exclude_none=True)


def parse(topic: str, payload: bytes) -> Alert:
    """Validate an alert and check it was published on its own source's topic."""
    if len(payload) > 16 * 1024:
        raise ValueError("alert payload too large")
    alert = Alert.model_validate_json(payload)
    if topic != topics.alert(alert.region_id, alert.source_id):
        raise ValueError("alert topic does not match its region and source")
    return alert


class AlertStore:
    """Bounded, de-duplicated, newest-last alert history held by central."""

    def __init__(self, limit: int = 500):
        self.limit = limit
        self._alerts: OrderedDict[str, dict] = OrderedDict()
        self._lock = RLock()
        self.duplicates = 0

    def add(self, alert: Alert, received_lamport: int | None = None) -> bool:
        """Store the alert. Return False for a repeated delivery of a stored alert."""
        with self._lock:
            if alert.alert_id in self._alerts:
                self.duplicates += 1
                return False
            record = alert.model_dump(mode="json", exclude_none=True)
            record["received_at"] = datetime.now(timezone.utc).isoformat()
            if received_lamport is not None:
                record["central_lamport"] = received_lamport
            self._alerts[alert.alert_id] = record
            while len(self._alerts) > self.limit:
                self._alerts.popitem(last=False)
            return True

    def list(self, *, region_id=None, severity=None, limit=100) -> list[dict]:
        with self._lock:
            alerts = [dict(a) for a in self._alerts.values()
                      if (region_id is None or a["region_id"] == region_id)
                      and (severity is None or a["severity"] == severity)]
        return alerts[-limit:][::-1]          # newest first

    def clear(self) -> None:
        with self._lock:
            self._alerts.clear()
            self.duplicates = 0

"""Shared central input contract. Optional metadata preserves the edge context."""

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class RegionalStatistics(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    scope: Literal["edge_assigned_buses"]
    active_buses: int = Field(ge=0)
    crowded_buses: int = Field(ge=0)
    average_occupancy_percent: float | None = Field(default=None, ge=0, le=100)
    occupancy_samples: int = Field(ge=0)
    active_buses_per_route: dict[str, Annotated[int, Field(ge=0)]]

    @model_validator(mode="after")
    def counts_are_consistent(self):
        if self.crowded_buses > self.active_buses or self.occupancy_samples > self.active_buses:
            raise ValueError("Regional sample counts exceed active bus count")
        if sum(self.active_buses_per_route.values()) != self.active_buses:
            raise ValueError("Per-route counts differ from active bus count")
        if (self.occupancy_samples == 0) != (self.average_occupancy_percent is None):
            raise ValueError("Occupancy average must be unavailable when there are no samples")
        return self


class EdgeMetadata(BaseModel):
    """Extra information central stores alongside the existing bus-state fields."""

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    schema_version: Literal["edge-central-v1"] = "edge-central-v1"
    city_id: str = Field(min_length=1)
    region_id: str = Field(min_length=1)
    edge_id: str = Field(min_length=1)
    route_id: str = Field(min_length=1)
    served_city_ids: list[str] = Field(default_factory=list)
    current_stop_city_id: str | None = None
    next_stop_city_id: str | None = None
    telemetry_schema_version: str | None = None
    capacity: int = Field(gt=0)
    passengers: int = Field(ge=0)
    occupancy_unit: Literal["passengers", "percent"]
    current_stop_id: str | None = None
    occupancy_percent: float = Field(ge=0, le=100)
    crowding_status: Literal["low", "medium", "high"]
    sensor_status: Literal["healthy"]
    source_timestamp: str
    processing_timestamp: str
    freshness: Literal["current"]
    route_progress_percent: float | None = Field(default=None, ge=0, le=100)
    statistics: RegionalStatistics

    @field_validator("source_timestamp", "processing_timestamp")
    @classmethod
    def timestamp_has_timezone(cls, value: str) -> str:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("Timestamp needs a UTC offset")
        return value

    @model_validator(mode="after")
    def occupancy_matches_counts(self):
        if self.passengers > self.capacity:
            raise ValueError("Passenger count exceeds capacity")
        if abs(self.occupancy_percent - self.passengers / self.capacity * 100) > 1e-8:
            raise ValueError("Occupancy percentage does not match the received counts")
        return self


class BusStateUpdate(BaseModel):
    """Existing central fields, with optional context for new edge senders."""

    model_config = ConfigDict(extra="forbid", strict=True, allow_inf_nan=False)
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    speed: float = Field(ge=0)  # km/h, copied from speed_kmh
    direction_id: int = Field(ge=0)
    next_stop_id: str | None = None
    bus_status: str = Field(min_length=1)
    occupancy: int = Field(ge=0)
    sequence_number: int = Field(ge=0)
    lamport: int = Field(ge=0)
    edge_metadata: EdgeMetadata | None = None

    @model_validator(mode="after")
    def occupancy_uses_declared_unit(self):
        if self.edge_metadata is not None:
            meta = self.edge_metadata
            expected = meta.passengers if meta.occupancy_unit == "passengers" else meta.occupancy_percent
            if self.occupancy != expected:
                raise ValueError("Central occupancy differs from its declared unit")
        return self

"""Versioned output proposal; EDGE-03 must agree the wire contract before sending."""

from services.edge.config import EdgeConfig
from services.edge.processing.regional_aggregator import aggregate

OUTPUT_SCHEMA_VERSION = "edge-state-proposal-v1"


def build_output(config: EdgeConfig, states: list[dict]) -> dict:
    """Return a JSON-serializable per-bus snapshot and explicitly scoped totals."""
    return {"schema_version": OUTPUT_SCHEMA_VERSION,
            "city_id": config.city_id, "region_id": config.region_id,
            "edge_id": config.edge_id, "buses": states, "statistics": aggregate(states)}

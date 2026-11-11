"""Check outgoing bus information and manage its MQTT connection to central.

This is an explicit sending boundary, not an automatic EDGE-02 forwarding loop.
Pass Carson's agreed Pydantic model in as the contract. This avoids copying his
model here or importing his database configuration into the edge service.
"""

import json
from typing import Any

from pydantic import BaseModel

from services.edge.config import EdgeConfig
from services.edge.validation.bus_registry import BusRegistry


class EdgeToCentral:
    """Own the connection and check every payload again before publishing it."""

    def __init__(self, config: EdgeConfig, registry: BusRegistry,
                 contract: type[BaseModel]) -> None:
        self.config = config
        self.registry = registry
        self.contract = contract
        self._client: Any | None = None

    def validate_payload(self, bus_id: str, payload: dict) -> str:
        """Return JSON only when the bus is ours and Carson's model accepts it.

        The caller supplies agreed central field names/values. This does not
        guess bus status, Lamport values or what central means by occupancy.
        """
        assignment = self.registry.get(bus_id)
        cfg = self.config
        if assignment is None or (assignment.city_id, assignment.region_id, assignment.edge_id) != (
                cfg.city_id, cfg.region_id, cfg.edge_id):
            raise ValueError("Cannot send a bus assigned to another edge")

        # Reject misspelled/unsupported fields instead of silently dropping them.
        if not isinstance(payload, dict) or payload.keys() - self.contract.model_fields.keys():
            raise ValueError("Payload contains unsupported central fields")

        # Use the recipient's actual model, with no automatic string-to-number fixes.
        checked = self.contract.model_validate(payload, strict=True)
        # NaN and infinity are not valid JSON measurements.
        return json.dumps(checked.model_dump(mode="json"), allow_nan=False)

    def connect(self, host: str, port: int = 1883) -> None:
        """Open the outgoing MQTT connection when the caller explicitly asks."""
        if self._client is not None:
            raise RuntimeError("Central connection already started")
        import paho.mqtt.client as mqtt

        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"{self.config.edge_id}-to-central",
        )
        client.reconnect_delay_set(min_delay=1, max_delay=10)
        try:
            client.connect(host, port, keepalive=60)
            client.loop_start()
        except Exception:
            client.disconnect()
            raise
        self._client = client

    def send(self, bus_id: str, payload: dict, *, timeout: float = 5.0) -> None:
        """Validate first, publish one bus state and wait for the broker receipt.

        A broker receipt does not prove Carson stored the update in his database.
        The caller must supply a current accepted update; the EDGE-02 snapshot
        cannot be sent directly because it is a different proposed format.
        """
        encoded = self.validate_payload(bus_id, payload)
        if self._client is None or not self._client.is_connected():
            raise ConnectionError("Central MQTT connection is not ready")

        # This is the topic Carson's existing subscriber listens to.
        topic = f"transit/{self.config.region_id}/bus/{bus_id}/state"
        receipt = self._client.publish(topic, encoded, qos=1, retain=False)
        if receipt.rc != 0:
            raise ConnectionError("MQTT broker did not accept the publish request")
        receipt.wait_for_publish(timeout=timeout)
        if not receipt.is_published():
            raise TimeoutError("MQTT broker receipt timed out")

    def close(self) -> None:
        """Release the connection when this edge process stops."""
        if self._client is not None:
            try:
                self._client.disconnect()
            finally:
                self._client.loop_stop()
                self._client = None

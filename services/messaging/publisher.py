"""MQTT publisher that survives a temporary loss of the broker connection (MQTT-04).

Paho reconnects on its own and keeps unacknowledged QoS 1 messages, resending
them after the connection returns. This wrapper makes that behaviour explicit:

* a message published while offline is reported as "queued", not as an error
* the offline queue is bounded; a message that does not fit is reported as
  "dropped" and counted, never silently assumed delivered
* close() reports how many accepted messages the broker never acknowledged

Payloads are sent unchanged, so a delayed message still carries the sequence
number, Lamport stamp and timestamp it was generated with. Receivers use those
to recognise it as stale.
"""

import logging
from threading import Condition
import time

logger = logging.getLogger(__name__)

SENT, QUEUED, DROPPED = "sent", "queued", "dropped"


class ReliablePublisher:
    def __init__(self, client_id, host="127.0.0.1", port=1883, *, max_queued=1000,
                 configure=None, client_factory=None):
        if isinstance(max_queued, bool) or not isinstance(max_queued, int) or max_queued < 1:
            raise ValueError("max_queued must be a positive integer")
        import paho.mqtt.client as mqtt
        self._mqtt = mqtt
        self.host, self.port, self.max_queued = host, port, max_queued
        factory = client_factory or (lambda: mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id))
        self.client = factory()
        if configure is not None:
            configure(self.client)   # credentials/TLS, e.g. configure_mqtt_client
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)
        self.client.max_queued_messages_set(max_queued)
        self._changed = Condition()
        self._connected = False
        self._connections = 0
        self._closing = False
        self._outstanding = set()   # message IDs accepted but not yet acknowledged
        self.stats = dict(sent=0, queued=0, dropped=0, acknowledged=0, disconnects=0, reconnects=0)

    # ---- paho callbacks (network thread) ----
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            logger.error("MQTT connection refused: %s", reason_code)
            return
        with self._changed:
            self._connected = True
            self._connections += 1
            if self._connections > 1:
                self.stats["reconnects"] += 1
                logger.info("MQTT reconnected; resending %d unacknowledged message(s)", len(self._outstanding))
            self._changed.notify_all()

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        with self._changed:
            if self._connected and not self._closing:
                self.stats["disconnects"] += 1
            self._connected = False
            self._changed.notify_all()
            expected = self._closing
        if not expected:
            logger.warning("MQTT disconnected (%s); publishing continues into the offline queue", reason_code)

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        with self._changed:
            if mid in self._outstanding:
                self._outstanding.discard(mid)
                self.stats["acknowledged"] += 1
                self._changed.notify_all()

    # ---- caller API ----
    @property
    def connected(self):
        with self._changed:
            return self._connected

    @property
    def unacknowledged(self):
        with self._changed:
            return len(self._outstanding)

    def connect(self, timeout=10.0):
        """Open the first connection. Later reconnects happen in the background."""
        self.client.connect(self.host, self.port, keepalive=60)
        self.client.loop_start()
        if not self.wait_connected(timeout):
            raise TimeoutError("MQTT publisher connection did not become ready")

    def wait_connected(self, timeout):
        with self._changed:
            return self._changed.wait_for(lambda: self._connected, timeout)

    def publish(self, topic, payload):
        """Return "sent", "queued" (offline, kept for resend) or "dropped" (queue full)."""
        mqtt = self._mqtt
        with self._changed:
            info = self.client.publish(topic, payload, qos=1, retain=False)
            if info.rc == mqtt.MQTT_ERR_QUEUE_SIZE:
                self.stats["dropped"] += 1
                outcome = DROPPED
            elif info.rc in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
                self._outstanding.add(info.mid)
                outcome = SENT if info.rc == mqtt.MQTT_ERR_SUCCESS else QUEUED
                self.stats[outcome] += 1
            else:
                raise ConnectionError(f"MQTT publish failed: {mqtt.error_string(info.rc)}")
        if outcome == DROPPED:
            logger.error("Dropped message for %s: offline queue of %d is full", topic, self.max_queued)
        return outcome

    def flush(self, timeout=5.0):
        """Wait for broker acknowledgements. Return the number still missing."""
        deadline = time.monotonic() + timeout
        with self._changed:
            self._changed.wait_for(lambda: not self._outstanding, max(0.0, deadline - time.monotonic()))
            return len(self._outstanding)

    def close(self, flush_timeout=5.0):
        """Disconnect. Return how many accepted messages were never acknowledged."""
        missing = self.flush(flush_timeout)
        if missing:
            logger.warning("%d message(s) were not acknowledged by the broker before shutdown", missing)
        with self._changed:
            self._closing = True
        try:
            self.client.disconnect()
        finally:
            self.client.loop_stop()
        return missing

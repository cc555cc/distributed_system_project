"""MQTT publisher that survives a temporary loss of the broker connection (MQTT-04).

Paho reconnects on its own and keeps unacknowledged QoS 1 messages, resending
them after the connection returns. This wrapper makes that behaviour explicit:

* a message published while offline is reported as "queued", not as an error
* the offline queue is bounded; a message that does not fit is reported as
  "dropped" and counted, never silently assumed delivered
* close() reports how many accepted messages the broker never acknowledged
* subscribe() registers a topic filter that is restored after every reconnect,
  including after a broker restart (MQTT-05, MQTT-10)
* messages stay in publish order across a reconnect (see below)

Ordering across a reconnect: Paho marks the client connected and runs
on_connect *before* it resends the messages kept from the old connection, so a
message published from another thread in that gap overtakes them. The edge
would then reject the older, resent readings as stale. The MQTT-06 benchmark
measured exactly this. To prevent it, once a connection is lost with messages
still unacknowledged (or a message is handed to Paho while it is offline), every
new message waits in a local backlog. A background thread sends the backlog in
order once the connection is back and every older message is acknowledged.

Payloads are sent unchanged, so a delayed message still carries the sequence
number, Lamport stamp and timestamp it was generated with. Receivers use those
to recognise it as stale.

Locking: Paho calls on_publish while holding its own message lock, so this class
never holds its condition lock while calling into Paho. A separate send lock,
never taken by Paho's network thread, keeps sends in order.
"""

from collections import deque
import logging
from threading import Condition, Lock, Thread
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
        self.client_id = client_id
        self.host, self.port, self.max_queued = host, port, max_queued
        factory = client_factory or (lambda: mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id))
        self.client = factory()
        if configure is not None:
            configure(self.client)   # credentials/TLS, e.g. configure_mqtt_client
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        self.client.on_message = self._on_message
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)
        self.client.max_queued_messages_set(max_queued)
        self._changed = Condition()
        self._send_lock = Lock()    # serialises sends; never taken by the network thread
        self._connected = False
        self._connections = 0
        self._closing = False
        self._outstanding = set()   # message IDs accepted but not yet acknowledged
        self._resend_pending = False  # Paho holds messages it will resend after reconnecting
        self._backlog = deque()     # (topic, payload) waiting for the gate to clear
        self._sending = False
        self._early_acks = set()    # acks that arrived before publish() recorded the ID
        self._flusher = None
        self._subscriptions = {}    # topic filter -> handler(topic, payload)
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
            filters = list(self._subscriptions)
            self._changed.notify_all()
        # A clean session forgets subscriptions, so restore them on every connect.
        for topic_filter in filters:
            client.subscribe(topic_filter, qos=1)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        with self._changed:
            if self._connected and not self._closing:
                self.stats["disconnects"] += 1
            self._connected = False
            # Paho will resend what is unacknowledged; nothing newer may overtake it.
            if self._outstanding:
                self._resend_pending = True
            self._changed.notify_all()
            expected = self._closing
        if not expected:
            logger.warning("MQTT disconnected (%s); publishing continues into the offline queue", reason_code)

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        with self._changed:
            if mid in self._outstanding:
                self._outstanding.discard(mid)
                if not self._outstanding:
                    self._resend_pending = False
                self.stats["acknowledged"] += 1
                self._changed.notify_all()
            elif self._sending:
                self._early_acks.add(mid)

    def _on_message(self, client, userdata, message):
        with self._changed:
            handlers = [handler for topic_filter, handler in self._subscriptions.items()
                        if self._mqtt.topic_matches_sub(topic_filter, message.topic)]
        for handler in handlers:
            try:
                handler(message.topic, message.payload)
            except Exception:
                logger.exception("Message handler failed for %s", message.topic)

    # ---- sending ----
    def _send(self, topic, payload):
        """Hand one message to Paho. Caller holds _send_lock, not _changed."""
        mqtt = self._mqtt
        with self._changed:
            self._sending = True
            self._early_acks.clear()
        try:
            info = self.client.publish(topic, payload, qos=1, retain=False)
        except BaseException:
            with self._changed:
                self._sending = False
                self._early_acks.clear()
            raise
        with self._changed:
            # The broker can acknowledge before publish() returns the message ID.
            self._sending = False
            acknowledged_early = info.mid in self._early_acks
            self._early_acks.clear()
            if info.rc == mqtt.MQTT_ERR_QUEUE_SIZE:
                self.stats["dropped"] += 1
                return DROPPED
            if info.rc not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
                raise ConnectionError(f"MQTT publish failed: {mqtt.error_string(info.rc)}")
            if acknowledged_early:
                self.stats["acknowledged"] += 1
            else:
                self._outstanding.add(info.mid)
                if info.rc == mqtt.MQTT_ERR_NO_CONN:
                    self._resend_pending = True
            self._changed.notify_all()
            return SENT if info.rc == mqtt.MQTT_ERR_SUCCESS else QUEUED

    def _held_back(self):
        return not self._connected or self._resend_pending or bool(self._backlog)

    def _backlog_ready(self):
        return bool(self._backlog) and self._connected and not self._resend_pending

    def _drain_backlog(self):
        with self._send_lock:
            while True:
                with self._changed:
                    if not self._backlog_ready():
                        return
                    topic, payload = self._backlog.popleft()
                if self._send(topic, payload) == DROPPED:
                    logger.error("Dropped message for %s: offline queue of %d is full", topic, self.max_queued)

    def _flush_backlog_forever(self):
        while True:
            with self._changed:
                self._changed.wait_for(lambda: self._closing or self._backlog_ready())
                if self._closing:
                    return
            self._drain_backlog()

    # ---- caller API ----
    def subscribe(self, topic_filter, handler):
        """Deliver messages matching topic_filter to handler(topic, payload), across reconnects."""
        with self._changed:
            self._subscriptions[topic_filter] = handler
            connected = self._connected
        if connected:
            self.client.subscribe(topic_filter, qos=1)

    @property
    def connected(self):
        with self._changed:
            return self._connected

    @property
    def unacknowledged(self):
        """Accepted messages not yet acknowledged, including the local backlog."""
        with self._changed:
            return len(self._outstanding) + len(self._backlog)

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
        """Return "sent", "queued" (offline or behind a resend, kept) or "dropped" (queue full)."""
        with self._send_lock:
            with self._changed:
                held_back = self._held_back()
                if held_back:
                    if len(self._outstanding) + len(self._backlog) >= self.max_queued:
                        self.stats["dropped"] += 1
                        outcome = DROPPED
                    else:
                        self._backlog.append((topic, payload))
                        self.stats["queued"] += 1
                        outcome = QUEUED
                        if self._flusher is None:
                            self._flusher = Thread(target=self._flush_backlog_forever, daemon=True,
                                                   name=f"{self.client_id}-backlog")
                            self._flusher.start()
                        self._changed.notify_all()
            if not held_back:
                outcome = self._send(topic, payload)
                if outcome != DROPPED:
                    with self._changed:
                        self.stats[outcome] += 1
        if outcome == DROPPED:
            logger.error("Dropped message for %s: offline queue of %d is full", topic, self.max_queued)
        return outcome

    def flush(self, timeout=5.0):
        """Wait for broker acknowledgements. Return the number still missing."""
        deadline = time.monotonic() + timeout
        with self._changed:
            self._changed.wait_for(lambda: not self._outstanding and not self._backlog,
                                   max(0.0, deadline - time.monotonic()))
            return len(self._outstanding) + len(self._backlog)

    def close(self, flush_timeout=5.0):
        """Disconnect. Return how many accepted messages were never acknowledged."""
        missing = self.flush(flush_timeout)
        if missing:
            logger.warning("%d message(s) were not acknowledged by the broker before shutdown", missing)
        with self._changed:
            self._closing = True
            self._changed.notify_all()
        try:
            self.client.disconnect()
        finally:
            self.client.loop_stop()
        return missing

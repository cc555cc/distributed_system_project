"""MQTT publisher that survives a temporary loss of the broker connection (MQTT-04).

Paho reconnects on its own and resends unacknowledged QoS 1 messages. On top of
that this wrapper reports every message as "sent", "queued" (kept while offline)
or "dropped" (bounded queue full), restores subscriptions after every reconnect,
and keeps messages in publish order across a reconnect.

Ordering: Paho runs on_connect *before* it resends the old connection's
messages, so a new message published in that gap would overtake them and the
edge would reject the older ones as stale (found by the MQTT-06 benchmark).
The connection is therefore an explicit state machine (``LinkState``): only in
ONLINE do messages go straight to Paho. In every other state they wait in a
local backlog that a background thread sends, in order, once the link is back
to ONLINE.

Locking: Paho calls on_publish while holding its own message lock, so this class
never holds ``_changed`` while calling into Paho. ``_send_lock`` keeps sends in
order and is never taken by Paho's network thread.
"""

import logging
import time
from collections import deque
from enum import Enum
from threading import Condition, Lock, Thread

logger = logging.getLogger(__name__)

SENT, QUEUED, DROPPED = "sent", "queued", "dropped"


class LinkState(Enum):
    CONNECTING = "connecting"  # before the first successful connection
    ONLINE = "online"  # connected; new messages go straight to Paho
    RESENDING = "resending"  # connected; Paho still owes resends from before
    OFFLINE = "offline"  # connection lost; Paho reconnects in the background
    CLOSED = "closed"


# Allowed transitions. Staying in the same state is always allowed.
TRANSITIONS = {
    LinkState.CONNECTING: {LinkState.ONLINE, LinkState.CLOSED},
    LinkState.ONLINE: {LinkState.RESENDING, LinkState.OFFLINE, LinkState.CLOSED},
    LinkState.RESENDING: {LinkState.ONLINE, LinkState.OFFLINE, LinkState.CLOSED},
    LinkState.OFFLINE: {LinkState.ONLINE, LinkState.RESENDING, LinkState.CLOSED},
    LinkState.CLOSED: set(),
}
CONNECTED = {LinkState.ONLINE, LinkState.RESENDING}


class ReliablePublisher:
    def __init__(
        self,
        client_id,
        host="127.0.0.1",
        port=1883,
        *,
        max_queued=1000,
        configure=None,
        client_factory=None,
    ):
        if type(max_queued) is not int or max_queued < 1:  # bool is not accepted
            raise ValueError("max_queued must be a positive integer")
        import paho.mqtt.client as mqtt

        self._mqtt = mqtt
        self.client_id = client_id
        self.host, self.port, self.max_queued = host, port, max_queued
        factory = client_factory or (
            lambda: mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
        )
        self.client = factory()
        if configure is not None:
            configure(self.client)  # credentials/TLS, e.g. configure_mqtt_client
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_publish = self._on_publish
        self.client.on_message = self._on_message
        self.client.reconnect_delay_set(min_delay=1, max_delay=10)
        self.client.max_queued_messages_set(max_queued)
        self._changed = Condition()
        self._send_lock = Lock()
        self._state = LinkState.CONNECTING
        self._outstanding = set()  # message IDs handed to Paho, not yet acknowledged
        self._backlog = deque()  # (topic, payload) waiting for ONLINE
        self._sending = False
        self._early_acks = set()  # acks that arrived before publish() returned the ID
        self._flusher = None
        self._subscriptions = {}  # topic filter -> handler(topic, payload)
        self.stats = dict.fromkeys(
            ("sent", "queued", "dropped", "acknowledged", "disconnects", "reconnects"),
            0,
        )

    @property
    def state(self) -> LinkState:
        with self._changed:
            return self._state

    def _move(self, new: LinkState) -> None:
        """Change state. Caller holds _changed. Never raises: Paho threads call this."""
        if new is not self._state and new not in TRANSITIONS[self._state]:
            logger.error("Ignored publisher transition %s -> %s", self._state, new)
            return
        self._state = new
        self._changed.notify_all()

    # ---- paho callbacks (network thread) ----
    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            logger.error("MQTT connection refused: %s", reason_code)
            return
        with self._changed:
            if self._state is LinkState.OFFLINE:
                self.stats["reconnects"] += 1
                logger.info(
                    "MQTT reconnected; resending %d unacknowledged message(s)",
                    len(self._outstanding),
                )
            # Paho resends what is unacknowledged; nothing newer may overtake it.
            self._move(LinkState.RESENDING if self._outstanding else LinkState.ONLINE)
            filters = list(self._subscriptions)
        # A clean session forgets subscriptions, so restore them on every connect.
        for topic_filter in filters:
            client.subscribe(topic_filter, qos=1)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties):
        with self._changed:
            unexpected = self._state in CONNECTED
            if unexpected:
                self.stats["disconnects"] += 1
                self._move(LinkState.OFFLINE)
        if unexpected:
            logger.warning(
                "MQTT disconnected (%s); publishing continues into the offline queue",
                reason_code,
            )

    def _on_publish(self, client, userdata, mid, reason_code, properties):
        with self._changed:
            if mid in self._outstanding:
                self._outstanding.discard(mid)
                self.stats["acknowledged"] += 1
                if not self._outstanding and self._state is LinkState.RESENDING:
                    self._move(LinkState.ONLINE)
                self._changed.notify_all()
            elif self._sending:
                self._early_acks.add(mid)

    def _on_message(self, client, userdata, message):
        with self._changed:
            handlers = [
                handler
                for topic_filter, handler in self._subscriptions.items()
                if self._mqtt.topic_matches_sub(topic_filter, message.topic)
            ]
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
        with self._changed:  # one critical section, so no ack can slip between
            self._sending = False
            acknowledged_early = info.mid in self._early_acks
            self._early_acks.clear()
            if info.rc == mqtt.MQTT_ERR_QUEUE_SIZE:
                self.stats["dropped"] += 1
                return DROPPED
            if info.rc not in (mqtt.MQTT_ERR_SUCCESS, mqtt.MQTT_ERR_NO_CONN):
                raise ConnectionError(
                    f"MQTT publish failed: {mqtt.error_string(info.rc)}"
                )
            if acknowledged_early:  # the broker answered before publish() returned
                self.stats["acknowledged"] += 1
            else:
                self._outstanding.add(info.mid)
                if info.rc == mqtt.MQTT_ERR_NO_CONN and self._state is LinkState.ONLINE:
                    self._move(LinkState.RESENDING)  # Paho keeps it for a resend
            self._changed.notify_all()
            return SENT if info.rc == mqtt.MQTT_ERR_SUCCESS else QUEUED

    def _backlog_ready(self):
        return bool(self._backlog) and self._state is LinkState.ONLINE

    def _drain_backlog(self):
        with self._send_lock:
            while True:
                with self._changed:
                    if not self._backlog_ready():
                        return
                    topic, payload = self._backlog.popleft()
                if self._send(topic, payload) == DROPPED:
                    self._log_dropped(topic)

    def _flush_backlog_forever(self):
        while True:
            with self._changed:
                self._changed.wait_for(
                    lambda: self._state is LinkState.CLOSED or self._backlog_ready()
                )
                if self._state is LinkState.CLOSED:
                    return
            self._drain_backlog()

    def _log_dropped(self, topic):
        logger.error(
            "Dropped message for %s: offline queue of %d is full",
            topic,
            self.max_queued,
        )

    # ---- caller API ----
    def subscribe(self, topic_filter, handler):
        """Deliver messages matching topic_filter to handler(topic, payload), across reconnects."""
        with self._changed:
            self._subscriptions[topic_filter] = handler
            connected = self._state in CONNECTED
        if connected:
            self.client.subscribe(topic_filter, qos=1)

    @property
    def connected(self):
        return self.state in CONNECTED

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
            return self._changed.wait_for(lambda: self._state in CONNECTED, timeout)

    def publish(self, topic, payload):
        """Return "sent", "queued" (offline or behind a resend, kept) or "dropped" (queue full)."""
        with self._send_lock:
            with self._changed:
                held_back = self._state is not LinkState.ONLINE or bool(self._backlog)
                if held_back:
                    outcome = self._hold(topic, payload)
            if not held_back:
                outcome = self._send(topic, payload)
                if outcome != DROPPED:
                    with self._changed:
                        self.stats[outcome] += 1
        if outcome == DROPPED:
            self._log_dropped(topic)
        return outcome

    def _hold(self, topic, payload):
        """Keep a message in the backlog until ONLINE. Caller holds _changed."""
        if len(self._outstanding) + len(self._backlog) >= self.max_queued:
            self.stats["dropped"] += 1
            return DROPPED
        self._backlog.append((topic, payload))
        self.stats["queued"] += 1
        if self._flusher is None:
            self._flusher = Thread(
                target=self._flush_backlog_forever,
                daemon=True,
                name=f"{self.client_id}-backlog",
            )
            self._flusher.start()
        self._changed.notify_all()
        return QUEUED

    def flush(self, timeout=5.0):
        """Wait for broker acknowledgements. Return the number still missing."""
        deadline = time.monotonic() + timeout
        with self._changed:
            self._changed.wait_for(
                lambda: not self._outstanding and not self._backlog,
                max(0.0, deadline - time.monotonic()),
            )
            return len(self._outstanding) + len(self._backlog)

    def close(self, flush_timeout=5.0):
        """Disconnect. Return how many accepted messages were never acknowledged."""
        missing = self.flush(flush_timeout)
        if missing:
            logger.warning(
                "%d message(s) were not acknowledged by the broker before shutdown",
                missing,
            )
        with self._changed:
            self._move(LinkState.CLOSED)
        try:
            self.client.disconnect()
        finally:
            self.client.loop_stop()
        return missing

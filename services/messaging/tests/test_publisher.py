"""MQTT-04: ReliablePublisher bookkeeping, checked offline with a stand-in client."""

from types import SimpleNamespace

import paho.mqtt.client as mqtt
import pytest

from services.messaging.publisher import ReliablePublisher

OK = SimpleNamespace(is_failure=False)


class FakeClient:
    """Mimics what Paho reports: success online, NO_CONN offline, QUEUE_SIZE when full."""

    def __init__(self):
        self.online, self.kept, self.mid, self.limit = True, [], 0, 0
        self.calls = []

    def reconnect_delay_set(self, **kwargs):
        self.calls.append(("reconnect_delay", kwargs))

    def max_queued_messages_set(self, size):
        self.limit = size

    def publish(self, topic, payload, qos, retain):
        assert qos == 1 and retain is False
        self.mid += 1
        if len(self.kept) >= self.limit:
            return SimpleNamespace(rc=mqtt.MQTT_ERR_QUEUE_SIZE, mid=self.mid)
        self.kept.append(self.mid)
        return SimpleNamespace(rc=mqtt.MQTT_ERR_SUCCESS if self.online else mqtt.MQTT_ERR_NO_CONN, mid=self.mid)

    def acknowledge_all(self):
        for mid in self.kept:
            self.on_publish(self, None, mid, OK, None)
        self.kept = []

    def disconnect(self):
        self.calls.append("disconnect")

    def loop_stop(self):
        self.calls.append("loop_stop")


@pytest.fixture
def publisher():
    fake = FakeClient()
    publisher = ReliablePublisher("bus-1-publisher", max_queued=3, client_factory=lambda: fake)
    fake.on_connect(fake, None, None, OK, None)
    return publisher


def test_online_messages_are_sent_and_acknowledged(publisher):
    assert publisher.connected
    assert publisher.publish("t", "one") == "sent"
    assert publisher.unacknowledged == 1
    publisher.client.acknowledge_all()
    assert publisher.unacknowledged == 0
    assert publisher.stats == {"sent": 1, "queued": 0, "dropped": 0, "acknowledged": 1, "disconnects": 0, "reconnects": 0}


def test_offline_messages_are_queued_then_acknowledged_after_reconnect(publisher):
    client = publisher.client
    client.online = False
    client.on_disconnect(client, None, None, "connection lost", None)
    assert not publisher.connected
    assert [publisher.publish("t", str(n)) for n in range(2)] == ["queued", "queued"]
    assert publisher.unacknowledged == 2            # not assumed delivered
    client.online = True
    client.on_connect(client, None, None, OK, None)
    from services.messaging.relay import wait_for
    wait_for(lambda: len(client.kept) == 2, timeout=2, what="offline messages sent after reconnect")
    client.acknowledge_all()                        # broker acknowledges
    assert publisher.stats == {"sent": 0, "queued": 2, "dropped": 0, "acknowledged": 2, "disconnects": 1, "reconnects": 1}
    assert publisher.flush(0) == 0


def test_full_offline_queue_reports_dropped_messages(publisher):
    publisher.client.online = False
    outcomes = [publisher.publish("t", str(n)) for n in range(5)]
    assert outcomes == ["queued"] * 3 + ["dropped"] * 2
    assert publisher.stats["dropped"] == 2 and publisher.unacknowledged == 3


def test_close_reports_messages_the_broker_never_acknowledged(publisher):
    publisher.publish("t", "one")
    publisher.publish("t", "two")
    assert publisher.close(flush_timeout=0) == 2
    assert publisher.client.calls[-2:] == ["disconnect", "loop_stop"]


def test_refused_connection_is_not_counted_as_connected(publisher):
    client = publisher.client
    client.on_disconnect(client, None, None, "lost", None)
    client.on_connect(client, None, None, SimpleNamespace(is_failure=True), None)
    assert not publisher.connected and publisher.stats["reconnects"] == 0
    assert not publisher.wait_connected(0)


def test_other_publish_errors_are_raised(publisher, monkeypatch):
    monkeypatch.setattr(publisher.client, "publish",
                        lambda *a, **k: SimpleNamespace(rc=mqtt.MQTT_ERR_PAYLOAD_SIZE, mid=9))
    with pytest.raises(ConnectionError):
        publisher.publish("t", "too big")


@pytest.mark.parametrize("bad", [0, -1, True, 1.5])
def test_queue_bound_must_be_a_positive_integer(bad):
    with pytest.raises(ValueError):
        ReliablePublisher("x", max_queued=bad, client_factory=FakeClient)


class RecordingClient(FakeClient):
    def __init__(self):
        super().__init__()
        self.wire = []                        # payloads in the order handed to Paho

    def publish(self, topic, payload, qos, retain):
        self.wire.append(payload)
        return super().publish(topic, payload, qos, retain)

    def acknowledge(self, mid):
        self.kept.remove(mid)
        self.on_publish(self, None, mid, OK, None)


def test_messages_published_right_after_reconnect_wait_for_the_resends():
    """Paho resends kept messages only after on_connect returns; newer ones must not overtake them."""
    from services.messaging.relay import wait_for
    client = RecordingClient()
    publisher = ReliablePublisher("bus-1-publisher", client_factory=lambda: client)
    client.on_connect(client, None, None, OK, None)
    client.online = False                                      # lost, but not yet noticed
    assert [publisher.publish("t", f"old{n}") for n in (1, 2)] == ["queued", "queued"]
    assert client.wire == ["old1"]                             # kept by Paho; old2 waits behind it
    client.on_disconnect(client, None, None, "lost", None)
    client.online = True
    client.on_connect(client, None, None, OK, None)          # Paho's resend of old1 comes after this
    assert publisher.publish("t", "new3") == "queued"          # held back, not on the wire
    assert publisher.publish("t", "new4") == "queued"
    assert client.wire == ["old1"] and publisher.unacknowledged == 4
    client.acknowledge(1)
    wait_for(lambda: client.wire == ["old1", "old2", "new3", "new4"], timeout=2, what="backlog in order")
    client.acknowledge_all()
    assert publisher.flush(1) == 0
    assert publisher.publish("t", "new5") == "sent"            # caught up: straight through
    publisher.close(flush_timeout=0)


def test_messages_published_while_offline_are_sent_in_order_after_reconnect():
    from services.messaging.relay import wait_for
    client = RecordingClient()
    publisher = ReliablePublisher("bus-1-publisher", client_factory=lambda: client)
    client.on_connect(client, None, None, OK, None)
    assert publisher.publish("t", "a") == "sent"               # in flight when the link drops
    client.online = False
    client.on_disconnect(client, None, None, "lost", None)
    assert [publisher.publish("t", n) for n in ("b", "c")] == ["queued", "queued"]
    client.online = True
    client.on_connect(client, None, None, OK, None)
    assert client.wire == ["a"]                                # b, c wait until a is acknowledged
    client.acknowledge_all()
    wait_for(lambda: client.wire == ["a", "b", "c"], timeout=2, what="offline messages in order")
    publisher.close(flush_timeout=0)


def test_held_back_messages_count_toward_the_queue_bound():
    client = RecordingClient()
    publisher = ReliablePublisher("bus-1-publisher", max_queued=3, client_factory=lambda: client)
    client.on_connect(client, None, None, OK, None)
    client.online = False
    publisher.publish("t", "old1")
    publisher.publish("t", "old2")
    client.on_disconnect(client, None, None, "lost", None)
    client.online = True
    client.on_connect(client, None, None, OK, None)
    assert [publisher.publish("t", n) for n in ("a", "b")] == ["queued", "dropped"]
    assert publisher.stats["dropped"] == 1
    assert publisher.close(flush_timeout=0) == 3               # reported, not assumed delivered


def test_ack_arriving_before_publish_returns_is_not_lost():
    client = RecordingClient()
    original = client.publish
    def instant_ack(topic, payload, qos, retain):
        info = original(topic, payload, qos, retain)
        client.acknowledge(info.mid)                          # broker answered inside publish()
        return info
    client.publish = instant_ack
    publisher = ReliablePublisher("bus-1-publisher", client_factory=lambda: client)
    client.on_connect(client, None, None, OK, None)
    assert publisher.publish("t", "x") == "sent"
    assert publisher.unacknowledged == 0 and publisher.stats["acknowledged"] == 1

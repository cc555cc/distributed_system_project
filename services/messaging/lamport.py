"""Lamport logical clock shared by the bus simulator, edge workers and central.

Each process owns one clock and follows two rules:

* local event or send:  clock = clock + 1, and the message carries that value
* receive:              clock = max(clock, received value) + 1

A message's stamp is therefore always lower than the receiver's clock after
delivery, whatever the wall clocks on the two machines say.
"""

from threading import Lock


def _checked(value: object, name: str) -> int:
    # bool is an int subclass; True must not be accepted as clock value 1.
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


class LamportClock:
    """Thread-safe counter: MQTT callbacks and worker loops may share one clock."""

    def __init__(self, start: int = 0) -> None:
        self._value = _checked(start, "start")
        self._lock = Lock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._value

    def tick(self) -> int:
        """Local event or send. Return the stamp to put on the outgoing message."""
        with self._lock:
            self._value += 1
            return self._value

    def receive(self, received: int) -> int:
        """Receive rule. Return this process's clock after the delivery."""
        received = _checked(received, "received clock")
        with self._lock:
            self._value = max(self._value, received) + 1
            return self._value

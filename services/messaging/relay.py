"""TCP relay between an MQTT client and the broker, for fault experiments.

A client connects to the relay's local port instead of the broker. The relay can:

* ``cut()``      drop every connection and refuse new ones (connection loss)
* ``restore()``  accept connections again
* ``delay``      hold each forwarded chunk for that many seconds (network delay)

The broker itself keeps running, so this isolates one client's network path.
Used by the live reconnection tests (MQTT-04) and the delivery benchmarks
(MQTT-06, MQTT-11).
"""

import socket
import time
from threading import Lock, Thread


class Link:
    def __init__(self, broker_host, broker_port, *, listen_host="127.0.0.1"):
        self.broker = (broker_host, broker_port)
        self.server = socket.create_server((listen_host, 0))
        self.port = self.server.getsockname()[1]
        self.up, self.open, self.sockets = True, True, []
        self.delay = 0.0
        self._lock = Lock()
        Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while self.open:
            try:
                near, _ = self.server.accept()
            except OSError:
                return
            if not self.up:
                near.close()
                continue
            try:
                far = socket.create_connection(self.broker, timeout=5)
            except OSError:
                near.close()
                continue
            far.settimeout(None)
            for sock in (near, far):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            with self._lock:
                self.sockets += [near, far]
            Thread(target=self._pump, args=(near, far), daemon=True).start()
            Thread(target=self._pump, args=(far, near), daemon=True).start()

    def _pump(self, source, target):
        try:
            while data := source.recv(65536):
                if self.delay:
                    time.sleep(self.delay)
                target.sendall(data)
        except OSError:
            pass
        finally:
            for sock in (source, target):
                try:
                    sock.close()
                except OSError:
                    pass

    def cut(self):
        self.up = False
        with self._lock:
            sockets, self.sockets = self.sockets, []
        for sock in sockets:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

    def restore(self):
        self.up = True

    def close(self):
        self.cut()
        self.open = False
        self.server.close()


def wait_for(condition, timeout=20.0, what="condition", interval=0.05):
    """Poll until condition() is true; raise AssertionError on timeout."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(interval)
    raise AssertionError(f"Timed out waiting for {what}")

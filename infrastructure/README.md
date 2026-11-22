# Infrastructure: MQTT broker

One Mosquitto 2.x broker carries all messaging. Its configuration is [mosquitto/mosquitto.conf](./mosquitto/mosquitto.conf); [compose.yaml](./compose.yaml) runs it in a container with Podman or Docker.

## Start the broker

Run these from the repository root.

| Action | Podman | Docker |
| --- | --- | --- |
| Start | `podman compose -f infrastructure/compose.yaml up -d` | `docker compose -f infrastructure/compose.yaml up -d` |
| Logs | `podman logs -f transit-mosquitto` | `docker logs -f transit-mosquitto` |
| Stop, keep data | `podman compose -f infrastructure/compose.yaml stop` | `docker compose -f infrastructure/compose.yaml stop` |
| Remove | `podman compose -f infrastructure/compose.yaml down` | `docker compose -f infrastructure/compose.yaml down` |

`podman compose` needs a compose provider: `pip install podman-compose`. On Windows and macOS, Podman also needs its VM: `podman machine init` once, then `podman machine start`.

Without a container runtime, install Mosquitto and run `mosquitto -c infrastructure/mosquitto/mosquitto.conf` after changing `persistence_location` to a local directory.

## Check the broker

```text
pip install -r requirements.txt
python -m services.messaging.tests.smoke_broker
```

The script connects a publisher and a subscriber as separate clients and sends one message on every topic below. It prints `PASS` or `FAIL` per topic and exits non-zero on failure. Add `--host <address>` to check a broker on another machine.

## Connection settings

| Setting | Value |
| --- | --- |
| Host | `MQTT_HOST`, default `localhost` |
| Port | `MQTT_PORT`, default `1883` (plain TCP, no TLS) |
| Authentication | Shared demo remains anonymous; edge credential/TLS/signature support and tested ACL generation are described in the root README; broker-wide MQTT-07 deployment remains separate |
| QoS | 1 for every publish and subscribe |
| Retained messages | Not used |
| Keepalive | 60 seconds |
| Client ID | Unique per process; the broker disconnects the older of two clients sharing an ID |

Services read `MQTT_HOST` and `MQTT_PORT` from the environment or the root `.env`. To use a broker on another machine, set `MQTT_HOST` to that machine's address and allow inbound TCP 1883 through its firewall.

## Topics

| Purpose | Topic | Publisher | Subscriber and filter |
| --- | --- | --- | --- |
| Bus telemetry | `transit/buses/<region_id>/<bus_id>/telemetry` | Bus simulator, one client per bus (`<bus_id>-publisher`) | That region's edge service: `transit/buses/<region_id>/+/telemetry` |
| Validated bus state | `transit/<region_id>/bus/<bus_id>/state` | Regional edge service (`<edge_id>-to-central`) | Central service (`central-server`): `transit/+/bus/+/state` |
| Alerts | `transit/alerts/<region_id>/<source_id>` | The service that detects the problem | Central service: `transit/alerts/#` |
| Command to one bus | `transit/commands/<region_id>/<bus_id>` | Central service | That bus only |
| Command to a region | `transit/commands/<region_id>/all` | Central service | Every bus in the region |

The telemetry and state topics are the ones the simulator, edge and central code already use. The alert and command topics are proposed and not yet used by any service; confirm them through TEAM-02 before MQTT-05 builds on them.

Region IDs must not be `buses`, `alerts` or `commands`, and a bus ID must not be `all`. Python code should build topic strings with [services/messaging/topics.py](../services/messaging/topics.py) rather than typing them out.


## Heartbeats and queue health

The bus publisher sends `transit/heartbeats/bus/<region_id>/<bus_id>`. Its payload has independent heartbeat states for four logical channels: GPS, speed, stop status and passenger occupancy. Missing channel entries do not refresh that channel. Heartbeats run separately from telemetry generation, including while the simulator reports sensor faults.

Each edge sends `transit/heartbeats/edge/<region_id>/<edge_id>` to central. Central subscribes independently and exposes `GET /health/edges`, including sensor reports, edge liveness, queue measurements and least-loaded-peer recommendations. A missing edge becomes unavailable after its timeout without needing a final message from that edge.

Default heartbeat interval is 2 seconds; sensor and edge timeouts are each 10 seconds. Recovery requires repeated heartbeats over 4 seconds. Set `HEARTBEAT_INTERVAL_SECONDS`, `SENSOR_HEARTBEAT_TIMEOUT_SECONDS`, `EDGE_HEARTBEAT_TIMEOUT_SECONDS`, `HEARTBEAT_RECOVERY_SECONDS`, `EDGE_OVERLOAD_PERCENT` (80), `EDGE_OVERLOAD_SUSTAIN_SECONDS` (6) and `EDGE_QUEUE_SIZE` (50, configurable from 1 to 50) before starting compose to customize them.

The queue percentage measures telemetry backlog, not passenger occupancy or CPU utilization. The 50/70/75/79/80 bands are reported through health snapshots. Sustained overload produces a recommendation; no workload migration, replication, takeover or extra instance is automatically performed. Existing static bus assignments remain authoritative.

The image starts `services.edge.main --single` for each of the four configured edge containers. For a host-based launch, `python -m services.edge.main` creates four separate processes from the map configuration. Do not run the host launcher and the same compose edge workers simultaneously: duplicate MQTT client IDs would compete.

Heartbeat state is in memory and resets when central restarts. Session IDs and increasing heartbeat sequences prevent delayed duplicates from refreshing liveness. These endpoints do not establish broker authentication or confirm a crash; those remain separate responsibilities.


For UI load experiments, `/health/simulation` provides an isolated stateful bus-telemetry simulation and virtual clock. Its instance controls never change these Compose workers. Telemetry, ownership transfer, replica takeover, recovery and scaling run in the single-coordinator simulation; the live MQTT-worker topology remains fixed. See the root README for the request, advance, capacity and third-instance experiment endpoints. Queue capacity defaults to 50 waiting messages in both the live edge and the experiment.

## Message ordering (MQTT-03)

Every bus telemetry message carries three ordering fields. Each answers a different question, and only the first decides whether a state is stored.

| Field | Set by | Meaning | Used for |
| --- | --- | --- | --- |
| `sequence_number` | Bus | Counts that bus's readings, starting at 1 and rising by 1. A reading dropped by a failed sensor leaves a gap. | Edge and central discard a reading whose sequence is not higher than the last one accepted for that bus. |
| `lamport_clock` | Bus | The bus's Lamport clock when it sent the reading. | Ordering events across buses, edges and central without trusting wall clocks. |
| `timestamp` | Bus | UTC wall-clock time the reading was generated, ISO 8601 with offset. | Reporting only. It is never used to order or reject messages. |

Every process (each bus, each edge worker, central) owns one Lamport clock, implemented in [services/messaging/lamport.py](../services/messaging/lamport.py):

- **Send or local event:** `clock = clock + 1`; the message carries the new value.
- **Receive:** `clock = max(clock, received value) + 1`.

How the stamps travel:

1. The bus ticks its clock for every reading and sends the value as `lamport_clock`.
2. The edge applies the receive rule to every validated reading it takes off its queue, including ones it then rejects as stale or duplicate.
3. When the edge accepts a reading it ticks its clock and sends that value to central as `edge_metadata.edge_lamport`. The bus's own stamp is forwarded unchanged as `lamport`. A delivery that is retried after a broker failure keeps its original stamp.
4. Central applies the receive rule to `edge_lamport` for every valid bus-state message, stored or not. A sender that supplies no `edge_lamport` is treated as relaying the bus's `lamport`.

So for one reading, `lamport` < `edge_lamport` < central's clock after delivery. The shared model rejects a state whose `edge_lamport` is not greater than its `lamport`.

`edge_lamport` is an optional addition to the edge-to-central contract and needs confirming through TEAM-02. Central keeps its clock in memory and does not store it; `edge_lamport` is stored with the rest of `edge_metadata`.

Limits: clocks restart from 0 when a process restarts. Heartbeats do not carry Lamport stamps; they are ordered by session ID and heartbeat sequence. Edge workers do not yet exchange stamps with each other, so stamps from two workers are only ordered through the buses and central they share. Buses do not receive messages yet; when commands arrive (MQTT-05), the command consumer passes its clock to the simulator so received stamps raise later send stamps.

Run the checks with `python -m pytest services/messaging/tests/test_lamport.py -q`.

## Connection loss, delayed and duplicate messages (MQTT-04)

All messages use QoS 1, which is at-least-once: a message can arrive late or more than once, and receivers must tolerate both. Ordering never depends on arrival order; it depends on the fields in [Message ordering](#message-ordering-mqtt-03).

What each client does when its connection drops:

| Client | Reconnect | While disconnected | After reconnect |
| --- | --- | --- | --- |
| Bus publisher ([publisher.py](../services/messaging/publisher.py)) | Automatic, 1-10 s backoff | Keeps generating readings. Up to 1000 unacknowledged messages are held in memory and reported as `queued`; any beyond that are reported as `dropped` and counted. Heartbeats are skipped, not queued. | Queued readings are sent in their original order with their original sequence number, Lamport stamp and timestamp. |
| Edge subscriber | Automatic, 1-10 s backoff | Clean session: the broker does not keep telemetry for it. | Resubscribes in `on_connect`. Readings published during the gap are lost and show up as a gap in accepted sequence numbers. |
| Edge-to-central sender | Automatic, 1-10 s backoff | Keeps only the latest accepted state per bus. | Sends the latest state; a pending state older than `EDGE_FRESHNESS_SECONDS` is discarded instead. |
| Central subscriber | Automatic, 1-30 s backoff | Persistent session: the broker queues bus states for it. | Resubscribes and receives the queued states late, in order. |

How a late or repeated message is identified:

| Where | Check | Result |
| --- | --- | --- |
| Edge | Sequence lower than the last accepted for that bus | `STALE`, state unchanged, nothing forwarded |
| Edge | Same sequence and same content | `DUPLICATE`, state unchanged, nothing forwarded |
| Edge | Same sequence, different content | `CONFLICT`, state unchanged, nothing forwarded |
| Central | Sequence not higher than the stored one | `stale update`, row unchanged |

The publisher's totals (`sent`, `queued`, `dropped`, `acknowledged`, `disconnects`, `reconnects`) are logged when a bus process ends, and `close()` returns how many accepted messages the broker never acknowledged.

Limits: the bus's offline queue is in memory, so readings are lost if the bus process stops during an outage. When the queue is full the newest reading is the one dropped. With publisher signing enabled, a signed message that is delivered twice or arrives after the signature's time window is rejected by edge authentication before the sequence check. Behaviour across a broker restart is covered by MQTT-10.

Checks:

```text
python -m pytest services/messaging/tests/test_publisher.py services/messaging/tests/test_delivery_faults.py -q
```

The live reconnection tests need a dedicated broker and are skipped without `EDGE_LIVE_MQTT_HOST`. Each client connects through a TCP relay that the test cuts and restores:

```powershell
$env:EDGE_LIVE_MQTT_HOST = "127.0.0.1"
$env:EDGE_LIVE_MQTT_PORT = "1883"
python -m pytest services/messaging/tests/test_live_reconnect.py -q
```

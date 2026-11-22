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
| Authentication | Default demo broker is anonymous. The secured broker (`compose.secure.yaml`, [MQTT-07](#authentication-and-topic-authorization-mqtt-07)) requires a per-service account and enforces topic ACLs |
| QoS | 1 for every publish and subscribe |
| Per-client queue | `max_queued_messages 10000` ([why](#scaling-with-bus-count-mqtt-11)) |
| Retained messages | Not used |
| Keepalive | 60 seconds |
| Client ID | Unique per process; the broker disconnects the older of two clients sharing an ID |

Services read `MQTT_HOST` and `MQTT_PORT` from the environment or the root `.env`. To use a broker on another machine, set `MQTT_HOST` to that machine's address and allow inbound TCP 1883 through its firewall.

## Topics

| Purpose | Topic | Publisher | Subscriber and filter |
| --- | --- | --- | --- |
| Bus telemetry | `transit/buses/<region_id>/<bus_id>/telemetry` | Bus simulator, one client per bus (`<bus_id>-publisher`) | That region's edge service: `transit/buses/<region_id>/+/telemetry` |
| Validated bus state | `transit/<region_id>/bus/<bus_id>/state` | Regional edge service (`<edge_id>-to-central`) | Central service (`central-server`): `transit/+/bus/+/state` |
| Alerts | `transit/alerts/<region_id>/<source_id>` | The bus or edge that detects the problem (`source_id` is its own ID) | Central service: `transit/alerts/#` |
| Command to one bus | `transit/commands/<region_id>/<bus_id>` | Central service | That bus only |
| Command to a region | `transit/commands/<region_id>/all` | Central service | Every bus in the region |

Every topic above is in use. Alerts and commands are described in [MQTT-05](#alerts-and-simulation-commands-mqtt-05).

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

Limits: clocks restart from 0 when a process restarts. Heartbeats do not carry Lamport stamps; they are ordered by session ID and heartbeat sequence. Edge workers do not exchange stamps with each other, so stamps from two workers are only ordered through the buses and central they share. Alerts and commands carry Lamport stamps too: central applies the receive rule to every alert, and a bus applies it to every command before acting, using the same clock as its telemetry, so its next reading is stamped later than the command ([MQTT-05](#alerts-and-simulation-commands-mqtt-05)).

Run the checks with `python -m pytest services/messaging/tests/test_lamport.py -q`.

## Connection loss, delayed and duplicate messages (MQTT-04)

All messages use QoS 1, which is at-least-once: a message can arrive late or more than once, and receivers must tolerate both. Ordering never depends on arrival order; it depends on the fields in [Message ordering](#message-ordering-mqtt-03).

What each client does when its connection drops:

| Client | Reconnect | While disconnected | After reconnect |
| --- | --- | --- | --- |
| Bus publisher ([publisher.py](../services/messaging/publisher.py)) | Automatic, 1-10 s backoff | Keeps generating readings. Up to 1000 unacknowledged messages are held in memory and reported as `queued`; any beyond that are reported as `dropped` and counted. Heartbeats are skipped, not queued. | Queued readings are sent in their original order with their original sequence number, Lamport stamp and timestamp. Newer readings wait until every older one is acknowledged (see below). Command subscriptions are restored. |
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

**Ordering across a reconnect.** Paho marks a client connected and calls `on_connect` *before* it resends the messages kept from the lost connection. The MQTT-06 benchmark caught a reading published in that gap overtaking the resends: for example 36 arrived before 21-35, and the edge would then have rejected 21-35 as stale. `ReliablePublisher` now holds new readings in a local backlog whenever older ones are still unacknowledged after a disconnect, and a background thread sends the backlog in order once they are acknowledged. Before the change, two of four 10-bus outage runs had 15 and 35 late arrivals; after it, none in any run (MQTT-06 and MQTT-11 tables). The same change stops the publisher holding its own lock while calling Paho; Paho calls `on_publish` under its lock, so the old order could deadlock.

Limits: the bus's offline queue is in memory, so readings are lost if the bus process stops during an outage. When the queue is full the newest reading is the one dropped. With publisher signing enabled, a signed message that is delivered twice or arrives after the signature's time window is rejected by edge authentication before the sequence check. Behaviour across a broker restart is covered by [MQTT-10](#broker-restart-mqtt-10).

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

## Alerts and simulation commands (MQTT-05)

Both message types are JSON with a schema version, a unique ID (QoS 1 may deliver twice), a UTC timestamp and the sender's Lamport stamp. Models: [alerts.py](../services/messaging/alerts.py) and [commands.py](../services/messaging/commands.py).

**Alerts** go to `transit/alerts/<region_id>/<source_id>`:

| Source | When | `kind`, `severity` |
| --- | --- | --- |
| Bus simulator ([run_buses.py](../services/data_simulation/run_buses.py)) | Its sensors change from healthy to failed, and back | `sensor_failure` warning; `sensor_recovered` info |
| Edge worker ([edge_server.py](../services/edge/edge_server.py)) | Its ingress queue is full and telemetry is being rejected; at most one alert per 30 s | `edge_overload` critical |

Central subscribes to `transit/alerts/#` and accepts an alert only if it arrived on its own source's topic. It drops repeated deliveries by `alert_id` and keeps the newest 500 in memory. `GET /alerts?region_id=&severity=&limit=` lists them newest first.

**Commands** go to one bus (`transit/commands/<region>/<bus_id>`) or a whole region (`transit/commands/<region>/all`). `POST /commands` publishes one:

```json
{"region_id": "west", "bus_id": "bus-302-1", "action": "set_traffic", "params": {"traffic_factor": 0.5}}
```

| `action` | `params` | Effect on the running simulated bus |
| --- | --- | --- |
| `pause` / `resume` | none | Stop generating readings / continue from the same point |
| `set_traffic` | `traffic_factor` in (0, 1] | Speed from the next stop segment |
| `set_demand` | `demand`: `peak` or `off_peak` | Boarding from the next stop |
| `set_sensor_failure_rate` | `sensor_failure_rate` in [0, 1] | Failure probability from the next reading |

Omit `bus_id` to address the region. Unknown regions, buses outside the region and invalid parameters are rejected (404/422) and nothing is published. 202 means MQTT accepted the command; a bus that is offline receives it after reconnecting only if it is within the broker's session (bus clients use clean sessions, so commands sent while a bus is down are not kept).

Only the intended buses act, at three levels:

1. Each bus subscribes to exactly two topics: its own and its region's `all`.
2. On the secured broker the ACL lets a bus read only those two topics, so it cannot even receive other buses' commands.
3. `CommandConsumer` checks the command's region and target again and ignores anything else. It acts once per `command_id` and applies the Lamport receive rule before acting.

Edges and central do not subscribe to command topics. Operator-only access to `POST /commands` is CENTRAL-09.

```text
python -m pytest services/messaging/tests/test_alerts_commands.py -q
python -m pytest services/messaging/tests/test_live_alerts_commands.py -q     # needs EDGE_LIVE_MQTT_HOST
```

The live test runs central's real MQTT module and three bus clients in two regions. A one-bus command, a region command and a command to the other region each change only the intended buses. A simulated sensor failure arrives at central as an alert.

## Delivery measurements (MQTT-06)

[benchmark.py](../services/messaging/benchmark.py) runs simulated buses as separate MQTT clients (`ReliablePublisher`, QoS 1). Each publishes telemetry-sized messages at a fixed rate through its own TCP relay ([relay.py](../services/messaging/relay.py)) to the broker. One subscriber on the region filter (like an edge) records every arrival. The relay disturbs only the bus-to-broker path:

| Scenario | Disturbance |
| --- | --- |
| `normal` | none |
| `delayed` | every packet held 200 ms during the middle third of the run |
| `outage` | every bus connection cut for 3 s, a third of the way in |

Delay is measured from generation on the bus to arrival at the subscriber, including any time in the bus's offline queue. "Late" counts readings that arrived after a higher sequence from the same bus; the edge would reject them as stale. Lost messages are listed by bus and sequence number ([delivery.py](../services/messaging/delivery.py)), never inferred from counts.

Recorded 2026-11-22: Mosquitto 2.1.4 in Podman (WSL 2), Windows 11, 8-core Intel laptop, Python 3.12, paho-mqtt 2.1. 10 buses, 5 messages/s each, 15 s ([mqtt06_delivery.json](../services/messaging/results/mqtt06_delivery.json)):

| Scenario | Sent | Received | Duplicates | Lost | Late | Lamport errors | p50 ms | p95 ms | p99 ms | Max ms | Reconnects |
|---|---|---|---|---|---|---|---|---|---|---|---|
| normal | 750 | 750 | 0 | 0 | 0 | 0 | 1.32 | 2.93 | 3.29 | 4.43 | 0 |
| delayed | 750 | 750 | 0 | 0 | 0 | 0 | 2.28 | 222.79 | 228.84 | 233.88 | 0 |
| outage | 750 | 750 | 0 | 0 | 0 | 0 | 2.29 | 5600.83 | 6789.99 | 7009.48 | 10 |

- **Normal:** every message arrives once, in order, within about 4 ms.
- **Delayed:** the 200 ms hold shows in p95+ only; nothing is lost or reordered.
- **Outage:** every message still arrives, in order, after each bus reconnects. The worst delay (about 7 s) is the 3 s cut plus the reconnect backoff: Paho retries at 1, 2 and 4 s, so the first successful attempt can come about 4 s after the link returns. Lowering `reconnect_delay_set(max_delay=...)` shortens that at the cost of more connection attempts during a long outage.
- **Duplicates:** single duplicates appear in some outage runs (see MQTT-11), when a PUBACK was lost with the connection and Paho resent with the DUP flag. The edge reports them as `DUPLICATE`.

```text
python -m services.messaging.benchmark --host 127.0.0.1 --port 1883 --buses 10 --scenarios normal delayed outage --duration 15 --output results.json
```

## Authentication and topic authorization (MQTT-07)

The secured broker ([mosquitto-secure/mosquitto.conf](./mosquitto-secure/mosquitto.conf)) sets `allow_anonymous false` with a password file and an ACL file. [broker_security.py](../services/messaging/broker_security.py) generates both from `shared/routes.json`: one account per service, with a unique random password, Mosquitto `$7$` PBKDF2-SHA512 hashes, and the ACL from `mosquitto_acl()`.

| Account | May write | May read |
| --- | --- | --- |
| `central` | `transit/commands/#`, `transit/assignments/+` | `transit/+/bus/+/state`, `transit/heartbeats/edge/+/+`, `transit/alerts/#` |
| `<edge_id>` | its own heartbeat and alert topics; the state topic of every bus in its region (failover may hand it any of them; central's ownership check rejects a non-owner) | its region's telemetry, sensor heartbeats and ownership assignments |
| `<bus_id>` | its own telemetry, heartbeat and alert topics | its own and its region's command topics |

```text
python -m services.messaging.broker_security        # writes mosquitto-secure/acl, mosquitto-secure/passwd, secure.env
podman compose -f infrastructure/compose.yaml -f infrastructure/compose.secure.yaml --env-file infrastructure/secure.env up -d --build
podman compose -f infrastructure/compose.yaml -f infrastructure/compose.secure.yaml --env-file infrastructure/secure.env run --rm buses
```

`passwd` and `secure.env` contain secrets and are git-ignored; so is the generated `acl`, so it cannot drift from the network file. Re-running the generator replaces every password. Clients send `MQTT_USERNAME`/`MQTT_PASSWORD`. The bus launcher picks each bus's account from `MQTT_BUS_PASSWORDS_JSON` and fails rather than share an account. Central now applies the same credential settings as the edges.

Tested against the secured broker (`test_live_broker_security.py`, 6 tests):

- Anonymous logins, wrong passwords, another account's password and unknown users are refused at CONNECT.
- `bus-302-1` is refused (PUBACK "not authorized") when it publishes another bus's telemetry, its own ID in another region, an edge state, a command, an edge alert or an edge heartbeat.
- `edge-west-1` may publish states for buses in its own region (any of them, for failover) but not for another region's buses.
- A bus subscribing to `transit/commands/#` receives only its own and its region's commands, and a bus subscribing to `transit/buses/#` receives no other bus's telemetry.
- With every service on its own account, bus → edge telemetry, edge → central state, central → bus command and bus → central alert all still work.

Ali's live edge authentication test (`test_auth_edge_server.py`) also passes against a broker loaded with these generated files.

Limits: in the compose stack passwords cross the private container network in plain TCP. Each client therefore sets `EDGE_ALLOW_INSECURE_MQTT=true` explicitly; expose the broker beyond the host only through a TLS listener with `MQTT_TLS=true` on the clients (supported by `configure_mqtt_client`, not deployed here). With files bind-mounted from Windows, Mosquitto 2.1 warns that `passwd`/`acl` are not owned by the `mosquitto` user; on Linux, `chown 1883:1883` them. Operator login for the HTTP API is CENTRAL-09.

## Physical time and clock offsets (MQTT-08)

How physical timestamps are handled ([clock_sync.py](../services/messaging/clock_sync.py)):

- `timestamp` is the UTC wall-clock time the sender generated the event, ISO 8601 with an explicit offset. It is for reporting only.
- Nothing orders, accepts or rejects telemetry, alerts or commands by timestamp. Acceptance uses per-bus sequence numbers; cross-process ordering uses Lamport stamps.
- Freshness is measured with the receiver's own monotonic clock from when it accepted a reading, so a sender's wrong clock cannot make data look fresh or stale.
- The only cross-machine wall-clock comparison is publisher signing: a signed message must be under 30 s old and at most 5 s in the future at the edge.

Synchronisation assumption: every host runs NTP or equivalent and stays within 2 s (`MAX_ASSUMED_SKEW_SECONDS`) of UTC. NTP normally achieves tens of milliseconds. A larger error does not corrupt ordering or state; it makes timestamps misleading and, beyond the signing window, gets signed messages rejected. `classify_timestamp()` reports a timestamp as `ok`, `ahead` or `behind` without changing anything.

`test_physical_clocks.py` feeds simulated clocks (fixed offset plus drift in ppm) into the real simulator, command consumer, edge processor and signature check:

| Case | Physical timestamps | Lamport / sequence |
| --- | --- | --- |
| Two buses at -3 s and +3 s; reading B is generated 1 s after A | B's timestamp is 5 s *earlier* than A's | Unaffected |
| Central's clock +2 s commands a bus whose clock is -2 s | The bus's next reading, caused by the command, carries an earlier timestamp than the command | Reading's Lamport stamp is greater than the command's |
| Bus clock steps back an hour between readings 1 and 2; a replay of 1 has a timestamp an hour ahead | Misleading in both directions | Edge accepts 2 and rejects the replay of 1 as `STALE` |
| Bus clock wrong by 25 years | Reading looks ancient | Edge marks it `current` from its own accept time, `stale` after the freshness window |
| Signed messages from a bus clock -31 s to +6 s off | -2 to +4.9 s accepted; +6 s and -31 s rejected | The window is the synchronisation requirement |
| 100 ppm drift | 0.36 s error after one hour | Unaffected |

`estimate_offset()` implements Cristian's algorithm for a request/response pair. The tests check that its estimate is within ±round-trip/2 of the true offset for symmetric and asymmetric delays.

```text
python -m pytest services/messaging/tests/test_physical_clocks.py -q
```

## Concurrent events and causality (MQTT-09)

A Lamport stamp proves only one direction: if *a* happened before *b*, then L(a) < L(b). It cannot show that two events are concurrent. [causality.py](../services/messaging/causality.py) records a vector clock beside each process's real `LamportClock` to classify every pair of events as `->` (happened before), `<-` or `||` (concurrent). Vector clocks are for analysis only; messages carry just the Lamport stamp.

`python -m services.messaging.causality` prints the scenario below. Two buses, an edge and central exchange messages; B1's first reading is delayed and arrives after its second, and edge-to-central states are also reordered. Rows are in Lamport total order (stamp, then process name):

```text
bus-1                                   bus-2                                   edge                                    central
--------------------------------------  --------------------------------------  --------------------------------------  --------------------------------------
(1) S B1 seq 1 [m1]                     |                                       |                                       |
|                                       (1) S B2 seq 1 [m2]                     |                                       |
(2) S B1 seq 2 [m3]                     |                                       |                                       |
(3) L B1 sensor fault                   |                                       |                                       |
|                                       |                                       (3) R recv B1 seq 2 [m3]                |
|                                       |                                       (4) S state B1 seq 2 [m4]               |
|                                       |                                       (5) R recv B1 seq 1 (late: STALE) [m1]  |
|                                       |                                       (6) R recv B2 seq 1 [m2]                |
|                                       |                                       (7) S state B2 seq 1 [m5]               |
|                                       |                                       |                                       (8) R recv state B2 [m5]
|                                       |                                       |                                       (9) R recv state B1 [m4]
|                                       |                                       |                                       (10) S cmd pause bus-2 [m6]
|                                       (11) R recv cmd [m6]                    |                                       |
|                                       (12) S B2 seq 2 (after cmd) [m7]        |                                       |

(n) Lamport stamp; S send, R receive, L local event; [mN] links a send to its receive.

               bus-1.1    bus-2.1  central.3    bus-1.3    bus-2.3
    bus-1.1                    ||         ->         ->         ->
    bus-2.1         ||                    ->         ||         ->
  central.3         <-         <-                    ||         ->
    bus-1.3         <-         ||         ||                    ||
    bus-2.3         <-         <-         <-         ||
```

How to read it:

- **Causal chain:** B1 seq 1 → … → the command → B2 seq 2, and the stamps rise along every chain.
- **Late delivery:** the edge receives B1 seq 1 at stamp 5, after seq 2. Its clock still moves forward, and the real edge processor rejects that reading as `STALE`.
- **Concurrency Lamport cannot show:** "B1 sensor fault" (3) and "cmd pause bus-2" (10) are concurrent, as are B1 seq 1 and B2 seq 1. The vector clocks reveal this.

`test_concurrency.py` checks the scenario and five seeded random runs: 6 buses, 2 edges and central, 120 steps, random delays (so deliveries are out of order), commands and local events. In every run each event obeys the send/receive rules, and every causal pair has rising Lamport stamps. Equal stamps occur only between concurrent events. The Lamport total order is a valid linearisation of causality, and concurrent pairs with different stamps exist.

```text
python -m pytest services/messaging/tests/test_concurrency.py -q
```

## Broker restart (MQTT-10)

`test_live_broker_restart.py` stops and starts the broker container itself, running this directory's `mosquitto.conf` (persistence on). It uses real clients: a bus `ReliablePublisher` with command subscriptions, the edge `TelemetrySubscriber`, and central's MQTT module.

| Client | Session | After the broker returns |
| --- | --- | --- |
| Bus publisher | clean | Reconnects (1-10 s backoff), resubscribes to its command topics, resends readings queued during the outage in order |
| Edge subscriber | clean | Reconnects and resubscribes in `on_connect`; the broker kept nothing for it |
| Central subscriber | persistent | Reconnects; states the broker queued for it before the restart are delivered, because persistence wrote them to disk |

Result of the recorded run: readings 1-3 arrived; 4-6 were generated while the broker was down and resent as soon as the bus reconnected, but the edge had not yet resubscribed. The broker therefore had no subscriber to deliver them to. The sequence tracker reported them as lost (`[4, 5, 6]`) rather than delivered, and 7 arrived normally. Whether 4-6 survive depends on which client reconnects first, so the test accepts either outcome but requires every reading to be either received or listed as lost. Commands reach the bus again after the restart.

For central, states published while it was away (2, 3) survived the restart in its persistent session and were applied in order after it reconnected, followed by 4.

Limits: telemetry published while the edge is reconnecting can be lost, as above; a persistent edge session would keep it at the cost of delivering stale readings after long outages. Messages accepted but not yet written to disk are lost if the broker crashes rather than stopping cleanly (Mosquitto writes persistence on shutdown and every `autosave_interval`, default 30 min).

```powershell
$env:MQTT_RESTART_HOST = "127.0.0.1"; $env:MQTT_RESTART_PORT = "18831"
$env:MQTT_RESTART_CONTAINER = "mqtt-restart-test"; $env:MQTT_RESTART_RUNTIME = "podman"
python -m pytest services/messaging/tests/test_live_broker_restart.py -q -s
```

## Scaling with bus count (MQTT-11)

The MQTT-06 benchmark was run with the same setup at 5, 10, 25 and 50 buses, each publishing 5 readings/s for 15 s. It was then stressed at 20 readings/s with 50 and 100 buses. Every bus is its own client and connection.

Default broker queue ([mqtt11_scaling.json](../services/messaging/results/mqtt11_scaling.json), [mqtt11_stress.json](../services/messaging/results/mqtt11_stress.json)):

| Scenario | Buses | Offered msg/s | Sent | Received | Dup | Lost | Late | Lamport errors | p50 ms | p95 ms | p99 ms | Max ms | Throughput msg/s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| normal | 5 | 25 | 375 | 375 | 0 | 0 | 0 | 0 | 2.15 | 3.05 | 3.32 | 4.03 | 24.3 |
| normal | 10 | 50 | 750 | 750 | 0 | 0 | 0 | 0 | 2.02 | 3.03 | 3.33 | 16.06 | 48.5 |
| normal | 25 | 125 | 1875 | 1875 | 0 | 0 | 0 | 0 | 1.63 | 3.01 | 3.75 | 14.76 | 121.1 |
| normal | 50 | 250 | 3750 | 3750 | 0 | 0 | 0 | 0 | 2.04 | 3.64 | 4.68 | 25.3 | 241.9 |
| outage | 5 | 25 | 375 | 375 | 0 | 0 | 0 | 0 | 2.11 | 2280.87 | 2874.89 | 3030.66 | 24.3 |
| outage | 10 | 50 | 750 | 750 | 0 | 0 | 0 | 0 | 2.24 | 5229.22 | 6655.07 | 7020.45 | 48.4 |
| outage | 25 | 125 | 1875 | 1876 | 1 | 0 | 0 | 0 | 2.39 | 5723.62 | 6870.36 | 7125.99 | 121.0 |
| **outage** | **50** | 250 | 3750 | 3610 | 0 | **140** | 0 | 0 | 2.73 | 6289.42 | 7135.58 | 7364.07 | 232.9 |
| normal | 50 | 1000 | 15000 | 15000 | 0 | 0 | 0 | 0 | 0.52 | 3.62 | 29.75 | 44.5 | 967.6 |
| **normal** | **100** | 2000 | 30000 | 29045 | 0 | **955** | 0 | 0 | 168.51 | 665.3 | 705.01 | 736.78 | 1851.5 |

With `max_queued_messages 10000` ([mqtt11_outage_tuned.json](../services/messaging/results/mqtt11_outage_tuned.json), [mqtt11_stress_tuned.json](../services/messaging/results/mqtt11_stress_tuned.json)):

| Scenario | Buses | Offered msg/s | Sent | Received | Dup | Lost | Late | Lamport errors | p50 ms | p95 ms | p99 ms | Max ms | Throughput msg/s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| outage | 50 | 250 | 3750 | 3751 | 1 | 0 | 0 | 0 | 2.84 | 6070.63 | 7088.43 | 7354.42 | 241.8 |
| normal | 100 | 2000 | 30000 | 30000 | 0 | 0 | 0 | 0 | 7.34 | 305.27 | 332.62 | 453.16 | 1934.5 |

What the results show:

- **Normal load, 5 to 50 buses:** throughput rises linearly with bus count and matches the offered rate. Median delay stays about 2 ms and p99 under 5 ms; the occasional maximum of 15-25 ms is scheduling jitter on the test machine.
- **Ordering rules hold everywhere:** in every run, including outages and the 2000 msg/s stress, there were 0 late arrivals and 0 Lamport errors. Each bus's readings arrived in sequence order with rising Lamport stamps (after the MQTT-04 ordering fix above).
- **Measured limitation 1, the subscriber queue:** at 50 buses with an outage, the broker logged "Outgoing messages are being dropped for client bench-sub-…". After the 3 s cut, 50 buses resent about 1400 queued readings at once to the one regional subscriber, exceeding Mosquitto's default per-client queue of 1000. The publishers had received PUBACKs, so this loss is invisible to senders. The receiver's sequence tracker listed all 140 lost readings. `mosquitto.conf` now sets `max_queued_messages 10000`, and the rerun lost nothing.
- **Measured limitation 2, one Python subscriber:** at 2000 msg/s a single Python subscriber nears its limit (about 1900 msg/s here). With the larger queue nothing is lost, but delay grows (p95 about 300 ms) as the backlog builds. Each region's edges subscribe to their own region's filter, so load splits by region. Edges also validate and process every reading, so measure the edge (EDGE-10) before assuming the broker is the bottleneck.
- **Outages:** delay is dominated by the reconnect backoff and grows little with bus count.

Reproduce:

```text
python -m services.messaging.benchmark --port 1883 --buses 5 10 25 50 --scenarios normal outage --rate 5 --duration 15 --output services/messaging/results/mqtt11_scaling.json
python -m services.messaging.benchmark --port 1883 --buses 50 100 --rate 20 --duration 15 --output services/messaging/results/mqtt11_stress.json
```

Results depend on the machine; rerun on the presentation machine before quoting them.

## Running every messaging check

```text
python -m pytest services/messaging -q          # offline checks; live ones skip without settings
```

| Live test | Broker | Settings |
| --- | --- | --- |
| `test_live_reconnect.py`, `test_live_alerts_commands.py`, short benchmark in `test_messaging_tools.py` | any dedicated broker | `EDGE_LIVE_MQTT_HOST`, `EDGE_LIVE_MQTT_PORT` |
| `test_live_broker_restart.py` | container running `infrastructure/mosquitto` that the test may stop/start | `MQTT_RESTART_HOST`, `MQTT_RESTART_PORT`, `MQTT_RESTART_CONTAINER`, `MQTT_RESTART_RUNTIME` |
| `test_live_broker_security.py` | container running `infrastructure/mosquitto-secure` with generated files | `MQTT_SECURE_HOST`, `MQTT_SECURE_PORT`, optional `MQTT_SECURE_ENV_FILE` |

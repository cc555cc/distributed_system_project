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
| Authentication | None yet; anonymous clients are allowed until MQTT-07 |
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

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

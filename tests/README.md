# Global acceptance tests

Run from the repository root:

```powershell
python -m pytest tests services/central/tests/test_evaluation.py -q
```

| File | Acceptance covered |
|---|---|
| `test_contract.py` | TEAM-02: configured publishers validate at edge; central mapping and MQTT topics match the shared contract; bad assignments cannot alter state |
| `test_system_integration.py` | TEAM-05/06/08/14: central/public consistency, full modeled processing path, named controls, demand-to-dispatch-to-passenger feedback |
| `test_system_quality.py` | TEAM-18: backend access/privacy checks, one batch per threshold crossing, bounded queues, failure/recovery and central outage delivery |
| `test_live_pipeline.py` | TEAM-06: real MQTT, four edge processes, PostgreSQL and public passenger payload; optional dedicated services |
| `../services/central/tests/test_evaluation.py` | CENTRAL-10: observed arrival error and matched scheduling-on/off runs under normal/delayed traffic |

The ordinary run uses a fixed clock and real validation, processing, mapping, scheduler and FastAPI code. It does not pretend that in-memory delivery is a network broker. The native test uses the existing isolated-schema fixture and actual processes. Browser checks additionally exercise named action buttons and nearby-location UI.

## Native integration

Use a **dedicated broker**, not the running preview broker: fixed edge client identities can otherwise disconnect existing workers. Use a dedicated PostgreSQL development database; the fixture creates and removes only a unique test schema.

```powershell
$env:EDGE_LIVE_DATABASE_URL = "postgresql://USER:PASSWORD@localhost:5433/transit"
$env:EDGE_LIVE_MQTT_HOST = "127.0.0.1"
$env:EDGE_LIVE_MQTT_PORT = "1884" # replace with the dedicated test broker port
python -m pytest tests/test_live_pipeline.py -q
```

To save evaluation measurements, pass `-o junit_family=legacy --junitxml=PATH`. Each CENTRAL-10 case records JSON properties containing actual samples and matched-run results.

## Agreed wire formats

| Boundary | Source of truth | Format / topic |
|---|---|---|
| Simulator to edge | `services/data_simulation/generate_data.py`, `services/edge/validation/validator.py` | `bus-telemetry-v1`; `transit/buses/<region>/<bus>/telemetry` |
| Edge to central | `shared/schema/bus_state.py`, `services/edge/transformation/central_mapper.py` | `BusStateUpdate` plus `edge-central-v1` metadata; `transit/<region>/bus/<bus>/state` |
| Sensor/edge health | `shared/schema/health.py`, `services/messaging/topics.py` | Bus and edge heartbeat models/topics |
| Commands | `services/messaging/commands.py`, `services/messaging/topics.py` | Agreed command model; `transit/commands/<region>/<bus>` |
| Passenger | `services/central/routers/passenger.py` | Public snapshot, arrivals, journeys and nearby pickups; management metadata excluded |

Keep IDs and units unchanged. Telemetry uses latitude/longitude and speed_kmh; the mapper intentionally converts them to central lat/lon/speed. Counts and capacity remain passengers/seats, separate from edge waiting-message capacity. Contract tests cover every configured bus and route assignment.

## Evaluation results and limits

Matched 240-second seeded runs start with one active Route 302 bus and the same reserve fleet, stop queues and demand. With normal traffic, scheduling increased boardings from 25 to 89 and reduced queue burden from 817,328 to 814,071 passenger-seconds. Mean occupancy changed from 99.62% to 97.84%. With delayed traffic, both runs boarded 5 passengers and had the same queue burden within this short window, despite 8 confirmed dispatches. This exposes the delay in service benefits; it does not claim every dispatch immediately reduces waiting. Queue burden is the area under the waiting count, not an invented individual waiting time. ETA checks compare issued estimates with observed arrivals at 2-second resolution.

| Quality | Evidence and practical limit |
|---|---|
| Security/privacy | Public passenger access; protected management APIs; cross-origin writes rejected; logout/expiry checks; no passenger identities. Demo `ali` / `123` is intentionally simple, with no rate limiting or durable multi-process sessions. HTTPS/production identity management are outside this demo. |
| Reliability/failure handling | Queue bounds, sequence ordering, central retry and edge recovery checked. Modeled ownership authority and replicas are in memory; whole-process loss is not durable consensus. Native tests verify transport and independent worker failure/recovery. |
| Scaling/load balancing | Both primary queues at 70% for 6 source seconds grant one assistance batch. Both must fall below 70% to rearm. New readings for a draining third-edge bus remain at the source for retry; accepted jobs finish before handback/standby. A failed primary may still use the third for takeover. This is simulated capacity, not a physical hardware benchmark or Kubernetes deployment. |
| Interoperability/concurrency | Shared versioned formats plus existing converter, Lamport, FIFO, replay and handoff tests. Nearby walking distances are straight-line estimates at 4.5 km/h, not pedestrian routing. |
| Transparency | Passenger data hides edge internals; operator health shows source, queues, faults, batches and acknowledgements. Unavailable predictions remain unavailable. |

The excluded report/presentation/contribution tickets are not restored by this test folder. Completion refers to the agreed classroom demonstration, not production deployment.

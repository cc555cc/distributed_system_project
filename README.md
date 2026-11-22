# Distributed Transit System

A public-transit simulation connecting bus telemetry, MQTT messaging, regional edge services, a central API and PostgreSQL, and passenger/operator interfaces.

## Project progress

**45/77 completed (58.4%), 15 partial (19.5%), 17 pending (22.1%).** The unfinished portion is **32/77 (41.6%)**. Percentages count tickets, not remaining effort; partial work is not counted as half-complete. Values are rounded.

| Section | Completed | Partial | Pending | Total |
|---|---:|---:|---:|---:|
| Ali — Edge | 12 | 0 | 0 | 12 |
| Zachary — Simulator | 8 | 2 | 1 | 11 |
| Julian — Messaging | 11 | 0 | 0 | 11 |
| Carson — Central | 2 | 4 | 6 | 12 |
| Danial — UI | 5 | 3 | 3 | 11 |
| Shared integration | 7 | 6 | 7 | 20 |
| **Total** | **45** | **15** | **17** | **77** |

This `ali` checkout integrates the fetched `main` implementation, including Carson's central-driven live failover and dashboard endpoints, Julian's messaging work, Danial's frontend and the edge simulation/authentication/conversion work. Your four broader scenario tests are retained in each server, lifecycle and pipeline file. The audit counts each ticket once across the pushed branches, regardless of who contributed a particular change. The agreed demonstration now covers map movement, area isolation, runtime controls, sensor/transport conditions, delivery retries and recovery. The live MQTT feed and controlled demonstration are explicitly selectable. Completion is assessed against this simulation scope; journey search, arrival predictions and operator permissions remain unfinished.

Verification: **422 tests passed, 18 opt-in service tests skipped (440 collected)** using `python -m pytest services/data_simulation/tests services/edge/tests services/central/tests services/messaging/tests -q --tb=short -ra`. Four new high-level scenario tests cover movement/runtime controls, sensor/validation conditions, transport/process/central failures, and invalid controls/area isolation. Earlier live recovery and MQTT command checks remain documented; these skipped service-dependent tests were not all rerun for the controlled demo.

The configured Durham network has 373 stops, five routes and 20 buses (four per route, two per direction). Central now polls edge health, reconciles ownership, publishes regional assignments and exposes dashboard endpoints. Stateful replication, queued-update transfer, cross-region recovery, balancing and third-instance scaling also have the agreed single-coordinator simulation evidence. These demonstrations do not establish durable distributed consensus or arbitrary network-partition safety. The default demo broker is anonymous; a separate secured broker configuration and credential/ACL tests are provided in the messaging area. Central UI login/roles remain unfinished.

Largest remaining areas: simulator fleet configuration, sensor extensibility and scheduling features; route-based ETA with traffic and stop waiting time; demand scheduling, A-to-B planning and recommendations; backend operator authorization; remaining UI features and integration; and report/demo/submission work. Pause/resume and traffic/demand/sensor commands are implemented, and the Lamport/messaging work is recorded complete. Carson's ETA still uses direct GPS distance to stops and a fallback speed, so CENTRAL-03 remains partial.

## Architecture and structure

Intended data flow: **Bus simulator → MQTT broker → Regional edge services → Central API/database → Frontend**. Scheduling commands feed back to the simulator. The complete pipeline is still under development.

```text
project/
|-- services/
|   |-- data_simulation/        # Zach: configured simulator and optional MQTT publisher
|   |   |-- export_routes.py
|   |   |-- generate_data.py
|   |   |-- run_buses.py
|   |   `-- tests/
|   |-- simulator/              # old reserved placeholder; active code is data_simulation/
|   |   |-- main.py
|   |   `-- tests/
|   |-- messaging/              # Julian: MQTT and synchronization (details: infrastructure/README.md)
|   |   |-- mqtt_client.py      # central subscriber: states, edge heartbeats, alerts; publishes commands
|   |   |-- topics.py           # agreed MQTT topic names
|   |   |-- lamport.py          # Lamport clock (MQTT-03)
|   |   |-- publisher.py        # ReliablePublisher: offline queue, ordered resend, resubscribe (MQTT-04/10)
|   |   |-- alerts.py, commands.py  # alert/command messages, bus command consumer (MQTT-05)
|   |   |-- benchmark.py, relay.py, delivery.py  # delivery/scaling measurements (MQTT-06/11)
|   |   |-- broker_security.py  # broker passwd/ACL/credential generation (MQTT-07)
|   |   |-- clock_sync.py       # physical-time rules, skewed clocks, offset estimate (MQTT-08)
|   |   |-- causality.py        # vector-clock analysis and event diagrams (MQTT-09)
|   |   |-- results/            # recorded benchmark results
|   |   `-- tests/
|   |-- edge/                   # Ali: local ingress, processing and monitoring
|   |   |-- main.py             # launch configured independent edge processes
|   |   |-- edge_server_health.py # sensor deadlines and queue measurements
|   |   |-- auth_edge_server.py # signed publishers, permissions and MQTT security settings
|   |   |-- data_to_json_converter.py # canonical JSON, vendor JSON and CSV adapters
|   |   |-- config.py           # network configuration and cache policy
|   |   |-- edge_server.py      # accepted-message queue adapter
|   |   |-- communication/      # data_to_edge.py and edge_to_central.py
|   |   |-- validation/         # validator.py and bus_registry.py
|   |   |-- cache/              # edge_state_cache.py
|   |   |-- processing/         # processor.py: route, occupancy and statistics
|   |   |-- transformation/     # central_mapper.py: EDGE-03 mapping
|   |   `-- tests/              # validation, processing and forwarding tests
|   `-- central/                # Carson: API, ingestion and scheduling
|       |-- health.py           # central edge-heartbeat receiver and health API
|       |-- main.py
|       |-- routers/
|       |   |-- bus_states.py
|       |   |-- routes.py
|       |   `-- stops.py
|       `-- tests/
|-- frontend/                   # Danial: passenger/operator UI
|-- shared/
|   |-- examples/               # example telemetry and edge network proposal
|   |-- routes.json             # active Durham route snapshot
|   `-- schema/
|       |-- bus_state.py        # shared central input and edge metadata model
|       |-- health.py           # shared heartbeat models and monotonic deadlines
|       |-- network.py          # shared network loading and assignment checks
|       |-- schema.sql          # database tables and constraints
|       `-- seed.sql            # sample routes and stops
|-- infrastructure/             # broker/deployment configuration
|   |-- README.md               # broker startup, connection settings and topics
|   |-- compose.yaml            # runs Mosquitto with Podman or Docker
|   `-- mosquitto/
|       `-- mosquitto.conf
|-- tests/                      # end-to-end integration tests
|-- docs/                       # diagrams and report evidence
|-- legacy/                     # archived local remnants; no active source
|   `-- central_server/          # old directories and generated caches
|-- .env.example                # configuration template
|-- .gitignore
|-- requirements.txt
|-- README.md
`-- SOFE4790U-Project-Proposal.docx
```

This directory guide describes the integrated checkout and reserved locations, excluding Git internals and generated caches. `legacy/central_server/` contains only old empty directories and generated caches; all active source stays in `services/`. Use `services/central/` for central work. The legacy directory is local only because its contents are empty directories or ignored caches. Git does not track empty directories, and the empty placeholders are currently untracked, so those locations may be absent in a fresh clone. Keep unfinished files empty until their tasks are implemented.

| Branch | Put implementation here |
| --- | --- |
| `zachary` | `services/data_simulation/`; coordinate `shared/routes.json` |
| `julian` | `services/messaging/`; broker configuration in `infrastructure/` |
| `ali` | `services/edge/` |
| `carson` | `services/central/`; database SQL in `shared/schema/` |
| `danial` | `frontend/` |

Put service tests in that service's `tests/` directory, end-to-end tests in root `tests/`, and report evidence in `docs/`. Coordinate shared contract changes through TEAM-02. The central subscriber and local EDGE-01/02 pipeline are implemented. Regional failure, balancing and scaling scenarios are verified in the UI-controlled simulation; live MQTT workers also receive central-published ownership assignments.


## Backend implementation currently on `main`

EDGE-01 receiving/validation and EDGE-02 processing are connected to EDGE-03 mapping and automatic forwarding. The current merge verification passed **417 tests with 18 integration skips** across simulator, edge, central and messaging tests. Historical service-enabled runs are recorded separately below. EDGE-03 is verified against the current shared model and central ingestion code. All changes, including compatible central receiver support, were developed on `ali` and are now merged into `main` through `carson`.

### Follow the data

`main.py` is now the instance launcher. It creates each configured worker in a separate process, with independent subscriber, queue, processor/cache, sender and monitor objects. It is not an edge HTTP server. `communication/data_to_edge.py` still handles incoming data and the processing loop; its older single-instance command delegates to the launcher for compatibility.

1. `communication/data_to_edge.py` decodes incoming MQTT telemetry, validates it and queues accepted messages.
2. `edge_server.py` passes the queued reading to `processing/processor.py`. That file keeps the small route, occupancy, statistics and snapshot-building helpers together. `cache/edge_state_cache.py` stores bus states and remembered sequences, scoped by city/region/bus. Cache updates remain locked.
3. `transformation/central_mapper.py` maps only the newly accepted bus update to the central contract. It does not resend every bus in a snapshot or invent missing status/clock values.
4. `communication/edge_to_central.py` rechecks ownership, metadata and the shared model, then sends to `transit/<region_id>/bus/<bus_id>/state`.
5. Central's MQTT callback validates that same model, checks region/route/stop relationships and writes newer state to PostgreSQL. `GET /bus_states/{bus_id}` returns the stored core fields and edge metadata.

`shared/schema/bus_state.py` is the single model imported by both edge and central. Existing core fields remain available; optional `edge_metadata` preserves city, region, edge, route, passenger count, capacity, source/processing timestamps, current stop, occupancy percentage, crowding, freshness, route progress and assignment-scoped statistics. Central stores it as JSONB instead of silently discarding extra information.

EDGE-02 uses two main files: `cache/edge_state_cache.py` for state storage, and `processing/processor.py` for checks and calculations. Small helpers remain functions in the processor; separate modules can be added if a responsibility grows enough to need one. Communication and EDGE-03 mapping remain separate.

### Field mapping

| Incoming/processed information | Central field | Meaning |
| --- | --- | --- |
| `latitude`, `longitude` | `lat`, `lon` | Same coordinates; no conversion |
| `speed_kmh` | `speed` | km/h |
| `bus_status` | `bus_status` | Supplied status; not inferred from speed |
| `lamport_clock` | `lamport` | Supplied logical-clock value; not fabricated |
| `sequence_number`, `direction_id`, `next_stop_id` | Same names | Preserve accepted sequence and route direction/stop |
| `passengers` or exact `occupancy_percent` | `occupancy` | Explicit `CENTRAL_OCCUPANCY_UNIT` setting |
| City/region/edge/route, counts, timestamps and derived values | `edge_metadata` | Typed context preserved in storage/API |
| Local statistics | `edge_metadata.statistics` | `edge_assigned_buses`; not a complete regional total |

Confirm occupancy units and bus-status meanings with Carson before team deployment. Choose `passengers` or `percent` explicitly; fractional percentages cannot fit central's integer field and are rejected rather than silently rounded. New forwarding inputs need `bus_status` and `lamport_clock`. Missing values are not replaced with defaults. Faulty/offline sensor readings are not forwarded as healthy occupancy updates.

### Run and configure

Use Python 3.11 or newer and install the repository dependencies plus pytest. The existing pinned versions are retained; `paho-mqtt` is now listed in `requirements.txt`.

```powershell
python -m pip install -r requirements.txt
python -m pip install pytest
python -m pytest services/data_simulation/tests services/edge/tests services/central/tests -q
$env:EDGE_NETWORK_CONFIG = "shared/routes.json"
$env:CITY_ID = "DURHAM"
$env:REGION_ID = "west"
$env:EDGE_ID = "edge-west-1"
$env:CENTRAL_OCCUPANCY_UNIT = "passengers"
python -m services.edge.communication.data_to_edge
```

The occupancy setting above is an explicit example, not evidence of Carson's approval. Start the broker and updated central receiver before the edge service. `MQTT_HOST`/`MQTT_PORT` default to `127.0.0.1`/`1883`; `CENTRAL_MQTT_HOST`/`CENTRAL_MQTT_PORT` can select a separate outbound broker. Cache settings remain `EDGE_MAX_BUSES` (1000), `EDGE_FRESHNESS_SECONDS` (30), `EDGE_EXPIRE_SECONDS` (300), `EDGE_MEDIUM_THRESHOLD` (50), and `EDGE_HIGH_THRESHOLD` (80).

For an existing development database, add the optional metadata column before running this branch's central receiver:

```sql
ALTER TABLE bus_states ADD COLUMN IF NOT EXISTS edge_metadata JSONB;
```

The fresh-development `schema.sql` already includes this column. Do not rerun its table-dropping setup just to add a column. No live database migration has been executed by this change.

### Carson's map with the standardized simulator and edge pipeline

The active `shared/routes.json` now mirrors Carson's Durham SQL seed: **373 stops, five routes, west/east regions and twenty registered buses**. Routes 302 and 900 belong to west; routes 407, 409 and 901 belong to east. Stop IDs, coordinates, names, bus assignments and both directional stop patterns match his seed. The fictional two-city/five-stop map is no longer active.

| Region | Routes | Edge IDs |
| --- | --- | --- |
| west | 302, 900 | `edge-west-1`, `edge-west-2` |
| east | 407, 409, 901 | `edge-east-1`, `edge-east-2` |

`city_id: "DURHAM"` labels this dataset; west/east are processing regions, not invented cities. This data does not currently meet a separate two-city requirement. The model remains configurable for additional cities, but no synthetic intercity link is added to Carson's map.

The shared network loader supports explicit `directions` stop lists. A return trip is not assumed to be the forward trip reversed: route 302, for example, has 50 stops in direction 0 and 53 in direction 1. The original five-stop restriction was removed. Simulator interpolation, validation and edge route-progress checks all use the same directional patterns. Each route has four registered buses, two per travel direction, initially divided between the region's two edge IDs. Central-driven assignment messages now update live ownership; replicated-state and atomic queued-update handoff remain covered by the stateful simulation.

Carson's new `GET /routes/{route_id}/stops?direction=0` or `direction=1` endpoint is merged into the routes router. The duplicate route-stop endpoint was removed from the stops router. Omitting direction returns both ordered patterns; an invalid direction receives HTTP 422. Existing dictionary-shaped stop responses are retained.

Zach's standardized simulator remains in `services/data_simulation/`. His movement calculation, separate seeded random sources, configurable traffic and sensor-drop behaviour are kept. The common local network allows the simulator and edge to start without querying central for every reading. Their messages still use the shared BUS-01 field format and the existing central mapper/metadata model. Your communication-file comment edits were preserved unchanged during this merge.

```powershell
# Print standard JSON from all twenty configured Durham buses.
python -m services.data_simulation.run_buses --updates 100 --interval 1
# Publish the identical messages to Julian's MQTT broker.
python -m services.data_simulation.run_buses --mqtt --host 127.0.0.1 --port 1883
# Run one of Carson's registered buses for a short check.
python -m services.data_simulation.run_buses --bus bus-302-1 --updates 3 --interval 0
```

Start matching west/east edge workers and the updated central receiver before live publishing. `--interval 0` skips real waiting for tests; movement still uses the normal simulation step. Each process keeps independent counters/random state and emits finite streams bounded by its update limit and route end. Session restart ordering and continuous route repetition remain outside this change.

Messages retain `schema_version: "bus-telemetry-v1"`, registered bus/city/region/route IDs, latitude/longitude, speed in km/h, passenger count and capacity, direction and stop IDs, UTC timestamp, sequence number, logical-clock value, bus status and sensor status. Commands from central are applied by each bus process with the same Lamport clock as its telemetry (MQTT-05). A failed sensor reports to stderr and skips its reading rather than publishing invented data.

`shared/schema/seed.sql` is restored byte-for-byte from the merged Carson revision. `shared/routes.json` is its startup snapshot for the simulator/edge. The seed-consistency test compares all region, route, stop, route-stop and bus records against `export_routes.seed_sql(load_network())`, so formatting/comments can differ without data drifting. If Carson changes the seed, refresh the snapshot and rerun this check. No live database was reseeded or migrated; the previous synthetic data remains recoverable in the local pre-merge checkpoint.

### Delivery and freshness

Only accepted new updates are staged. Stale, duplicate and conflicting readings do not generate new sends. A broker failure leaves the latest pending payload per owned bus for retry; a newer accepted update replaces that slot. Expired pending updates are dropped rather than advertised as current. This queue and the cache are in memory, so restarts do not preserve unsent updates or sequence history. A broker receipt is not a database-write acknowledgement.

Freshness uses a monotonic clock. Cache entries expire only under the configured safe-capacity policy; sequence floors remain remembered during the process lifetime. Route progress uses reported stops, not GPS proximity. Stale/offline buses stay out of active totals, and unknown occupancy stays out of averages. No replication, failover, automatic workload migration, instance scaling, ETA, journey planning or scheduling was added. Queue thresholds only produce recommendations.

### Heartbeats, instances and workload thresholds

Carson's branch was merged first at `c29865b`, preserving his demo stack, ingestion fixes and terminal endpoint. The heartbeat additions are based on that version. His ETA draft is not changed or presented as a working ETA endpoint.

`python -m services.edge.main` creates all four configured edge processes. Use `--edge edge-west-1 --edge edge-west-2` to select workers. `--single` uses `EDGE_ID` and checks that `CITY_ID`/`REGION_ID` match the shared map; the compose image uses this mode so each container creates only its own instance. Static assignments are read from configuration. The launcher logs worker exits and stops children on shutdown; it does not claim automatic takeover or recovery synchronization.

A bus heartbeat has separate `gps`, `speed`, `stop_status` and `occupancy` states. These are four logical sensor channels inside the current simulator, not four independent physical devices. Heartbeats run on a separate publisher timer; simulated sensor faults still report `faulty` even when no telemetry reading is emitted. An omitted channel stops refreshing that sensor's receipt deadline. Only its assigned edge processes sensor heartbeats and queues bus telemetry, so a peer's traffic does not falsely inflate this worker's queue.

The edge forwards sensor-health summaries inside its own periodic heartbeat. Central computes edge timeout/recovery independently and exposes `GET /health/edges`. It returns `unknown`, `healthy`, `unavailable` or `recovering`, the latest workload/sensor report, and scoped load recommendations. A live heartbeat does not prove valid sensor data: `reported_state` can be `faulty`, and accepted-telemetry age is tracked separately. Old/duplicate heartbeat sequences and retired sessions do not refresh liveness. Timeouts use local monotonic receipt clocks; UTC source timestamps are for reporting.

| Setting | Default | Purpose |
| --- | --- | --- |
| `HEARTBEAT_INTERVAL_SECONDS` | 2 | Bus/edge heartbeat period |
| `SENSOR_HEARTBEAT_TIMEOUT_SECONDS` | 10 | Missing sensor deadline |
| `EDGE_HEARTBEAT_TIMEOUT_SECONDS` | 10 | Central's missing-edge deadline |
| `HEARTBEAT_RECOVERY_SECONDS` | 4 | Repeated-pulse recovery window |
| `EDGE_QUEUE_SIZE` | 50 | Maximum queued telemetry messages per instance (1-50) |
| `EDGE_OVERLOAD_PERCENT` | 80 | Queue utilization threshold |
| `EDGE_OVERLOAD_SUSTAIN_SECONDS` | 6 | Sustained high-load window before recommendation |

Queue utilization is `queue_size / queue_capacity * 100`, entirely separate from passenger occupancy. The 50/70/75/79/80 bands are labelled notice/prepare/ready/near-capacity/overloaded in health reports. Workload also includes incoming-message rate since startup, average processing latency, active/assigned bus counts and pending outgoing updates. These are local queue measurements, not a measurement of CPU capacity.

After sustained overload, central uses least-queue-utilization selection among healthy peers in the same city/region. It recommends considering migration to the least-loaded peer, or additional capacity when none has room. Every recommendation explicitly says `migration_performed: false`. This live-monitor recommendation endpoint does not perform transfers. The separate simulation coordinator described below synchronizes bus state, transfers ownership and models third-instance activation and scale-down.

An expired heartbeat means unavailable communication/telemetry, not a confirmed crash or proof that the bus stopped. Central marks sensor reports uncertain when their reporting edge is unavailable. Manual GPS entry and incident prediction are not implemented. Health state and recovery session history are in memory; process restarts reset them. Authentication, broker failure guarantees and real multi-process network verification remain outside the offline test results.

Heartbeat monitoring covers EDGE-04 and workload metrics cover EDGE-07, with a health endpoint contribution to CENTRAL-05. The simulation coordinator covers the EDGE-05/06/08/09/10/11/12 scenarios described below. The two new test files use injected clocks and include source-heartbeat → edge → central/API checks. No artificial sleeping or live services are required for these tests; the live MQTT/PostgreSQL tests remain opt-in.

### Test ownership

Object health/queue behaviour is tested in `services/edge/tests/test_edge_server_testing.py`, instance creation/recovery/scaling in `services/edge/tests/test_edge_lifecycle.py`, and telemetry flow/state handoffs in `services/edge/tests/test_edge_pipeline.py`. Each has four broader scenario tests, retaining the existing checks and the new upstream assertions. Additional live-failover, messaging, security and converter tests remain separate. Central SQL/API and live MQTT/PostgreSQL integration tests remain in the central test directory.

Your EDGE-03 tests are in `services/edge/tests/test_edge_to_central.py`: mapping, the shared validation model, outgoing ownership/field checks, the MQTT topic and sender connection/failure handling. They import the shared model directly, without importing central services or needing a database URL. EDGE-01 and EDGE-02 keep their existing test files. Run `python -m pytest services/edge/tests -q` for the edge validation, processing, forwarding and heartbeat checks.

Central/database/API integration coverage is kept under Carson's area: `services/central/tests/test_edge_ingestion_pipeline.py` and `services/central/tests/test_live_edge_ingestion.py`, alongside his ingestion regression tests. Simulator/configuration tests are in `services/data_simulation/tests/test_simulation.py` (23 tests). Run `python -m pytest services/central/tests -q` for central ingestion/health checks and the optional live tests. These tests are preserved for central integration, not counted as completion of Carson's tickets.

Production roles remain separate: `central_mapper.py` maps fields, `edge_to_central.py` sends, and `shared/schema/bus_state.py` is the common model. Carson's existing MQTT callback and ingestion functions receive the messages; the edge does not duplicate those central operations.

### End-to-end verification

Offline tests now include real messages generated by Zach's simulator for every configured Durham bus, both real route directions and Carson's route-stop API. They execute the full code path: input decoding/validation, queue, processing, mapping, outgoing validation, central MQTT callback, real central SQL statements via a SQLite adapter, and API readback. They compare coordinates, speed, counts, clocks, timestamps, city/region/route context, crowding and summaries. They also check invalid types/identifiers, stale and duplicate messages, conflicts, broker-failure retries and the actual runtime forwarding loop. SQLite and the in-memory transport do not substitute for a live PostgreSQL/MQTT run.

Two opt-in tests are provided for dedicated test services:

```powershell
$env:EDGE_LIVE_DATABASE_URL = "postgresql://USER:PASSWORD@localhost:5432/TEST_DATABASE"
$env:EDGE_LIVE_MQTT_HOST = "127.0.0.1"
$env:EDGE_LIVE_MQTT_PORT = "1883"
python -m pytest services/central/tests/test_live_edge_ingestion.py -q
```

Both live tests use unique PostgreSQL schemas and remove only their test data. They require permission to create/drop schemas and are skipped when `EDGE_LIVE_DATABASE_URL` is absent. Use a dedicated broker: the four-instance test uses the configured edge IDs and bus topics and must not share an active project broker. The tests passed against isolated Mosquitto 2 and PostgreSQL 17 containers, which were removed afterward. The second test launches four actual child processes through `main.run_instance`, exercises all ten configured buses and verifies coordinates, speed, occupancy, stop/direction/status, clocks, timestamps, city/route/region and edge owner in database/API readback. It checks each process's workload metrics and 50-message capacity, passes GPS fault/restoration through MQTT to the health API, stops one process, observes timeout while its peer remains healthy, and restarts it through recovering to healthy. The complete command `python -m pytest services/data_simulation/tests services/edge/tests services/central/tests -q --tb=short -ra` passed **292 tests, no skips** on the earlier snapshot with those dedicated services and the isolated authenticated-broker settings configured; current merge verification is 417 passed and 18 skipped.

All **12 edge tickets are complete for the agreed simulation demonstration**: EDGE-01/02/03/04/07 have the existing pipeline/live-service evidence, and EDGE-05/06/08/09/10/11/12 have tested stateful simulation scenarios and fleet measurements. The simulation uses the real validator, route/occupancy processor, cache/sequence logic and central mapping contract. Its reliable single coordinator models atomic ownership changes, replication and a retained pending-request journal. It does not add distributed consensus, disk durability, network partitions or a live interprocess replication protocol to the separate MQTT workers. The full-code-path central integration test also sends post-handoff payloads through central ingestion SQL and API readback using its SQLite adapter. This verification does not claim a separate human contract approval from Carson.

## Branch workflow

Everyone uses their first-name branch listed above. All five branches start from the same updated `main`; completed tasks merge back into `main`.

1. Pull the latest `main` before starting a task, then switch to your own branch and merge the latest `main` into it.
2. Implement and test the ticket on your branch. Keep unfinished work on that branch.
3. When the task is complete, fetch and merge the latest `origin/main` into your branch. Resolve any conflicts and rerun the relevant checks.
4. Push your branch and open a pull request into `main`. Merge only the completed task when checks pass and there are no unresolved conflicts.
5. After the task is merged, everyone can pull the updated `main` and merge it into their own branch before continuing.

## Priorities

Priorities indicate implementation order, not completion status:

- **P0:** Foundational setup, contracts, core data flow and required integration/submission work.
- **P1:** Main features, reliability, security, testing and documentation.
- **P2:** Deeper evaluation, reproducible evidence and demonstration polish.

## Individual task lists

Existing central startup, ingestion and route lookup cover parts of CENTRAL-01–03; their remaining requirements are not complete.

Status: ✅ Done · 🟨 Partial · ⬜ Pending. Mark a task Done only after its implementation and relevant checks are complete; commit and push the README update. Pending means completion has not been verified.

<details>
<summary><strong>Zachary — Bus Simulation</strong> — 11 tickets</summary>

Generate simulated bus movement, sensor telemetry and configurable scenarios using the agreed shared format.

| Ticket | Priority | Description and completion criteria | Status |
| --- | --- | --- | --- |
| `BUS-01` | **P0** | Create a standard JSON format for bus telemetry. Include bus ID, region ID, route ID, GPS coordinates, speed, passenger count, capacity, current/next stop, timestamp, sequence number and sensor status. Agree on logical-clock metadata with Julian.<br><br>**Done when:** Example messages contain the required fields, use correct data types, and pass validation. Julian and Ali can use the format without modifying field names. | 🟩 Done (`ali`; shared telemetry contract verified) |
| `BUS-02` | **P0** | Define at least two simulated regions with multiple routes, stops, GPS coordinates and bus assignments. Each route must contain stops in the correct travel order. Use the agreed west/east city areas, with explicit region identifiers under the existing Durham map.<br><br>**Done when:** Every bus belongs to a valid region and route. Stops have unique IDs and coordinates. The simulator can load route data without hardcoded routes in its logic. The agreed west/east city areas load valid routes, stops and buses without mixing assignments; the shared configuration retains the DURHAM city identifier. | 🟩 Done (agreed west/east map scope; topology and ownership isolation verified) |
| `BUS-03` | **P0** | Build a Python simulator that generates continuous bus movement and sensor data. Each bus should update its location, speed, stop status and passenger count at configurable intervals.<br><br>**Done when:** At least three buses move between stops and continuously generate valid telemetry. Buses can run simultaneously without interfering with one another. | 🟩 Done (`ali`; multi-bus generation and MQTT ingestion verified) |
| `BUS-04` | **P1** | Simulate normal traffic, traffic congestion, peak/off-peak passenger demand and sensor failures. Traffic should affect bus speed and journey duration, while demand affects passenger occupancy.<br><br>**Done when:** Each scenario creates measurable changes. For example, a traffic delay increases travel time, while peak demand increases passenger counts without exceeding the simulated capacity rules. | 🟩 Done (traffic/demand/fault scenarios produce tested telemetry changes) |
| `BUS-05` | **P1** | Add controls to start, pause, resume and stop simulations. Allow supported commands to change traffic, demand or sensor conditions while the simulator is running.<br><br>**Done when:** Valid commands change the requested scenario. Invalid commands or nonexistent bus/route IDs are rejected without crashing the simulator. | 🟩 Done (controlled demo start/stop, clock pause/resume and condition controls verified; live publishers still launch from CLI) |
| `BUS-06` | **P1** | Test the simulator's GPS movement, route assignments, passenger counts, sensor readings and simultaneous bus operation. Include normal and invalid-data scenarios.<br><br>**Done when:** Tests confirm valid movement and sensor values. Multiple buses operate correctly, and errors are reported without unexpectedly stopping the entire simulation. | 🟩 Done (20-bus movement, route/field consistency, sensor and invalid-reading scenarios tested) |
| `BUS-07` | **P1** | Add bus states such as travelling, arriving, stopped, boarding, departing and out of service. Simulate waiting at bus stops before continuing along the route.<br><br>**Done when:** Buses transition through valid states in the correct order. Stop durations affect simulated journey time, and out-of-service buses stop accepting passengers. | 🟩 Done (controlled demo arrival, stop, boarding, departure and out-of-service states tested) |
| `BUS-08` | **P1** | Allow the simulator to run different fleet sizes, such as 5, 10, 25 and 50 buses, using configuration instead of manually creating each bus.<br><br>**Done when:** Each fleet size starts successfully, uses valid bus IDs and route assignments, and produces the expected number of active bus streams. | Partial (fleet experiments exist; simulator fleet-control acceptance remains) |
| `BUS-09` | **P1** | Allow Carson's scheduler to request changes in bus service. Support adding an available simulated bus to a route or adjusting the time between departures.<br><br>**Done when:** A valid scheduling command changes the simulated service. The changes appear in subsequent telemetry and can be observed by Carson's central server. | Pending |
| `BUS-10` | **P2** | Create repeatable test scenarios for normal traffic, peak demand, traffic congestion and sensor failures. Use fixed simulation settings so results can be compared.<br><br>**Done when:** Running the same scenario with the same configuration produces consistent, comparable results. Expected behaviours are documented and checked. | 🟩 Done (seeded repeatable scenarios and checks) |
| `BUS-11` | **P1** | Support adding new buses and optional sensor types through versioned configuration objects. Avoid rewriting the main simulator whenever a supported sensor is added.<br><br>**Done when:** A new registered bus or supported sensor can be added through configuration. Existing buses and sensor messages continue working correctly. | Partial (optional sensor extension remains) |

</details>

<details>
<summary><strong>Julian — MQTT & Synchronization</strong> — 11 tickets</summary>

Transport data between services, maintain synchronization information and ensure messaging remains reliable as the number of buses increases.

| Ticket | Priority | Description and completion criteria | Status |
| --- | --- | --- | --- |
| `MQTT-01` | **P0** | Set up a Mosquitto MQTT broker and define standard topics for bus telemetry, regional processing, alerts and simulation commands. Identify which services publish and subscribe to each topic.<br><br>**Done when:** Independent programs can connect to the broker and exchange messages using the agreed topics. Topic naming and connection settings are documented. | 🟩 Done (`main` infrastructure; broker/topic exchange verified) |
| `MQTT-02` | **P0** | Connect Zachary's bus simulator to the MQTT broker. Publish each bus's telemetry to its assigned region and bus topic.<br><br>**Done when:** At least three buses publish messages successfully, and Ali's regional subscriber receives the correct telemetry. | 🟩 Done (`ali`; simulator telemetry reaches regional ingestion) |
| `MQTT-03` | **P0** | Implement Lamport logical clocks and agree on per-bus sequence numbering. Include UTC timestamps to record when sensor events were generated.<br><br>**Done when:** Logical clocks follow the correct send/receive update rules. Per-bus sequence numbers increase, and every published message includes the agreed ordering metadata. | 🟩 Done (`julian`; send/receive rules across bus, edge and central verified) |
| `MQTT-04` | **P0** | Handle MQTT connection loss, reconnection, delayed delivery and duplicate messages. Preserve sequence and clock information so Ali can identify stale telemetry.<br><br>**Done when:** Clients reconnect after a temporary disconnection. Duplicate or delayed messages can be identified, and tests confirm they do not cause Ali's accepted bus state to move backward. | 🟩 Done (`julian`; live reconnect, delayed/duplicate delivery and ordered resend verified) |
| `MQTT-05` | **P1** | Implement MQTT communication for system alerts and simulation commands. Ensure alerts and commands reach the intended subscribing services.<br><br>**Done when:** A failure alert reaches the central service, and a simulation command reaches its intended recipient. Unrelated services do not act on the command. | 🟩 Done (`julian`; live alert to central and targeted commands verified) |
| `MQTT-06` | **P1** | Test message delivery and communication performance. Measure messages sent, messages received, delivery delays and duplicate deliveries.<br><br>**Done when:** Tests produce recorded results showing the communication system's behaviour under normal conditions and temporary delays. | 🟩 Done (`julian`; normal/delayed/outage delivery results recorded) |
| `MQTT-07` | **P1** | Add MQTT authentication and topic-based authorization. Only registered services should connect and publish or subscribe to their approved topics.<br><br>**Done when:** Invalid credentials are rejected. A bus publisher cannot publish to another bus's protected topic. Authorized services continue communicating normally. | 🟩 Done (`julian`; secured broker: invalid logins and cross-topic publishes rejected live) |
| `MQTT-08` | **P1** | Define how physical timestamps are handled using UTC and clock synchronization assumptions. Simulate different clock offsets and compare physical timestamps against Lamport ordering.<br><br>**Done when:** Tests show how clock differences can affect timestamps and demonstrate correct logical-clock updates despite those differences. | 🟩 Done (`julian`; clock-offset tests and UTC/synchronisation rules) |
| `MQTT-09` | **P2** | Test logical-clock behaviour when multiple buses and services generate concurrent events. Include delayed and out-of-order delivery scenarios.<br><br>**Done when:** Event diagrams and tests show correct Lamport values, identify causal relationships and distinguish genuinely concurrent events. | 🟩 Done (`julian`; event diagram, causal and concurrent events tested) |
| `MQTT-10` | **P1** | Test what happens when the MQTT broker temporarily becomes unavailable. Implement client reconnection and resubscription where necessary.<br><br>**Done when:** After the broker restarts, clients reconnect and resume communication. Any lost or unrecovered messages are identified rather than assumed delivered. | 🟩 Done (`julian`; live broker restart, resubscription and lost-message reporting) |
| `MQTT-11` | **P1** | Test MQTT communication with increasing numbers of buses, such as 5, 10, 25 and 50. Monitor message throughput, queue delays, duplicates and ordering behaviour.<br><br>**Done when:** Results compare multiple bus counts. The system preserves the agreed ordering rules, and any performance limitations are measured and explained. | 🟩 Done (`julian`; 5/10/25/50-bus results, limits measured and explained) |

</details>

<details>
<summary><strong>Ali — Regional Edge Computing</strong> — 12 tickets</summary>

Receive valid telemetry, process regional bus information, share workloads between edge servers and recover when servers fail.

Each region initially has two active edge servers that divide processing responsibilities.

| Ticket | Priority | Description and completion criteria | Status |
| --- | --- | --- | --- |
| `EDGE-01` | **P0** | Build regional MQTT subscribers that receive bus telemetry. Validate JSON structure, GPS, speed, capacity, timestamps, registered bus IDs and region assignments. Prevent invalid messages from entering the edge processing system. Validate that each bus belongs to the declared city and region.<br><br>**Done when:** Valid messages enter the processing queue. Invalid messages are rejected and logged safely. Tests 01–12 pass, including verification that invalid data does not reach edge processing. A bus claiming the wrong city or region is rejected before processing. | 🟩 Done (`ali`; pipeline/health verification) |
| `EDGE-02` | **P0** | Create a regional cache storing the latest valid state of each bus. Track location, speed, current route, next stop and occupancy. Calculate occupancy percentage and crowding level. Check sequence numbers before updating cached information. Keep bus caches and regional calculations separated by city and region.<br><br>**Done when:** Each bus has an accurate latest state. Duplicate or older messages do not overwrite newer data. Occupancy calculations and stop information are correct. Expired information is marked stale. Updates and calculations for one city/region do not overwrite or mix with another. | 🟩 Done (`ali`; pipeline/health verification) |
| `EDGE-03` | **P0** | Agree the outgoing field mapping with Carson, including field names, data types, units, required/optional values and meanings. Map accepted edge updates to that contract, validate them again in `communication/edge_to_central.py`, then connect automatic MQTT forwarding. Cover per-bus positions, speeds, occupancy, stop information, timestamps, sequence/Lamport metadata, city/region identifiers and scoped summaries through agreed topics or envelopes. Do not silently drop information Carson needs or invent unavailable values.<br><br>**Done when:** The mapping is verified against Carson's expected input. Live end-to-end tests trace multiple buses from telemetry input through edge validation/processing and MQTT to Carson's database and API. Received field names, types, units, identifiers and values match the agreed contract, including city/region separation and every agreed information category. Missing, invalid, duplicate, stale and conflicting updates are handled as agreed without corrupting current state. Rejected edge updates are not forwarded as accepted data. Tests verify database/API results, not just broker receipts, and record reproducible evidence. | 🟩 Done (`ali`; pipeline/health verification) |
| `EDGE-04` | **P1** | Implement heartbeat monitoring for all regional edge servers. Detect when one server stops responding and report whether it is healthy, unavailable or recovering.<br><br>**Done when:** Stopping an edge server triggers failure detection within a configured timeout. The failure status is reported to the monitoring system. | 🟩 Done (`ali`; pipeline/health verification) |
| `EDGE-05` | **P1** | Run two active edge servers per region. Divide bus workloads between them and replicate the latest required state. If one server fails, transfer its workload to the surviving server.<br><br>**Done when:** Both servers process assigned buses during normal operation. After one fails, the survivor processes the region's buses using synchronized state without conflicting updates. | 🟩 Done (`ali`; simulation scope) |
| `EDGE-06` | **P1** | Test regional processing, validation, caching, duplicate messages, server failures, backup takeover and recovery. Include normal and failure scenarios.<br><br>**Done when:** Tests confirm that valid information is processed correctly, invalid information is rejected, and edge processing continues after a supported single-server failure. | 🟩 Done (`ali`; simulation scope) |
| `EDGE-07` | **P1** | Monitor each edge server's active bus count, assigned workloads, incoming message rate, processing latency and queue backlog. Use these measurements to identify overloaded servers.<br><br>**Done when:** Each server reports measurable workload information. The system can distinguish a lightly loaded server from an overloaded one. | 🟩 Done (`ali`; pipeline/health verification) |
| `EDGE-08` | **P1** | Implement workload distribution between the two active servers. Aim for approximately 50/50 bus assignments and move workloads when a server becomes overloaded or unavailable.<br><br>**Done when:** Bus assignments are balanced under normal conditions. Reassignment works without losing the latest required state or allowing two authoritative owners for one bus. | 🟩 Done (`ali`; simulation scope) |
| `EDGE-09` | **P1** | Implement safe ownership transfer and recovery. Synchronize returning servers before restoring their workloads. Support a controlled transfer to another healthy region if both local servers fail.<br><br>**Done when:** A recovered server receives current state before resuming work. Tests demonstrate that ownership remains consistent and cross-region takeover works when capacity is available. | 🟩 Done (`ali`; simulation scope) |
| `EDGE-10` | **P2** | Measure performance with 5, 10, 25 and 50 simulated buses. Compare processing latency, throughput, workload distribution and failover duration.<br><br>**Done when:** Results demonstrate how performance changes with increasing bus counts and different failure conditions. | 🟩 Done (`ali`; simulation scope) |
| `EDGE-11` | **P2** | Add optional automatic scaling. When both regional servers remain overloaded, start a third edge server, synchronize it and redistribute bus assignments. Support safe scale-down when demand decreases.<br><br>**Done when:** A sustained overload triggers Edge C when resources permit. Edge C receives valid assignments and processes them correctly. Scale-down preserves state and ownership. | 🟩 Done (`ali`; simulation scope) |
| `EDGE-12` | **P1** | Make bus ownership configurable so newly added edge servers and healthy servers in other regions can take over workloads without changing the processing logic.<br><br>**Done when:** A new edge server can register, receive an approved assignment and begin processing. Ownership changes prevent duplicate authoritative processing. | 🟩 Done (`ali`; simulation scope) |

</details>

<details>
<summary><strong>Carson — Central Server & Smart Scheduling</strong> — 12 tickets</summary>

Receive processed regional data, calculate arrival times, plan A-to-B journeys, recommend suitable buses and adjust transit schedules based on demand.

| Ticket | Priority | Description and completion criteria | Status |
| --- | --- | --- | --- |
| `CENTRAL-01` | **P0** | Build a FastAPI central server and database for buses, routes, stops, regions and current bus information. Load the agreed route definitions and provide basic API endpoints.<br><br>**Done when:** The server starts correctly, stores valid information and returns expected bus, route and stop records through its APIs. | 🟩 Done (database and bus/route/stop API integration verified) |
| `CENTRAL-02` | **P0** | Receive Ali's processed regional telemetry and update the central database. Maintain the latest accepted state for each bus and prevent outdated messages from replacing newer information.<br><br>**Done when:** Valid regional updates appear in the database and APIs. Duplicate or outdated messages do not corrupt current bus information. | 🟩 Done (newer-state ingestion and duplicate/stale rejection verified) |
| `CENTRAL-03` | **P0** | Calculate estimated bus arrival times using remaining route distance, current speed, simulated traffic delays and stop waiting times. Estimate travel durations along the route rather than using only straight-line distance.<br><br>**Done when:** Active buses return reasonable arrival estimates. Traffic delays affect ETA results, and missing information produces an unavailable estimate rather than an invented value. | Partial (route-distance, traffic and stop-wait ETA remain) |
| `CENTRAL-04` | **P1** | Analyze passenger occupancy and demand across routes. Identify overcrowded or underused routes and recommend increasing or decreasing bus frequency using agreed thresholds.<br><br>**Done when:** Peak and off-peak scenarios produce explainable scheduling recommendations. A single unusual reading does not unnecessarily trigger repeated changes. | Pending |
| `CENTRAL-05` | **P1** | Create APIs for system health, alerts, current routes, scheduling recommendations and simulation commands. Provide the information Danial needs for passenger and operator interfaces.<br><br>**Done when:** Valid API requests return the expected information. Operator commands are accepted only through the appropriate backend endpoints. | Partial (dashboard/health/alerts/commands exist; full API set and operator restrictions remain) |
| `CENTRAL-06` | **P1** | Test database updates, bus information, ETA calculations, scheduling recommendations, API responses and invalid requests. Include cases with missing or stale regional data.<br><br>**Done when:** Tests confirm correct API responses, current-state consistency and appropriate error handling. | Partial (ETA/scheduling/full API coverage remains) |
| `CENTRAL-07` | **P1** | Build an A-to-B journey planner. Given a starting stop and destination, find suitable bus routes, available buses and supported transfers. Calculate estimated waiting, riding and total journey times. Find valid journeys using the selected city's routes and stops.<br><br>**Done when:** Valid A-to-B searches return usable journey options. Invalid or unreachable stops return clear results. Transfer and travel times are calculated without double-counting delays. Journey results use the selected city; stops from another city are not combined into unsupported trips. | Pending |
| `CENTRAL-08` | **P1** | Connect scheduling decisions to Zachary's simulator. Allow an approved scheduling decision to request more buses or change departure frequency.<br><br>**Done when:** A scheduling command changes the simulated dispatch plan. The central server receives updated information confirming the change. | Pending |
| `CENTRAL-09` | **P1** | Add role-based access and privacy safeguards. Passengers may view public transit information, while authorized operators can access management features and send simulation commands.<br><br>**Done when:** Unauthorized users cannot execute operator-only actions. Access checks occur in the backend, and unnecessary personal information is not stored. | Pending |
| `CENTRAL-10` | **P2** | Compare predicted bus arrivals against simulated arrival times. Evaluate how scheduling recommendations affect passenger waiting times and route overcrowding.<br><br>**Done when:** Recorded results show ETA errors and demand-response outcomes under normal and delayed scenarios. | Pending |
| `CENTRAL-11` | **P2** | Separate backend functionality into clear modules for data ingestion, database operations, route management, scheduling, recommendations and APIs. Avoid tightly coupling unrelated functions.<br><br>**Done when:** Modules can be tested independently, existing APIs continue working, and responsibilities are clearly separated. | Partial (remaining backend responsibilities and independent module checks remain) |
| `CENTRAL-12` | **P1** | Build suggestions.py to rank suitable A-to-B journeys. Compare waiting time, traffic-adjusted riding time, occupancy and transfers. Explain why one bus or route is recommended over alternatives.<br><br>**Done when:** A valid journey search returns a recommended option and alternatives. The result includes estimated journey time, crowding information and a clear reason for the recommendation. | Pending |

CENTRAL-01/02 are verified by the backend integration evidence. CENTRAL-03 remains partial: the current `carson` ETA endpoint still needs route-distance accumulation, traffic-delay and stop-wait handling.

</details>

<details>
<summary><strong>Danial — Passenger App & Operator Dashboard</strong> — 11 tickets</summary>

Build one web application containing three interfaces: Passenger Journey Planner, Operator Dashboard and Simulation Control Panel.

| Ticket | Priority | Description and completion criteria | Status |
| --- | --- | --- | --- |
| `UI-01` | **P0** | Build the main web application with navigation between Passenger Planner, Operator Dashboard and Simulation Controls. Use consistent layouts and make the pages usable on desktop and mobile.<br><br>**Done when:** All three pages open and navigation works. Passenger and operator views are clearly separated. | 🟩 Done (three-page layout/navigation implemented; integration tracked separately) |
| `UI-02` | **P0** | Create a passenger map showing bus locations, routes, stops, bus IDs and occupancy. Allow passengers to select a bus or stop to view additional information. Allow passengers to select the agreed west/east city area and view its routes, stops and buses.<br><br>**Done when:** Valid buses display at the correct locations. Missing or invalid positions are not presented as live information. Changing area refreshes the map without retaining buses from the previous area. | 🟩 Done (agreed area selection, moving markers, last-known positions and occupancy verified in browser) |
| `UI-03` | **P0** | Connect the frontend to Carson's APIs. Retrieve live bus locations, routes, occupancy and other available information. Refresh the display as the central server receives new updates.<br><br>**Done when:** Bus movements appear automatically. Loading, missing data and API errors are handled without crashing the application. | 🟩 Done (backend-fed map/dashboard updates; live and controlled-demo adapters, missing/error handling verified) |
| `UI-04` | **P1** | Build the operator dashboard showing active buses, routes, regional occupancy, traffic delays, arrival estimates, server health and alerts.<br><br>**Done when:** Operators can identify crowded routes, delayed buses, inactive services and reported system problems. | Partial (dashboard implemented; remaining demand/arrival information and verification remain) |
| `UI-05` | **P1** | Build the Simulation Control Panel. Provide controls to start/pause simulations, change demand, introduce traffic delays, simulate sensor faults and trigger regional server failures.<br><br>**Done when:** Valid operator commands reach the backend and produce the intended changes. Invalid commands show understandable error messages. | 🟩 Done (controlled scenarios, clock speed, bus/edge/central recovery and invalid-control responses verified) |
| `UI-06` | **P1** | Test passenger searches, bus displays, API updates, missing information, invalid inputs, simulation controls and user permissions.<br><br>**Done when:** Important user flows work, invalid actions are handled correctly and the UI remains functional during API failures. | Partial (control/map/status scenarios tested; journey searches and permission flows remain) |
| `UI-07` | **P1** | Allow passengers to choose a starting stop and destination. Display suitable A-to-B routes, available buses, stops and supported transfers. Use the passenger's selected city to filter origin/destination stops, routes and available buses.<br><br>**Done when:** Valid journeys are shown with estimated durations. Unknown stops, identical locations and unavailable journeys receive clear messages. Journey searches and results stay within the selected city. | Pending |
| `UI-08` | **P1** | Display regions, edge servers, server health, workload distribution and failover events. Include additional edge instances when reported by the backend.<br><br>**Done when:** Operators can distinguish healthy, overloaded, failed and recovering servers. Server status reflects backend information rather than assumed values. | 🟩 Done (backend-derived instance states, queues, assignments, standby instances and failure/recovery events) |
| `UI-09` | **P2** | Add a simple comparison between normal and peak-hour operations. Display demand changes, traffic delays, busy routes and scheduling recommendations.<br><br>**Done when:** Comparisons use actual simulation or backend results and are labelled clearly. | Pending |
| `UI-10` | **P2** | Prepare a complete demonstration showing journey search, live bus tracking, traffic delays, changing passenger demand and server failures.<br><br>**Done when:** The demonstration can be repeated reliably and shows the key features working together. | Partial (repeatable movement/fault/recovery walkthrough provided; journey-search demonstration remains) |
| `UI-11` | **P1** | Show intelligent bus recommendations. Display the recommended bus, alternatives, waiting times, traffic delays, occupancy, transfers and estimated total journey time. Explain the recommendation in simple language.<br><br>**Done when:** Passengers can compare suitable options, understand why one is recommended and see when information is unavailable or outdated. | Pending |

</details>

## Shared tickets

<details>
<summary><strong>Integration, documentation and submission</strong> — 20 tickets</summary>

| Ticket | Priority | Task | Status |
| --- | --- | --- | --- |
| `TEAM-01` | **P0** | Set up GitHub repository, branches, shared folders and development environment | 🟩 Done (branch-wide integration evidence; simulation where applicable) |
| `TEAM-02` | **P0** | Finalize telemetry schema, MQTT topics, regional JSON contracts and central API formats | Partial (shared formats implemented; team-wide finalization/adoption remains) |
| `TEAM-03` | **P0** | Integrate bus simulator → MQTT → regional edge server | 🟩 Done (branch-wide integration evidence; simulation where applicable) |
| `TEAM-04` | **P0** | Integrate regional edge → central server and verify database updates | 🟩 Done (branch-wide integration evidence; simulation where applicable) |
| `TEAM-05` | **P0** | Integrate central server → passenger interface | Partial (API/UI adapters exist; central-to-passenger integration verification remains) |
| `TEAM-06` | **P0** | Complete first end-to-end system integration test | Partial (backend verified; full user-facing system integration remains) |
| `TEAM-07` | **P1** | Test delayed messages, duplicate events and regional edge failover | 🟩 Done (branch-wide integration evidence; simulation where applicable) |
| `TEAM-08` | **P1** | Test dynamic demand scheduling and simulation controls | Partial (manual demand/scenario commands exist; scheduling/control integration remains) |
| `TEAM-09` | **P1** | Capture integration screenshots, metrics and testing evidence | Partial (metrics recorded; complete screenshot evidence remains) |
| `TEAM-10` | **P1** | Complete report sections, diagrams, findings, references and contribution matrix draft | Pending |
| `TEAM-11` | **P0** | Finalize README, verify clean installation, submit code and final report | Pending |
| `TEAM-12` | **P2** | Prepare demonstration script and individual speaking sections | Pending |
| `TEAM-13` | **P0** | Verify independent multi-process deployment across two regions | 🟩 Done (branch-wide integration evidence; simulation where applicable) |
| `TEAM-14` | **P1** | Test complete scheduling feedback loop, from demand detection to adjusted dispatch frequency | Pending |
| `TEAM-15` | **P1** | Test failure recovery during delayed, duplicated and temporarily unavailable messages | 🟩 Done (branch-wide integration evidence; simulation where applicable) |
| `TEAM-16` | **P2** | Run scalability benchmarks across different simulated fleet sizes | 🟩 Done (branch-wide integration evidence; simulation where applicable) |
| `TEAM-17` | **P2** | Compare system architecture and capabilities with existing public transit solutions | Pending |
| `TEAM-18` | **P1** | Evaluate security, privacy, fault tolerance, scalability and architectural limitations | Partial (component evidence exists; full system security/privacy evaluation remains) |
| `TEAM-19` | **P1** | Finalize individual contribution percentages and supporting implementation evidence | Pending |
| `TEAM-20` | **P2** | Create PowerPoint slides, rehearse the 10-minute presentation and prepare Q&A answers | Pending |

</details>


### Edge instance file responsibilities

| File | Responsibility / tickets |
|---|---|
| `services/edge/main.py` | EDGE-05 launcher and simulation coordinator; `--plan` shows fixed MQTT-worker assignments; `--benchmark` measures stateful simulation scenarios. |
| `services/edge/edge_server.py` | Defines one edge instance and connects its queue, processing, forwarding and health reporting. |
| `services/edge/edge_server_health.py` | EDGE-04 sensor heartbeat tracking/reporting and EDGE-07 queue metrics; shared timeout rules are in `shared/schema/health.py`. |
| `services/central/health.py` | Detects missing edge heartbeats and exposes health to the UI, even when an edge has stopped. |
| `services/edge/tests/test_edge_server_testing.py` | Object behaviour: heartbeat health, measurements and queue capacity. |
| `services/edge/tests/test_edge_lifecycle.py` | Instance creation/configuration, registration, recovery timing, scale-up and safe scale-down. |
| `services/edge/tests/test_edge_pipeline.py` | Pipeline behaviour: publisher telemetry, replication, pending-update takeover, ownership, replay protection, cross-region handoffs and fleet throughput. |

The current map uses logical route boundaries: west owns routes 302/900 (four buses per instance), east owns 407/409/901 (six buses per instance). These are processing responsibilities, not physical server locations or GPS polygons. A moving bus keeps its explicit owner. Run `python -m services.edge.main --plan` to inspect assignments without opening connections. The simulation starts from these assignments, balances buses within their home region and permits controlled cross-region takeover when local workers fail. It keeps the original route region and coordinates when a remote instance hosts that work. The MQTT launcher starts from that plan and follows subsequent central ownership assignments.


### UI-controlled edge cluster simulation

Use the existing Simulation Controls page to run a repeatable demonstration:

1. Click **Start buses**, then choose a clock speed and **Run clock**. One simulation second moves a normal bus about 8.33 metres (30 km/h); heavy traffic uses 9 km/h. Three simulation seconds are spent at each stop. Buses begin at configured stops and stop at their route terminal rather than teleporting.
2. Select one bus and apply a condition. Restore it before comparing another condition. The panel shows local distance/GPS, central's last position/sequence, heartbeat age, sensor states, retry attempts and pending delivery.
3. Use **Controlled demonstration** on the Passenger Planner and Operator Dashboard. The selection is shared across open pages. Unknown, unavailable, degraded and intentionally stopped buses are not shown as fresh live positions.
4. Simulate a central outage or edge failure. Pending delivery is bounded to the latest state per bus, retries wait at least two simulation seconds, and recovery preserves sequence ordering. Pending central data older than 30 simulation seconds is discarded rather than presented as fresh.
5. Restore the bus/server and observe recovering then healthy status and backend events. Stop buses or pause the clock when finished. Reset clears demonstration state; central restart also clears it.

| Condition | Evidence and expected result |
|---|---|
| Normal / stationary / out of service | Movement follows elapsed time; stationary buses still report; out-of-service buses stop moving and boarding |
| Traffic / peak demand | Lower movement speed / higher passenger count, within capacity |
| Occupancy sensor faulty | GPS and heartbeat continue; occupancy is explicitly faulty; central keeps the last trusted full reading |
| Invalid GPS | Actual validator rejects the reading; rejection does not prove hardware failure |
| All sensors silent | No new sensor readings/heartbeats; unavailable after 6 simulation seconds; no crash inferred |
| Connection loss | Local GPS continues, central position freezes, delivery retries wait 2 simulation seconds |
| Process failure | Explicitly injected process failure; local updates and heartbeats stop; this does not kill an OS process |
| Unexplained silence | No received heartbeat; unavailable with cause unknown |
| Central outage | No modeled acknowledgement; latest pending updates retry, then deliver or expire safely |

The demonstration uses the real telemetry validator, processor, cache, mapper and shared central model. Its acknowledgement/transport/coordinator are modeled in memory; it does not write these experiments to PostgreSQL or replace live MQTT integration tests. Failure evidence distinguishes deliberate injection from a diagnosis of an unexplained live outage. Eight additional tickets are now complete under the agreed scope: BUS-02/05/06/07 and UI-02/03/05/08. Of the 17 tickets discussed, 14 are complete; UI-04, UI-06 and UI-10 remain partial because ETA, journey search and permissions are not supplied by these controls.


Central advances the simulation automatically from elapsed monotonic real time, including when no browser is open. At 1×, one real second represents one simulation second; Controls supports 2×, 5× and 10×, plus clock pause/resume. Recovery completes after two simulation seconds. Browser polling only reads state, so opening another page does not speed it up. Keep one central API process because the simulation remains in memory. `/advance` is still available for deterministic tests; pause the clock before manually stepping a demonstration.

Controls and the Operator Dashboard show the same simulated instance states, assignments and recent events. Recovering is amber, healthy is green, failed is red and standby is grey. The dashboard also shows separately labelled live heartbeat reports, per-bus sensor health and central's live failure/reassignment event history. Simulated failures do not stop live OS processes or write simulated bus states into PostgreSQL.

Bus controls publish the existing `/commands` API through MQTT to running publishers: pause/resume, peak/off-peak demand, traffic factor and sensor failure rate, scoped to west or east. A published command is not an application acknowledgement. Pause/resume and sensor faults apply before the next reading; demand and traffic apply at the next stop segment. Resume does not start a stopped publisher process. Clock speed affects the edge model only; live bus publishers keep their configured update interval.

Verification covered real-time modeled recovery on both pages, west-only bus pause while east continued, sensor fault/restoration in the dashboard, and live worker failure/takeover/recovery. After handback, all four live workers stayed healthy beyond the heartbeat timeout. Handed-back cache entries retain replay protection but no longer inflate their former owner's active/pending heartbeat counts. BUS-02 is complete for the agreed west/east map scope. The controlled demo supports start/stop and full bus-state transitions; starting an OS publisher process remains a CLI operation. Operator authorization remains separate CENTRAL-09/UI-06 work.

Each edge queue holds at most 50 waiting messages; the default modeled processing delay is 5 ms. `EdgeClusterSimulation` in `main.py` starts two active instances per region and a standby third instance in each region. It uses actual shared-route telemetry with the existing validator, processor, cache and central mapper. Its central-shaped output is visible in `central_states`; it does not publish experiments into the live database or change running MQTT workers. Use one central API process: simulation state and replicas are in memory and reset on restart. Existing `/requests` remains a synthetic queue-only experiment; use `/telemetry` or `/telemetry/generate` to exercise bus processing and stateful handoffs.

| Method | Path (prefix `/health/simulation`) | Purpose / body |
|---|---|---|
| GET | (prefix itself) | Queues, metrics, states, ownership versions, replica sequences and recent events |
| PATCH | `/instances/{edge_id}` | Set `capacity` (1-50), `processing_ms` (1-60000); assigned instances stop through failure/scale-down actions |
| POST | `/telemetry` | Submit a complete shared-format telemetry object |
| POST | `/telemetry/generate` | `{"bus_id":"bus-302-1","count":20}` generates valid repeated sensor readings from a shared-route sample |
| POST | `/advance` | `{"seconds":6}` advances virtual time; no real sleeping |
| PATCH | `/demo` | Optional `running`, `bus_id` plus `condition`, or `central_available` controls the repeatable fleet demonstration |
| PATCH | `/clock` | `{"running":true,"speed":2.0}` controls automatic real-time progression (speed greater than 0, at most 10) |
| POST | `/instances/{edge_id}/fail` | Lose the worker's local cache/queue and trigger takeover using surviving replicas and the coordinator's journal |
| POST | `/instances/{edge_id}/recover` | Synchronize before a 2-second modeled recovery window; no assignments while recovering |
| POST | `/instances` | Register `edge_id`, `region_id`, optional `capacity` and `processing_ms` |
| POST | `/transfer` | `{"bus_id":"bus-302-1","target_id":"edge-west-3"}`; target must already be healthy/active |
| POST | `/instances/{edge_id}/scale-down` | Preflight capacity and transfer all owned buses before stopping |
| PATCH | `/policy` | `{"auto_scale":true}` enables the standby third instance |
| POST | `/reset` | Optional `{"fleet_size":50}`; accepted sizes 1-50, default restores the configured twenty buses |

The coordinator serializes changes under one lock. Accepted bus states and sequence floors are copied to healthy/recovering instances in the same city, including the other region so both local failures can be demonstrated. Accepted pending telemetry is retained in the coordinator's journal. A handoff installs the latest surviving replica, advances an ownership version, changes the registry owner and transfers that bus's queued updates in order. Only the current owner/version may commit. Old owners, duplicate/stale sequences and conflicting replays cannot overwrite a newer state. A full target delays takeover without dropping accepted journal entries. Loss of every replica prevents recovery rather than resetting sequence history. These are explicit simulation assumptions; network consensus and loss of the coordinator are outside this model.

Normal ownership aims for equal bus counts (difference at most one). Queue pressure also considers processing speed and predicted waiting time. Telemetry moves by whole-bus assignment rather than distributing concurrent messages from one bus across owners. A geographic route keeps its original city/region when hosted elsewhere. Healthy local capacity is preferred; if no local worker remains healthy, an eligible healthy same-city server in another region can take over. Returning workers synchronize before accepting work; controlled transfers can restore their assignments.

If both regional queues remain at least 80% full for 6 modeled seconds, the standby third worker synchronizes and starts processing reassigned buses. After all active regional queues stay below 20% for 10 modeled seconds, the automatically started third worker transfers its buses back and stops when capacity permits. Failed standby workers are not automatically resurrected. Dynamic registration also synchronizes state before transfers. Insufficient capacity blocks scale-down without losing ownership or queued telemetry.

Repeatable demonstration in `/docs`: reset; set `processing_ms` to 60000 on both west workers (deliberately slow stress settings); generate 40 readings each for `bus-302-1` and `bus-302-2`; advance 5 seconds (third still inactive); advance 1 (third active with real telemetry assignments); set the two original workers back to 5 ms; advance 1 to drain the readings; advance 10 to observe safe scale-down. Failure/recovery and dynamic registration have their own routes above. Danial's included UI uses these backend controls; complete browser integration and remaining simulator controls still need verification.

### Fleet performance evidence (EDGE-10)

Run `python -m services.edge.main --benchmark`. Each scenario sends one valid reading per bus per virtual second for 20 rounds and fails `edge-west-1` halfway through. Larger fleets clone the configured bus definitions on the same real routes in an isolated registry; shared map files are not rewritten. All accepted readings were processed, with zero rejected or pending readings in the measured runs.

| Buses | Processed | Normal / after-failure modeled mean latency (ms) | Measured harness updates/s | Measured handoff decision (ms) |
|---|---:|---:|---:|---:|
| 5 | 100 | 6 / 7 | 558.24 | 0.442 |
| 10 | 200 | 9 / 11 | 512.31 | 1.172 |
| 25 | 500 | 18.8 / 23.8 | 286.47 | 4.200 |
| 50 | 1000 | 35 / 45 | 136.32 | 10.319 |

Latency uses the virtual clock and configured 5 ms service time; measured harness throughput and handoff-decision time use the local wall clock. These figures include local simulation/validation/processing overhead, not network or PostgreSQL throughput. The failure is explicitly injected, so measured handoff time excludes heartbeat detection. Hardware/load changes affect measured values. Normal west/east assignment counts were respectively `1/1;2/1`, `2/2;3/3`, `5/5;8/7`, `10/10;15/15`; after the west failure its survivor owned 2, 4, 10 and 20 buses respectively. Separate tests cover both-local-server failure, cross-region recovery, capacity pressure and third-instance scale-up/scale-down.

### Shared health definitions

`shared/schema/health.py` defines the evidence rules. The edge reports sensor liveness, declared faults, the age of the last accepted telemetry and its speed. Central combines those observations with its own edge timeout. `/health/edges` includes a `buses` list, each bus's `health.status` and `health.reasons`, each sensor's `health_status`, and each edge's `workload_status`. Existing heartbeat `status` remains a liveness field: a sensor can be alive while reporting a fault.

| Evidence | Result |
|---|---|
| Edge has never reported or its heartbeat expires | Bus health `unknown`; reason `edge_unknown` / `edge_unavailable`; sensor health unknown |
| All four sensor channels expire while edge still reports | Bus `telemetry_unavailable` |
| One or more sensors report a fault or expire | Bus `degraded`, with sensor-specific reasons; sensor `faulty` / `unavailable` |
| Edge/sensor heartbeats resume within the recovery window | Bus `recovering`, unless a known fault/missing channel already makes it degraded |
| Sensor evidence is incomplete | Bus `unknown` |
| All sensors are healthy but accepted telemetry is absent or expired | Bus `data_stale` |
| All sensors healthy, accepted telemetry fresh and speed zero | Bus `stationary` |
| All sensors healthy and accepted telemetry fresh | Bus `healthy` |
| Healthy edge queue at/above the configured threshold | Workload `high_load`, then `overloaded` after the sustained interval |
| Edge is not healthy | Workload `unknown`; old queue reports are not treated as current |

Reading freshness uses `SENSOR_HEARTBEAT_TIMEOUT_SECONDS` (10 seconds by default); stationary requires fresh accepted telemetry, not just an alive heartbeat. An accepted reading's age continues increasing even if fresh heartbeats arrive. Classification is recalculated when the health API is read, so timeouts do not need a new packet. Queue overload and heartbeat liveness are separate statuses. A bus that is stationary is not automatically broken down. No missing-heartbeat pattern diagnoses a crash or mechanical failure; those need separate explicit evidence or simulation events, which are not added here.


### Edge input conversion and authentication

Four files add these features: `services/edge/data_to_json_converter.py`, `services/edge/auth_edge_server.py`, `services/edge/tests/test_converter_edge_server.py`, and `services/edge/tests/test_auth_edge_server.py`. Tests use pytest's `test_` filename prefix for automatic discovery. Existing ingress, sender and simulator files connect these modules to the running pipeline.

The order is **decode bounded input -> authenticate signed topic/body -> convert a declared format -> validate shared telemetry and registered identity -> check edge ownership -> queue -> process -> central mapping/forwarding**. Sensor heartbeats are authenticated before health monitoring too. The converter is an adapter for known formats, not arbitrary data interpretation.

| Declared format | Supported input / normalization |
|---|---|
| `canonical-v1` | Existing shared JSON objects/UTF-8 JSON; field types and meanings remain unchanged |
| `vendor-v1` | Explicit `vehicleId/city/area/route`, `location.lat/lng`, `speed.value/unit`, `load.passengers/capacity`, `observedAt`, `sequence/lamport/direction`, `state/sensorState`, `currentStop/nextStop` fields; `version` must equal `vendor-v1` |
| `csv-v1` | UTF-8 header plus exactly one record, using shared telemetry column names; numeric columns are converted to their declared types |

Vendor speed units are explicitly `km/h`, `mph` or `m/s`, normalized to `speed_kmh`. Vendor timestamps accept timezone-aware ISO strings or Unix **seconds**, normalized to UTC. Vendor bus states map `stopped` to `at_stop` and preserve the supported `moving`, `boarding`, `out_of_service` meanings; sensor states map `ok/fault/offline` to `healthy/faulty/offline`. Identifiers must still match the shared registry and incoming topic. Missing fields, unknown units/states/versions, invalid numbers/ranges, ambiguous JSON keys, malformed CSV and inputs over 64 KiB are rejected. No location, city, identity or missing sensor value is guessed.

Existing canonical JSON can be published unchanged in demo mode. Alternative MQTT input uses `{"format":"vendor-v1","data":OTHER_OBJECT}` or `{"format":"csv-v1","data":"HEADER\nRECORD"}`. Standalone `data_to_json(raw, source_format=..., expected_region=..., expected_bus=..., registry=...)` returns shared-format JSON; `convert_to_telemetry` returns the validated Python object. Tests compare all three paths through the actual processor and central mapper to show equivalent output, rather than just checking that JSON parses. This demonstrates format interoperability; it does not claim cross-hardware/OS testing.

`auth_edge_server.py` uses HMAC-SHA256 to verify a publisher's shared secret and binds the signature to its principal, exact topic, timestamp, nonce and payload. The signing envelope has `auth` and `payload` fields. A bus principal is `bus:<registered bus ID>`. `sign_message(topic, payload, principal, secret)` creates that envelope; `encode_bus_message` lets the existing simulator sign telemetry and heartbeats from environment-provided keys. Alternative-format envelopes go inside the signed `payload`. The edge only accepts its currently assigned buses, rejects topic/payload identity mismatches, and rejects tampering, replays, packets 30 seconds old or timestamps more than 5 seconds ahead. Replay tracking is bounded at 4096 live entries and is in memory; it resets with the process. Sender-side permissions restrict outgoing states and edge heartbeats to the configured scope.

| Setting | Meaning |
|---|---|
| `EDGE_PUBLISHER_KEYS_JSON` | Secret JSON object mapping `bus:<id>` to strong signing keys (at least 32 UTF-8 bytes each); provision outside Git |
| `EDGE_AUTH_REQUIRED=true` | Reject unsigned telemetry/heartbeats; missing keys fail startup. Supplying keys enables this by default unless explicitly set false |
| `EDGE_AUTH_REQUIRED=false` | Explicit unsigned demo compatibility; this is also the default when no keys exist |
| `MQTT_USERNAME`, `MQTT_PASSWORD` | Per-process broker credentials; both must be provided together |
| `MQTT_TLS=true` | Enable certificate/hostname verification using the system trust store or `MQTT_TLS_CA_FILE`; configure the appropriate broker port |
| `EDGE_ALLOW_INSECURE_MQTT=true` | Explicit local-test exception for password authentication without TLS; production connections should use verified TLS |

Signing authenticates incoming publisher data but does not encrypt it. The client configures credentials/TLS before connecting. Use unique account credentials and provision each publisher with its own signing key; the trusted simulator launcher may hold the fleet's key mapping to start its bus processes. For per-bus broker credentials, run the existing simulator with `--bus <id>` and that account's environment. Never commit or print the real keys/passwords.

Broker enforcement is separate: `mosquitto_acl(load_network())` generates ACL text for the fixed shared topology. Bus broker usernames are the **bare bus IDs**, edge usernames are edge IDs, and the central reader is `central`. Buses may write only their telemetry/heartbeat topics; edges may read their regional feeds and write their own assigned state/health topics; central may read state and edge health. Install the generated ACL and a hashed password file on the broker, disable anonymous access and configure server certificates for TLS. See the official [Paho credential/TLS API](https://eclipse.dev/paho/files/paho.mqtt.python/html/client.html) and [Mosquitto password/ACL settings](https://mosquitto.org/man/mosquitto-conf-5.html). This change leaves the team's shared demo broker configuration unchanged; dynamic simulated ownership is not automatically deployed into live broker ACLs.

An earlier service-enabled snapshot passed **292 tests, no skips**, with isolated PostgreSQL, the existing demo broker and a separate password/ACL-enabled Mosquitto broker. The live security test rejects anonymous/wrong-password logins and cross-bus, forged-state and forged-edge-heartbeat publishes. It confirms unauthorized reads are not delivered (Mosquitto may acknowledge the subscription itself), while an authorized signed message reaches the edge queue. The authenticated broker test uses `EDGE_AUTH_LIVE_MQTT_HOST`, `EDGE_AUTH_LIVE_MQTT_PORT` and `EDGE_AUTH_LIVE_PASSWORD` against dedicated test accounts `edge-west-1` and `bus-302-1`; its local transport is intentionally plaintext. Certificate-verification configuration is covered separately; a live TLS handshake is not demonstrated here. For the integrated snapshot, the latest local verification is 417 passed and 18 service-dependent skips; those earlier 289/3 counts no longer describe the current suite. Temporary test credentials and containers are removed after verification.

These additions demonstrate edge publisher authentication, authorization and input-format interoperability. Broker credentials and ACLs are now deployable through `compose.secure.yaml` (MQTT-07, [infrastructure/README.md](infrastructure/README.md#authentication-and-topic-authorization-mqtt-07)); central UI login/roles (CENTRAL-09) and TLS certificates remain separate work; the whole project's security should not be marked complete solely from these edge tests.

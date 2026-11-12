# Distributed Transit System

A public-transit simulation connecting bus telemetry, MQTT messaging, regional edge services, a central API and PostgreSQL, and passenger/operator interfaces.

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
|   |-- messaging/              # Julian: MQTT and synchronization
|   |   |-- mqtt_client.py      # existing central subscriber
|   |   `-- tests/
|   |-- edge/                   # Ali: local ingress and processing
|   |   |-- config.py           # network configuration and cache policy
|   |   |-- edge_server.py      # accepted-message queue adapter
|   |   |-- communication/      # data_to_edge.py and edge_to_central.py
|   |   |-- validation/         # validator.py and bus_registry.py
|   |   |-- cache/              # edge_state_cache.py
|   |   |-- processing/         # processor.py: route, occupancy and statistics
|   |   |-- transformation/     # central_mapper.py: EDGE-03 mapping
|   |   `-- tests/              # validation, processing and forwarding tests
|   `-- central/                # Carson: API, ingestion and scheduling
|       |-- main.py
|       |-- routers/
|       |   |-- bus_states.py
|       |   |-- routes.py
|       |   `-- stops.py
|       `-- tests/
|-- frontend/                   # Danial: passenger/operator UI
|-- shared/
|   |-- examples/               # example telemetry and edge network proposal
|   |-- routes.json             # canonical two-city network
|   `-- schema/
|       |-- bus_state.py        # shared central input and edge metadata model
|       |-- network.py          # shared network loading and assignment checks
|       |-- schema.sql          # database tables and constraints
|       `-- seed.sql            # sample routes and stops
|-- infrastructure/             # broker/deployment configuration
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

This tree reflects the current local workspace, excluding Git internals and generated cache contents. `legacy/central_server/` contains only old empty directories and generated caches; all active source stays in `services/`. Use `services/central/` for central work. The legacy directory is local only because its contents are empty directories or ignored caches. Git does not track empty directories, and the empty placeholders are currently untracked, so those locations may be absent in a fresh clone. Keep unfinished files empty until their tasks are implemented.

| Branch | Put implementation here |
| --- | --- |
| `zachary` | `services/data_simulation/`; coordinate `shared/routes.json` |
| `julian` | `services/messaging/`; broker configuration in `infrastructure/` |
| `ali` | `services/edge/` |
| `carson` | `services/central/`; database SQL in `shared/schema/` |
| `danial` | `frontend/` |

Put service tests in that service's `tests/` directory, end-to-end tests in root `tests/`, and report evidence in `docs/`. Coordinate shared contract changes through TEAM-02. The central subscriber and local EDGE-01/02 pipeline are implemented. Further regional features and end-to-end integration remain unfinished.


## Edge implementation on `ali`

EDGE-01 receiving/validation and EDGE-02 processing are connected to EDGE-03 mapping and automatic forwarding. There are **156 passing offline tests and one skipped live integration test**. EDGE-03 remains partial until live MQTT/PostgreSQL delivery and the team's field semantics are verified. All changes, including compatible central receiver support, are on `ali`; they have not been deployed to `main` or confirmed by Carson.

### Follow the data

There is no separate edge `main.py` or edge HTTP server. `communication/data_to_edge.py` runs the MQTT worker: receive, validate, queue and pass accepted readings through processing and forwarding. `communication/edge_to_central.py` owns outgoing validation and delivery. A running worker is still needed to keep MQTT connections alive; instance orchestration and balancing belong to later tickets.

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
$env:CITY_ID = "CITY-A"
$env:REGION_ID = "A-north"
$env:EDGE_ID = "edge-A-north-1"
$env:CENTRAL_OCCUPANCY_UNIT = "passengers"
python -m services.edge.communication.data_to_edge
```

The occupancy setting above is an explicit example, not evidence of Carson's approval. Start the broker and updated central receiver before the edge service. `MQTT_HOST`/`MQTT_PORT` default to `127.0.0.1`/`1883`; `CENTRAL_MQTT_HOST`/`CENTRAL_MQTT_PORT` can select a separate outbound broker. Cache settings remain `EDGE_MAX_BUSES` (1000), `EDGE_FRESHNESS_SECONDS` (30), `EDGE_EXPIRE_SECONDS` (300), `EDGE_MEDIUM_THRESHOLD` (50), and `EDGE_HIGH_THRESHOLD` (80).

For an existing development database, add the optional metadata column before running this branch's central receiver:

```sql
ALTER TABLE bus_states ADD COLUMN IF NOT EXISTS edge_metadata JSONB;
```

The fresh-development `schema.sql` already includes this column. Do not rerun its table-dropping setup just to add a column. No live database migration has been executed by this change.

### Shared BUS-01/BUS-02 network and simulator

`shared/routes.json` is the canonical `transit-network-v1` definition used by the simulator, edge configuration adapter and central seed generator. It contains two cities, 12 distinct stops in each, and 11 unique routes. Each city has five local routes plus the one shared intercity route, so six routes serve each city. Every route has exactly five stops, with both travel directions supported. Coordinates are synthetic simulation points, not claims about real transit locations.

| Route in each city | Forward stop order |
| --- | --- |
| R1 | Home → Neighborhood 1 → School → Mall → Station |
| R2 | Neighborhood 2 → Park → University → Restaurant → Station |
| R3 | Neighborhood 3 → School → Park → Mall → Station |
| R4 | Home → Neighborhood 2 → University → Casino → Station |
| R5 | Neighborhood 3 → Mall → Amusement Park → Restaurant → Station |
| Shared AB-R6 | City A Station → City A Park → City B Park → City B Mall → City B Station |

All stop, route and bus IDs are globally unique. `served_city_ids` explicitly declares cross-city coverage. The intercity bus retains its registered City A / A-north processing assignment, while `current_stop_city_id` and `next_stop_city_id` record which city's stops it is travelling between. These physical stop-city fields and the served-city list reach central metadata. This does not implement dynamic ownership transfer or cross-region failover.

The 11 configured buses each have a capacity, route, region and edge owner. The existing two edge IDs per region divide initial assignments; load balancing and takeover still belong to later tickets. Direction 1 reverses a route's same five stops. The legacy Durham example remains supported for regression tests, but it is not the preferred configuration and no longer matches the new seed dataset.

Zach's three simulator files were imported from his `54d5302` revision, not by merging his central code into `ali`. Movement interpolation, separate seeded random sources, configurable traffic and sensor-drop behaviour are retained. Route/stop loading now uses the common local network instead of requiring a central HTTP server just to start the simulator. Sample peak/off-peak, slowdown and fault settings live in bus configuration, not hardcoded bus IDs.

```powershell
# Print standard JSON from all 11 buses without requiring a broker.
python -m services.data_simulation.run_buses --updates 100 --interval 1
# Or publish the identical messages to Julian's MQTT broker.
python -m services.data_simulation.run_buses --mqtt --host 127.0.0.1 --port 1883
# Select a particular registered bus for a short check.
python -m services.data_simulation.run_buses --bus A-R1-BUS-01 --updates 3 --interval 0
```

Start the appropriate edge workers and updated central receiver before live publishing. `--interval 0` skips real waiting for tests; movement still uses the normal simulation step. Each process keeps independent counters/random state and emits finite streams bounded by the update setting and route end; repeating routes and session restart ordering are not implemented here.

Messages use `schema_version: "bus-telemetry-v1"`: `bus_id`, `city_id`, `region_id`, `route_id`, `latitude`, `longitude`, `speed_kmh`, `passengers`, `capacity`, `direction_id`, `current_stop_id`, optional `next_stop_id`, physical stop-city IDs, `timestamp`, `sequence_number`, `lamport_clock`, `bus_status` and `sensor_status`. UTC source times are generated at the simulator. Its logical clock increments for each local reading event; Julian still owns receive/command clock synchronization. Sensor failures keep Zach's existing behaviour: report the fault to stderr, skip the invalid reading and continue movement.

`export_routes.seed_sql(load_network())` generates `shared/schema/seed.sql` from the same routes, directions, stops and registered buses. No live database was reseeded. Use a fresh dedicated development database for this dataset; the old Durham data is not automatically migrated. If the network changes, regenerate the seed and run the seed-consistency test.

### Delivery and freshness

Only accepted new updates are staged. Stale, duplicate and conflicting readings do not generate new sends. A broker failure leaves the latest pending payload per owned bus for retry; a newer accepted update replaces that slot. Expired pending updates are dropped rather than advertised as current. This queue and the cache are in memory, so restarts do not preserve unsent updates or sequence history. A broker receipt is not a database-write acknowledgement.

Freshness uses a monotonic clock. Cache entries expire only under the configured safe-capacity policy; sequence floors remain remembered during the process lifetime. Route progress uses reported stops, not GPS proximity. Stale/offline buses stay out of active totals, and unknown occupancy stays out of averages. No replication, failover, balancing, scaling, ETA, journey planning or scheduling was added.

### Test ownership

Your EDGE-03 tests are in `services/edge/tests/test_edge_to_central.py`: mapping, the shared validation model, outgoing ownership/field checks, the MQTT topic and sender connection/failure handling. They import the shared model directly, without importing central services or needing a database URL. EDGE-01 and EDGE-02 keep their existing test files. Run `python -m pytest services/edge/tests -q` for 104 passing edge tests.

Central/database/API integration coverage is kept under Carson's area: `services/central/tests/test_edge_ingestion_pipeline.py` and `services/central/tests/test_live_edge_ingestion.py`, alongside his ingestion regression tests. Simulator/configuration tests are in `services/data_simulation/tests/test_simulation.py` (24 tests). Run `python -m pytest services/central/tests -q` for 28 passing tests and one skipped live test. These tests are preserved for central integration, not counted as completion of Carson's tickets.

Production roles remain separate: `central_mapper.py` maps fields, `edge_to_central.py` sends, and `shared/schema/bus_state.py` is the common model. Carson's existing MQTT callback and ingestion functions receive the messages; the edge does not duplicate those central operations.

### End-to-end verification

Offline tests now include real messages generated by Zach's simulator for every configured bus and the intercity crossing. They execute the full code path: input decoding/validation, queue, processing, mapping, outgoing validation, central MQTT callback, real central SQL statements via a SQLite adapter, and API readback. They compare coordinates, speed, counts, clocks, timestamps, city/region/route context, crowding and summaries. They also check invalid types/identifiers, stale and duplicate messages, conflicts, broker-failure retries and the actual runtime forwarding loop. SQLite and the in-memory transport do not substitute for a live PostgreSQL/MQTT run.

An opt-in test is provided for dedicated test services:

```powershell
$env:EDGE_LIVE_DATABASE_URL = "postgresql://USER:PASSWORD@localhost:5432/TEST_DATABASE"
$env:EDGE_LIVE_MQTT_HOST = "127.0.0.1"
$env:EDGE_LIVE_MQTT_PORT = "1883"
python -m pytest services/central/tests/test_live_edge_ingestion.py -q
```

That test creates a unique PostgreSQL schema and unique bus/region/topic identifiers, sends telemetry over a real broker, reads the central database/API result and removes only its test schema. It requires permission to create/drop a schema. It is skipped when the database URL is absent. The Docker engine was unavailable in this environment, so **live delivery has not been verified**. Complete that run and confirm field semantics before marking EDGE-03 Done.

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
| `BUS-01` | **P0** | Create a standard JSON format for bus telemetry. Include bus ID, region ID, route ID, GPS coordinates, speed, passenger count, capacity, current/next stop, timestamp, sequence number and sensor status. Agree on logical-clock metadata with Julian.<br><br>**Done when:** Example messages contain the required fields, use correct data types, and pass validation. Julian and Ali can use the format without modifying field names. | 🟨 Partial (standard implemented; team/live review pending) |
| `BUS-02` | **P0** | Define at least two simulated regions with multiple routes, stops, GPS coordinates and bus assignments. Each route must contain stops in the correct travel order. Define routes, stops and bus assignments for both cities, with city identifiers.<br><br>**Done when:** Every bus belongs to a valid region and route. Stops have unique IDs and coordinates. The simulator can load route data without hardcoded routes in its logic. Both cities load valid routes, stops and buses without mixing assignments. | ✅ Done (shared configuration verified) |
| `BUS-03` | **P0** | Build a Python simulator that generates continuous bus movement and sensor data. Each bus should update its location, speed, stop status and passenger count at configurable intervals.<br><br>**Done when:** At least three buses move between stops and continuously generate valid telemetry. Buses can run simultaneously without interfering with one another. | 🟨 Partial (multi-bus generation verified; live integration pending) |
| `BUS-04` | **P1** | Simulate normal traffic, traffic congestion, peak/off-peak passenger demand and sensor failures. Traffic should affect bus speed and journey duration, while demand affects passenger occupancy.<br><br>**Done when:** Each scenario creates measurable changes. For example, a traffic delay increases travel time, while peak demand increases passenger counts without exceeding the simulated capacity rules. | ⬜ Pending |
| `BUS-05` | **P1** | Add controls to start, pause, resume and stop simulations. Allow supported commands to change traffic, demand or sensor conditions while the simulator is running.<br><br>**Done when:** Valid commands change the requested scenario. Invalid commands or nonexistent bus/route IDs are rejected without crashing the simulator. | ⬜ Pending |
| `BUS-06` | **P1** | Test the simulator's GPS movement, route assignments, passenger counts, sensor readings and simultaneous bus operation. Include normal and invalid-data scenarios.<br><br>**Done when:** Tests confirm valid movement and sensor values. Multiple buses operate correctly, and errors are reported without unexpectedly stopping the entire simulation. | ⬜ Pending |
| `BUS-07` | **P1** | Add bus states such as travelling, arriving, stopped, boarding, departing and out of service. Simulate waiting at bus stops before continuing along the route.<br><br>**Done when:** Buses transition through valid states in the correct order. Stop durations affect simulated journey time, and out-of-service buses stop accepting passengers. | ⬜ Pending |
| `BUS-08` | **P1** | Allow the simulator to run different fleet sizes, such as 5, 10, 25 and 50 buses, using configuration instead of manually creating each bus.<br><br>**Done when:** Each fleet size starts successfully, uses valid bus IDs and route assignments, and produces the expected number of active bus streams. | ⬜ Pending |
| `BUS-09` | **P1** | Allow Carson's scheduler to request changes in bus service. Support adding an available simulated bus to a route or adjusting the time between departures.<br><br>**Done when:** A valid scheduling command changes the simulated service. The changes appear in subsequent telemetry and can be observed by Carson's central server. | ⬜ Pending |
| `BUS-10` | **P2** | Create repeatable test scenarios for normal traffic, peak demand, traffic congestion and sensor failures. Use fixed simulation settings so results can be compared.<br><br>**Done when:** Running the same scenario with the same configuration produces consistent, comparable results. Expected behaviours are documented and checked. | ⬜ Pending |
| `BUS-11` | **P1** | Support adding new buses and optional sensor types through versioned configuration objects. Avoid rewriting the main simulator whenever a supported sensor is added.<br><br>**Done when:** A new registered bus or supported sensor can be added through configuration. Existing buses and sensor messages continue working correctly. | ⬜ Pending |

</details>

<details>
<summary><strong>Julian — MQTT & Synchronization</strong> — 11 tickets</summary>

Transport data between services, maintain synchronization information and ensure messaging remains reliable as the number of buses increases.

| Ticket | Priority | Description and completion criteria | Status |
| --- | --- | --- | --- |
| `MQTT-01` | **P0** | Set up a Mosquitto MQTT broker and define standard topics for bus telemetry, regional processing, alerts and simulation commands. Identify which services publish and subscribe to each topic.<br><br>**Done when:** Independent programs can connect to the broker and exchange messages using the agreed topics. Topic naming and connection settings are documented. | ⬜ Pending |
| `MQTT-02` | **P0** | Connect Zachary's bus simulator to the MQTT broker. Publish each bus's telemetry to its assigned region and bus topic.<br><br>**Done when:** At least three buses publish messages successfully, and Ali's regional subscriber receives the correct telemetry. | ⬜ Pending |
| `MQTT-03` | **P0** | Implement Lamport logical clocks and agree on per-bus sequence numbering. Include UTC timestamps to record when sensor events were generated.<br><br>**Done when:** Logical clocks follow the correct send/receive update rules. Per-bus sequence numbers increase, and every published message includes the agreed ordering metadata. | ⬜ Pending |
| `MQTT-04` | **P0** | Handle MQTT connection loss, reconnection, delayed delivery and duplicate messages. Preserve sequence and clock information so Ali can identify stale telemetry.<br><br>**Done when:** Clients reconnect after a temporary disconnection. Duplicate or delayed messages can be identified, and tests confirm they do not cause Ali's accepted bus state to move backward. | ⬜ Pending |
| `MQTT-05` | **P1** | Implement MQTT communication for system alerts and simulation commands. Ensure alerts and commands reach the intended subscribing services.<br><br>**Done when:** A failure alert reaches the central service, and a simulation command reaches its intended recipient. Unrelated services do not act on the command. | ⬜ Pending |
| `MQTT-06` | **P1** | Test message delivery and communication performance. Measure messages sent, messages received, delivery delays and duplicate deliveries.<br><br>**Done when:** Tests produce recorded results showing the communication system's behaviour under normal conditions and temporary delays. | ⬜ Pending |
| `MQTT-07` | **P1** | Add MQTT authentication and topic-based authorization. Only registered services should connect and publish or subscribe to their approved topics.<br><br>**Done when:** Invalid credentials are rejected. A bus publisher cannot publish to another bus's protected topic. Authorized services continue communicating normally. | ⬜ Pending |
| `MQTT-08` | **P1** | Define how physical timestamps are handled using UTC and clock synchronization assumptions. Simulate different clock offsets and compare physical timestamps against Lamport ordering.<br><br>**Done when:** Tests show how clock differences can affect timestamps and demonstrate correct logical-clock updates despite those differences. | ⬜ Pending |
| `MQTT-09` | **P2** | Test logical-clock behaviour when multiple buses and services generate concurrent events. Include delayed and out-of-order delivery scenarios.<br><br>**Done when:** Event diagrams and tests show correct Lamport values, identify causal relationships and distinguish genuinely concurrent events. | ⬜ Pending |
| `MQTT-10` | **P1** | Test what happens when the MQTT broker temporarily becomes unavailable. Implement client reconnection and resubscription where necessary.<br><br>**Done when:** After the broker restarts, clients reconnect and resume communication. Any lost or unrecovered messages are identified rather than assumed delivered. | ⬜ Pending |
| `MQTT-11` | **P1** | Test MQTT communication with increasing numbers of buses, such as 5, 10, 25 and 50. Monitor message throughput, queue delays, duplicates and ordering behaviour.<br><br>**Done when:** Results compare multiple bus counts. The system preserves the agreed ordering rules, and any performance limitations are measured and explained. | ⬜ Pending |

</details>

<details>
<summary><strong>Ali — Regional Edge Computing</strong> — 12 tickets</summary>

Receive valid telemetry, process regional bus information, share workloads between edge servers and recover when servers fail.

Each region initially has two active edge servers that divide processing responsibilities.

| Ticket | Priority | Description and completion criteria | Status |
| --- | --- | --- | --- |
| `EDGE-01` | **P0** | Build regional MQTT subscribers that receive bus telemetry. Validate JSON structure, GPS, speed, capacity, timestamps, registered bus IDs and region assignments. Prevent invalid messages from entering the edge processing system. Validate that each bus belongs to the declared city and region.<br><br>**Done when:** Valid messages enter the processing queue. Invalid messages are rejected and logged safely. Tests 01–12 pass, including verification that invalid data does not reach edge processing. A bus claiming the wrong city or region is rejected before processing. | ✅ Done (`ali`; local tests) |
| `EDGE-02` | **P0** | Create a regional cache storing the latest valid state of each bus. Track location, speed, current route, next stop and occupancy. Calculate occupancy percentage and crowding level. Check sequence numbers before updating cached information. Keep bus caches and regional calculations separated by city and region.<br><br>**Done when:** Each bus has an accurate latest state. Duplicate or older messages do not overwrite newer data. Occupancy calculations and stop information are correct. Expired information is marked stale. Updates and calculations for one city/region do not overwrite or mix with another. | ✅ Done (`ali`; local tests) |
| `EDGE-03` | **P0** | Agree the outgoing field mapping with Carson, including field names, data types, units, required/optional values and meanings. Map accepted edge updates to that contract, validate them again in `communication/edge_to_central.py`, then connect automatic MQTT forwarding. Cover per-bus positions, speeds, occupancy, stop information, timestamps, sequence/Lamport metadata, city/region identifiers and scoped summaries through agreed topics or envelopes. Do not silently drop information Carson needs or invent unavailable values.<br><br>**Done when:** The mapping is verified against Carson's expected input. Live end-to-end tests trace multiple buses from telemetry input through edge validation/processing and MQTT to Carson's database and API. Received field names, types, units, identifiers and values match the agreed contract, including city/region separation and every agreed information category. Missing, invalid, duplicate, stale and conflicting updates are handled as agreed without corrupting current state. Rejected edge updates are not forwarded as accepted data. Tests verify database/API results, not just broker receipts, and record reproducible evidence. | 🟨 Partial (`ali`; coded, live verification pending) |
| `EDGE-04` | **P1** | Implement heartbeat monitoring for all regional edge servers. Detect when one server stops responding and report whether it is healthy, unavailable or recovering.<br><br>**Done when:** Stopping an edge server triggers failure detection within a configured timeout. The failure status is reported to the monitoring system. | ⬜ Pending |
| `EDGE-05` | **P1** | Run two active edge servers per region. Divide bus workloads between them and replicate the latest required state. If one server fails, transfer its workload to the surviving server.<br><br>**Done when:** Both servers process assigned buses during normal operation. After one fails, the survivor processes the region's buses using synchronized state without conflicting updates. | ⬜ Pending |
| `EDGE-06` | **P1** | Test regional processing, validation, caching, duplicate messages, server failures, backup takeover and recovery. Include normal and failure scenarios.<br><br>**Done when:** Tests confirm that valid information is processed correctly, invalid information is rejected, and edge processing continues after a supported single-server failure. | ⬜ Pending |
| `EDGE-07` | **P1** | Monitor each edge server's active bus count, assigned workloads, incoming message rate, processing latency and queue backlog. Use these measurements to identify overloaded servers.<br><br>**Done when:** Each server reports measurable workload information. The system can distinguish a lightly loaded server from an overloaded one. | ⬜ Pending |
| `EDGE-08` | **P1** | Implement workload distribution between the two active servers. Aim for approximately 50/50 bus assignments and move workloads when a server becomes overloaded or unavailable.<br><br>**Done when:** Bus assignments are balanced under normal conditions. Reassignment works without losing the latest required state or allowing two authoritative owners for one bus. | ⬜ Pending |
| `EDGE-09` | **P1** | Implement safe ownership transfer and recovery. Synchronize returning servers before restoring their workloads. Support a controlled transfer to another healthy region if both local servers fail.<br><br>**Done when:** A recovered server receives current state before resuming work. Tests demonstrate that ownership remains consistent and cross-region takeover works when capacity is available. | ⬜ Pending |
| `EDGE-10` | **P2** | Measure performance with 5, 10, 25 and 50 simulated buses. Compare processing latency, throughput, workload distribution and failover duration.<br><br>**Done when:** Results demonstrate how performance changes with increasing bus counts and different failure conditions. | ⬜ Pending |
| `EDGE-11` | **P2** | Add optional automatic scaling. When both regional servers remain overloaded, start a third edge server, synchronize it and redistribute bus assignments. Support safe scale-down when demand decreases.<br><br>**Done when:** A sustained overload triggers Edge C when resources permit. Edge C receives valid assignments and processes them correctly. Scale-down preserves state and ownership. | ⬜ Pending |
| `EDGE-12` | **P1** | Make bus ownership configurable so newly added edge servers and healthy servers in other regions can take over workloads without changing the processing logic.<br><br>**Done when:** A new edge server can register, receive an approved assignment and begin processing. Ownership changes prevent duplicate authoritative processing. | ⬜ Pending |

</details>

<details>
<summary><strong>Carson — Central Server & Smart Scheduling</strong> — 12 tickets</summary>

Receive processed regional data, calculate arrival times, plan A-to-B journeys, recommend suitable buses and adjust transit schedules based on demand.

| Ticket | Priority | Description and completion criteria | Status |
| --- | --- | --- | --- |
| `CENTRAL-01` | **P0** | Build a FastAPI central server and database for buses, routes, stops, regions and current bus information. Load the agreed route definitions and provide basic API endpoints.<br><br>**Done when:** The server starts correctly, stores valid information and returns expected bus, route and stop records through its APIs. | 🟨 Partial |
| `CENTRAL-02` | **P0** | Receive Ali's processed regional telemetry and update the central database. Maintain the latest accepted state for each bus and prevent outdated messages from replacing newer information.<br><br>**Done when:** Valid regional updates appear in the database and APIs. Duplicate or outdated messages do not corrupt current bus information. | 🟨 Partial |
| `CENTRAL-03` | **P0** | Calculate estimated bus arrival times using remaining route distance, current speed, simulated traffic delays and stop waiting times. Estimate travel durations along the route rather than using only straight-line distance.<br><br>**Done when:** Active buses return reasonable arrival estimates. Traffic delays affect ETA results, and missing information produces an unavailable estimate rather than an invented value. | 🟨 Partial |
| `CENTRAL-04` | **P1** | Analyze passenger occupancy and demand across routes. Identify overcrowded or underused routes and recommend increasing or decreasing bus frequency using agreed thresholds.<br><br>**Done when:** Peak and off-peak scenarios produce explainable scheduling recommendations. A single unusual reading does not unnecessarily trigger repeated changes. | ⬜ Pending |
| `CENTRAL-05` | **P1** | Create APIs for system health, alerts, current routes, scheduling recommendations and simulation commands. Provide the information Danial needs for passenger and operator interfaces.<br><br>**Done when:** Valid API requests return the expected information. Operator commands are accepted only through the appropriate backend endpoints. | ⬜ Pending |
| `CENTRAL-06` | **P1** | Test database updates, bus information, ETA calculations, scheduling recommendations, API responses and invalid requests. Include cases with missing or stale regional data.<br><br>**Done when:** Tests confirm correct API responses, current-state consistency and appropriate error handling. | ⬜ Pending |
| `CENTRAL-07` | **P1** | Build an A-to-B journey planner. Given a starting stop and destination, find suitable bus routes, available buses and supported transfers. Calculate estimated waiting, riding and total journey times. Find valid journeys using the selected city's routes and stops.<br><br>**Done when:** Valid A-to-B searches return usable journey options. Invalid or unreachable stops return clear results. Transfer and travel times are calculated without double-counting delays. Journey results use the selected city; stops from another city are not combined into unsupported trips. | ⬜ Pending |
| `CENTRAL-08` | **P1** | Connect scheduling decisions to Zachary's simulator. Allow an approved scheduling decision to request more buses or change departure frequency.<br><br>**Done when:** A scheduling command changes the simulated dispatch plan. The central server receives updated information confirming the change. | ⬜ Pending |
| `CENTRAL-09` | **P1** | Add role-based access and privacy safeguards. Passengers may view public transit information, while authorized operators can access management features and send simulation commands.<br><br>**Done when:** Unauthorized users cannot execute operator-only actions. Access checks occur in the backend, and unnecessary personal information is not stored. | ⬜ Pending |
| `CENTRAL-10` | **P2** | Compare predicted bus arrivals against simulated arrival times. Evaluate how scheduling recommendations affect passenger waiting times and route overcrowding.<br><br>**Done when:** Recorded results show ETA errors and demand-response outcomes under normal and delayed scenarios. | ⬜ Pending |
| `CENTRAL-11` | **P2** | Separate backend functionality into clear modules for data ingestion, database operations, route management, scheduling, recommendations and APIs. Avoid tightly coupling unrelated functions.<br><br>**Done when:** Modules can be tested independently, existing APIs continue working, and responsibilities are clearly separated. | ⬜ Pending |
| `CENTRAL-12` | **P1** | Build suggestions.py to rank suitable A-to-B journeys. Compare waiting time, traffic-adjusted riding time, occupancy and transfers. Explain why one bus or route is recommended over alternatives.<br><br>**Done when:** A valid journey search returns a recommended option and alternatives. The result includes estimated journey time, crowding information and a clear reason for the recommendation. | ⬜ Pending |

CENTRAL-01, CENTRAL-02 and CENTRAL-03 remain partial until their completion criteria are verified.

</details>

<details>
<summary><strong>Danial — Passenger App & Operator Dashboard</strong> — 11 tickets</summary>

Build one web application containing three interfaces: Passenger Journey Planner, Operator Dashboard and Simulation Control Panel.

| Ticket | Priority | Description and completion criteria | Status |
| --- | --- | --- | --- |
| `UI-01` | **P0** | Build the main web application with navigation between Passenger Planner, Operator Dashboard and Simulation Controls. Use consistent layouts and make the pages usable on desktop and mobile.<br><br>**Done when:** All three pages open and navigation works. Passenger and operator views are clearly separated. | ⬜ Pending |
| `UI-02` | **P0** | Create a passenger map showing bus locations, routes, stops, bus IDs and occupancy. Allow passengers to select a bus or stop to view additional information. Allow passengers to select a city and view that city's routes, stops and buses.<br><br>**Done when:** Valid buses display at the correct locations. Missing or invalid positions are not presented as live information. Changing city refreshes the map without retaining live buses from the previous city. | ⬜ Pending |
| `UI-03` | **P0** | Connect the frontend to Carson's APIs. Retrieve live bus locations, routes, occupancy and other available information. Refresh the display as the central server receives new updates.<br><br>**Done when:** Bus movements appear automatically. Loading, missing data and API errors are handled without crashing the application. | ⬜ Pending |
| `UI-04` | **P1** | Build the operator dashboard showing active buses, routes, regional occupancy, traffic delays, arrival estimates, server health and alerts.<br><br>**Done when:** Operators can identify crowded routes, delayed buses, inactive services and reported system problems. | ⬜ Pending |
| `UI-05` | **P1** | Build the Simulation Control Panel. Provide controls to start/pause simulations, change demand, introduce traffic delays, simulate sensor faults and trigger regional server failures.<br><br>**Done when:** Valid operator commands reach the backend and produce the intended changes. Invalid commands show understandable error messages. | ⬜ Pending |
| `UI-06` | **P1** | Test passenger searches, bus displays, API updates, missing information, invalid inputs, simulation controls and user permissions.<br><br>**Done when:** Important user flows work, invalid actions are handled correctly and the UI remains functional during API failures. | ⬜ Pending |
| `UI-07` | **P1** | Allow passengers to choose a starting stop and destination. Display suitable A-to-B routes, available buses, stops and supported transfers. Use the passenger's selected city to filter origin/destination stops, routes and available buses.<br><br>**Done when:** Valid journeys are shown with estimated durations. Unknown stops, identical locations and unavailable journeys receive clear messages. Journey searches and results stay within the selected city. | ⬜ Pending |
| `UI-08` | **P1** | Display regions, edge servers, server health, workload distribution and failover events. Include additional edge instances when reported by the backend.<br><br>**Done when:** Operators can distinguish healthy, overloaded, failed and recovering servers. Server status reflects backend information rather than assumed values. | ⬜ Pending |
| `UI-09` | **P2** | Add a simple comparison between normal and peak-hour operations. Display demand changes, traffic delays, busy routes and scheduling recommendations.<br><br>**Done when:** Comparisons use actual simulation or backend results and are labelled clearly. | ⬜ Pending |
| `UI-10` | **P2** | Prepare a complete demonstration showing journey search, live bus tracking, traffic delays, changing passenger demand and server failures.<br><br>**Done when:** The demonstration can be repeated reliably and shows the key features working together. | ⬜ Pending |
| `UI-11` | **P1** | Show intelligent bus recommendations. Display the recommended bus, alternatives, waiting times, traffic delays, occupancy, transfers and estimated total journey time. Explain the recommendation in simple language.<br><br>**Done when:** Passengers can compare suitable options, understand why one is recommended and see when information is unavailable or outdated. | ⬜ Pending |

</details>

## Shared tickets

<details>
<summary><strong>Integration, documentation and submission</strong> — 20 tickets</summary>

| Ticket | Priority | Task | Status |
| --- | --- | --- | --- |
| `TEAM-01` | **P0** | Set up GitHub repository, branches, shared folders and development environment | ⬜ Pending |
| `TEAM-02` | **P0** | Finalize telemetry schema, MQTT topics, regional JSON contracts and central API formats | ⬜ Pending |
| `TEAM-03` | **P0** | Integrate bus simulator → MQTT → regional edge server | ⬜ Pending |
| `TEAM-04` | **P0** | Integrate regional edge → central server and verify database updates | ⬜ Pending |
| `TEAM-05` | **P0** | Integrate central server → passenger interface | ⬜ Pending |
| `TEAM-06` | **P0** | Complete first end-to-end system integration test | ⬜ Pending |
| `TEAM-07` | **P1** | Test delayed messages, duplicate events and regional edge failover | ⬜ Pending |
| `TEAM-08` | **P1** | Test dynamic demand scheduling and simulation controls | ⬜ Pending |
| `TEAM-09` | **P1** | Capture integration screenshots, metrics and testing evidence | ⬜ Pending |
| `TEAM-10` | **P1** | Complete report sections, diagrams, findings, references and contribution matrix draft | ⬜ Pending |
| `TEAM-11` | **P0** | Finalize README, verify clean installation, submit code and final report | ⬜ Pending |
| `TEAM-12` | **P2** | Prepare demonstration script and individual speaking sections | ⬜ Pending |
| `TEAM-13` | **P0** | Verify independent multi-process deployment across two regions | ⬜ Pending |
| `TEAM-14` | **P1** | Test complete scheduling feedback loop, from demand detection to adjusted dispatch frequency | ⬜ Pending |
| `TEAM-15` | **P1** | Test failure recovery during delayed, duplicated and temporarily unavailable messages | ⬜ Pending |
| `TEAM-16` | **P2** | Run scalability benchmarks across different simulated fleet sizes | ⬜ Pending |
| `TEAM-17` | **P2** | Compare system architecture and capabilities with existing public transit solutions | ⬜ Pending |
| `TEAM-18` | **P1** | Evaluate security, privacy, fault tolerance, scalability and architectural limitations | ⬜ Pending |
| `TEAM-19` | **P1** | Finalize individual contribution percentages and supporting implementation evidence | ⬜ Pending |
| `TEAM-20` | **P2** | Create PowerPoint slides, rehearse the 10-minute presentation and prepare Q&A answers | ⬜ Pending |

</details>

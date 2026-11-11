# Distributed Transit System

A public-transit simulation connecting bus telemetry, MQTT messaging, regional edge services, a central API and PostgreSQL, and passenger/operator interfaces.

## Architecture and structure

Intended data flow: **Bus simulator → MQTT broker → Regional edge services → Central API/database → Frontend**. Scheduling commands feed back to the simulator. The complete pipeline is still under development.

```text
project/
|-- services/
|   |-- simulator/              # Zachary: simulation and telemetry
|   |   |-- main.py
|   |   `-- tests/
|   |-- messaging/              # Julian: MQTT and synchronization
|   |   |-- mqtt_client.py      # existing central subscriber
|   |   `-- tests/
|   |-- edge/                   # Ali: local ingress and processing
|   |   |-- main.py             # runtime entry point
|   |   |-- config.py           # network configuration and cache policy
|   |   |-- edge_server.py      # accepted-message queue adapter
|   |   |-- communication/      # mqtt_subscriber.py and edge_to_central.py
|   |   |-- validation/         # validator.py and bus_registry.py
|   |   |-- cache/              # bus_state_cache.py
|   |   |-- processing/         # coordinator, route, occupancy, aggregation
|   |   |-- transformation/     # versioned output builder
|   |   `-- tests/              # ingress regression and EDGE-02 acceptance tests
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
|   |-- routes.json             # shared route definitions
|   `-- schema/
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
| `zachary` | `services/simulator/`; coordinate shared route definitions |
| `julian` | `services/messaging/`; broker configuration in `infrastructure/` |
| `ali` | `services/edge/` |
| `carson` | `services/central/`; database SQL in `shared/schema/` |
| `danial` | `frontend/` |

Put service tests in that service's `tests/` directory, end-to-end tests in root `tests/`, and report evidence in `docs/`. Coordinate shared contract changes through TEAM-02. The central subscriber and local EDGE-01/02 pipeline are implemented. Further regional features and end-to-end integration remain unfinished.


## Edge implementation on `ali`

EDGE-01 ingress is preserved and reorganized by responsibility. EDGE-02 consumes its existing validated queue; it does not introduce another subscriber. The three test files contain 100 passing offline checks, including outgoing-contract checks against Carson's actual model. Broker interruption/reconnection over a real network remains unverified.

### Run and configure

Use Python 3.11 or newer. From the repository root:

```powershell
python -m pip install pytest "paho-mqtt>=2,<3"
python -m pytest services/edge/tests -q
$env:EDGE_NETWORK_CONFIG = "shared/examples/edge_network.json"
$env:CITY_ID = "DURHAM"
$env:REGION_ID = "west"
$env:EDGE_ID = "edge-west-1"
python -m services.edge.main
```

Start an MQTT broker before the final command. `MQTT_HOST`/`MQTT_PORT` default to `127.0.0.1`/`1883`. Optional settings are `EDGE_MAX_BUSES` (1000), `EDGE_FRESHNESS_SECONDS` (30), `EDGE_EXPIRE_SECONDS` (300), `EDGE_MEDIUM_THRESHOLD` (50), and `EDGE_HIGH_THRESHOLD` (80).

`shared/examples/edge_network.json` is a versioned configuration proposal derived from the current SQL seed's actual routes, ordered directional stops, buses and initial edge assignments. `DURHAM` labels this existing dataset; it does not invent a second city's routes. A second city can be configured in the same file and uses identical processing code; two-city isolation is tested with fixtures. Agree actual city mappings under BUS-02/TEAM-02 before deployment. The example is a configuration snapshot, not a replacement for the shared SQL source; update it deliberately if source assignments change.

The network file uses `schema_version: "edge-network-proposal-v1"`, a `routes` list (`city_id`, `region_id`, `route_id`, ordered `stops`, optional `direction_id` defaulting to 0), and a `buses` list (`bus_id`, `city_id`, `region_id`, `route_id`, `edge_id`). Each bus needs an explicit edge owner and a matching route. Bus IDs are globally unique under the existing MQTT topic contract. All definitions load once at startup; there are no per-message database queries. Static ownership checks are included, but ownership transfer and balancing are not implemented.

### Local processing contract

`EdgeProcessor.process(validated_telemetry)` returns a typed `ProcessingResult`: `ACCEPTED`, `DUPLICATE`, `STALE`, `CONFLICT`, `NOT_ASSIGNED`, `INVALID_ROUTE`, or `CAPACITY_EXCEEDED`. Only accepted results include output JSON. `EdgeIngress.drain_pending(subscriber)` returns these results for queued messages, and `processor.snapshot()` recalculates freshness when no messages arrive. The runtime loop consumes the same queue without retaining an unbounded history or publishing results.

The existing ingress topic remains `transit/buses/<region_id>/<bus_id>/telemetry`. Optional `city_id` must match the bus registry; if omitted it is resolved from that registry. Optional `direction_id` defaults to 0 for compatibility. Route stops must match the configured directional ordering. Progress is based only on an explicitly reported current stop, never proximity or an invented GPS estimate. It remains unavailable when the input cannot establish progress.

The cache key is `(city_id, region_id, bus_id)`. A local lock covers sequence checking, calculations, cache commit and output snapshot. Equal sequences with different validated fields conflict; exact duplicates and older updates do not refresh freshness or mutate state. Freshness uses monotonic receipt time, while source timestamps are preserved. Capacity never silently evicts active buses: only entries beyond the configured expiry can release space, and their sequence floors remain until process restart. State and sequence history are in memory and do not survive restarts.

Output `schema_version: "edge-state-proposal-v1"` contains `city_id`, `region_id`, `edge_id`, a `buses` array and `statistics`. Each bus includes its identifiers, coordinates, speed, counts/capacity, stop/direction/progress fields, occupancy/crowding, sequence, source/processing timestamps and snapshot freshness. Statistics explicitly use `scope: "edge_assigned_buses"`, with active/crowded bus counts, average occupancy, occupancy sample count and active buses per route. Stale and offline buses are excluded from active totals. Faulty/offline sensor occupancy is unavailable and excluded from occupancy averages; raw readings remain identifiable rather than becoming invented estimates.

This output is a proposal, not Carson's agreed ingestion format. City mappings, directional stop semantics, faulty-sensor interpretation, occupancy thresholds and the final output envelope need TEAM-02 agreement. No shared central API or MQTT format was silently replaced. Automatic EDGE-03 forwarding, monitoring, replication, failover, scaling, ETA, journey planning and scheduling remain outside this implementation.

### How to read the edge code

Follow the files in this order:

1. `main.py` loads configuration, creates the subscriber, then runs the queue-processing loop.
2. `communication/mqtt_subscriber.py` receives MQTT bytes, decodes JSON, calls validation and queues accepted readings.
3. `validation/validator.py` checks the measurements; `bus_registry.py` checks registered city/region/route assignments.
4. `edge_server.py` passes a queued reading to `processing/processor.py`.
5. The processor checks ownership and sequence, checks capacity, calculates route/occupancy data, then saves the new state under one lock. Rejected readings stop before the save.
6. `regional_aggregator.py` calculates local statistics; `transformation/output_builder.py` returns the proposed JSON snapshot.
7. `communication/edge_to_central.py` is the separate outgoing boundary. Its `validate_payload()` checks ownership and validates an explicitly mapped central payload against a supplied central Pydantic model; `connect()`, `send()` and `close()` manage MQTT. It rejects unsupported fields instead of silently dropping them. The edge does not import central database configuration at startup or maintain a second copy of Carson's model.

The sender expects Carson's current per-bus fields (`lat`, `lon`, `speed`, `direction_id`, `next_stop_id`, `bus_status`, `occupancy`, `sequence_number`, `lamport`) and topic `transit/<region_id>/bus/<bus_id>/state`. It does not invent status, convert crowding to central occupancy, or fabricate a Lamport clock. The caller must supply agreed values from a current accepted update. Model validation checks the recipient's field requirements; it cannot prove every value is semantically correct or guarantee database acceptance.

This sender is not connected to the automatic processing loop. The mapping of status, occupancy and clock fields, plus how city metadata reaches central, still needs agreement before full EDGE-03 integration. Broker receipts only confirm MQTT delivery to the broker, not a central database write. Live network integration remains unverified.

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
| `BUS-01` | **P0** | Create a standard JSON format for bus telemetry. Include bus ID, region ID, route ID, GPS coordinates, speed, passenger count, capacity, current/next stop, timestamp, sequence number and sensor status. Agree on logical-clock metadata with Julian.<br><br>**Done when:** Example messages contain the required fields, use correct data types, and pass validation. Julian and Ali can use the format without modifying field names. | ⬜ Pending |
| `BUS-02` | **P0** | Define at least two simulated regions with multiple routes, stops, GPS coordinates and bus assignments. Each route must contain stops in the correct travel order. Define routes, stops and bus assignments for both cities, with city identifiers.<br><br>**Done when:** Every bus belongs to a valid region and route. Stops have unique IDs and coordinates. The simulator can load route data without hardcoded routes in its logic. Both cities load valid routes, stops and buses without mixing assignments. | ⬜ Pending |
| `BUS-03` | **P0** | Build a Python simulator that generates continuous bus movement and sensor data. Each bus should update its location, speed, stop status and passenger count at configurable intervals.<br><br>**Done when:** At least three buses move between stops and continuously generate valid telemetry. Buses can run simultaneously without interfering with one another. | ⬜ Pending |
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
| `EDGE-03` | **P0** | Process and forward regional bus information to Carson. Send validated positions, speeds, occupancy, stop information, timestamps and regional summaries through the agreed MQTT topic. Include `city_id` and `region_id` in the JSON sent to Carson.<br><br>**Done when:** Carson receives correctly formatted regional information for multiple buses. Invalid or outdated data is not forwarded as current information, and Carson has enough data to calculate ETAs. Forwarded messages preserve the validated city and region identifiers. | 🟨 Partial (`ali`; outgoing boundary only) |
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

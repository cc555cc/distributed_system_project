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
|   |-- edge/                   # Ali: regional processing and failover
|   |   |-- main.py
|   |   `-- tests/
|   `-- central/                # Carson: API, ingestion and scheduling
|       |-- main.py
|       |-- routers/
|       |   |-- bus_states.py
|       |   |-- routes.py
|       |   `-- stops.py
|       `-- tests/
|-- frontend/                   # Danial: passenger/operator UI
|-- shared/
|   |-- routes.json             # shared route definitions
|   `-- schema/
|       |-- schema.sql          # database tables and constraints
|       `-- seed.sql            # sample routes and stops
|-- infrastructure/             # broker/deployment configuration
|-- tests/                      # end-to-end integration tests
|-- docs/                       # diagrams and report evidence
|-- legacy/                     # archived local remnants; no active source
|   `-- central_server/          # old empty directories and generated caches
|-- .env.example                # empty configuration template
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

Put service tests in that service's `tests/` directory, end-to-end tests in root `tests/`, and report evidence in `docs/`. Coordinate shared contract changes through TEAM-02. The existing central subscriber is implemented; simulator, regional edge and frontend implementations remain unfinished.

## Running the existing central service

Run these commands from the repository root in PowerShell. Python 3.10 or newer, PostgreSQL and an MQTT broker such as Mosquitto are required.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install "paho-mqtt>=2,<3"
```

The dependency pins come from the existing central implementation. MQTT is imported by the code but is not yet listed in `requirements.txt`.

Set configuration in the same terminal, substituting local database credentials:

```powershell
$env:DATABASE_URL = "postgresql://USER:PASSWORD@localhost:5432/transit"
$env:MQTT_HOST = "localhost"
$env:MQTT_PORT = "1883"
$env:MQTT_TOPIC = "transit/+/bus/+/state"
```

The app also loads a root `.env` file if present. Keep credentials out of Git; `.env.example` is currently empty.

Create a dedicated PostgreSQL development database named `transit`, then initialize it:

```powershell
psql "$env:DATABASE_URL" -v ON_ERROR_STOP=1 -f shared/schema/schema.sql
psql "$env:DATABASE_URL" -v ON_ERROR_STOP=1 -f shared/schema/seed.sql
```

`schema.sql` drops and recreates the transit tables; use a database whose transit data can be replaced. The seed supplies routes and stops, but does not register buses or configure regional ownership. Register valid buses before sending their state updates, as foreign keys require them.

Start the MQTT broker separately, then launch the API:

```powershell
python -m uvicorn services.central.main:app --reload
```

Open `http://127.0.0.1:8000/docs` to inspect the API:

| Method | Path | Purpose |
| --- | --- | --- |
| PUT | `/bus_states/{bus_id}` | Validate field types and insert/update a registered bus state |
| GET | `/bus_states/{bus_id}` | Read a bus state |
| GET | `/routes/{route_id}` | Read route metadata |
| GET | `/stops/{stop_id}` | Read stop coordinates |
| GET | `/routes/{route_id}/stops` | Read stops in direction/position order |

The central MQTT subscriber expects `transit/<region_id>/bus/<bus_id>/state` and uses the same bus-state model as the HTTP update endpoint. Existing state is replaced only by a higher sequence number. Current validation checks field types; coordinate ranges, allowed statuses, regional ownership and full synchronization remain task work. Imports and route registration have been checked; a complete database/broker integration run has not been verified.

## Branch workflow

Everyone uses their first-name branch listed above. All five branches start from the same updated `main`; completed tasks merge back into `main`.

1. Pull the latest `main` before starting a task, then switch to your own branch and merge the latest `main` into it.
2. Implement and test the ticket on your branch. Keep unfinished work on that branch.
3. When the task is complete, fetch and merge the latest `origin/main` into your branch. Resolve any conflicts and rerun the relevant checks.
4. Push your branch and open a pull request into `main`. Merge only the completed task when checks pass and there are no unresolved conflicts.
5. After the task is merged, everyone can pull the updated `main` and merge it into their own branch before continuing.

## Priorities

The supplied task list did not include priorities. The labels below are proposed implementation order, not completion status:

- **P0:** Foundational setup, contracts, core data flow and required integration/submission work.
- **P1:** Main features, reliability, security, testing and documentation.
- **P2:** Deeper evaluation, reproducible evidence and demonstration polish.

## Individual task lists

<details>
<summary><strong>Zachary — Bus Simulation</strong> — 10 tickets</summary>

| Ticket / priority | Task |
| --- | --- |
| `BUS-01` · **P0** | Define shared JSON telemetry schema and example sensor messages |
| `BUS-02` · **P0** | Create simulated bus routes, stops, coordinates and regional assignments |
| `BUS-03` · **P0** | Develop Python bus simulator for continuous GPS and sensor data generation |
| `BUS-04` · **P1** | Implement peak/off-peak demand, traffic delays and sensor-failure scenarios |
| `BUS-05` · **P1** | Build simulation controls for starting, stopping and modifying scenarios |
| `BUS-06` · **P1** | Test simulator reliability, multi-bus operation and sensor data validity |
| `BUS-07` · **P1** | Implement bus lifecycle events: boarding, arriving, departing and out of service |
| `BUS-08` · **P1** | Support configurable fleet sizes: 5, 10, 25 and 50 buses |
| `BUS-09` · **P1** | Implement simulated dispatch-frequency adjustments from Carson's scheduler |
| `BUS-10` · **P2** | Create reproducible simulation test scenarios with expected outputs |

Report deliverables: Telemetry schema, bus simulation design, route definitions, simulation scenarios, bus state diagram and test results.

</details>

<details>
<summary><strong>Julian — MQTT & Synchronization</strong> — 10 tickets</summary>

| Ticket / priority | Task |
| --- | --- |
| `MQTT-01` · **P0** | Configure Mosquitto broker, MQTT topics and communication settings |
| `MQTT-02` · **P0** | Connect Zachary's simulator to MQTT publisher and test subscriber |
| `MQTT-03` · **P0** | Implement Lamport clocks, per-bus sequence-number conventions and timestamp handling |
| `MQTT-04` · **P0** | Handle reconnects, duplicate messages and delayed/out-of-order delivery |
| `MQTT-05` · **P1** | Implement multicast-style publish-subscribe alerts and simulation commands |
| `MQTT-06` · **P1** | Test communication ordering, reliability and delivery latency |
| `MQTT-07` · **P1** | Configure MQTT authentication and topic access permissions |
| `MQTT-08` · **P1** | Implement physical-clock synchronization assumptions and simulated clock-offset tests |
| `MQTT-09` · **P2** | Evaluate causal ordering, sequence numbers and delayed-message scenarios |
| `MQTT-10` · **P1** | Test MQTT broker interruption, reconnection and recovery limitations |

Report deliverables: MQTT architecture, topic hierarchy, Lamport clock diagrams, message-ordering examples, synchronization methods, security considerations and communication performance results.

</details>

<details>
<summary><strong>Ali — Regional Edge Computing</strong> — 10 tickets</summary>

| Ticket / priority | Task |
| --- | --- |
| `EDGE-01` · **P0** | Build regional MQTT subscribers and validate incoming bus telemetry |
| `EDGE-02` · **P0** | Implement regional bus-state cache, route processing and occupancy calculations |
| `EDGE-03` · **P0** | Aggregate and forward validated regional telemetry to Carson's central server |
| `EDGE-04` · **P1** | Implement heartbeat monitoring and regional edge failure detection |
| `EDGE-05` · **P1** | Implement primary/backup edge recovery, replication and basic load balancing |
| `EDGE-06` · **P1** | Test regional processing, failure detection, backup recovery and performance |
| `EDGE-07` · **P1** | Implement workload monitoring for bus counts, throughput and processing latency |
| `EDGE-08` · **P1** | Develop workload migration and regional load-balancing policies |
| `EDGE-09` · **P1** | Implement primary/backup ownership coordination and state synchronization after recovery |
| `EDGE-10` · **P2** | Benchmark scalability, throughput, processing latency and failover duration |

Report deliverables: Edge architecture diagram, validation design, caching methodology, heartbeat and failover sequence, replication and load-balancing algorithms, performance graphs and recovery tests.

</details>

<details>
<summary><strong>Carson — Central Server & Scheduling</strong> — 10 tickets</summary>

| Ticket / priority | Task |
| --- | --- |
| `CENTRAL-01` · **P0** | Build FastAPI central server and database models for buses, routes, stops and regions |
| `CENTRAL-02` · **P0** | Implement processed regional telemetry ingestion and database updates |
| `CENTRAL-03` · **P0** | Develop route-management APIs and initial estimated arrival-time calculations |
| `CENTRAL-04` · **P1** | Implement demand-based scheduling and peak/off-peak dispatch recommendations |
| `CENTRAL-05` · **P1** | Develop system-health APIs, alerts, operator permissions and simulation endpoints |
| `CENTRAL-06` · **P1** | Test central services, route estimates, scheduling, API responses and consistency |
| `CENTRAL-07` · **P1** | Implement passenger route recommendations using origin/destination stops |
| `CENTRAL-08` · **P1** | Connect dynamic scheduling decisions to actual simulated dispatch-frequency adjustments |
| `CENTRAL-09` · **P1** | Implement role-based access restrictions and privacy safeguards |
| `CENTRAL-10` · **P2** | Evaluate ETA accuracy, scheduling effectiveness and demand-response performance |

Report deliverables: Database ER diagram, API architecture, route-management design, ETA algorithms, scheduling methodology, demand results, security/privacy and central-server performance.

</details>

<details>
<summary><strong>Danial — Passenger App & Operator Dashboard</strong> — 10 tickets</summary>

| Ticket / priority | Task |
| --- | --- |
| `UI-01` · **P0** | Create passenger app and operator dashboard wireframes and frontend structure |
| `UI-02` · **P0** | Build passenger route map, bus-location displays, stops and occupancy components |
| `UI-03` · **P0** | Integrate frontend with Carson's APIs to display live bus information |
| `UI-04` · **P1** | Implement arrival estimates, system health, alerts and operator monitoring |
| `UI-05` · **P1** | Develop simulation controls for demand surges, delays, faults and server failures |
| `UI-06` · **P1** | Test passenger/operator interfaces and capture integration screenshots |
| `UI-07` · **P1** | Implement passenger origin/destination search and route recommendations |
| `UI-08` · **P1** | Display regional topology, primary/backup edge status and failover alerts |
| `UI-09` · **P2** | Create peak/off-peak performance and dispatch recommendation visualizations |
| `UI-10` · **P2** | Prepare complete frontend demonstration walkthrough and evidence screenshots |

Report deliverables: Frontend architecture, passenger dashboard, operator dashboard, simulation controls, screenshots, user flows and usability testing.

</details>

## Shared tickets

<details>
<summary><strong>Integration, documentation and submission</strong> — 20 tickets</summary>

| Ticket / priority | Task |
| --- | --- |
| `TEAM-01` · **P0** | Set up GitHub repository, branches, shared folders and development environment |
| `TEAM-02` · **P0** | Finalize telemetry schema, MQTT topics, regional JSON contracts and central API formats |
| `TEAM-03` · **P0** | Integrate bus simulator → MQTT → regional edge server |
| `TEAM-04` · **P0** | Integrate regional edge → central server and verify database updates |
| `TEAM-05` · **P0** | Integrate central server → passenger interface |
| `TEAM-06` · **P0** | Complete first end-to-end system integration test |
| `TEAM-07` · **P1** | Test delayed messages, duplicate events and regional edge failover |
| `TEAM-08` · **P1** | Test dynamic demand scheduling and simulation controls |
| `TEAM-09` · **P1** | Capture integration screenshots, metrics and testing evidence |
| `TEAM-10` · **P1** | Complete report sections, diagrams, findings, references and contribution matrix draft |
| `TEAM-11` · **P0** | Finalize README, verify clean installation, submit code and final report |
| `TEAM-12` · **P2** | Prepare demonstration script and individual speaking sections |
| `TEAM-13` · **P0** | Verify independent multi-process deployment across two regions |
| `TEAM-14` · **P1** | Test complete scheduling feedback loop, from demand detection to adjusted dispatch frequency |
| `TEAM-15` · **P1** | Test failure recovery during delayed, duplicated and temporarily unavailable messages |
| `TEAM-16` · **P2** | Run scalability benchmarks across different simulated fleet sizes |
| `TEAM-17` · **P2** | Compare system architecture and capabilities with existing public transit solutions |
| `TEAM-18` · **P1** | Evaluate security, privacy, fault tolerance, scalability and architectural limitations |
| `TEAM-19` · **P1** | Finalize individual contribution percentages and supporting implementation evidence |
| `TEAM-20` · **P2** | Create PowerPoint slides, rehearse the 10-minute presentation and prepare Q&A answers |

</details>

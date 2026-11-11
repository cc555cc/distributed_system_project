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

Put service tests in that service's `tests/` directory, end-to-end tests in root `tests/`, and report evidence in `docs/`. Coordinate shared contract changes through TEAM-02. The existing central subscriber is implemented; simulator, regional edge and frontend implementations remain unfinished.

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

Existing central startup, ingestion and route lookup cover parts of CENTRAL-01–03; their remaining requirements are not complete.

Status: ✅ Done · 🟨 Partial · ⬜ Pending. Mark a task Done only after its implementation and relevant checks are complete; commit and push the README update. Pending means completion has not been verified.

<details>
<summary><strong>Zachary — Bus Simulation</strong> — 11 tickets</summary>

| Ticket / priority | Task | Status |
| --- | --- | --- |
| `BUS-01` · **P0** | Define shared JSON telemetry schema and example sensor messages | ⬜ Pending |
| `BUS-02` · **P0** | Create simulated bus routes, stops, coordinates and regional assignments | ⬜ Pending |
| `BUS-03` · **P0** | Develop Python bus simulator for continuous GPS and sensor data generation | ⬜ Pending |
| `BUS-04` · **P1** | Implement peak/off-peak demand, traffic delays and sensor-failure scenarios | ⬜ Pending |
| `BUS-05` · **P1** | Build simulation controls for starting, stopping and modifying scenarios | ⬜ Pending |
| `BUS-06` · **P1** | Test simulator reliability, multi-bus operation and sensor data validity | ⬜ Pending |
| `BUS-07` · **P1** | Implement bus lifecycle events: boarding, arriving, departing and out of service | ⬜ Pending |
| `BUS-08` · **P1** | Support configurable fleet sizes: 5, 10, 25 and 50 buses | ⬜ Pending |
| `BUS-09` · **P1** | Implement simulated dispatch-frequency adjustments from Carson's scheduler | ⬜ Pending |
| `BUS-10` · **P2** | Create reproducible simulation test scenarios with expected outputs | ⬜ Pending |
| `BUS-11` · **P1** | Support registering new buses and sensor types using versioned configuration/schema objects | ⬜ Pending |

Report deliverables: Telemetry schema, bus simulation design, route definitions, simulation scenarios, bus state diagram and test results.

</details>

<details>
<summary><strong>Julian — MQTT & Synchronization</strong> — 11 tickets</summary>

| Ticket / priority | Task | Status |
| --- | --- | --- |
| `MQTT-01` · **P0** | Configure Mosquitto broker, MQTT topics and communication settings | ⬜ Pending |
| `MQTT-02` · **P0** | Connect Zachary's simulator to MQTT publisher and test subscriber | ⬜ Pending |
| `MQTT-03` · **P0** | Implement Lamport clocks, per-bus sequence-number conventions and timestamp handling | ⬜ Pending |
| `MQTT-04` · **P0** | Handle reconnects, duplicate messages and delayed/out-of-order delivery | ⬜ Pending |
| `MQTT-05` · **P1** | Implement multicast-style publish-subscribe alerts and simulation commands | ⬜ Pending |
| `MQTT-06` · **P1** | Test communication ordering, reliability and delivery latency | ⬜ Pending |
| `MQTT-07` · **P1** | Configure MQTT authentication and topic access permissions | ⬜ Pending |
| `MQTT-08` · **P1** | Implement physical-clock synchronization assumptions and simulated clock-offset tests | ⬜ Pending |
| `MQTT-09` · **P2** | Evaluate causal ordering, sequence numbers and delayed-message scenarios | ⬜ Pending |
| `MQTT-10` · **P1** | Test MQTT broker interruption, reconnection and recovery limitations | ⬜ Pending |
| `MQTT-11` · **P1** | Stress-test concurrent telemetry, Lamport clocks, per-bus ordering, queue delays and message throughput | ⬜ Pending |

Report deliverables: MQTT architecture, topic hierarchy, Lamport clock diagrams, message-ordering examples, synchronization methods, security considerations and communication performance results.

</details>

<details>
<summary><strong>Ali — Regional Edge Computing</strong> — 12 tickets</summary>

| Ticket / priority | Task | Status |
| --- | --- | --- |
| `EDGE-01` · **P0** | Build regional MQTT subscribers and validate incoming bus telemetry | ⬜ Pending |
| `EDGE-02` · **P0** | Implement regional bus-state cache, route processing and occupancy calculations | ⬜ Pending |
| `EDGE-03` · **P0** | Aggregate and forward validated regional telemetry to Carson's central server | ⬜ Pending |
| `EDGE-04` · **P1** | Implement heartbeat monitoring and regional edge failure detection | ⬜ Pending |
| `EDGE-05` · **P1** | Implement primary/backup edge recovery, replication and basic load balancing | ⬜ Pending |
| `EDGE-06` · **P1** | Test regional processing, failure detection, backup recovery and performance | ⬜ Pending |
| `EDGE-07` · **P1** | Implement workload monitoring for bus counts, throughput and processing latency | ⬜ Pending |
| `EDGE-08` · **P1** | Develop workload migration and regional load-balancing policies | ⬜ Pending |
| `EDGE-09` · **P1** | Implement primary/backup ownership coordination and state synchronization after recovery | ⬜ Pending |
| `EDGE-10` · **P2** | Benchmark scalability, throughput, processing latency and failover duration | ⬜ Pending |
| `EDGE-11` · **P2** | Implement threshold-based horizontal scaling: launch Edge C, transfer bus assignments and safely scale down | ⬜ Pending |
| `EDGE-12` · **P1** | Extend workload ownership to dynamically added and cross-region servers, including synchronized recovery | ⬜ Pending |

Report deliverables: Edge architecture diagram, validation design, caching methodology, heartbeat and failover sequence, replication and load-balancing algorithms, performance graphs and recovery tests.

</details>

<details>
<summary><strong>Carson — Central Server & Scheduling</strong> — 12 tickets</summary>

| Ticket / priority | Task | Status |
| --- | --- | --- |
| `CENTRAL-01` · **P0** | Build FastAPI central server and database models for buses, routes, stops and regions | 🟨 Partial |
| `CENTRAL-02` · **P0** | Implement processed regional telemetry ingestion and database updates | 🟨 Partial |
| `CENTRAL-03` · **P0** | Develop route-management APIs and initial estimated arrival-time calculations | 🟨 Partial |
| `CENTRAL-04` · **P1** | Implement demand-based scheduling and peak/off-peak dispatch recommendations | ⬜ Pending |
| `CENTRAL-05` · **P1** | Develop system-health APIs, alerts, operator permissions and simulation endpoints | ⬜ Pending |
| `CENTRAL-06` · **P1** | Test central services, route estimates, scheduling, API responses and consistency | ⬜ Pending |
| `CENTRAL-07` · **P1** | Implement passenger route recommendations using origin/destination stops | ⬜ Pending |
| `CENTRAL-08` · **P1** | Connect dynamic scheduling decisions to actual simulated dispatch-frequency adjustments | ⬜ Pending |
| `CENTRAL-09` · **P1** | Implement role-based access restrictions and privacy safeguards | ⬜ Pending |
| `CENTRAL-10` · **P2** | Evaluate ETA accuracy, scheduling effectiveness and demand-response performance | ⬜ Pending |
| `CENTRAL-11` · **P2** | Separate ingestion, route/scheduling logic and API handling so individual workloads can scale | ⬜ Pending |
| `CENTRAL-12` · **P1** | Build `suggestions.py` for ETA-, occupancy-, and demand-aware passenger recommendations | ⬜ Pending |

Report deliverables: Database ER diagram, API architecture, route-management design, ETA algorithms, scheduling methodology, demand results, security/privacy and central-server performance.

</details>

<details>
<summary><strong>Danial — Passenger App & Operator Dashboard</strong> — 11 tickets</summary>

| Ticket / priority | Task | Status |
| --- | --- | --- |
| `UI-01` · **P0** | Create passenger app and operator dashboard wireframes and frontend structure | ⬜ Pending |
| `UI-02` · **P0** | Build passenger route map, bus-location displays, stops and occupancy components | ⬜ Pending |
| `UI-03` · **P0** | Integrate frontend with Carson's APIs to display live bus information | ⬜ Pending |
| `UI-04` · **P1** | Implement arrival estimates, system health, alerts and operator monitoring | ⬜ Pending |
| `UI-05` · **P1** | Develop simulation controls for demand surges, delays, faults and server failures | ⬜ Pending |
| `UI-06` · **P1** | Test passenger/operator interfaces and capture integration screenshots | ⬜ Pending |
| `UI-07` · **P1** | Implement passenger origin/destination search and route recommendations | ⬜ Pending |
| `UI-08` · **P1** | Display regional topology, primary/backup edge status and failover alerts | ⬜ Pending |
| `UI-09` · **P2** | Create peak/off-peak performance and dispatch recommendation visualizations | ⬜ Pending |
| `UI-10` · **P2** | Prepare complete frontend demonstration walkthrough and evidence screenshots | ⬜ Pending |
| `UI-11` · **P1** | Display intelligent recommendations, alternative buses, waiting times and crowding information | ⬜ Pending |

Report deliverables: Frontend architecture, passenger dashboard, operator dashboard, simulation controls, screenshots, user flows and usability testing.

</details>

## Shared tickets

<details>
<summary><strong>Integration, documentation and submission</strong> — 20 tickets</summary>

| Ticket / priority | Task | Status |
| --- | --- | --- |
| `TEAM-01` · **P0** | Set up GitHub repository, branches, shared folders and development environment | ⬜ Pending |
| `TEAM-02` · **P0** | Finalize telemetry schema, MQTT topics, regional JSON contracts and central API formats | ⬜ Pending |
| `TEAM-03` · **P0** | Integrate bus simulator → MQTT → regional edge server | ⬜ Pending |
| `TEAM-04` · **P0** | Integrate regional edge → central server and verify database updates | ⬜ Pending |
| `TEAM-05` · **P0** | Integrate central server → passenger interface | ⬜ Pending |
| `TEAM-06` · **P0** | Complete first end-to-end system integration test | ⬜ Pending |
| `TEAM-07` · **P1** | Test delayed messages, duplicate events and regional edge failover | ⬜ Pending |
| `TEAM-08` · **P1** | Test dynamic demand scheduling and simulation controls | ⬜ Pending |
| `TEAM-09` · **P1** | Capture integration screenshots, metrics and testing evidence | ⬜ Pending |
| `TEAM-10` · **P1** | Complete report sections, diagrams, findings, references and contribution matrix draft | ⬜ Pending |
| `TEAM-11` · **P0** | Finalize README, verify clean installation, submit code and final report | ⬜ Pending |
| `TEAM-12` · **P2** | Prepare demonstration script and individual speaking sections | ⬜ Pending |
| `TEAM-13` · **P0** | Verify independent multi-process deployment across two regions | ⬜ Pending |
| `TEAM-14` · **P1** | Test complete scheduling feedback loop, from demand detection to adjusted dispatch frequency | ⬜ Pending |
| `TEAM-15` · **P1** | Test failure recovery during delayed, duplicated and temporarily unavailable messages | ⬜ Pending |
| `TEAM-16` · **P2** | Run scalability benchmarks across different simulated fleet sizes | ⬜ Pending |
| `TEAM-17` · **P2** | Compare system architecture and capabilities with existing public transit solutions | ⬜ Pending |
| `TEAM-18` · **P1** | Evaluate security, privacy, fault tolerance, scalability and architectural limitations | ⬜ Pending |
| `TEAM-19` · **P1** | Finalize individual contribution percentages and supporting implementation evidence | ⬜ Pending |
| `TEAM-20` · **P2** | Create PowerPoint slides, rehearse the 10-minute presentation and prepare Q&A answers | ⬜ Pending |

</details>

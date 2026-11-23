# Frontend

The main interfaces are:

| Page | Audience and purpose |
|---|---|
| `user.html` | Passenger map, routes, stops, bus tracking and occupancy |
| `control.html` | Operator simulation, fleet, dispatch and failure controls |
| `health.html` | Read-only bus/sensor status, route capacity, edge workload and alerts |

`operator.html` remains a detailed read-only overview linked from the control panel. `index.html` redirects to `user.html`; the old `controls.html` URL redirects to `control.html`. Central serves these pages and enforces operator access on both HTML and APIs.

## Run

From the repository root, with the configured database and broker running:

    python -m uvicorn services.central.main:app --host 127.0.0.1 --port 8001

Then open http://127.0.0.1:8001/user.html. This page is public. Control, health and operator pages require login: demo username `operator`, password `TransitDemo123!`. Override these with `OPERATOR_USERNAME` and `OPERATOR_PASSWORD`. Sessions expire after one hour and are revoked on sign-out; restarting central clears them. This is a single-process classroom login, with public passenger access and an operator role. For HTTPS deployments set `COOKIE_SECURE=true` and supply your own credentials.

## Mock data or real server

Open js/config.js.

- USE_MOCK = true shows fake buses, no server needed.
- USE_MOCK = false uses the same central origin that serves the page. Do not serve operator HTML through a separate unprotected static server.

## Repeatable demonstration

1. Open the public passenger page and the operator control page. Select the controlled demonstration feed. Central starts the demo clock by default (`DEMO_AUTOSTART=false` disables this).
2. Select a stop ahead of a bus and optionally a later destination on its route. Compare arrival times, available seats and recommendations. Full buses remain visible but are not recommended for a group without enough seats. Estimates require current valid telemetry.
3. Apply peak demand to that route in Control. Counts vary within capacity using a repeatable per-bus random generator. After three distinct fresh samples, approve the service recommendation or enable automatic demo scheduling. Observe the queued bus, dispatch event and subsequent central sequence confirmation.
4. Open Health to compare recorded normal/peak occupancy, speed and waiting demand. Inspect measured arrival error, sensor heartbeat ages, validation outcomes, mapping contract and central acknowledgements. These counters describe the in-memory demonstration, not PostgreSQL writes.
5. Apply traffic, sensor, connection or edge failure conditions. Compare passenger availability with Health; restore the component and verify current readings return. Unknown causes remain unknown rather than being labelled crashes.
6. Sign out. Passenger tracking remains public; operator pages redirect to login and management APIs reject unauthenticated requests.

Journeys support direct routes and one transfer between shared stop IDs in the selected area. Times use ordered stop geometry, current speed, demo traffic and stop dwell; they are estimates rather than road-navigation predictions. Service changes here control the demonstration. Native live MQTT dispatch, durable shared login sessions and controlled scheduling-effect evaluation remain separate work.

## Bus Health

Open `health.html` or use **Bus Health** in the navigation. Select the live or controlled-demo data source, region, route, bus and sensor. This page requires the central API, including when legacy mock mode is enabled. It displays backend health evidence, current route capacity samples, edge workload and alerts; it does not issue management commands. Use `control.html` to inject faults or run the existing scheduling/balancing demonstration. Missing reports are shown as unknown or unavailable, not as confirmed hardware failures.

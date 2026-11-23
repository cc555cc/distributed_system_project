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

Then open http://127.0.0.1:8001/user.html. This page is public. Control, health and operator pages require login: demo username `ali`, password `123`. Override these with `OPERATOR_USERNAME` and `OPERATOR_PASSWORD`. Sessions expire after one hour and are revoked on sign-out; restarting central clears them. This is a single-process classroom login, with public passenger access and an operator role. For HTTPS deployments set `COOKIE_SECURE=true` and supply your own credentials.

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

Journeys support direct routes and one transfer between shared stop IDs in the selected area. Times use ordered stop geometry, current speed, demo traffic and stop dwell; they are estimates rather than road-navigation predictions. Service changes here control the demonstration. Native live MQTT dispatch and durable shared login sessions remain separate work. Matched scheduling-effect evaluation is covered by the central tests.

## Bus Health

Open `health.html` or use **Bus Health** in the navigation. Select the live or controlled-demo data source, region, route, bus and sensor. This page requires the central API, including when legacy mock mode is enabled. It displays backend health evidence, current route capacity samples, edge workload and alerts; it does not issue management commands. Use `control.html` to inject faults or run the existing scheduling/balancing demonstration. Missing reports are shown as unknown or unavailable, not as confirmed hardware failures.

## Fleet limits, pressure and visible feedback

Set **Fleet limit = 50** and **Active buses = 20** to retain 30 reserve buses. Reserves do not move or publish sensor requests until dispatched. The fleet limit caps registration; it is not queue occupancy. Setting both values to 50 starts 50 streams. With the demonstration's 100 ms processing setting, east receives more traffic and activates its third edge after sustained backlog.

For a short balancing demonstration, start 20 buses, select West, processing **250 ms**, and send **100 requests**. Watch the queue bars reach the 70% threshold, the third edge activate after 6 simulation seconds, assignments transfer, the accepted batch drains and ownership returns before standby. Slower processing can exceed even three workers' capacity; overload remains visible instead of being reported as fixed.

Health and Control refresh their shared overview every second. Sensor faults appear in **Sensor issues - all buses**, even if a different bus is selected. The passenger page removes unreliable predictions. Restore the bus to watch healthy readings return. Use **Custom %** to set route occupancy, such as 50%, 80% or 100%; these values enter normal telemetry before reaching passenger recommendations.

## Named use cases

Use the buttons at the top of Control instead of entering request counts. **Show load balancing** demonstrates one batch at a 70% crossing, then standby; it rearms only after both primary queues fall below 70%. **Show scheduling loop** resets the demo, applies peak demand and enables the real demo scheduler. The result table shows the decision, departure interval and central confirmation. **Check access security** verifies actual unauthenticated responses without signing out.

Click the Passenger map or **Try a nearby location** for pickup recommendations at real nearby stops. Walking time is estimated using straight-line distance; buses that arrive before walking plus a boarding margin are excluded. A no-service result is valid and never filled with guessed buses.

## Clickable failure-case table

Control starts with **Combined failure tests**. Each row has **Run** and **Evidence**; **Run all situations** verifies all nine cases. Run live starts immediately and follows the central elapsed-time clock. Health and Passenger update every second while the situation unfolds; the demo keeps moving after its checks complete. Expand the evidence for actual per-second queues, owners, sensors and delivery counts. A pass is based on those observations, never just a successful command response. Use **Normal service** to reset the demo, or the visible clock controls to pause/resume and change speed.

The total-loss case is expected to remain unavailable: cached central positions are last-known data, and lost replica sequence floors are not invented. The table marks that correct fail-closed response as PASS.

## Shared real-time clock and navigation

Speed and pause are visible above the scenario table. One second at 1x advances one source second; 5x advances five. A three-second normal baseline precedes each live failure injection. Run all queues the situations on this same clock. Completed evidence is retained while the fleet continues moving. Manual scenario changes interrupt the prior script instead of letting old failure actions leak into the new scene.

Health shows current speed, source time and live case progress with its queues and sensor reports. Passenger shows that same clock and refreshes arrivals/recommendations from current central state. Its navigation offers Operator login when signed out, and Control/Health/Sign out for an authenticated operator. Login/logout changes propagate to the other open pages; passenger data remains public.

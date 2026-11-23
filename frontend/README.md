# Frontend

The main interfaces are:

| Page | Audience and purpose |
|---|---|
| `user.html` | Passenger map, routes, stops, bus tracking and occupancy |
| `control.html` | Operator simulation, fleet, dispatch and failure controls |
| `health.html` | Read-only bus/sensor status, route capacity, edge workload and alerts |

`operator.html` remains a detailed read-only overview linked from the control panel. `index.html` redirects to `user.html`; the old `controls.html` URL redirects to `control.html`. All pages reuse the existing JavaScript; the redirects contain no duplicate application logic. Page separation does not implement the unfinished backend authorization ticket.

## Run

From this folder:

    python -m http.server 5500

Then open http://localhost:5500

## Mock data or real server

Open js/config.js.

- USE_MOCK = true shows fake buses, no server needed.
- USE_MOCK = false uses the central server at API_URL (default http://localhost:8000). The server needs CORS turned on for http://localhost:5500.

## Bus Health

Open `health.html` or use **Bus Health** in the navigation. Select the live or controlled-demo data source, region, route, bus and sensor. This page requires the central API, including when legacy mock mode is enabled. It displays backend health evidence, current route capacity samples, edge workload and alerts; it does not issue management commands. Use `control.html` to inject faults or run the existing scheduling/balancing demonstration. Missing reports are shown as unknown or unavailable, not as confirmed hardware failures.

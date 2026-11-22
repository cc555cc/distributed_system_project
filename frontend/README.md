# Frontend

Passenger map, operator dashboard and simulation controls.

## Run

From this folder:

    python -m http.server 5500

Then open http://localhost:5500

## Mock data or real server

Open js/config.js.

- USE_MOCK = true shows fake buses, no server needed.
- USE_MOCK = false uses the central server at API_URL (default http://localhost:8000). The server needs CORS turned on for http://localhost:5500.

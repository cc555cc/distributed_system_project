const edgeTable = document.getElementById("edge-table");
const messageBox = document.getElementById("message");
const resetButton = document.getElementById("reset-button");
const demoBus = document.getElementById("demo-bus");
const demoResult = document.getElementById("demo-result");
let demoMap = null;
let demoMarkers = null;
let demoRoute = null;
let selectedRoute = null;
let fleetRoutesLoaded = false;
if (typeof L !== "undefined") {
  demoMap = L.map("demo-map").setView([43.9, -78.9], 11);
  L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors", maxZoom: 19
  }).addTo(demoMap);
  demoMarkers = L.layerGroup().addTo(demoMap);
  demoRoute = L.layerGroup().addTo(demoMap);
} else {
  document.getElementById("demo-map").textContent = "Map library unavailable. Positions and distances remain in the table below.";
}

async function drawDemo(simulation) {
  if (!fleetRoutesLoaded) {
    const routes = await getJson("/health/simulation/routes");
    ["new-bus-route", "schedule-route"].forEach(function (name) {
      const select = document.getElementById(name);
      select.replaceChildren(...routes.map(function (route) {
        const option = document.createElement("option");
        option.value = route.route_id;
        option.textContent = route.route_id + " — " + route.name;
        return option;
      }));
    });
    fleetRoutesLoaded = true;
  }
  const ids = Object.keys(simulation.demo_buses);
  if (demoBus.dataset.ids !== ids.join(",")) {
    const selected = demoBus.value;
    demoBus.dataset.ids = ids.join(",");
    demoBus.replaceChildren(...ids.map(function (id) {
      const option = document.createElement("option");
      option.value = id;
      option.textContent = id + " (" + simulation.demo_buses[id].region_id + ")";
      return option;
    }));
    if (ids.includes(selected)) demoBus.value = selected;
  }
  document.getElementById("schedule-status").textContent = Object.entries(simulation.schedules).map(function ([route, schedule]) {
    return route + ": every " + schedule.headway_seconds + " s; queued " +
      (schedule.pending.join(", ") || "none") + "; next slot " + (schedule.next_departure ?? "not set");
  }).join(" · ") || "No dispatch requests yet.";
  document.getElementById("demo-summary").textContent =
    (simulation.demo_running ? "Buses running" : "Buses stopped intentionally") + " · Central " +
    (simulation.central_available ? "available" : "unavailable") + " · " + simulation.central_pending + " pending central updates";
  const id = demoBus.value;
  const bus = simulation.demo_buses[id];
  if (!bus) {
    document.getElementById("demo-details").textContent = "No demonstration readings yet.";
    if (demoMarkers) demoMarkers.clearLayers();
    if (demoRoute) demoRoute.clearLayers();
    selectedRoute = null;
    return;
  }
  const central = simulation.central_states[id];
  const gps = bus.local;
  const position = function (lat, lon) { return lat.toFixed(5) + ", " + lon.toFixed(5); };
  const rows = [
    ["Condition", bus.condition], ["Bus phase", bus.phase], ["Observed health", simulation.demo_running ? bus.status : "stopped intentionally"],
    ["Evidence", bus.evidence], ["Sensors", Object.entries(bus.sensors).map(function ([sensor, state]) { return sensor + ": " + state; }).join(", ")],
    ["Local distance", Math.round(bus.distance_m) + " m"],
    ["Local GPS", gps ? position(gps.latitude, gps.longitude) : "No local reading"],
    ["Last central position", central ? position(central.lat, central.lon) : "Not received"],
    ["Last received speed", central ? central.speed + " km/h" : "Unavailable"],
    ["Heartbeat age", bus.heartbeat_age_seconds.toFixed(1) + " simulation s"],
    ["Bus delivery", bus.delivery_pending ? "Pending; retry at " + bus.retry_at.toFixed(1) + " s" : "No pending reading"],
    ["Attempts / rejected readings", bus.attempts + " / " + bus.rejected_readings],
    ["Current edge", simulation.ownership[id].edge_id],
    ["Last central sequence", central ? central.sequence_number : "Not received"]
  ];
  document.getElementById("demo-details").innerHTML = tableHtml(["Measurement", "Value"],
    rows.map(function (row) { return row.map(escapeHtml); }));
  if (!demoMap) return;
  demoMarkers.clearLayers();
  Object.entries(simulation.central_states).forEach(function ([name, state]) {
    const health = simulation.demo_buses[name]?.status;
    L.circleMarker([state.lat, state.lon], { radius: name === id ? 8 : 4,
      color: health === "healthy" || health === "recovering" ? "#0275d8" : "#777", fillOpacity: 0.8 })
      .bindPopup(escapeHtml(name) + " · last central position").addTo(demoMarkers);
  });
  if (gps) L.circleMarker([gps.latitude, gps.longitude], {radius: 11, color: "#e08a00", fillOpacity: 0.1})
    .bindPopup(escapeHtml(id) + " · local GPS").addTo(demoMarkers);
  if (selectedRoute !== bus.route_id) {
    const route = bus.route_id;
    selectedRoute = route;
    try {
      const stops = await getJson("/health/simulation/routes/" + route + "/stops");
      if (selectedRoute !== route) return;
      demoRoute.clearLayers();
      const points = stops.filter(function (stop) { return stop.direction_id === 0; }).map(function (stop) { return [stop.lat, stop.lon]; });
      L.polyline(points, {color: "#0275d8", weight: 2}).addTo(demoRoute);
      demoMap.fitBounds(points);
    } catch (error) {
      selectedRoute = null;
      demoResult.textContent = "Route unavailable: " + error.message;
    }
  }
}

document.getElementById("demo-controls").addEventListener("click", async function (event) {
  const button = event.target.closest("button");
  if (!button) return;
  const body = {
    "demo-start": {running: true}, "demo-stop": {running: false},
    "demo-apply": {bus_id: demoBus.value, condition: document.getElementById("demo-condition").value},
    "demo-restore": {bus_id: demoBus.value, condition: "normal"},
    "central-fail": {central_available: false}, "central-restore": {central_available: true}
  }[button.id];
  const fleetAction = ["fleet-apply", "bus-register", "headway-apply", "bus-dispatch"].includes(button.id);
  if (!body && !fleetAction) return;
  button.disabled = true;
  try {
    if (button.id === "fleet-apply") {
      await resetSimulation(Number(document.getElementById("fleet-size").value));
      await configureDemo({running: true});
    } else if (button.id === "bus-register") {
      await sendPost("/health/simulation/buses", {
        schema_version: "bus-config-v1", bus_id: document.getElementById("new-bus-id").value.trim(),
        route_id: document.getElementById("new-bus-route").value,
        capacity: Number(document.getElementById("new-bus-capacity").value), direction_id: 0
      });
    } else if (button.id === "headway-apply" || button.id === "bus-dispatch") {
      const request = {route_id: document.getElementById("schedule-route").value,
        action: button.id === "bus-dispatch" ? "dispatch" : "set_headway"};
      if (request.action === "dispatch") request.bus_id = demoBus.value;
      else request.headway_seconds = Number(document.getElementById("headway").value);
      await sendPost("/health/simulation/schedule", request);
    } else {
      await configureDemo(body);
    }
    chooseDemo();
    demoResult.textContent = "Applied. Watch the readings and event history for the result.";
    await loadWorkers();
  } catch (error) {
    demoResult.textContent = "Could not apply: " + error.message;
  } finally {
    button.disabled = false;
  }
});
demoBus.addEventListener("change", loadWorkers);

function showMessage(text, isError) {
  messageBox.textContent = text;
  messageBox.className = isError ? "message error" : "message";
}

async function loadWorkers() {
  try {
    const simulation = await getSimulation();
    edgeTable.innerHTML = simulationTable(simulation, true);
    document.getElementById("simulation-clock").textContent = simulationClockText(simulation);
    await drawDemo(simulation);
  } catch (error) {
    console.error("Could not load simulation", error);
    document.getElementById("demo-summary").textContent = "Backend unavailable. Positions below are the last received readings.";
    document.getElementById("simulation-clock").textContent = "Clock status unavailable.";
    edgeTable.textContent = "Could not load the edge servers: " + error.message;
  }
}

edgeTable.addEventListener("click", async function (event) {
  const button = event.target.closest("button");
  if (!button) {
    return;
  }
  const edgeId = button.dataset.id;

  try {
    if (button.dataset.action === "fail") {
      await failEdge(edgeId);
      showMessage(edgeId + " was failed.", false);
    } else {
      await recoverEdge(edgeId);
      showMessage(edgeId + " is recovering.", false);
    }
  } catch (error) {
    showMessage("Could not do that: " + error.message, true);
  }
  loadWorkers();
});

document.getElementById("clock-controls").addEventListener("click", async function (event) {
  const button = event.target.closest("button");
  if (!button) return;
  try {
    await setSimulationClock(button.dataset.running === "true", Number(document.getElementById("clock-speed").value));
    await loadWorkers();
  } catch (error) {
    showMessage(error.message, true);
  }
});

document.getElementById("bus-controls").addEventListener("click", async function (event) {
  const button = event.target.closest("button[data-command]");
  if (!button) return;
  const action = button.dataset.command;
  const params = {};
  if (action === "set_demand") params.demand = document.getElementById("bus-demand").value;
  if (action === "set_traffic") params.traffic_factor = Number(document.getElementById("bus-traffic").value);
  if (action === "set_sensor_failure_rate") params.sensor_failure_rate = Number(document.getElementById("bus-fault").value);
  button.disabled = true;
  try {
    const result = await sendBusCommand(document.getElementById("bus-region").value, action, params);
    showMessage("Command published: " + action + " (" + result.command_id + "). Check live telemetry for the result.", false);
  } catch (error) {
    showMessage("Command failed: " + error.message, true);
  } finally {
    button.disabled = false;
  }
});

resetButton.addEventListener("click", async function () {
  try {
    await resetSimulation();
    showMessage("Simulation was reset.", false);
  } catch (error) {
    showMessage("Could not reset: " + error.message, true);
  }
  loadWorkers();
});

loadWorkers();
setInterval(loadWorkers, 1000);

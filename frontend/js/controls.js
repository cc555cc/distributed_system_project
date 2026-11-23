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
    ["new-bus-route", "schedule-route", "demand-route"].forEach(function (name) {
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
      await resetSimulation(Number(document.getElementById("fleet-size").value), Number(document.getElementById("active-buses").value), 100);
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
    await renderSystemOverview(demoView() ? "demo" : "live");
    const simulation = await getSimulation();
    edgeTable.innerHTML = simulationTable(simulation, true);
    document.getElementById("simulation-clock").textContent = simulationClockText(simulation);
    await drawDemo(simulation);
    const demand = await getJson("/scheduling/status?source=demo");
    const selectedDemand = demand.routes.find(r => r.route_id === document.getElementById('demand-route').value);
    const latest = demand.history.filter(h => h.route_id === selectedDemand?.route_id).slice(-1)[0];
    document.getElementById('scenario-status').innerHTML = selectedDemand ? tableHtml(['Route', 'Occupancy', 'Interval', 'Decision', 'Result'], [[
      escapeHtml(selectedDemand.route_id), selectedDemand.occupancy_percent === null ? 'Waiting for readings' : Math.round(selectedDemand.occupancy_percent) + '%',
      selectedDemand.headway_seconds + ' s', selectedDemand.recommendation ? escapeHtml(selectedDemand.recommendation.action.replaceAll('_',' ') + ' / ' + selectedDemand.recommendation.status) : selectedDemand.samples + '/3 samples',
      latest ? escapeHtml(latest.execution) : 'No dispatch yet'
    ]]) : '';
    document.getElementById("demand-status").innerHTML = tableHtml(["Route", "Occupancy", "Waiting", "Samples", "Interval", "Recommendation", "Action"],
      demand.routes.map(function (route) {
        const decision = route.recommendation;
        return [escapeHtml(route.route_id), route.occupancy_percent === null ? "Unavailable" : Math.round(route.occupancy_percent) + "%",
          route.waiting_passengers ?? "Unavailable", route.samples + " / 3", route.headway_seconds + " s",
          decision ? escapeHtml(decision.action.replaceAll("_", " ") + " · " + decision.status) : "No sustained signal",
          decision?.status === "pending" ? '<button data-approve="' + escapeHtml(route.route_id) + '">Approve</button>' : "-"];
      }));
    document.getElementById("demand-execution").textContent = demand.history.slice(-3).map(function (decision) {
      return "Route " + decision.route_id + ": " + decision.execution +
        (decision.confirmation_sequence ? " (sequence " + decision.confirmation_sequence + ")" : "") +
        "; occupancy " + Math.round(decision.occupancy_before) + "% → " +
        (decision.occupancy_after === null ? "unavailable" : Math.round(decision.occupancy_after) + "%") +
        "; waiting " + decision.waiting_before + " → " + decision.waiting_after;
    }).join(" · ");
  } catch (error) {
    console.error("Could not load simulation", error);
    document.getElementById("demo-summary").textContent = "Backend unavailable. Positions below are the last received readings.";
    document.getElementById("simulation-clock").textContent = "Clock status unavailable.";
    edgeTable.textContent = "Could not load the edge servers: " + error.message;
  }
}

document.getElementById("demand-controls").addEventListener("click", async function (event) {
  const button = event.target.closest("button");
  if (!button) return;
  button.disabled = true;
  const feedback = document.getElementById("demand-feedback");
  try {
    let result;
    if (button.dataset.approve) result = await sendPost("/scheduling/" + encodeURIComponent(button.dataset.approve) + "/approve");
    else if (button.id === "demand-apply") result = await sendPost("/scheduling/demand", {route_id: document.getElementById("demand-route").value, profile: document.getElementById("demand-profile").value, occupancy_percent: Number(document.getElementById("demand-occupancy").value)});
    else result = await sendPost("/scheduling/policy", {automatic: button.id === "automatic-on"});
    chooseDemo();
    feedback.textContent = result.feedback || (result.profile ? "Demand profile applied: " + result.profile + ". Waiting for sustained samples." : "Automatic scheduling: " + result.automatic);
    await loadWorkers();
  } catch (error) { feedback.textContent = error.message; }
  finally { button.disabled = false; }
});

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

// A burst follows the real telemetry path and does not create extra buses.
document.getElementById('load-send').addEventListener('click', async function () {
  const feedback = document.getElementById('load-feedback');
  try {
    const r = await sendPost('/health/simulation/workload', {region_id: document.getElementById('load-region').value,
      count: Number(document.getElementById('load-count').value), processing_ms: Number(document.getElementById('load-processing').value)});
    chooseDemo(); feedback.textContent = r.accepted + ' accepted | ' + r.rejected + ' backpressured | watch third edge';
    await loadWorkers();
  } catch (error) { feedback.textContent = error.message; }
});

// Named use cases call existing backend controls, so every page sees the same experiment.
document.getElementById('scenario-controls').addEventListener('click', async function (event) {
  const button = event.target.closest('button[data-scenario]');
  if (!button) return;
  const output = document.getElementById('scenario-feedback');
  const actions = document.querySelectorAll('#scenario-controls button');
  actions.forEach(b => b.disabled = true);
  output.textContent = 'Applying ' + button.textContent.toLowerCase() + '...';
  try {
    if (button.dataset.scenario === 'security') {
      // Deliberately omit the session; this proves access is enforced on the backend.
      const responses = await Promise.all([
        fetch('/user.html', {credentials:'omit'}), fetch('/health/overview', {credentials:'omit'}),
        fetch('/health/simulation/workload', {method:'POST', credentials:'omit', headers:{'Content-Type':'application/json'}, body:JSON.stringify({region_id:'west',count:1})})
      ]);
      output.textContent = responses[0].status === 200 && responses[1].status === 401 && responses[2].status === 401 ?
        'PASS: passenger public; health and commands reject unauthenticated access.' : 'Access check failed: ' + responses.map(r => r.status).join('/');
    } else {
      const payload = ['normal','capacity','balancing','scheduling'].includes(button.dataset.scenario) ? {} : {bus_id:demoBus.value || undefined};
      const result = await sendPost('/health/simulation/scenarios/' + button.dataset.scenario, payload);
      chooseDemo(); output.textContent = result.message;
      await loadWorkers();
    }
  } catch (error) { output.textContent = error.message; }
  finally { actions.forEach(b => b.disabled = false); }
});


// The case table reads backend checks and per-second evidence; it never assumes a click passed.
async function loadCombinedCases() {
  const cases = await getJson('/health/simulation/cases');
  document.getElementById('case-table').innerHTML = tableHtml(['Situation', 'Expected response', 'Result', 'Test'], cases.map(function (c) {
    return [escapeHtml(c.title), escapeHtml(c.expected), c.result ? tag(c.result.passed ? 'PASS' : 'FAIL', c.result.passed ? 'good' : 'bad') : 'Not run',
      '<button data-case="' + escapeHtml(c.id) + '" aria-label="Run ' + escapeHtml(c.title) + '">Run</button> ' +
      '<button data-evidence="' + escapeHtml(c.id) + '" aria-label="Evidence for ' + escapeHtml(c.title) + '"' + (c.result ? '' : ' disabled') + '>Evidence</button>'];
  }));
  return cases;
}
function showCaseEvidence(result) {
  const edge = function (frame, id) {
    const w = frame.edges.find(e => e.edge_id === id);
    return escapeHtml(w.state) + (w.emergency ? ' · takeover' : '') + '<br>' + w.queue + '/' + w.capacity + ' queued · ' + w.assigned.length + ' buses';
  };
  document.getElementById('case-details').innerHTML = '<h3>' + escapeHtml(result.case_id.replaceAll('_',' ')) + ' ' +
    tag(result.passed ? 'PASS' : 'FAIL', result.passed ? 'good' : 'bad') + '</h3><ul>' + result.checks.map(function (c) {
      return '<li>' + tag(c.passed ? 'PASS' : 'FAIL', c.passed ? 'good' : 'bad') + ' ' + escapeHtml(c.label) + '</li>';
    }).join('') + '</ul>' + tableHtml(['Time', 'Edge A', 'Edge B', 'Standby', 'Selected bus', 'Central ACKs', 'Central pending', 'Accepted journal'], result.timeline.map(function (f) {
      return [f.second + ' s', edge(f,'edge-west-1'), edge(f,'edge-west-2'), edge(f,'edge-west-3'), escapeHtml(f.bus_health) +
        '<br>' + Object.entries(f.sensor_status).filter(([name,state]) => state !== 'healthy').map(([name,state]) => escapeHtml(name + ': ' + state)).join('<br>'),
        f.acknowledged, f.central_pending, f.journal_pending];
    }));
  document.getElementById('case-evidence').open = true;
}
document.getElementById('combined-cases').addEventListener('click', async function (event) {
  const button = event.target.closest('button');
  if (!button) return;
  const status = document.getElementById('case-feedback');
  const controls = document.querySelectorAll('#combined-cases button, #scenario-controls button');
  controls.forEach(b => b.disabled = true);
  try {
    if (button.dataset.evidence) {
      showCaseEvidence(await getJson('/health/simulation/cases/' + button.dataset.evidence));
    } else {
      status.textContent = 'Running accelerated simulation; measuring queues, ownership and delivery...';
      const result = await sendPost('/health/simulation/cases' + (button.dataset.case ? '/' + button.dataset.case : ''));
      chooseDemo();
      const outcomes = Array.isArray(result) ? result : [result];
      const passed = outcomes.filter(r => r.passed).length;
      status.textContent = passed + '/' + outcomes.length + ' expected outcomes verified. Demo paused; open Health or start Normal service.';
      showCaseEvidence(outcomes.find(r => !r.passed) || outcomes[outcomes.length-1]);
      await loadWorkers();
    }
  } catch (error) { status.textContent = error.message; }
  finally { controls.forEach(b => b.disabled = false); await loadCombinedCases(); }
});
loadCombinedCases().then(function () { document.getElementById('case-feedback').textContent = 'Choose a situation to verify.'; })
  .catch(function (error) { document.getElementById('case-feedback').textContent = error.message; });

function demoView() {
  return localStorage.getItem("transit-data-source") !== "live";
}

function chooseDemo() {
  if (!demoView()) localStorage.setItem("transit-data-source", "demo");
  const select = document.getElementById("data-source");
  if (select) select.value = "demo";
}

async function refreshAuthNavigation() {
  try {
    const response = await fetch(API_URL + '/auth/status', {cache:'no-store'});
    const auth = await response.json();
    const entry = document.getElementById('operator-entry');
    if (entry) entry.hidden = auth.authenticated;
    document.querySelectorAll('.operator-link').forEach(link => link.hidden = !auth.authenticated);
    const logout = document.getElementById('operator-logout');
    if (logout) logout.hidden = !auth.authenticated;
    if (!auth.authenticated && ['/control.html','/controls.html','/health.html','/operator.html'].includes(location.pathname)) {
      location.assign('/login.html?next=' + encodeURIComponent(location.pathname));
    }
  } catch (error) { console.error('Session status unavailable',error); }
}
document.addEventListener('DOMContentLoaded', function () {
  refreshAuthNavigation();
  if (document.getElementById('operator-entry')) setInterval(refreshAuthNavigation,3000);
  const logout = document.getElementById('operator-logout');
  if (logout) logout.addEventListener('click', async function () {
    try {
      const response = await fetch(API_URL + '/auth/logout', {method:'POST'});
      if (!response.ok) throw new Error('Sign-out failed');
      localStorage.setItem('transit-auth-revision',String(Date.now()));
      location.assign('/user.html');
    } catch (error) { logout.textContent = 'Retry sign out'; }
  });
  const select = document.getElementById('data-source');
  if (!select) return;
  select.value = demoView() ? 'demo' : 'live';
  select.addEventListener('change',function () {
    localStorage.setItem('transit-data-source',select.value);
    location.reload();
  });
});
window.addEventListener('storage',function (event) {
  if (event.key === 'transit-data-source') location.reload();
  if (event.key === 'transit-auth-revision') refreshAuthNavigation();
  if (event.key === 'transit-state-revision') refreshTransitViews();
});

// A control in another tab changed the backend. Discard cached snapshots and
// refresh immediately; the backend remains the only source of bus positions.
function refreshTransitViews() {
  simulationRead = null;
  publicRead = null;
  window.dispatchEvent(new Event('transit-state-changed'));
}
window.addEventListener('focus', refreshTransitViews);
document.addEventListener('visibilitychange', function () {
  if (!document.hidden) refreshTransitViews();
});

window.addEventListener('focus',refreshAuthNavigation);
document.addEventListener('visibilitychange',function () { if (!document.hidden) refreshAuthNavigation(); });

let simulationRead = null;
let simulationReadAt = 0;

async function getJson(path) {
  const response = await fetch(API_URL + path, {cache: 'no-store'});
  requireOperator(response);
  if (!response.ok) {
    throw new Error("Request failed: " + response.status);
  }
  return response.json();
}

let publicRead = null;
let publicReadAt = 0;
function passengerSource() { return demoView() ? "demo" : "live"; }
function requireOperator(response) {
  if (response.status === 401 && location.pathname !== "/user.html") {
    location.assign("/login.html?next=" + encodeURIComponent(location.pathname));
  }
}
async function getPublicSnapshot() {
  if (!publicRead || Date.now() - publicReadAt > 200) {
    publicReadAt = Date.now();
    publicRead = getJson("/passenger/snapshot?source=" + passengerSource()).catch(function (error) { publicRead = null; throw error; });
  }
  return publicRead;
}
async function getRoutes() {
  return USE_MOCK ? mockRoutes : getJson("/passenger/routes?source=" + passengerSource());
}
async function getRouteStops(routeId) {
  return USE_MOCK ? mockRouteStops(routeId) : getJson("/passenger/routes/" + encodeURIComponent(routeId) + "/stops?source=" + passengerSource());
}
async function getBus(busId) {
  if (USE_MOCK) return mockBus(busId);
  const bus = (await getPublicSnapshot()).buses.find(function (bus) { return bus.bus_id === busId; });
  return bus ? bus.state : null;
}
async function getBusEta(busId) {
  // Central computes ETAs for live buses only; demo and mock buses have none.
  // The /passenger/ copy is public, so the passenger page works without an operator login.
  if (demoView() || USE_MOCK) return null;
  return getJson("/passenger/buses/" + encodeURIComponent(busId) + "/eta");
}
async function getFleetIds(routes) {
  const ids = new Set(routes.map(function (route) { return route.route_id; }));
  if (USE_MOCK) return routes.flatMap(function (route) {
    return Array.from({length: BUSES_PER_ROUTE}, function (_, index) { return {id: "bus-" + route.route_id + "-" + (index+1), route: route.route_id}; });
  });
  return (await getPublicSnapshot()).buses.filter(function (bus) { return ids.has(bus.route_id); }).map(function (bus) { return {id: bus.bus_id, route: bus.route_id}; });
}

async function sendPost(path, body, method = "POST") {
  simulationRead = null;
  const response = await fetch(API_URL + path, {
    method: method,
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
  requireOperator(response);
  if (!response.ok) {
    let text = "Request failed: " + response.status;
    try {
      const body = await response.json();
      if (typeof body.detail === "string") {
        text = body.detail;
      } else if (Array.isArray(body.detail)) {
        text = body.detail.map(function (item) {
          return item.loc.join(".") + ": " + item.msg;
        }).join("; ");
      }
    } catch (error) {
      text = "Request failed: " + response.status;
    }
    throw new Error(text);
  }
  const result = await response.json();
  simulationRead = null;
  publicRead = null;
  localStorage.setItem('transit-state-revision', String(Date.now()));
  return result;
}

async function configureDemo(body) {
  return sendPost("/health/simulation/demo", body, "PATCH");
}

async function setSimulationClock(running, speed) {
  return sendPost("/health/simulation/clock", { running: running, speed: speed }, "PATCH");
}

async function sendBusCommand(region, action, params = {}) {
  return sendPost("/commands", { region_id: region, action: action, params: params });
}

async function getEdgeHealth() {
  if (demoView()) {
    const simulation = await getSimulation();
    return {
      edges: simulation.workers.map(function (worker) {
        return { edge_id: worker.edge_id, region_id: worker.region_id,
          status: worker.state === "failed" ? "unavailable" : worker.state === "standby" ? "unknown" : worker.state,
          age_seconds: null, workload_status: worker.state === "healthy" ?
            (worker.queue_utilization_percent >= 80 ? "overloaded" : "normal") : "unknown",
          last_report: { workload: worker } };
      }),
      buses: Object.entries(simulation.demo_buses).map(function ([id, bus]) {
        return { bus_id: id, edge_id: simulation.ownership[id].edge_id,
          health: { status: simulation.demo_running ? bus.status : "paused",
            reasons: [simulation.demo_running ? bus.evidence : "demonstration stopped intentionally"] },
          sensors: Object.fromEntries(Object.entries(bus.sensors).map(function ([name, state]) {
            return [name, { health_status: bus.status === "unavailable" ? "unavailable" : state }];
          })) };
      })
    };
  }
  if (USE_MOCK) {
    return mockHealth();
  }
  return getJson("/health/edges");
}

async function getHealthEvents() {
  if (demoView()) return (await getSimulation()).events.slice().reverse();
  return USE_MOCK ? [] : getJson("/health/events");
}

async function getSimulation() {
  if (USE_MOCK) {
    return mockSimulation();
  }
  if (!simulationRead || Date.now() - simulationReadAt > 200) {
    simulationReadAt = Date.now();
    simulationRead = getJson("/health/simulation").catch(function (error) {
      simulationRead = null;
      throw error;
    });
  }
  return simulationRead;
}

async function failEdge(edgeId) {
  if (USE_MOCK) {
    return mockFail(edgeId);
  }
  return sendPost("/health/simulation/instances/" + encodeURIComponent(edgeId) + "/fail");
}

async function recoverEdge(edgeId) {
  if (USE_MOCK) {
    return mockRecover(edgeId);
  }
  return sendPost("/health/simulation/instances/" + encodeURIComponent(edgeId) + "/recover");
}

async function resetSimulation(fleetSize, activeBuses, processingMs) {
  if (USE_MOCK) {
    return mockReset();
  }
  return sendPost("/health/simulation/reset", fleetSize === undefined ? undefined : {fleet_size: fleetSize, active_buses: activeBuses ?? fleetSize, processing_ms: processingMs ?? 100});
}

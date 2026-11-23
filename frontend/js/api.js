function demoView() {
  return localStorage.getItem("transit-data-source") === "demo";
}

function chooseDemo() {
  localStorage.setItem("transit-data-source", "demo");
  const select = document.getElementById("data-source");
  if (select) select.value = "demo";
}

document.addEventListener("DOMContentLoaded", function () {
  const select = document.getElementById("data-source");
  if (!select) return;
  select.value = demoView() ? "demo" : "live";
  select.addEventListener("change", function () {
    localStorage.setItem("transit-data-source", select.value);
    location.reload();
  });
});
window.addEventListener("storage", function (event) {
  if (event.key === "transit-data-source") location.reload();
});

let simulationRead = null;
let simulationReadAt = 0;

async function getJson(path) {
  const response = await fetch(API_URL + path);
  if (!response.ok) {
    throw new Error("Request failed: " + response.status);
  }
  return response.json();
}

async function getRoutes() {
  if (demoView()) return getJson("/health/simulation/routes");
  if (USE_MOCK) {
    return mockRoutes;
  }
  return getJson("/routes");
}

async function getRouteStops(routeId) {
  if (demoView()) return getJson("/health/simulation/routes/" + encodeURIComponent(routeId) + "/stops");
  if (USE_MOCK) {
    return mockRouteStops(routeId);
  }
  return getJson("/routes/" + routeId + "/stops");
}

async function getBus(busId) {
  if (demoView()) {
    const simulation = await getSimulation();
    const data = simulation.central_states[busId];
    if (!data) return null;
    return { ...data, simulation_age_seconds: simulation.virtual_seconds - simulation.central_received_at[busId],
      simulation_status: simulation.demo_running ? simulation.demo_buses[busId]?.status || "unknown" : "paused",
      distance_m: simulation.demo_buses[busId]?.distance_m || 0,
      last_updated: data.edge_metadata.source_timestamp };
  }
  if (USE_MOCK) {
    return mockBus(busId);
  }
  //getFleetIds just fetched every bus in one request; use that copy.
  if (busId in fleetRows) {
    const row = fleetRows[busId];
    return row.last_updated === null ? null : row;  //never reported
  }
  const response = await fetch(API_URL + "/bus_states/" + busId);
  if (response.status === 404) {
    return null;
  }
  if (!response.ok) {
    throw new Error("Request failed: " + response.status);
  }
  return response.json();
}

let fleetRows = {};

async function getFleetIds(routes) {
  const ids = new Set(routes.map(function (route) { return route.route_id; }));
  if (demoView()) return (await getSimulation()).registered_buses.filter(function (bus) {
    return ids.has(bus.route_id);
  }).map(function (bus) { return {id: bus.bus_id, route: bus.route_id}; });
  if (USE_MOCK) {
    //mock data uses the bus-*route*-*n* naming, guessing still works here.
    return routes.flatMap(function (route) {
      return Array.from({length: BUSES_PER_ROUTE}, function (_, i) {
        return {id: "bus-" + route.route_id + "-" + (i + 1), route: route.route_id};
      });
    });
  }
  // ask central which buses exist instead of just guessing bus name
  const rows = await getJson("/buses");
  fleetRows = {};
  rows.forEach(function (row) { fleetRows[row.bus_id] = row; });
  return rows.filter(function (row) { return ids.has(row.route_id); })
    .map(function (row) { return {id: row.bus_id, route: row.route_id}; });
}

async function sendPost(path, body, method = "POST") {
  simulationRead = null;
  const response = await fetch(API_URL + path, {
    method: method,
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body)
  });
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

async function resetSimulation(fleetSize) {
  if (USE_MOCK) {
    return mockReset();
  }
  return sendPost("/health/simulation/reset", fleetSize === undefined ? undefined : {fleet_size: fleetSize});
}

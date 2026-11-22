async function getJson(path) {
  const response = await fetch(API_URL + path);
  if (!response.ok) {
    throw new Error("Request failed: " + response.status);
  }
  return response.json();
}

async function getRoutes() {
  if (USE_MOCK) {
    return mockRoutes;
  }
  return getJson("/routes");
}

async function getRouteStops(routeId) {
  if (USE_MOCK) {
    return mockRouteStops(routeId);
  }
  return getJson("/routes/" + routeId + "/stops");
}

async function getBus(busId) {
  if (USE_MOCK) {
    return mockBus(busId);
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

async function sendPost(path, body, method = "POST") {
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
      }
    } catch (error) {
      text = "Request failed: " + response.status;
    }
    throw new Error(text);
  }
  return response.json();
}

async function setSimulationClock(running, speed) {
  return sendPost("/health/simulation/clock", { running: running, speed: speed }, "PATCH");
}

async function sendBusCommand(region, action, params = {}) {
  return sendPost("/commands", { region_id: region, action: action, params: params });
}

async function getEdgeHealth() {
  if (USE_MOCK) {
    return mockHealth();
  }
  return getJson("/health/edges");
}

async function getHealthEvents() {
  return USE_MOCK ? [] : getJson("/health/events");
}

async function getSimulation() {
  if (USE_MOCK) {
    return mockSimulation();
  }
  return getJson("/health/simulation");
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

async function resetSimulation() {
  if (USE_MOCK) {
    return mockReset();
  }
  return sendPost("/health/simulation/reset");
}

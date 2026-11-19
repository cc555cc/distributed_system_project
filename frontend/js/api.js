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

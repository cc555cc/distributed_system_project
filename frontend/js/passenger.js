const regionSelect = document.getElementById("region-select");
const statusText = document.getElementById("status");
const routeList = document.getElementById("route-list");
const busList = document.getElementById("bus-list");

const map = L.map("map").setView([43.9, -78.9], 11);
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: "&copy; OpenStreetMap contributors",
  maxZoom: 19
}).addTo(map);

const routeLayer = L.layerGroup().addTo(map);
const busLayer = L.layerGroup().addTo(map);

const colors = ["#d9534f", "#0275d8", "#2e9e4f", "#e08a00", "#7b3fb5"];
let routes = [];
let routeBounds = {};
let stopNames = {};
let busMarkers = {};
let loadNumber = 0;
let timer = null;

function fillRegions() {
  REGIONS.forEach(function (region) {
    const option = document.createElement("option");
    option.value = region.id;
    option.textContent = region.name;
    regionSelect.appendChild(option);
  });
}

function drawRoute(route, stops, color) {
  const bounds = L.latLngBounds([]);
  const directions = {};

  stops.forEach(function (stop) {
    if (!directions[stop.direction_id]) {
      directions[stop.direction_id] = [];
    }
    directions[stop.direction_id].push([stop.lat, stop.lon]);
    stopNames[stop.stop_id] = stop.name;

    const marker = L.circleMarker([stop.lat, stop.lon], {
      radius: 4,
      color: color,
      fillColor: "#ffffff",
      fillOpacity: 1,
      weight: 2
    });
    marker.bindPopup("<b>" + stop.name + "</b><br>Stop " + stop.stop_id + "<br>" + route.name);
    marker.addTo(routeLayer);
    bounds.extend([stop.lat, stop.lon]);
  });

  Object.keys(directions).forEach(function (direction) {
    L.polyline(directions[direction], { color: color, weight: 3, opacity: 0.7 }).addTo(routeLayer);
  });

  routeBounds[route.route_id] = bounds;

  const item = document.createElement("li");
  item.innerHTML = '<span class="dot" style="background:' + color + '"></span>' + route.name;
  item.addEventListener("click", function () {
    map.fitBounds(bounds);
  });
  routeList.appendChild(item);
}

async function loadRegion(regionId) {
  loadNumber++;
  const myNumber = loadNumber;
  clearInterval(timer);

  routeLayer.clearLayers();
  busLayer.clearLayers();
  routeList.innerHTML = "";
  busList.innerHTML = "";
  routes = [];
  routeBounds = {};
  stopNames = {};
  busMarkers = {};
  statusText.textContent = "Loading routes...";

  let regionRoutes = [];
  try {
    const allRoutes = await getRoutes();
    regionRoutes = allRoutes.filter(function (route) {
      return route.region_id === regionId;
    });
    for (let i = 0; i < regionRoutes.length; i++) {
      const stops = await getRouteStops(regionRoutes[i].route_id);
      if (myNumber !== loadNumber) {
        return;
      }
      drawRoute(regionRoutes[i], stops, colors[i % colors.length]);
    }
  } catch (error) {
    statusText.textContent = "Could not load routes. Is the server running?";
    return;
  }

  routes = regionRoutes;

  const allBounds = L.latLngBounds([]);
  Object.keys(routeBounds).forEach(function (id) {
    allBounds.extend(routeBounds[id]);
  });
  if (allBounds.isValid()) {
    map.fitBounds(allBounds);
  }

  await loadBuses(myNumber);
  if (myNumber !== loadNumber) {
    return;
  }
  timer = setInterval(function () {
    loadBuses(myNumber);
  }, REFRESH_SECONDS * 1000);
}

function busPopup(busId, bus, stale) {
  const nextStop = stopNames[bus.next_stop_id] || bus.next_stop_id || "unknown";
  let text = "<b>" + busId + "</b><br>";
  if (stale) {
    text += "<i>Not live, last known position</i><br>";
  }
  text += "Status: " + bus.bus_status + "<br>";
  text += "Speed: " + Math.round(bus.speed) + " km/h<br>";
  text += "Passengers: " + occupancyText(bus) + "<br>";
  text += "Next stop: " + nextStop;
  return text;
}

function showBus(item, bus) {
  const stale = isStale(bus);
  let crowding = "unknown";
  if (bus.edge_metadata) {
    crowding = bus.edge_metadata.crowding_status;
  }
  const className = "bus-icon " + (stale ? "stale" : crowding);
  const icon = L.divIcon({
    className: "",
    html: '<div class="' + className + '">' + item.route + "</div>",
    iconSize: [36, 22]
  });
  const popup = busPopup(item.id, bus, stale);

  if (busMarkers[item.id]) {
    busMarkers[item.id].setLatLng([bus.lat, bus.lon]);
    busMarkers[item.id].setIcon(icon);
    busMarkers[item.id].setPopupContent(popup);
  } else {
    const marker = L.marker([bus.lat, bus.lon], { icon: icon });
    marker.bindPopup(popup);
    marker.addTo(busLayer);
    busMarkers[item.id] = marker;
  }

  const row = document.createElement("li");
  row.textContent = item.id + ": " + (stale ? "not live" : occupancyText(bus));
  row.addEventListener("click", function () {
    map.setView(busMarkers[item.id].getLatLng(), 15);
    busMarkers[item.id].openPopup();
  });
  busList.appendChild(row);
}

async function loadBuses(myNumber) {
  const busIds = [];
  routes.forEach(function (route) {
    for (let i = 1; i <= BUSES_PER_ROUTE; i++) {
      busIds.push({ id: "bus-" + route.route_id + "-" + i, route: route.route_id });
    }
  });

  const results = await Promise.all(busIds.map(function (item) {
    return getBus(item.id).catch(function () {
      return "error";
    });
  }));

  if (myNumber !== loadNumber) {
    return;
  }

  busList.innerHTML = "";
  const seen = {};
  let shown = 0;
  let errors = 0;

  results.forEach(function (bus, index) {
    if (bus === "error") {
      errors++;
      return;
    }
    if (bus === null || !hasValidPosition(bus)) {
      return;
    }
    seen[busIds[index].id] = true;
    showBus(busIds[index], bus);
    shown++;
  });

  Object.keys(busMarkers).forEach(function (id) {
    if (!seen[id]) {
      busLayer.removeLayer(busMarkers[id]);
      delete busMarkers[id];
    }
  });

  if (busIds.length > 0 && errors === busIds.length) {
    statusText.textContent = "Server is not responding. No live buses to show.";
  } else if (shown === 0) {
    statusText.textContent = "No buses are reporting right now.";
  } else {
    statusText.textContent = shown + " buses on the map. Updated " + new Date().toLocaleTimeString();
  }
}

regionSelect.addEventListener("change", function () {
  loadRegion(regionSelect.value);
});

fillRegions();
loadRegion(regionSelect.value);

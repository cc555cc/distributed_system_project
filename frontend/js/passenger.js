const passengerFeed = document.getElementById("passenger-feed");
if (passengerFeed) passengerFeed.textContent = demoView() ? "Demonstration service" : "Live service updates";

const regionSelect = document.getElementById("region-select");
const statusText = document.getElementById("status");
const routeList = document.getElementById("route-list");
const busList = document.getElementById("bus-list");
const originSelect = document.getElementById("origin-stop");
const destinationSelect = document.getElementById("destination-stop");
let tripRequest = 0;

async function loadTripStops(regionId, request) {
  const stops = await getJson("/passenger/stops?region_id=" + regionId + "&source=" + passengerSource());
  if (request !== loadNumber) return;
  originSelect.replaceChildren();
  destinationSelect.replaceChildren(new Option("Show approaching buses", ""));
  stops.forEach(function (stop) {
    const name = stop.name + " (" + stop.stop_id + ")";
    originSelect.add(new Option(name, stop.stop_id));
    destinationSelect.add(new Option(name, stop.stop_id));
  });
  const snapshot = await getPublicSnapshot();
  if (request !== loadNumber) return;
  const approaching = snapshot.buses.find(function (bus) { return bus.region_id === regionId && bus.state && bus.state.next_stop_id; });
  if (approaching && stops.some(function (stop) { return stop.stop_id === approaching.state.next_stop_id; })) {
    originSelect.value = approaching.state.next_stop_id;
  }
}

async function updateTrip() {
  const request = ++tripRequest;
  const status = document.getElementById("journey-status");
  const results = document.getElementById("journey-results");
  const arrivalsList = document.getElementById("stop-arrivals");
  if (!originSelect.value) return;
  if (originSelect.value === destinationSelect.value) {
    status.textContent = "Choose a destination different from your starting stop.";
    results.replaceChildren(); arrivalsList.replaceChildren(); return;
  }
  const params = new URLSearchParams({source: passengerSource(), region_id: regionSelect.value,
    party_size: document.getElementById("party-size").value, stop_id: originSelect.value});
  try {
    const arriving = await getJson("/passenger/arrivals?" + params);
    let choices = arriving;
    if (destinationSelect.value) {
      params.delete("stop_id"); params.set("origin", originSelect.value); params.set("destination", destinationSelect.value);
      choices = await getJson("/passenger/journeys?" + params);
    }
    if (request !== tripRequest) return;
    arrivalsList.replaceChildren();
    arriving.arrivals.slice(0, 10).forEach(function (bus) {
      const item = document.createElement("li");
      item.textContent = bus.bus_id + " · Route " + bus.route_id + " · " + bus.eta_minutes.toFixed(1) + " min · " +
        bus.free_seats + " seats free (" + Math.round(bus.occupancy_percent) + "%)" +
        (bus.free_seats < Number(document.getElementById("party-size").value) ? " · Not enough seats for your group" : "");
      arrivalsList.appendChild(item);
    });
    if (!arriving.arrivals.length) arrivalsList.textContent = arriving.message;
    results.replaceChildren();
    const all = choices.recommended ? [choices.recommended, ...choices.alternatives.slice(0, 2)] : [];
    all.forEach(function (choice, index) {
      const item = document.createElement("p");
      const buses = choice.legs ? choice.legs.map(function (leg) { return leg.bus_id; }).join(" → ") : choice.bus_id;
      item.textContent = (index === 0 ? "Recommended: " : "Alternative: ") + buses + ". " + choice.explanation;
      results.appendChild(item);
    });
    if (!all.length) results.textContent = choices.unavailable.length ? "Suitable buses currently lack confirmed seats. Please compare later arrivals." : choices.message;
    status.textContent = choices.notice;
  } catch (error) {
    if (request !== tripRequest) return;
    status.textContent = "Predictions unavailable: " + error.message;
    results.replaceChildren(); arrivalsList.replaceChildren();
  }
}

document.getElementById("journey-form").addEventListener("submit", function (event) { event.preventDefault(); updateTrip(); });
[originSelect, destinationSelect].forEach(function (select) { select.addEventListener("change", updateTrip); });

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
    marker.on("click", function () { originSelect.value = stop.stop_id; updateTrip(); });
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
  tripRequest++;
  document.getElementById("journey-results").replaceChildren();
  document.getElementById("stop-arrivals").replaceChildren();
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
  try { await loadTripStops(regionId, myNumber); }
  catch (error) { document.getElementById("journey-status").textContent = "Stops unavailable: " + error.message; }

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
  if (bus.distance_m !== undefined) text += "Distance travelled: " + (bus.distance_m / 1000).toFixed(2) + " km<br>";
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
  row.textContent = item.id + ": " + (stale ? "last known position" : occupancyText(bus)) +
    (bus.distance_m === undefined ? "" : " · " + Math.round(bus.distance_m) + " m travelled");
  row.addEventListener("click", function () {
    map.setView(busMarkers[item.id].getLatLng(), 15);
    busMarkers[item.id].openPopup();
  });
  busList.appendChild(row);
}

async function loadBuses(myNumber) {
  let busIds;
  try {
    busIds = await getFleetIds(routes);
  } catch (error) {
    statusText.textContent = "Could not load fleet: " + error.message;
    return;
  }

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
  updateTrip();
}

regionSelect.addEventListener("change", function () {
  loadRegion(regionSelect.value);
});

fillRegions();
loadRegion(regionSelect.value);

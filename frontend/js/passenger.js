const passengerFeed = document.getElementById("passenger-feed");
if (passengerFeed) passengerFeed.textContent = demoView() ? "Demonstration service" : "Live service updates";

const regionSelect = document.getElementById("region-select");
const statusText = document.getElementById("status");
const routeList = document.getElementById("route-list");
const busList = document.getElementById("bus-list");
const originSelect = document.getElementById("origin-stop");
const destinationSelect = document.getElementById("destination-stop");
let tripRequest = 0;
let selectedLocation = null, locationMarker = null, tripStops = [];
// Keep the passenger's selection when another tab changes the feed.
const savedTrip = JSON.parse(sessionStorage.getItem('transit-trip') || '{}');

function clearLocation() {
  selectedLocation = null;
  if (locationMarker) { map.removeLayer(locationMarker); locationMarker = null; }
  document.getElementById('location-status').textContent = '';
}

async function loadTripStops(regionId, request) {
  const stops = await getJson("/passenger/stops?region_id=" + regionId + "&source=" + passengerSource());
  if (request !== loadNumber) return;
  tripStops = stops;
  originSelect.replaceChildren();
  destinationSelect.replaceChildren(new Option("Show approaching buses", ""));
  stops.forEach(function (stop) {
    const name = stop.name + " (" + stop.stop_id + ")";
    originSelect.add(new Option(name, stop.stop_id));
    destinationSelect.add(new Option(name, stop.stop_id));
  });
  if (savedTrip.region === regionId && stops.some(s => s.stop_id === savedTrip.origin)) {
    originSelect.value = savedTrip.origin;
    if (stops.some(s => s.stop_id === savedTrip.destination)) destinationSelect.value = savedTrip.destination;
    return;
  }
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
  if (!originSelect.value && !selectedLocation) return;
  if (originSelect.value === destinationSelect.value) {
    status.textContent = "Choose a destination different from your starting stop.";
    results.replaceChildren(); arrivalsList.replaceChildren(); return;
  }
  Object.assign(savedTrip, {region: regionSelect.value, origin: originSelect.value, destination: destinationSelect.value});
  sessionStorage.setItem('transit-trip', JSON.stringify(savedTrip));
  const params = new URLSearchParams({source: passengerSource(), region_id: regionSelect.value,
    party_size: document.getElementById("party-size").value, stop_id: originSelect.value});
  try {
    let arriving, choices;
    if (selectedLocation) {
      const nearby = new URLSearchParams({lat:selectedLocation.lat, lon:selectedLocation.lng,
        region_id:regionSelect.value, party_size:document.getElementById('party-size').value, source:passengerSource()});
      choices = await getJson('/passenger/nearby?' + nearby);
      if (request !== tripRequest) return;
      if (choices.recommended) originSelect.value = choices.recommended.stop_id;
      arriving = {arrivals:choices.options.concat(choices.unavailable), message:choices.message};
    } else {
      arriving = await getJson("/passenger/arrivals?" + params);
      choices = arriving;
    }
    if (destinationSelect.value && !selectedLocation) {
      params.delete("stop_id"); params.set("origin", originSelect.value); params.set("destination", destinationSelect.value);
      choices = await getJson("/passenger/journeys?" + params);
    }
    if (request !== tripRequest) return;
    document.getElementById('arrivals-heading').textContent = selectedLocation ? 'Nearby pickups' : 'Arriving at your stop';
    arrivalsList.innerHTML = tableHtml(['Bus / route', 'Occupancy', 'Arrives'], arriving.arrivals.slice(0,10).map(function (bus) {
      const full = bus.free_seats < Number(document.getElementById('party-size').value);
      return [escapeHtml(bus.bus_id) + '<br><small>Route ' + escapeHtml(bus.route_id) + '</small>' + (bus.stop_name ? '<br><small>' + escapeHtml(bus.stop_name) + '</small>' : ''),
        tag(Math.round(bus.occupancy_percent) + '%', full ? 'bad' : bus.occupancy_percent >= 80 ? 'warn' : 'good') +
        '<br><small>' + bus.free_seats + ' seats free</small>', bus.eta_minutes.toFixed(1) + ' min'];
    }));
    if (!arriving.arrivals.length) arrivalsList.textContent = arriving.message;
    results.replaceChildren();
    const all = choices.recommended ? [choices.recommended] : [];
    all.forEach(function (choice, index) {
      const item = document.createElement('div');
      item.className = index === 0 ? 'recommendation' : 'card';
      const legs = choice.legs || [choice];
      const wait = choice.waiting_minutes ?? choice.eta_minutes;
      const total = choice.total_minutes ?? choice.eta_minutes;
      item.innerHTML = '<small>' + (index === 0 ? 'RECOMMENDED' : 'ALTERNATIVE') + '</small><strong>' +
        legs.map(l => escapeHtml(l.bus_id) + ' · ' + escapeHtml(l.route_id)).join(' → ') + '</strong>' +
        '<div class="summary-strip"><span>Arrives <b>' + wait.toFixed(1) + ' min</b></span><span>' +
        (choice.legs ? 'Trip <b>' + total.toFixed(1) + ' min</b>' : 'Direct arrival') + '</span></div>' +
        '<small>' + Math.round(Math.max(...legs.map(l => l.occupancy_percent))) + '% occupied · ' + Math.min(...legs.map(l => l.free_seats)) + ' seats free · ' + (choice.transfers || 0) + ' transfers</small>' +
        '<small>' + (choice.stop_name ? escapeHtml(choice.stop_name) + ' · walk ' + choice.walking_metres + ' m' : 'Ranked by time, seats and transfers.') + '</small>';
      results.appendChild(item);
    });
    if (destinationSelect.value && choices.alternatives.length) {
      const alternatives = document.createElement('div');
      alternatives.innerHTML = '<h3>Alternatives</h3>' + tableHtml(['Bus / route', 'Occupancy', 'Trip'], choices.alternatives.slice(0,2).map(function (c) {
        return [c.legs.map(l => escapeHtml(l.bus_id + ' / ' + l.route_id)).join('<br>'),
          Math.round(Math.max(...c.legs.map(l => l.occupancy_percent))) + '%', c.total_minutes.toFixed(1) + ' min'];
      }));
      results.appendChild(alternatives);
    }
    if (!all.length) results.textContent = choices.unavailable.length ? "Suitable buses currently lack confirmed seats. Please compare later arrivals." : choices.message;
    status.textContent = "Live estimates · seats may change";
  } catch (error) {
    if (request !== tripRequest) return;
    status.textContent = "Predictions unavailable: " + error.message;
    results.replaceChildren(); arrivalsList.replaceChildren();
  }
}

document.getElementById("journey-form").addEventListener("submit", function (event) { event.preventDefault(); updateTrip(); });
[originSelect, destinationSelect].forEach(function (select) { select.addEventListener("change", function () {
  clearLocation();
  updateTrip();
}); });

const map = L.map("map").setView([43.9, -78.9], 11);
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
  attribution: "&copy; OpenStreetMap contributors",
  maxZoom: 19
}).addTo(map);

// A map point is converted to nearby reachable stops; no invented bus position is used.
function chooseLocation(point) {
  selectedLocation = point;
  destinationSelect.value = '';
  if (locationMarker) map.removeLayer(locationMarker);
  locationMarker = L.marker([point.lat, point.lng]).addTo(map).bindPopup('Your selected location').openPopup();
  document.getElementById('location-status').textContent = 'Nearby stops from ' + point.lat.toFixed(4) + ', ' + point.lng.toFixed(4);
  updateTrip();
}
map.on('click', function (event) { chooseLocation(event.latlng); });
document.getElementById('random-location').addEventListener('click', async function () {
  const button = document.getElementById('random-location');
  button.disabled = true;
  try {
    if (!tripStops.length) return;
    // Pick a demonstration point ahead of a real bus; the backend still checks reachability.
    const fleet = await getPublicSnapshot();
    const vehicle = fleet.buses.find(b => b.region_id === regionSelect.value && b.state && b.state.service_status === 'healthy' && b.state.next_stop_id);
    let candidates = tripStops;
    if (vehicle) {
      const ordered = (await getRouteStops(vehicle.route_id)).filter(s => s.direction_id === vehicle.state.direction_id);
      const next = ordered.findIndex(s => s.stop_id === vehicle.state.next_stop_id);
      if (next >= 0 && next + 1 < ordered.length) candidates = ordered.slice(next + 1);
    }
    const stop = candidates[Math.floor(Math.random()*candidates.length)];
    const point = {lat:stop.lat + (Math.random()-.5)*.0004, lng:stop.lon + (Math.random()-.5)*.0004};
    map.panTo([point.lat,point.lng]); chooseLocation(point);
  } catch (error) { document.getElementById('location-status').textContent = error.message; }
  finally { button.disabled = false; }
});

const routeLayer = L.layerGroup().addTo(map);
const busLayer = L.layerGroup().addTo(map);

const colors = ["#d9534f", "#0275d8", "#2e9e4f", "#e08a00", "#7b3fb5"];
let routes = [];
let routeBounds = {};
let stopNames = {};
let busMarkers = {};
let busEtas = {};
let loadNumber = 0;
let timer = null;

const ETA_STOPS_SHOWN = 3;

function fillRegions() {
  REGIONS.forEach(function (region) {
    const option = document.createElement("option");
    option.value = region.id;
    option.textContent = region.name;
    regionSelect.appendChild(option);
  });
  if (REGIONS.some(r => r.id === savedTrip.region)) regionSelect.value = savedTrip.region;
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
      bubblingMouseEvents: false,
      color: color,
      fillColor: "#ffffff",
      fillOpacity: 1,
      weight: 2
    });
    marker.bindPopup("<b>" + stop.name + "</b><br>Stop " + stop.stop_id + "<br>" + route.name);
    marker.on("click", function () { clearLocation(); originSelect.value = stop.stop_id; updateTrip(); });
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
  clearLocation();
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
  busEtas = {};
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

async function refreshEta(busId) {
  try {
    const result = await getBusEta(busId);
    if (result !== null) busEtas[busId] = result.stops;
  } catch (error) {
    // Keep the popup working without arrival times.
  }
}

function etaText(busId) {
  const stops = busEtas[busId];
  if (stops === undefined) return "<br><i>Loading arrival times…</i>";
  // Central lists every stop; passed stops (or all, for a stale/idle bus) have no estimate.
  const ahead = stops.filter(function (stop) {
    return stop.eta_minutes !== null && stop.available !== false;
  });
  if (ahead.length === 0) return "<br>No arrival estimates right now";
  return "<br><b>Next stops:</b><br>" + ahead.slice(0, ETA_STOPS_SHOWN).map(function (stop) {
    const name = escapeHtml(stopNames[stop.stop_id] || stop.stop_id);
    return name + ": " + (stop.eta_minutes < 1 ? "arriving" : Math.round(stop.eta_minutes) + " min");
  }).join("<br>");
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
  if (!stale && demoView() === false) {
    text += etaText(busId);
  }
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
    if (busMarkers[item.id].isPopupOpen()) {
      refreshEta(item.id);
    }
    busMarkers[item.id].setLatLng([bus.lat, bus.lon]);
    busMarkers[item.id].setIcon(icon);
    busMarkers[item.id].setPopupContent(popup);
  } else {
    const marker = L.marker([bus.lat, bus.lon], { icon: icon });
    marker.bindPopup(popup);
    marker.on("popupopen", function () {
      refreshEta(item.id);
    });
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
  try {
    const snapshot = await getPublicSnapshot();
    if (snapshot.clock) passengerFeed.textContent = 'Demonstration · ' + (snapshot.clock.running ? 'running' : 'paused') +
      ' · ' + snapshot.clock.speed + '× · ' + snapshot.clock.seconds.toFixed(1) + ' simulated s';
  } catch (error) { /* existing fleet error handling below reports unavailable data */ }
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

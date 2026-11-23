const regionFilter = document.getElementById("health-region");
const routeFilter = document.getElementById("health-route");
const busChoice = document.getElementById("health-bus");
const sensorChoice = document.getElementById("health-sensor");
const healthUpdated = document.getElementById("health-updated");
let healthRequest = 0;

function healthOptions(select, choices, placeholder) {
  const selected = select.value;
  select.replaceChildren();
  if (placeholder) select.add(new Option(placeholder, ""));
  choices.forEach(function (value) { select.add(new Option(value, value)); });
  if (choices.includes(selected)) select.value = selected;
}

function healthTag(status) {
  const kind = ["healthy", "stationary", "available"].includes(status) ? "good" :
    ["faulty", "unavailable", "telemetry_unavailable"].includes(status) ? "bad" :
    ["unknown", "paused"].includes(status) ? "grey" : "warn";
  return tag(escapeHtml(status), kind);
}

async function loadHealth() {
  const request = ++healthRequest;
  const source = demoView() ? "demo" : "live";
  const query = "?source=" + source;
  try {
    const [fleet, catalogue] = await Promise.all([getJson("/buses/health" + query), getJson("/sensors")]);
    if (request !== healthRequest) return;
    healthOptions(regionFilter, [...new Set(fleet.map(function (bus) { return bus.region_id; }))].sort(), "All regions");
    const regional = fleet.filter(function (bus) { return !regionFilter.value || bus.region_id === regionFilter.value; });
    healthOptions(routeFilter, [...new Set(regional.map(function (bus) { return bus.route_id; }))].sort(), "All routes");
    const visible = regional.filter(function (bus) { return !routeFilter.value || bus.route_id === routeFilter.value; });
    healthOptions(busChoice, visible.map(function (bus) { return bus.bus_id; }), null);
    healthOptions(sensorChoice, catalogue.sensors, "All sensors");
    if (!busChoice.value) {
      document.getElementById("bus-health").textContent = "No registered buses match these filters.";
      document.getElementById("sensor-health").textContent = "No bus selected.";
      document.getElementById("route-health").textContent = "No route selected.";
      document.getElementById("edge-health").textContent = "No edge selected.";
      document.getElementById("health-alerts").textContent = "No bus selected.";
      healthUpdated.textContent = "No health reports to display.";
      return;
    }
    const path = "/buses/" + encodeURIComponent(busChoice.value);
    const bus = await getJson(path + "/health" + query);
    const sensorPath = path + "/sensors" + (sensorChoice.value ? "/" + encodeURIComponent(sensorChoice.value) : "");
    const [report, route, alerts] = await Promise.all([
      getJson(sensorPath + query), getJson("/routes/" + encodeURIComponent(bus.route_id) + "/health" + query),
      getJson("/alerts" + query + "&bus_id=" + encodeURIComponent(bus.bus_id) + "&region_id=" + encodeURIComponent(bus.region_id) + "&limit=15").catch(function () { return null; })
    ]);
    if (request !== healthRequest) return;
    const age = bus.valid_telemetry_age_seconds;
    document.getElementById("bus-health").innerHTML = tableHtml(["Bus", "Region / route", "Edge", "Health", "Valid telemetry age", "Evidence"], [[
      escapeHtml(bus.bus_id), escapeHtml(bus.region_id + " / " + bus.route_id), escapeHtml(bus.edge_id),
      healthTag(bus.health.status), age === null ? "No valid reading" : age.toFixed(1) + " s",
      escapeHtml(bus.health.reasons.join("; "))
    ]]);
    if (source === "demo") {
      const delivery = document.createElement("p");
      delivery.textContent = "Central " + (bus.central_available ? "available" : "unavailable") +
        " · Delivery " + (bus.central_delivery_pending ? "pending" : "no pending update") +
        " · Last central update " + (bus.central_update_age_seconds === null ? "not received" : bus.central_update_age_seconds.toFixed(1) + " simulation s ago");
      document.getElementById("bus-health").appendChild(delivery);
    }
    const sensors = sensorChoice.value ? {[report.sensor]: report} : report.sensors;
    document.getElementById("sensor-health").innerHTML = tableHtml(["Sensor", "Observed health", "Heartbeat age"],
      Object.entries(sensors).map(function ([name, sensor]) {
        return [escapeHtml(name), healthTag(sensor.health_status || "unknown"),
          sensor.age_seconds === null || sensor.age_seconds === undefined ? "Never received" : sensor.age_seconds.toFixed(1) + " s"];
      }));
    const known = function (value, suffix = "") { return value === null || value === undefined ? "Unavailable" : value + suffix; };
    document.getElementById("route-health").innerHTML = tableHtml(["Route", "Availability", "Available / registered buses", "Configured seats", "Verified service capacity", "Passengers in samples", "Free seats in samples", "Occupancy samples"], [[
      escapeHtml(route.route_id), healthTag(route.availability), route.available_buses + " / " + route.bus_count,
      known(route.configured_capacity), known(route.verified_service_capacity), known(route.passengers),
      known(route.available_seats_in_samples), route.occupancy_samples
    ]]) + "<p>" + escapeHtml((route.notes || []).join("; ")) + "</p>";
    document.getElementById("edge-health").innerHTML = tableHtml(["Edge", "State", "Queue / capacity", "Queue utilization", "Assigned buses"],
      route.edge_workload.map(function (edge) {
        return [escapeHtml(edge.edge_id), healthTag(edge.status),
          known(edge.queue_size) + " / " + known(edge.queue_capacity),
          known(edge.queue_utilization_percent === null ? null : Math.round(edge.queue_utilization_percent), "%"), known(edge.assigned_buses)];
      }));
    const alertBox = document.getElementById("health-alerts");
    alertBox.replaceChildren();
    if (alerts === null || !alerts.length) {
      alertBox.textContent = alerts === null ? "Alert history unavailable." : "No recent alerts for this bus or region.";
    } else {
      const list = document.createElement("ul");
      alerts.forEach(function (alert) {
        const item = document.createElement("li");
        const time = alert.simulation_seconds === undefined ? alert.timestamp || "Current" : alert.simulation_seconds.toFixed(1) + " simulation s";
        item.textContent = alert.severity + " · " + time + " · " + alert.message;
        list.appendChild(item);
      });
      alertBox.appendChild(list);
    }
    healthUpdated.textContent = (source === "demo" ? "Demonstration" : "Live MQTT") + " · Updated " + new Date().toLocaleTimeString();
  } catch (error) {
    if (request !== healthRequest) return;
    healthUpdated.textContent = "Health reports unavailable: " + error.message;
    ["bus-health", "sensor-health", "route-health", "edge-health", "health-alerts"].forEach(function (id) {
      document.getElementById(id).textContent = "Unavailable. Previous readings are not presented as current health.";
    });
  }
}

[regionFilter, routeFilter, busChoice, sensorChoice].forEach(function (select) {
  select.addEventListener("change", loadHealth);
});
loadHealth();
setInterval(loadHealth, REFRESH_SECONDS * 1000);

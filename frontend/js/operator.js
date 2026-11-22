const updatedText = document.getElementById("updated");
const summaryBox = document.getElementById("summary");
const alertsBox = document.getElementById("alerts");
const busesBox = document.getElementById("buses");
const routesBox = document.getElementById("routes");
const edgesBox = document.getElementById("edges");
const eventsBox = document.getElementById("events");

let loading = false;

function edgeKind(status) {
  if (status === "healthy") {
    return "good";
  }
  if (status === "recovering") {
    return "warn";
  }
  if (status === "unavailable") {
    return "bad";
  }
  return "grey";
}

function loadKind(status) {
  if (status === "normal") {
    return "good";
  }
  if (status === "high_load") {
    return "warn";
  }
  if (status === "overloaded") {
    return "bad";
  }
  return "grey";
}

function crowdKind(crowding) {
  if (crowding === "low") {
    return "good";
  }
  if (crowding === "medium") {
    return "warn";
  }
  if (crowding === "high") {
    return "bad";
  }
  return "grey";
}

function buildAlerts(buses, health) {
  const alerts = [];

  buses.forEach(function (bus) {
    if (bus.data === null) {
      alerts.push({ kind: "grey", text: "No data yet from " + bus.id });
    } else if (bus.data.bus_status === "idle") {
      alerts.push({ kind: "grey", text: bus.id + " is at the terminal" });
    } else if (isStale(bus.data)) {
      alerts.push({ kind: "warn", text: bus.id + " has not reported for " + Math.round(ageSeconds(bus.data)) + " seconds" });
    } else if (crowdingOf(bus.data) === "high") {
      alerts.push({ kind: "bad", text: bus.id + " is crowded (" + Math.round(percentOf(bus.data)) + "%)" });
    }
  });

  if (health) {
    (health.buses || []).forEach(function (bus) {
      if (bus.health && bus.health.status !== "healthy") {
        alerts.push({ kind: bus.health.status === "unknown" ? "grey" : "warn",
          text: bus.bus_id + ": " + bus.health.status + " (" + (bus.health.reasons || []).join(", ") + ")" });
      }
    });
    health.edges.forEach(function (edge) {
      if (edge.status === "unavailable") {
        alerts.push({ kind: "bad", text: edge.edge_id + " is not sending heartbeats" });
      } else if (edge.status === "recovering") {
        alerts.push({ kind: "warn", text: edge.edge_id + " is recovering" });
      }
      if (edge.workload_status === "overloaded") {
        alerts.push({ kind: "bad", text: edge.edge_id + " is overloaded" });
      } else if (edge.workload_status === "high_load") {
        alerts.push({ kind: "warn", text: edge.edge_id + " has a high load" });
      }
    });
  }

  if (health && health.recommendations) {
    health.recommendations.forEach(function (rec) {
      if (rec.to_edge_id) {
        alerts.push({ kind: "warn", text: "Recommendation: move load from " + rec.from_edge_id + " to " + rec.to_edge_id + " (not done automatically)" });
      } else {
        alerts.push({ kind: "warn", text: "Recommendation: " + rec.from_edge_id + " needs extra capacity, no peer has room" });
      }
    });
  }

  return alerts;
}

function showSummary(buses, health, alerts) {
  let live = 0;
  let crowded = 0;
  buses.forEach(function (bus) {
    if (bus.data !== null && !isStale(bus.data)) {
      live++;
      if (crowdingOf(bus.data) === "high") {
        crowded++;
      }
    }
  });

  let edgeText = "?";
  if (health) {
    const healthy = health.edges.filter(function (edge) {
      return edge.status === "healthy";
    }).length;
    edgeText = healthy + " / " + health.edges.length;
  }

  const problems = alerts.filter(function (alert) {
    return alert.kind !== "grey";
  }).length;

  const stats = [
    [live + " / " + buses.length, "Buses live"],
    [crowded, "Crowded buses"],
    [edgeText, "Edge servers healthy"],
    [problems, "Alerts"]
  ];
  let html = "";
  stats.forEach(function (stat) {
    html += '<div class="stat"><div class="number">' + stat[0] + "</div>" + stat[1] + "</div>";
  });
  summaryBox.innerHTML = html;
}

function showAlerts(alerts) {
  if (alerts.length === 0) {
    alertsBox.innerHTML = "<p>No alerts. Everything looks normal.</p>";
    return;
  }
  let html = "<ul class='plain'>";
  alerts.forEach(function (alert) {
    html += "<li>" + tag(alert.kind === "grey" ? "info" : (alert.kind === "bad" ? "problem" : "warning"), alert.kind) + " " + alert.text + "</li>";
  });
  alertsBox.innerHTML = html + "</ul>";
}

function showBuses(buses, health) {
  const rows = buses.map(function (bus) {
    const report = health && (health.buses || []).find(function (item) { return item.bus_id === bus.id; });
    const busHealth = report && report.health ? report.health.status : "unknown";
    const sensors = report ? Object.entries(report.sensors || {}).map(function ([name, state]) {
      return name + ": " + (state.health_status || state.status || "unknown");
    }).join(", ") : "No sensor report";
    if (bus.data === null) {
      return [bus.id, bus.route, "no data", "-", "-", "-", "-", busHealth, sensors];
    }
    const stale = isStale(bus.data);
    return [
      bus.id,
      bus.route,
      stale ? tag("not live", "grey") : bus.data.bus_status,
      Math.round(bus.data.speed) + " km/h",
      occupancyText(bus.data),
      tag(crowdingOf(bus.data), crowdKind(crowdingOf(bus.data))),
      Math.round(ageSeconds(bus.data)) + " s ago",
      tag(busHealth, busHealth === "healthy" ? "good" : busHealth === "unknown" ? "grey" : "warn"),
      sensors
    ];
  });
  busesBox.innerHTML = tableHtml(["Bus", "Route", "Status", "Speed", "Passengers", "Crowding", "Last update", "Health", "Sensors"], rows);
}

function showRoutes(routes, buses) {
  const rows = routes.map(function (route) {
    let live = 0;
    let total = 0;
    let count = 0;
    buses.forEach(function (bus) {
      if (bus.route === route.route_id && bus.data !== null && !isStale(bus.data)) {
        live++;
        if (percentOf(bus.data) !== null) {
          total += percentOf(bus.data);
          count++;
        }
      }
    });

    let average = "-";
    let label = tag("no live buses", "grey");
    if (count > 0) {
      const percent = total / count;
      average = Math.round(percent) + "%";
      if (percent >= 80) {
        label = tag("crowded", "bad");
      } else if (percent >= 50) {
        label = tag("busy", "warn");
      } else {
        label = tag("normal", "good");
      }
    }
    return [route.name, route.region_id, live, average, label];
  });
  routesBox.innerHTML = tableHtml(["Route", "Region", "Live buses", "Average occupancy", "Status"], rows);
}

function showEdges(health) {
  if (health === null) {
    edgesBox.innerHTML = "<p>Edge server health is not available right now.</p>";
    return;
  }
  const rows = health.edges.map(function (edge) {
    let queue = "no report";
    if (edge.last_report) {
      queue = Math.round(edge.last_report.workload.queue_utilization_percent) + "%";
    }
    // Buses this edge reports owning (from its own heartbeat)
    const owned = (health.buses || []).filter(function (bus) {
      return bus.edge_id === edge.edge_id;
    }).map(function (bus) {
      return bus.bus_id;
    });
    const heartbeat = edge.age_seconds === null || edge.age_seconds === undefined
      ? "never"
      : Math.round(edge.age_seconds) + " s ago";
    return [
      edge.edge_id,
      edge.region_id,
      tag(edge.status, edgeKind(edge.status)),
      heartbeat,
      tag(edge.workload_status, loadKind(edge.workload_status)),
      queue,
      owned.length ? owned.join(", ") : "-"
    ];
  });
  edgesBox.innerHTML = tableHtml(["Edge server", "Region", "Status", "Last heartbeat", "Workload", "Queue", "Buses"], rows);
}

function showEvents(events) {
  if (events === null) {
    eventsBox.textContent = "Central event history unavailable.";
    return;
  }
  eventsBox.innerHTML = events.length ? "<ul class='plain'>" + events.slice(0, 10).map(function (event) {
    const description = event.type === "bus_reassigned"
      ? event.bus_id + ": " + event.from_edge + " -> " + event.to_edge
      : event.edge_id + ": " + event.from_status + " -> " + event.to_status;
    return "<li>" + escapeHtml(new Date(event.time).toLocaleTimeString() + ": " + description) + "</li>";
  }).join("") + "</ul>" : "<p>No central events yet.</p>";
}

async function loadAll() {
  if (loading) {
    return;
  }
  loading = true;

  try {
    const problems = [];
    let routes = [];
    let health = null;

    try {
      routes = await getRoutes();
    } catch (error) {
      problems.push("routes");
    }

    const ids = [];
    routes.forEach(function (route) {
      for (let i = 1; i <= BUSES_PER_ROUTE; i++) {
        ids.push({ id: "bus-" + route.route_id + "-" + i, route: route.route_id });
      }
    });

    const results = await Promise.all(ids.map(function (item) {
      return getBus(item.id).catch(function () {
        return "error";
      });
    }));

    let busErrors = 0;
    const buses = ids.map(function (item, index) {
      let data = results[index];
      if (data === "error") {
        busErrors++;
        data = null;
      }
      if (data !== null && !hasValidPosition(data)) {
        data = null;
      }
      return { id: item.id, route: item.route, data: data };
    });
    if (ids.length > 0 && busErrors === ids.length) {
      problems.push("buses");
    }

    try {
      health = await getEdgeHealth();
    } catch (error) {
      problems.push("edge health");
    }

    try {
      const simulation = await getSimulation();
      document.getElementById("simulation-edges").innerHTML = simulationTable(simulation);
      document.getElementById("simulation-clock").textContent = simulationClockText(simulation);
    } catch (error) {
      problems.push("simulation");
      document.getElementById("simulation-edges").textContent = "Simulation unavailable.";
      document.getElementById("simulation-clock").textContent = "Clock unavailable.";
    }
    let events = null;
    try {
      events = await getHealthEvents();
    } catch (error) {
      problems.push("event history");
    }
    const alerts = buildAlerts(buses, health);
    showSummary(buses, health, alerts);
    showAlerts(alerts);
    showBuses(buses, health);
    showRoutes(routes, buses);
    showEdges(health);
    showEvents(events);

    if (problems.length > 0) {
      updatedText.textContent = "Could not load: " + problems.join(", ") + ". Is the server running?";
    } else {
      updatedText.textContent = "Updated " + new Date().toLocaleTimeString();
    }
  } finally {
    loading = false;
  }
}

loadAll();
setInterval(loadAll, REFRESH_SECONDS * 1000);

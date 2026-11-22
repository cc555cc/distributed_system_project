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

function eventText(event) {
  if (event.action === "fail_instance") {
    return event.edge_id + " failed";
  }
  if (event.action === "recovery_complete") {
    return event.edge_id + " recovered";
  }
  if (event.action === "transfer_bus") {
    return event.bus_id + " moved from " + event.source + " to " + event.target;
  }
  if (event.action === "register_instance") {
    return event.edge_id + " was added";
  }
  if (event.action === "start_simulated_instance") {
    return event.edge_id + " started to share the load";
  }
  if (event.action === "scale_down") {
    return event.edge_id + " was removed";
  }
  return event.action;
}

function buildAlerts(buses, health, simulation) {
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

  if (simulation) {
    simulation.workers.forEach(function (worker) {
      if (worker.state === "failed") {
        alerts.push({ kind: "bad", text: worker.edge_id + " failed (simulation)" });
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

function showBuses(buses) {
  const rows = buses.map(function (bus) {
    if (bus.data === null) {
      return [bus.id, bus.route, "no data", "-", "-", "-", "-"];
    }
    const stale = isStale(bus.data);
    return [
      bus.id,
      bus.route,
      stale ? tag("not live", "grey") : bus.data.bus_status,
      Math.round(bus.data.speed) + " km/h",
      occupancyText(bus.data),
      tag(crowdingOf(bus.data), crowdKind(crowdingOf(bus.data))),
      Math.round(ageSeconds(bus.data)) + " s ago"
    ];
  });
  busesBox.innerHTML = tableHtml(["Bus", "Route", "Status", "Speed", "Passengers", "Crowding", "Last update"], rows);
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
    let assigned = "unknown";
    if (edge.last_report) {
      queue = Math.round(edge.last_report.workload.queue_utilization_percent) + "%";
      assigned = edge.last_report.workload.assigned_buses;
    }
    return [
      edge.edge_id,
      edge.region_id,
      tag(edge.status, edgeKind(edge.status)),
      tag(edge.workload_status, loadKind(edge.workload_status)),
      queue,
      assigned
    ];
  });
  edgesBox.innerHTML = tableHtml(["Edge server", "Region", "Status", "Workload", "Queue", "Buses"], rows);
}

function showEvents(simulation) {
  if (simulation === null) {
    eventsBox.innerHTML = "<p>Failover events are not available right now.</p>";
    return;
  }
  if (simulation.events.length === 0) {
    eventsBox.innerHTML = "<p>No failover events yet.</p>";
    return;
  }
  let html = "<ul class='plain'>";
  simulation.events.slice(-10).reverse().forEach(function (event) {
    html += "<li>t=" + event.time + "s: " + eventText(event) + "</li>";
  });
  eventsBox.innerHTML = html + "</ul>";
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
    let simulation = null;

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
      simulation = await getSimulation();
    } catch (error) {
      problems.push("failover events");
    }

    const alerts = buildAlerts(buses, health, simulation);
    showSummary(buses, health, alerts);
    showAlerts(alerts);
    showBuses(buses);
    showRoutes(routes, buses);
    showEdges(health);
    showEvents(simulation);

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

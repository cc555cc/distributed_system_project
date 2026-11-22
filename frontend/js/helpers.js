// Controls and dashboard read the same backend simulation state and clock.
function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, function (character) {
    return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[character];
  });
}

function simulationTable(simulation, actions = false) {
  const rows = simulation.workers.map(function (worker) {
    const edgeId = escapeHtml(worker.edge_id);
    const failed = worker.state === "failed";
    const kind = failed ? "bad" : worker.state === "recovering" ? "warn" :
      worker.state === "healthy" ? "good" : "grey";
    const row = [edgeId, escapeHtml(worker.region_id), tag(worker.state, kind),
      Math.round(worker.queue_utilization_percent) + "%", escapeHtml(worker.assigned_buses.join(", ")) || "-"];
    if (actions) {
      row.push('<button data-id="' + edgeId + '" data-action="fail"' +
        (failed ? " disabled" : "") + '>Fail</button> <button data-id="' + edgeId +
        '" data-action="recover"' + (failed ? "" : " disabled") + '>Recover</button>');
    }
    return row;
  });
  const headers = ["Edge server", "Region", "State", "Queue", "Assigned buses"];
  if (actions) headers.push("Action");
  const events = (simulation.events || []).slice(-5).reverse().map(function (event) {
    return '<li>' + Number(event.time || 0).toFixed(1) + ' s: ' + escapeHtml(event.action) +
      ' ' + escapeHtml(event.edge_id || event.bus_id || '') + '</li>';
  }).join('');
  return tableHtml(headers, rows) + '<p>Recent simulation events</p><ul>' + events + '</ul>';
}

function simulationClockText(simulation) {
  const clock = simulation.clock || { running: false, speed: 1 };
  return (clock.running ? "Running" : "Paused") + " · " + clock.speed +
    "× real time · " + Number(simulation.virtual_seconds || 0).toFixed(1) + " simulation seconds";
}

function hasValidPosition(bus) {
  return Number.isFinite(bus.lat) && Number.isFinite(bus.lon) &&
    bus.lat >= -90 && bus.lat <= 90 &&
    bus.lon >= -180 && bus.lon <= 180 &&
    !(bus.lat === 0 && bus.lon === 0);
}

function ageSeconds(bus) {
  return (Date.now() - new Date(bus.last_updated).getTime()) / 1000;
}

function isStale(bus) {
  const age = ageSeconds(bus);
  return isNaN(age) || age > STALE_SECONDS || bus.bus_status === "idle";
}

function crowdingOf(bus) {
  if (bus.edge_metadata) {
    return bus.edge_metadata.crowding_status;
  }
  return "unknown";
}

function percentOf(bus) {
  if (bus.edge_metadata) {
    return bus.edge_metadata.occupancy_percent;
  }
  return null;
}

function occupancyText(bus) {
  const meta = bus.edge_metadata;
  if (meta) {
    return meta.passengers + " / " + meta.capacity + " (" + Math.round(meta.occupancy_percent) + "%)";
  }
  return bus.occupancy + " passengers";
}

function tag(text, kind) {
  return '<span class="tag ' + kind + '">' + text + "</span>";
}

function tableHtml(headers, rows) {
  if (rows.length === 0) {
    return "<p>Nothing to show.</p>";
  }
  let html = "<table><tr>";
  headers.forEach(function (header) {
    html += "<th>" + header + "</th>";
  });
  html += "</tr>";
  rows.forEach(function (row) {
    html += "<tr><td>" + row.join("</td><td>") + "</td></tr>";
  });
  return html + "</table>";
}

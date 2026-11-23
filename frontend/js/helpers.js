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
      queueMeter(worker), String(worker.active_assigned_buses ?? worker.assigned_buses.length) + " active / " + worker.assigned_buses.length];
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
  if (bus.data_age_seconds !== undefined) return bus.data_age_seconds;
  if (bus.simulation_age_seconds !== undefined) return bus.simulation_age_seconds;
  return (Date.now() - new Date(bus.last_updated).getTime()) / 1000;
}

function isStale(bus) {
  const age = ageSeconds(bus);
  return isNaN(age) || age > STALE_SECONDS || ["idle", "out_of_service"].includes(bus.bus_status) ||
    ["unknown", "unavailable", "degraded", "paused"].includes(bus.service_status || bus.simulation_status);
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

// Both operator pages use one snapshot, including issues on unselected buses.
function queueMeter(w) {
  const p = Math.round(w.queue_utilization_percent || 0);
  return '<div class="queue-meter ' + (p >= (w.overload_percent || 80) ? 'busy' : '') + '"><span style="width:' + p + '%"></span></div>' +
    (w.queue_size ?? '?') + ' / ' + (w.queue_capacity ?? '?') + ' (' + p + '%)';
}
async function renderSystemOverview(source) {
  const box = document.getElementById('system-overview');
  if (!box) return;
  const d = await getJson('/health/overview?source=' + source);
  box.innerHTML = '<div class="summary-strip"><strong>' + escapeHtml(source) + '</strong><span>' + d.healthy_buses + ' / ' + d.bus_count +
    ' buses healthy</span><span>' + d.sensor_issues.length + ' sensor issues</span></div>' +
    (source === 'demo' ? '<p class="note">70% trigger · one batch per crossing · ' + Object.entries(d.assistance).map(([region, policy]) => escapeHtml(region) + ': ' + (policy.third_draining ? 'finishing batch' : policy.armed ? 'armed' : 'waiting for pressure to fall')).join(' | ') + '</p>' : '') +
    tableHtml(['Edge', 'State', 'Queue', 'Buses', 'Completed'], d.workers.map(function (w) {
      return [escapeHtml(w.edge_id), tag(w.assistance_draining ? 'finishing batch' : w.state === 'healthy' && w.queue_utilization_percent >= (w.overload_percent || 80) ? 'overloaded' : w.state, w.state === 'healthy' && w.queue_utilization_percent < (w.overload_percent || 80) ? 'good' : 'warn'), queueMeter(w),
        Array.isArray(w.assigned_buses) ? (w.active_assigned_buses ?? w.assigned_buses.length) + ' active / ' + w.assigned_buses.length : (w.assigned_buses ?? '-'), w.completed ?? '-'];
    })) + '<h3>Sensor issues - all buses</h3>' + (d.sensor_issues.length ?
      tableHtml(['Bus', 'Route', 'Sensor', 'Status'], d.sensor_issues.slice(0,60).map(function (i) {
        return [escapeHtml(i.bus_id), escapeHtml(i.route_id), escapeHtml(i.sensor), tag(escapeHtml(i.status), 'bad')];
      })) : '<p class="success">All reporting sensors healthy</p>') +
    '<details><summary>Recent balancing & health events</summary><ul>' + d.events.map(function (e) {
      return '<li>' + escapeHtml((e.time ?? e.timestamp ?? '') + ' - ' + (e.action || e.type || '') + ' ' + (e.bus_id || e.edge_id || '') +
        (e.source ? ' ' + e.source + ' to ' + e.target : '')) + '</li>';
    }).join('') + '</ul></details>';
  return d;
}

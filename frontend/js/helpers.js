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

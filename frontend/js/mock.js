const mockRoutes = [
  { route_id: "302", name: "Route 302", region_id: "west" },
  { route_id: "901", name: "PULSE 901", region_id: "east" }
];

const mockStops = {
  "302": [
    ["2576", "Whitby Station", 43.865496, -78.93794],
    ["166", "Brock Street Northbound @ Arthur", 43.869007, -78.937411],
    ["167", "Brock Street Northbound @ St Peter", 43.871084, -78.93832],
    ["168", "Brock Street Northbound @ Burns", 43.872514, -78.938948],
    ["169", "Brock Street Northbound @ Trent", 43.873574, -78.939405],
    ["170", "Brock Street Northbound @ Gilbert", 43.876644, -78.940811],
    ["171", "Brock Street Northbound @ Dundas", 43.879703, -78.942101],
    ["172", "Brock Street Northbound @ John", 43.882531, -78.943093]
  ],
  "901": [
    ["2595", "Oshawa Centre Terminal", 43.889169, -78.877279],
    ["1287", "Eastbound @ 431 Gibb", 43.886764, -78.876312],
    ["1288", "Gibb Eastbound @ Grenfell", 43.887267, -78.874165],
    ["1289", "Gibb Eastbound @ Montrave", 43.887759, -78.872047],
    ["3228", "Gibb Eastbound @ Nassau", 43.889194, -78.866103],
    ["94151", "Gibb Street Eastbound @ Centre Street", 43.88991, -78.86271],
    ["672", "Simcoe Northbound @ Elm St", 43.890377, -78.860193],
    ["674", "Simcoe Northbound @ John", 43.894039, -78.86186]
  ]
};

function mockRouteStops(routeId) {
  const stops = mockStops[routeId] || [];
  return stops.map(function (stop, index) {
    return {
      direction_id: 0,
      position: index,
      stop_id: stop[0],
      name: stop[1],
      lat: stop[2],
      lon: stop[3]
    };
  });
}

function mockBus(busId) {
  const parts = busId.split("-");
  const stops = mockStops[parts[1]];
  if (!stops) {
    return null;
  }

  const offset = Number(parts[2]) * 3;
  const position = Date.now() / 4000 + offset;
  const index = Math.floor(position) % (stops.length - 1);
  const part = position - Math.floor(position);
  const from = stops[index];
  const to = stops[index + 1];

  const passengers = 10 + (Math.floor(position) * 7 + offset * 5) % 40;
  const percent = passengers / 50 * 100;
  let crowding = "low";
  if (percent >= 80) {
    crowding = "high";
  } else if (percent >= 50) {
    crowding = "medium";
  }

  let updated = new Date();
  if (busId === "bus-901-2") {
    updated = new Date(Date.now() - 120000);
  }

  return {
    bus_id: busId,
    lat: from[2] + (to[2] - from[2]) * part,
    lon: from[3] + (to[3] - from[3]) * part,
    speed: 28 + (index % 3) * 4,
    direction_id: 0,
    current_stop_id: from[0],
    next_stop_id: to[0],
    bus_status: "travelling",
    occupancy: passengers,
    sequence_number: Math.floor(position),
    lamport: Math.floor(position),
    edge_metadata: {
      route_id: parts[1],
      capacity: 50,
      passengers: passengers,
      occupancy_percent: percent,
      crowding_status: crowding
    },
    last_updated: updated.toISOString()
  };
}

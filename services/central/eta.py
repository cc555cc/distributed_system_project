import math

#functions for calculating eta.

DEFAULT_SPEED = 30 #fallback for when bus is stopped

#get the location of a bus in a route, use its lat and lon to estimate the eta of the rest of the 
#stops in that direction
def eta_bus(conn, bus_id) -> list[dict] | None:
    stops_eta = []
    bus = conn.execute(
        "SELECT s.lat, s.lon, s.current_stop_id, b.route_id, s.direction_id, s.speed FROM bus_states s JOIN buses b ON b.bus_id = s.bus_id WHERE s.bus_id = %s",
        (bus_id,)).fetchone()

    if bus is None:
        return None

    stops = conn.execute(
        "SELECT rs.stop_id, st.lat, st.lon FROM route_stops rs JOIN stops st ON st.stop_id = rs.stop_id WHERE rs.route_id = %s AND rs.direction_id = %s ORDER BY rs.position",
        (bus[3], bus[4])).fetchall()

    avg_speed = bus[5] if bus[5] > 0 else DEFAULT_SPEED
    for stop in stops:
        distance = distance_km(bus[0], bus[1], stop[1], stop[2]) #provide bus and stop lat and lon
        eta = distance / avg_speed * 60 #in minutes
        stops_eta.append({"stop_id": stop[0], "eta_minutes": round(eta, 1)})

    return stops_eta

#haversine helper function to give real distance between two GPS points
EARTH_RADIUS_KM = 6371
def distance_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    x = (math.sin((lat2 - lat1) / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) /2) ** 2)
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(x))



    


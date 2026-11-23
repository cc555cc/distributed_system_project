import math

# functions for calculating eta.

DEFAULT_SPEED = 30  # fallback for when bus is stopped


# get the location of a bus in a route, use its lat and lon to estimate the eta of the rest of the
# stops in that direction
def eta_bus(conn, bus_id) -> list[dict] | None:
    bus = conn.execute(
        "SELECT s.lat, s.lon, s.next_stop_id, b.route_id, s.direction_id, s.speed "
        "FROM bus_states s JOIN buses b ON b.bus_id = s.bus_id WHERE s.bus_id = %s",
        (bus_id,),
    ).fetchone()
    if bus is None:
        return None
    lat, lon, next_stop, route_id, direction, speed = bus

    stops = conn.execute(
        "SELECT rs.stop_id, st.lat, st.lon FROM route_stops rs "
        "JOIN stops st ON st.stop_id = rs.stop_id "
        "WHERE rs.route_id = %s AND rs.direction_id = %s ORDER BY rs.position",
        (route_id, direction),
    ).fetchall()
    stop_ids = [stop[0] for stop in stops]
    if next_stop not in stop_ids:
        return []
    
    avg_speed = speed if speed and speed > 0 else DEFAULT_SPEED
    etas, travelled_km, here = [], 0.0, (lat, lon)
    for stop_id, stop_lat, stop_lon in stops[stop_ids.index(next_stop):]:
        travelled_km += distance_km(*here, stop_lat, stop_lon)
        here = (stop_lat, stop_lon)
        etas.append({"stop_id": stop_id, "eta_minutes": round(travelled_km / avg_speed * 60, 1)})
    return etas


# haversine helper function to give real distance between two GPS points
EARTH_RADIUS_KM = 6371


def distance_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    x = (
        math.sin((lat2 - lat1) / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(x))

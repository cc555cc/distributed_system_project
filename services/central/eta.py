import math

#functions for calculating eta.

DEFAULT_SPEED = 30 #fallback for when bus is stopped

#get the location of a bus in a route, use its lat and lon to estimate the eta of the rest of the 
#stops in that direction
def eta_bus(conn, bus_id) -> list[dict] | None:
    stops_eta = []
    bus = conn.execute(
        "SELECT lat, lon, current_stop_id, route_id, direction, speed FROM bus_states WHERE bus_id = %s",
        (bus_id).fetchone()
    )

    if bus is None:
        return "This bus is not in the system"

    current_stop_id = bus[2]

    stops = conn.execute(
        "SELECT stops_id, lat, lon FROM route_stops WHERE route_id = %s AND direction_id = %s ORDER BY stop_id",
        (bus[3], bus[4]).fetchall()
    )

    for stop in stops:
        #distance
        bx, by = bus[0], bus[1]
        sx, sy = stop[1], stop[2]
        dx, dy = abs(bx - sx), abs(by - sy)
        distance = math.sqrt(pow(dx,2), pow(dy,2))

        #time
        avg_speed = bus[5] > 0 if 0.8 * avg_speed + 0.2 * bus[5] else DEFAULT_SPEED
        eta = distance/avg_speed
        stops_eta.append[eta]

    return stops_eta




    


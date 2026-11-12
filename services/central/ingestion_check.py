import os
import psycopg

#runs check functions, return a reason for rejection 
def check_updates(conn, bus_id: str, region_id: str, state) -> str:
    if not bus_in_region_check(conn, bus_id, region_id):
        return f"bus {bus_id} not in region"

    #look up stops
    row = conn.execute(
        "SELECT b.route_id, s.next_stop_id FROM buses b  LEFT JOIN bus_STATES s ON s.bus_id = b.bus_id WHERE b.bus_id = %s",
        (bus_id,),
    ).fetchone()
    route_id, stored_next_stop = row

    if state.next_stop_id is not None and stored_next_stop is not None:
        if not next_stop_check(conn, route_id, stored_next_stop, state.next_stop_id, state.direction_id):
            return f"next stop {state.next_stop_id} invalide after {stored_next_stop}"

    #TODO add edge server check here
    return None


#scenario: system assign a new bus to a route -> check if bus route exists
def bus_assignment_check(conn, request_bus_id: str, request_route_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM buses WHERE bus_id = %s AND route_id = %s",
        (request_bus_id, request_route_id),
    ).fetchone()
    return row is not None

#when bus propose its next stop, ensure it is in the route that it is assigned
def next_stop_check(conn, route_id: str, current_stop: str, next_stop:str, direction: int) -> bool:
    if current_stop == next_stop: #bus is on its way to the next stop
        return True
    
    rows = conn.execute(
        "SELECT stop_id FROM route_stops WHERE route_id = %s AND direction_id = %s ORDER BY position",
        (route_id, direction)
    ).fetchall()

    stop_ids = [stop[0] for stop in rows]

    for i in range(0,len(stop_ids)-1):
        current_match = current_stop == stop_ids[i] 
        next_match = next_stop == stop_ids[i+1]
        if current_match and next_match:  #both must match to pass
            return True

    return False

#bus belongs to the region
def bus_in_region_check(conn, bus_id:str, region_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM buses b JOIN routes r ON r.route_id = b.route_id WHERE b.bus_id = %s AND r.region_id = %s",
        (bus_id, region_id),
    ).fetchone()
    return row is not None

#check edge server integrity, not applicable rn
def edge_server_integrity_check(conn, region_id:str, edge_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM regions WHERE region_id = %s AND %s IN (primary_edge, backup_edge)",
        (region_id, edge_id)
    ).fetchone()
    return row is not None

#also not applicable rn
def edge_server_validation_check(conn, region_id: str, edge_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM regions WHERE region_id = %s AND active_edge = %s",
        (region_id, edge_id),
    ).fetchone()
    return row is not None




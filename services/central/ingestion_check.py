# runs check functions, return a reason for rejection
def check_update(conn, bus_id: str, region_id: str, state) -> str | None:
    if not bus_in_region_check(conn, bus_id, region_id):
        return f"bus {bus_id} not in region"

    # look up stops
    row = conn.execute(
        "SELECT b.route_id, s.next_stop_id FROM buses b  LEFT JOIN bus_STATES s ON s.bus_id = b.bus_id WHERE b.bus_id = %s",
        (bus_id,),
    ).fetchone()
    if row is None:
        return f"bus {bus_id} not found"
    route_id, stored_next_stop = row
    metadata = getattr(state, "edge_metadata", None)
    if metadata is not None and metadata.route_id != route_id:
        return "metadata route differs from assigned route"

    # fencing: only the edge central currently assigns to this bus may update it
    # (rejects a failed/partitioned edge that comes back still thinking it owns the bus)
    if metadata is not None:
        reason = owner_check(bus_id, metadata.edge_id)
        if reason is not None:
            return reason

    if state.next_stop_id is not None and not next_stop_check(
        conn, route_id, stored_next_stop, state.next_stop_id, state.direction_id
    ):
        return f"next stop {state.next_stop_id} invalid after {stored_next_stop}"

    return None


# the sending edge must be the bus's current owner; buses central does not manage are not fenced
def owner_check(bus_id: str, edge_id: str) -> str | None:
    from services.central.ownership import bus_ownership

    owner = bus_ownership.owner(bus_id)
    if owner is not None and owner != edge_id:
        return f"edge {edge_id} is not the current owner of {bus_id} (owner: {owner})"
    return None


# scenario: system assign a new bus to a route -> check if bus route exists
def bus_assignment_check(conn, request_bus_id: str, request_route_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM buses WHERE bus_id = %s AND route_id = %s",
        (request_bus_id, request_route_id),
    ).fetchone()
    return row is not None


# when bus propose its next stop, ensure it is in the route that it is assigned
def next_stop_check(
    conn, route_id: str, current_stop: str | None, next_stop: str, direction: int
) -> bool:

    rows = conn.execute(
        "SELECT stop_id FROM route_stops WHERE route_id = %s AND direction_id = %s ORDER BY position",
        (route_id, direction),
    ).fetchall()

    stop_ids = [stop[0] for stop in rows]

    # A bus can report the same next stop while travelling toward it.
    # Its first reading still needs a stop on the assigned directional route.
    # update: in case of drop message, the checker should accept jumped stop so the bus doesn't get stuck for losing messages in between
    # it should now accept any valid stop after a direction change"
    if next_stop not in stop_ids:
        return False
    if current_stop is None or current_stop == next_stop:
        return True
    if current_stop not in stop_ids:
        return True

    new, old = stop_ids.index(next_stop), stop_ids.index(current_stop)
    # redirect bus that reached the terminal yto begin a new trip from the start.
    if old == len(stop_ids) - 1 and new <= 1:
        return True
    return new >= old


# bus belongs to the region
def bus_in_region_check(conn, bus_id: str, region_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM buses b JOIN routes r ON r.route_id = b.route_id WHERE b.bus_id = %s AND r.region_id = %s",
        (bus_id, region_id),
    ).fetchone()
    return row is not None


# check edge server integrity, not applicable rn
def edge_server_integrity_check(conn, region_id: str, edge_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM regions WHERE region_id = %s AND %s IN (primary_edge, backup_edge)",
        (region_id, edge_id),
    ).fetchone()
    return row is not None


# also not applicable rn
def edge_server_validation_check(conn, region_id: str, edge_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM regions WHERE region_id = %s AND active_edge = %s",
        (region_id, edge_id),
    ).fetchone()
    return row is not None

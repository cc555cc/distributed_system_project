"""PostgreSQL operations only. Routers handle HTTP; callers provide the connection URL."""

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from services.central.ingestion_check import check_update
from shared.schema.bus_state import BusStateUpdate


# bus states queries
def apply_bus_state(
    db_url,
    bus_id: str, region_id: str, state: BusStateUpdate, validator=check_update
) -> tuple[bool, str | None]:
    with psycopg.connect(db_url) as connection:
        if (
            state.edge_metadata is not None
            and state.edge_metadata.region_id != region_id
        ):
            return False, "metadata region differs from MQTT/API region"

        # validate sequence number first
        row = connection.execute(
            "SELECT sequence_number FROM bus_states WHERE bus_id = %s FOR UPDATE",
            (bus_id,),
        ).fetchone()
        if (
            row is not None and row[0] >= state.sequence_number
        ):  # a late message, drop it
            return False, "stale update"

        # use ingestion check first before pushing update to the database
        reason = validator(connection, bus_id, region_id, state)
        if reason is not None:
            return False, reason

        cursor = connection.execute(
            """
            INSERT INTO bus_states (bus_id, lat, lon, speed, direction_id, next_stop_id,
                                    bus_status, occupancy, sequence_number, lamport, edge_metadata, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (bus_id) DO UPDATE SET
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                speed = EXCLUDED.speed,
                direction_id = EXCLUDED.direction_id,
                next_stop_id = EXCLUDED.next_stop_id,
                bus_status = EXCLUDED.bus_status,
                occupancy = EXCLUDED.occupancy,
                sequence_number = EXCLUDED.sequence_number,
                lamport = EXCLUDED.lamport,
                edge_metadata = EXCLUDED.edge_metadata,
                last_updated = now()
            WHERE bus_states.sequence_number < EXCLUDED.sequence_number
            """,
            (
                bus_id,
                state.lat,
                state.lon,
                state.speed,
                state.direction_id,
                state.next_stop_id,
                state.bus_status,
                state.occupancy,
                state.sequence_number,
                state.lamport,
                Jsonb(state.edge_metadata.model_dump(mode="json"))
                if state.edge_metadata
                else None,
            ),
        )
        if cursor.rowcount == 0:
            return False, "stale update"
        return True, None



# bus states queries
def get_bus_state(db_url, bus_id: str):
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        row = connection.execute(
            "SELECT * FROM bus_states WHERE bus_id = %s", (bus_id,)
        ).fetchone()
    if row is None:
        raise ValueError(f"Bus: {bus_id} not found")
    return row



# bus states queries
def remove_bus_from_network(db_url, bus_id: str):
    with psycopg.connect(db_url) as connection:
        cursor = connection.execute(
            """
            UPDATE bus_states s SET
                current_stop_id = last.stop_id,
                next_stop_id    = last.stop_id,
                bus_status      = 'idle',
                occupancy       = 0,
                speed           = 0,
                edge_metadata   = NULL,
                last_updated    = now()
            FROM buses b,
                 LATERAL (
                     SELECT rs.stop_id FROM route_stops rs
                     WHERE rs.route_id = b.route_id AND rs.direction_id = s.direction_id
                     ORDER BY rs.position DESC LIMIT 1
                 ) AS last
            WHERE s.bus_id = %s AND b.bus_id = s.bus_id
            """,
            (bus_id,),
        )
    if cursor.rowcount == 0:
        raise ValueError(f"Bus: {bus_id} not found")
    return {"bus_id": bus_id, "bus_status": "idle"}



# buses queries
def get_all_buses(db_url, region_id: str | None = None, route_id: str | None = None):
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        rows = connection.execute(
            """
            SELECT b.bus_id, b.route_id, r.region_id,
                   s.lat, s.lon, s.speed, s.direction_id, s.next_stop_id, s.bus_status,
                   s.occupancy, s.sequence_number, s.edge_metadata, s.last_updated
            FROM buses b
            JOIN routes r ON r.route_id = b.route_id
            LEFT JOIN bus_states s ON s.bus_id = b.bus_id
            WHERE (CAST(%s AS TEXT) IS NULL OR r.region_id = %s)
              AND (CAST(%s AS TEXT) IS NULL OR b.route_id = %s)
            ORDER BY b.bus_id
            """,
            (region_id, region_id, route_id, route_id),
        ).fetchall()
    return rows



# routes queries
def get_all_route(db_url):
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        rows = connection.execute(
            "SELECT route_id, name, region_id FROM routes ORDER BY route_id"
        ).fetchall()
    return rows



# routes queries
def get_route_stops(
    db_url,
    route_id: str, direction: int | None = None
):
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        rows = connection.execute(
            """
            SELECT rs.direction_id, rs.position, s.stop_id, s.name, s.lat, s.lon
            FROM route_stops rs
            JOIN stops s ON s.stop_id = rs.stop_id
            WHERE rs.route_id = %s
              AND (%s::integer IS NULL OR rs.direction_id = %s)
            ORDER BY rs.direction_id, rs.position
            """,
            (route_id, direction, direction),
        ).fetchall()
    if not rows:
        raise ValueError("No stops found for this route and direction")
    return rows



# routes queries
def get_route_info(db_url, route_id: str):
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        row = connection.execute(
            "SELECT route_id, name, region_id FROM routes WHERE route_id = %s",
            (route_id,),
        ).fetchone()
    if row is None:
        raise ValueError("Route not found")
    return row



# stops queries
def get_all_stops(db_url):
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        row = connection.execute(
            "SELECT stop_id, name, lat, lon FROM stops ORDER BY stop_id"
        ).fetchall()
    return row



# stops queries
def get_stop_loc(db_url, stop_id: str):
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        row = connection.execute(
            "SELECT stop_id, lat, lon FROM stops WHERE stop_id = %s", (stop_id,)
        ).fetchone()
    if row is None:
        raise ValueError("Stop not found")
    return row



# Read database state before applying the pure ETA calculation.
def eta_bus(conn, bus_id) -> list[dict] | None:
    from services.central.eta import route_arrival
    stops_eta = []
    bus = conn.execute(
        "SELECT s.lat, s.lon, s.current_stop_id, b.route_id, s.direction_id, s.speed, s.edge_metadata, s.last_updated, s.bus_status FROM bus_states s JOIN buses b ON b.bus_id = s.bus_id WHERE s.bus_id = %s",
        (bus_id,),
    ).fetchone()

    if bus is None:
        return None

    stops = conn.execute(
        "SELECT rs.stop_id, st.lat, st.lon FROM route_stops rs JOIN stops st ON st.stop_id = rs.stop_id WHERE rs.route_id = %s AND rs.direction_id = %s ORDER BY rs.position",
        (bus[3], bus[4]),
    ).fetchall()

    from datetime import UTC, datetime
    stale = bus[7] is None or (datetime.now(UTC) - bus[7]).total_seconds() >= 30
    state = dict(lat=bus[0], lon=bus[1], current_stop_id=bus[2], speed=bus[5], edge_metadata=bus[6] or {})
    ordered = [dict(stop_id=stop[0], lat=stop[1], lon=stop[2]) for stop in stops]
    for stop in stops:
        estimate = None if stale or bus[8] in ("idle", "out_of_service") else route_arrival(ordered, state, stop[0])
        stops_eta.append({"stop_id": stop[0], "eta_minutes": round(estimate, 2) if estimate is not None else None,
                          "available": estimate is not None})

    return stops_eta



import os
from collections import Counter
from datetime import UTC, datetime
from typing import Literal

import psycopg
from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row

from services.central import health
from services.central.health import bus_health_snapshot
from shared.schema.network import load_network

router = APIRouter(tags=["routes"])
db_url = os.environ["DATABASE_URL"]


@router.get("/routes/{route_id}/health")
def get_route_health(route_id: str, source: Literal["live", "demo"] = "live"):
    network = load_network()
    if route_id not in {route["route_id"] for route in network["routes"]}:
        raise HTTPException(404, "Unknown route")
    buses = [bus for bus in bus_health_snapshot(source) if bus["route_id"] == route_id]
    notes = []
    if source == "demo":
        with health.simulation_clock.lock:
            state = health.simulation.snapshot()
        definitions = state["registered_buses"]
        states = state["central_states"]
        ages = {bus: state["virtual_seconds"] - received for bus, received in state["central_received_at"].items()}
        edges = [dict(edge_id=w["edge_id"], status=w["state"], queue_size=w["queue_size"],
                      queue_capacity=w["queue_capacity"], queue_utilization_percent=w["queue_utilization_percent"],
                      assigned_buses=len(w["assigned_buses"])) for w in state["workers"]]
    else:
        from services.central.routers.buses import get_all_buses

        definitions = network["buses"]
        try:
            rows = get_all_buses(route_id=route_id)
        except psycopg.Error:
            rows = []
            notes.append("Database unavailable; occupancy is not known")
        states = {row["bus_id"]: row for row in rows}
        ages = {row["bus_id"]: (datetime.now(UTC) - row["last_updated"]).total_seconds()
                for row in rows if row.get("last_updated") is not None}
        edges = []
        for edge in health.monitor.snapshot()["edges"]:
            workload = edge["last_report"]["workload"] if edge["last_report"] else {}
            edges.append(dict(edge_id=edge["edge_id"], status=edge["status"],
                              **{key: workload.get(key) for key in ("queue_size", "queue_capacity", "queue_utilization_percent", "assigned_buses")}))
    configured = {bus["bus_id"]: bus for bus in definitions if bus["route_id"] == route_id}
    samples, available = [], []
    for bus in buses:
        current = states.get(bus["bus_id"], {})
        if (bus["health"]["status"] not in ("healthy", "stationary")
                or ages.get(bus["bus_id"], 30) >= 30
                or current.get("bus_status") in (None, "out_of_service", "idle")):
            continue
        available.append(bus["bus_id"])
        metadata = current.get("edge_metadata") or {}
        passengers, capacity = metadata.get("passengers"), metadata.get("capacity")
        if isinstance(passengers, int) and isinstance(capacity, int) and 0 <= passengers <= capacity and capacity > 0:
            samples.append((passengers, capacity))
    passengers = sum(count for count, _ in samples)
    sample_capacity = sum(capacity for _, capacity in samples)
    owners = {bus["edge_id"] for bus in buses}
    return {"route_id": route_id, "source": source, "bus_count": len(buses),
            "status_counts": dict(Counter(bus["health"]["status"] for bus in buses)),
            "availability": "available" if available else "unknown" if any(bus["health"]["status"] == "unknown" for bus in buses) else "unavailable",
            "available_buses": len(available), "configured_capacity": sum(bus["capacity"] for bus in configured.values()),
            "verified_service_capacity": sum(configured[bus]["capacity"] for bus in available),
            "occupancy_samples": len(samples), "passengers": passengers if samples else None,
            "available_seats_in_samples": sample_capacity - passengers if samples else None,
            "occupancy_percent": passengers / sample_capacity * 100 if sample_capacity else None,
            "edge_workload": [edge for edge in edges if edge["edge_id"] in owners],
            "notes": notes,
            "buses": [{"bus_id": bus["bus_id"], "health": bus["health"]} for bus in buses]}


# list all
@router.get("/routes")
def get_all_route():
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        rows = connection.execute(
            "SELECT route_id, name, region_id FROM routes ORDER BY route_id"
        ).fetchall()
    return rows


# get all stops on a route in order, optionally filtered by direction
@router.get("/routes/{route_id}/stops")
def get_route_stops(
    route_id: str, direction: int | None = Query(default=None, ge=0, le=1)
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
        raise HTTPException(
            status_code=404, detail="No stops found for this route and direction"
        )
    return rows


# get a route's name and region by id
@router.get("/routes/{route_id}")
def get_route_info(route_id: str):
    with psycopg.connect(db_url, row_factory=dict_row) as connection:
        row = connection.execute(
            "SELECT route_id, name, region_id FROM routes WHERE route_id = %s",
            (route_id,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Route not found")
    return row

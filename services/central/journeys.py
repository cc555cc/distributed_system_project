"""Public transit context and direct/one-transfer journey calculations."""

from datetime import UTC, datetime

from services.central.eta import riding_minutes, route_arrival
from shared.schema.network import load_network, route_stops


def transit_context(source="demo"):
    from services.central import health

    if source == "demo":
        with health.simulation_clock.lock:
            snapshot = health.simulation.snapshot()
        network = load_network(health.simulation.network_path)
        definitions = snapshot["registered_buses"]
        states = snapshot["central_states"]
        ages = {bus: snapshot["virtual_seconds"] - stamp for bus, stamp in snapshot["central_received_at"].items()}
        status = {bus: (value["status"] if snapshot["ownership"][bus]["available"] else "unknown")
                  if snapshot["demo_running"] else "paused" for bus, value in snapshot["demo_buses"].items()}
        cruises, dwell = {}, {}
        for bus, value in snapshot["demo_buses"].items():
            if value["condition"] not in {"stopped", "process_failure", "out_of_service"}:
                cruises[bus] = 9.0 if value["condition"] == "traffic" else 30.0
        with health.simulation_clock.lock:
            if health.simulation.demo:
                dwell = {bus: value["dwell"] for bus, value in health.simulation.demo.buses.items()}
        return dict(network=network, definitions=definitions, states=states, ages=ages, status=status,
                    cruises=cruises, dwell=dwell, source=source, time=snapshot["virtual_seconds"], snapshot=snapshot,
                    generation=snapshot["session_id"])
    from services.central.routers.buses import get_all_buses

    rows = get_all_buses()
    return dict(network=load_network(), definitions=load_network()["buses"], states={row["bus_id"]: row for row in rows},
                ages={row["bus_id"]: (datetime.now(UTC) - row["last_updated"]).total_seconds() for row in rows if row.get("last_updated")},
                status={bus["bus_id"]: bus["health"]["status"] for bus in health.bus_health_snapshot("live")},
                cruises={}, dwell={}, source=source, time=datetime.now(UTC).timestamp(), generation="live")


def patterns(context, region_id=None):
    return {(route["route_id"], direction): route_stops(context["network"], route["route_id"], direction)
            for route in context["network"]["routes"] if region_id is None or route["region_id"] == region_id
            for direction in (0, 1)}


def vehicles(context, region_id):
    ordered = patterns(context, region_id)
    result = []
    for definition in context["definitions"]:
        bus_id = definition["bus_id"]
        state = context["states"].get(bus_id)
        if not state or context["ages"].get(bus_id, 30) >= 30 or context["status"].get(bus_id) not in ("healthy", "stationary"):
            continue
        if state.get("bus_status") in ("idle", "out_of_service"):
            continue
        stops = ordered.get((definition["route_id"], state["direction_id"]))
        metadata = state.get("edge_metadata") or {}
        capacity, passengers = metadata.get("capacity"), metadata.get("passengers")
        if not stops or capacity is None or passengers is None:
            continue
        cruise = context["cruises"].get(bus_id)
        speed = state["speed"] if state["speed"] > 0 else cruise
        result.append(dict(bus_id=bus_id, route_id=definition["route_id"], direction_id=state["direction_id"],
            state=state, stops=stops, cruise=cruise, speed=speed, dwell=context["dwell"].get(bus_id, 0),
            free_seats=max(0, capacity-passengers), occupancy_percent=passengers/capacity*100,
            passengers=passengers, capacity=capacity, data_age_seconds=context["ages"].get(bus_id)))
    return result


def arrival(vehicle, stop):
    return route_arrival(vehicle["stops"], vehicle["state"], stop, cruise_speed=vehicle["cruise"], dwell_remaining=vehicle["dwell"])


def public_vehicle(vehicle):
    return {key: vehicle[key] for key in ("bus_id", "route_id", "direction_id", "free_seats", "occupancy_percent", "passengers", "capacity", "data_age_seconds")}


def arrivals(context, stop_id, region_id=None):
    result = []
    for vehicle in vehicles(context, region_id):
        eta = arrival(vehicle, stop_id)
        if eta is not None:
            result.append(dict(public_vehicle(vehicle), eta_minutes=round(eta, 2)))
    return sorted(result, key=lambda item: item["eta_minutes"])


def plan_journeys(context, origin, destination, region_id=None):
    fleet = vehicles(context, region_id)
    options = []
    for first in fleet:
        ids = [stop["stop_id"] for stop in first["stops"]]
        wait = arrival(first, origin)
        if wait is None or not first["speed"] or origin not in ids:
            continue
        start = ids.index(origin)
        if destination in ids and ids.index(destination) > start:
            ride = riding_minutes(first["stops"], start, ids.index(destination), first["speed"])
            leg = dict(public_vehicle(first), origin=origin, destination=destination, ride_minutes=round(ride, 2))
            options.append(dict(bus_id=first["bus_id"], legs=[leg], waiting_minutes=round(wait, 2),
                                riding_minutes=round(ride, 2), transfers=0, total_minutes=round(wait+ride, 2)))
        for second in fleet:
            other = [stop["stop_id"] for stop in second["stops"]]
            if second["route_id"] == first["route_id"] or destination not in other or not second["speed"]:
                continue
            for transfer in set(ids[start+1:]) & set(other[:other.index(destination)]):
                ride1 = riding_minutes(first["stops"], start, ids.index(transfer), first["speed"])
                next_bus = arrival(second, transfer)
                # One minute to transfer; never recommend a connection that already left.
                if next_bus is None or next_bus < wait + ride1 + 1:
                    continue
                ride2 = riding_minutes(second["stops"], other.index(transfer), other.index(destination), second["speed"])
                legs = [dict(public_vehicle(first), origin=origin, destination=transfer, ride_minutes=round(ride1, 2)),
                        dict(public_vehicle(second), origin=transfer, destination=destination, ride_minutes=round(ride2, 2))]
                options.append(dict(bus_id=first["bus_id"], legs=legs, transfer_stop=transfer, transfers=1,
                    waiting_minutes=round(next_bus-ride1, 2), riding_minutes=round(ride1+ride2, 2), total_minutes=round(next_bus+ride2, 2)))
    return options

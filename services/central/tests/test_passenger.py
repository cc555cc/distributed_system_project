"""Passenger journeys, capacity recommendations, sustained scheduling and real authorization."""

from copy import deepcopy

import pytest
from fastapi.testclient import TestClient

from services.central.eta import route_arrival
from services.central.tests.client import OperatorClient
from shared.schema.network import load_network, route_stops


@pytest.fixture
def system(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/passenger-tests")
    from services.central import health, scheduling
    from services.central.main import app
    from services.edge.main import EdgeClusterSimulation

    monkeypatch.setattr(health, "simulation", EdgeClusterSimulation())
    monkeypatch.setattr(health, "simulation_clock", health.SimulationClock(clock=lambda: 0.0))
    monkeypatch.setattr(scheduling, "demo_scheduler", scheduling.DemandScheduler())
    operator = OperatorClient(app)
    operator.patch("/health/simulation/demo", json={"running": True})
    operator.post("/health/simulation/advance", json={"seconds": 2.0})
    return TestClient(app), operator, health, scheduling


def test_operator_pages_apis_and_sessions_are_protected(system):
    public, operator, health, _ = system
    from services.central.routers import auth

    assert public.get("/user.html").status_code == 200
    assert "journey-form" in public.get("/user.html").text
    assert "demo-controls" not in public.get("/user.html").text
    for path in ("/control.html", "/controls.html", "/health.html", "/operator.html"):
        response = public.get(path, follow_redirects=False)
        assert response.status_code == 303 and response.headers["location"].startswith("/login.html")
    for path in ("/health/simulation", "/buses/health", "/sensors", "/alerts", "/scheduling/status", "/openapi.json"):
        assert public.get(path).status_code == 401
    assert public.post("/health/simulation/instances/edge-west-1/fail").status_code == 401
    assert health.simulation.workers["edge-west-1"].state == "healthy"
    assert operator.get("/control.html").status_code == 200
    assert operator.post("/health/simulation/demo", json={"running": False}, headers={"Origin": "https://other.example"}).status_code == 403
    assert public.post("/auth/login", json={"username": "ali", "password": "wrong"}).status_code == 401
    assert public.get("/auth/status", headers={"X-Role": "operator"}).json()["authenticated"] is False
    token = operator.cookies.get(auth.COOKIE)
    assert len(token) >= 32
    public.cookies.set(auth.COOKIE, "tampered")
    assert public.get("/health/simulation").status_code == 401
    operator.post("/auth/logout")
    assert operator.get("/health/simulation").status_code == 401
    public.cookies.set(auth.COOKIE, token)
    assert public.get("/health/simulation").status_code == 401  # revoked, not just deleted in browser


def test_login_cookie_expiry_and_private_files_do_not_leak(system):
    public, _, _, _ = system
    from services.central.routers import auth

    response = public.post("/auth/login", json={"username": auth.DEFAULT_USER, "password": auth.DEFAULT_PASSWORD})
    assert response.status_code == 200
    cookie = response.headers["set-cookie"].lower()
    assert "httponly" in cookie and "samesite=strict" in cookie
    token = public.cookies.get(auth.COOKIE)
    auth.sessions[token] = 0
    assert public.get("/health/simulation").status_code == 401
    assert public.get("/frontend/control.html").status_code in (401, 404)
    assert public.get("/js/../control.html", follow_redirects=False).status_code in (303, 401, 404)
    assert public.get("/js/config.js").status_code == 200


def test_eta_uses_ordered_distance_traffic_and_known_dwell_without_fallback():
    stops = [dict(stop_id="a", lat=0.0, lon=0.0), dict(stop_id="b", lat=0.01, lon=0.0),
             dict(stop_id="c", lat=0.01, lon=0.01), dict(stop_id="d", lat=0.0, lon=0.01)]
    state = dict(lat=0.0, lon=0.0, speed=30.0, edge_metadata={"current_stop_id": "a"})
    eta = route_arrival(stops, state, "d")
    assert 6 < eta < 8  # three sides of the route, not the one-side straight distance
    slow = route_arrival(stops, {**state, "speed": 9.0}, "d")
    assert slow > eta * 3
    stopped = {**state, "speed": 0.0}
    assert route_arrival(stops, stopped, "d") is None
    assert route_arrival(stops, stopped, "a") == 0
    assert route_arrival(stops, stopped, "d", cruise_speed=30, dwell_remaining=3) > eta
    assert route_arrival(stops, state, "missing") is None


def test_public_arrivals_and_direct_journeys_use_actual_capacity(system):
    public, _, health, _ = system
    network = load_network()
    ordered = route_stops(network, "302", 0)
    origin, destination = ordered[3]["stop_id"], ordered[8]["stop_id"]
    params = dict(source="demo", region_id="west", stop_id=origin)
    response = public.get("/passenger/arrivals", params=params)
    assert response.status_code == 200 and response.json()["arrivals"]
    choice = response.json()["recommended"]
    assert choice["free_seats"] == choice["capacity"] - choice["passengers"]
    assert choice["eta_minutes"] >= 0 and choice["explanation"]
    result = public.get("/passenger/journeys", params={"origin": origin, "destination": destination, "region_id": "west"}).json()
    assert result["recommended"] and result["recommended"]["transfers"] == 0
    trip = result["recommended"]
    assert trip["total_minutes"] == pytest.approx(trip["waiting_minutes"] + trip["riding_minutes"], abs=.02)
    assert all(leg["route_id"] in {"302", "900"} for leg in trip["legs"])
    # A full bus remains listed but cannot be recommended for boarding.
    for bus_id in list(health.simulation.central_states):
        state = health.simulation.central_states[bus_id]
        state["edge_metadata"]["passengers"] = state["edge_metadata"]["capacity"]
        state["edge_metadata"]["occupancy_percent"] = 100.0
        state["edge_metadata"]["crowding_status"] = "high"
        state["occupancy"] = state["edge_metadata"]["capacity"]
    result = public.get("/passenger/arrivals", params=params).json()
    assert result["recommended"] is None and result["unavailable"]
    assert all(bus["free_seats"] == 0 for bus in result["arrivals"])
    snapshot = public.get("/passenger/snapshot").json()
    assert snapshot["buses"] and "edge_id" not in str(snapshot)


def test_missing_stale_invalid_and_cross_area_journeys_do_not_invent_results(system):
    public, operator, health, _ = system
    ordered = route_stops(load_network(), "302", 0)
    origin, destination = ordered[3]["stop_id"], ordered[8]["stop_id"]
    for params in ({"origin": "missing", "destination": destination}, {"origin": origin, "destination": origin},
                   {"origin": origin, "destination": destination, "region_id": "east"}):
        assert public.get("/passenger/journeys", params=params).status_code == 422
    assert public.get("/passenger/arrivals", params={"stop_id": origin, "party_size": 0}).status_code == 422
    operator.patch("/health/simulation/demo", json={"running": False})
    assert public.get("/passenger/arrivals", params={"stop_id": origin}).json()["recommended"] is None
    # No live map or private health state is substituted for unavailable demo data.
    health.simulation.central_received_at = {bus: -100 for bus in health.simulation.central_states}
    assert public.get("/passenger/journeys", params={"origin": origin, "destination": destination}).json()["options"] == []


def test_one_transfer_journey_respects_connection_timing_and_city_filter():
    from services.central.journeys import plan_journeys
    stops = [dict(stop_id=name, lat=0.0, lon=index*.001, name=name, city_id="C") for index, name in enumerate("axbcd")]
    network = dict(routes=[dict(route_id="R1", region_id="east"), dict(route_id="R2", region_id="east")])
    first = dict(bus_id="first", route_id="R1", city_id="C", region_id="east", capacity=20)
    second = dict(bus_id="second", route_id="R2", city_id="C", region_id="east", capacity=20)
    def state(stop, lat, lon, speed):
        return dict(lat=lat, lon=lon, speed=speed, direction_id=0, bus_status="travelling", edge_metadata={"current_stop_id": stop, "passengers": 2, "capacity": 20})
    # Patch only route geometry at the pure planning boundary for a known transfer.
    import services.central.journeys as journeys
    from unittest.mock import patch
    context = dict(network=network, definitions=[first, second], states={"first": state("a", 0., 0., 30.),
        "second": state("c", 0., .003, 2.)}, ages={"first": 0, "second": 0}, status={"first": "healthy", "second": "healthy"}, cruises={}, dwell={})
    geometry = {("R1", 0): [stops[0], stops[1], stops[2]], ("R2", 0): [stops[3], stops[1], stops[4]]}
    # Start first at a stop so waiting is known, second approaches the shared x slowly.
    context["states"]["first"]["speed"] = 0
    context["cruises"]["first"] = 30
    with patch.object(journeys, "patterns", return_value=geometry):
        options = plan_journeys(context, "a", "d", "east")
    assert options and options[0]["transfers"] == 1
    assert options[0]["total_minutes"] == pytest.approx(options[0]["waiting_minutes"] + options[0]["riding_minutes"], abs=.02)


def test_sustained_demand_approval_and_confirmed_dispatch(system):
    _, operator, health, scheduling = system
    assert operator.post("/scheduling/demand", json={"route_id": "302", "profile": "peak"}).status_code == 200
    assert not scheduling.demo_scheduler.decisions.get("302")
    operator.post("/health/simulation/advance", json={"seconds": 2.0})
    assert not scheduling.demo_scheduler.decisions.get("302")  # one high sample cannot change service
    for _ in range(2):
        operator.post("/health/simulation/advance", json={"seconds": 2.0})
    status = operator.get("/scheduling/status").json()
    route = next(route for route in status["routes"] if route["route_id"] == "302")
    assert route["occupancy_percent"] >= 80 and route["recommendation"]["status"] == "pending"
    approved = operator.post("/scheduling/302/approve").json()
    assert approved["status"] == "applied" and approved["headway_seconds"] < approved["old_headway_seconds"]
    new_bus = approved["dispatched_bus"]
    for _ in range(5):
        operator.post("/health/simulation/advance", json={"seconds": 2.0})
    after = operator.get("/scheduling/status").json()
    assert health.simulation.central_states[new_bus]["bus_status"] != "out_of_service"
    assert after["history"][-1]["execution"] == "confirmed by central telemetry"
    assert after["history"][-1]["confirmation_sequence"] > 0
    assert after["history"][-1]["waiting_after"] is not None
    assert after["comparisons"]["302"]["peak"]["occupancy_percent"] > after["comparisons"]["302"]["normal"]["occupancy_percent"]
    assert operator.post("/scheduling/302/approve").status_code == 422  # no repeated approval in cooldown


def test_random_counts_are_seeded_bounded_and_evaluation_uses_observed_arrivals(system):
    _, operator, health, _ = system
    from services.data_simulation.generate_data import DemoFleet
    definitions = load_network()["buses"]
    one = DemoFleet(health.simulation.network_path, definitions, 0, lambda event: None)
    two = DemoFleet(health.simulation.network_path, definitions, 0, lambda event: None)
    first = [one._reading(bus)["passengers"] for bus in one.buses.values()]
    second = [two._reading(bus)["passengers"] for bus in two.buses.values()]
    assert first == second and len(set(first)) > 1
    assert all(0 <= value <= 50 for value in first)
    for _ in range(20):
        operator.post("/health/simulation/advance", json={"seconds": 2.0})
    evaluation = operator.get("/scheduling/status").json()["eta_evaluation"]
    assert evaluation["sample_count"] > 0
    assert evaluation["mean_absolute_error_seconds"] >= 0


def test_automatic_demo_policy_requires_sustained_samples_and_obeys_cooldown(system):
    public, operator, _, _ = system
    assert public.post("/scheduling/policy", json={"automatic": True}).status_code == 401
    operator.post("/scheduling/demand", json={"route_id": "302", "profile": "peak"})
    operator.post("/scheduling/policy", json={"automatic": True})
    for _ in range(7):
        operator.post("/health/simulation/advance", json={"seconds": 2.0})
    result = operator.get("/scheduling/status").json()
    changes = [entry for entry in result["history"] if entry["route_id"] == "302"]
    assert result["automatic"] and len(changes) == 1
    assert changes[0]["execution"] == "confirmed by central telemetry"


def test_fleet_limit_and_active_count_are_distinct_and_visible_everywhere(system):
    public, operator, health, _ = system
    result = operator.post("/health/simulation/reset", json={"fleet_size": 50, "active_buses": 20})
    assert result.status_code == 200
    operator.patch("/health/simulation/demo", json={"running": True})
    operator.post("/health/simulation/advance", json={"seconds": 2.0})
    state = operator.get("/health/simulation").json()
    assert state["fleet_limit"] == 50 and state["active_buses"] == 20
    assert len(state["central_states"]) == 20
    overview = operator.get("/health/overview?source=demo").json()
    assert overview["healthy_buses"] == 20 and overview["sensor_issues"] == []
    assert len(overview["arrivals"]) == 20 and all(a["eta_minutes"] is not None for a in overview["arrivals"])
    assert len(public.get("/passenger/snapshot").json()["buses"]) == 50
    assert operator.post("/health/simulation/reset", json={"fleet_size": 20, "active_buses": 50}).status_code == 422


def test_real_request_burst_scales_third_worker_and_central_state_keeps_advancing(system):
    _, operator, health, _ = system
    initial = operator.get("/health/simulation").json()
    route_bus = next(bus["bus_id"] for bus in initial["registered_buses"] if bus["region_id"] == "west")
    old = initial["central_states"][route_bus]["sequence_number"]
    burst = operator.post("/health/simulation/workload", json={"region_id": "west", "count": 100, "processing_ms": 1000.0})
    assert burst.status_code == 200 and burst.json()["accepted"] == 100
    for _ in range(9): operator.post("/health/simulation/advance", json={"seconds": 1.0})
    overview = operator.get("/health/overview?source=demo").json()
    third = next(w for w in overview["workers"] if w["edge_id"] == "edge-west-3")
    assert third["state"] == "healthy" and len(third["assigned_buses"]) > 0
    assert all(w["queue_size"] <= w["queue_capacity"] for w in overview["workers"])
    assert operator.get("/health/simulation").json()["central_states"][route_bus]["sequence_number"] > old
    # Removing pressure lets processing drain; scale-down safely returns ownership.
    health.simulation.demo_running = False
    for w in health.simulation.workers.values(): w.processing_ms = 5.0
    health.simulation.advance(15)
    assert health.simulation.workers["edge-west-3"].state == "standby"
    assert health.simulation.journal == {}
    # Fifty active streams build real regional backlog at the demo service rate.
    from services.edge.main import EdgeClusterSimulation
    fleet = EdgeClusterSimulation(fleet_size=50)
    fleet.configure_demo(running=True)
    for w in fleet.workers.values(): w.processing_ms = 100.0
    fleet.advance(18)
    assert any(e["action"] == "assistance_batch" and e["edge_id"] == "edge-east-3" for e in fleet.events)


def test_custom_demand_and_sensor_fault_propagate_to_passenger_and_health(system):
    public, operator, _, _ = system
    assert operator.post("/scheduling/demand", json={"route_id": "302", "profile": "custom", "occupancy_percent": 80}).status_code == 200
    operator.post("/health/simulation/advance", json={"seconds": 2.0})
    buses = public.get("/passenger/snapshot").json()["buses"]
    bus = next(b for b in buses if b["bus_id"] == "bus-302-1")
    assert bus["state"]["edge_metadata"]["passengers"] == 40
    operator.patch("/health/simulation/demo", json={"bus_id": "bus-302-1", "condition": "sensor_fault"})
    operator.post("/health/simulation/advance", json={"seconds": 2.0})
    issues = operator.get("/health/overview?source=demo").json()["sensor_issues"]
    assert any(i["bus_id"] == "bus-302-1" and i["sensor"] == "occupancy" and i["status"] == "faulty" for i in issues)
    arrivals = public.get("/passenger/arrivals", params={"stop_id": bus["state"]["next_stop_id"], "region_id": "west"}).json()
    assert all(b["bus_id"] != "bus-302-1" for b in arrivals["arrivals"])
    operator.patch("/health/simulation/demo", json={"bus_id": "bus-302-1", "condition": "normal"})
    operator.post("/health/simulation/advance", json={"seconds": 3.0})
    assert not any(i["bus_id"] == "bus-302-1" for i in operator.get("/health/overview?source=demo").json()["sensor_issues"])


def test_workload_and_custom_settings_require_operator_and_reject_bad_input(system):
    public, operator, _, _ = system
    assert public.post("/health/simulation/workload", json={"region_id": "west", "count": 100}).status_code == 401
    for payload in ({"region_id": "nowhere", "count": 100}, {"region_id": "west", "count": 1001}):
        assert operator.post("/health/simulation/workload", json=payload).status_code == 422
    assert operator.post("/scheduling/demand", json={"route_id": "302", "profile": "custom"}).status_code == 422
    assert operator.post("/scheduling/demand", json={"route_id": "302", "profile": "custom", "occupancy_percent": 101}).status_code == 422

"""TEAM-05/06/08/14: observable results, not just successful command responses."""
from shared.schema.bus_state import BusStateUpdate
from shared.schema.network import load_network, route_stops


def advance(operator, seconds=2.0, count=1):
    for _ in range(count):
        assert operator.post("/health/simulation/advance", json={"seconds": seconds}).status_code == 200


def test_team05_central_state_matches_public_map_arrivals_and_recommendation(system):
    public, operator, health, _ = system
    state = health.simulation.central_states["bus-302-1"]
    entry = next(b for b in public.get("/passenger/snapshot").json()["buses"] if b["bus_id"] == "bus-302-1")
    assert entry["state"]["lat"] == state["lat"] and entry["state"]["sequence_number"] == state["sequence_number"]
    stop = route_stops(load_network(), "302", 0)[-1]
    result = public.get("/passenger/arrivals", params={"stop_id":stop["stop_id"],"region_id":"west"}).json()
    assert result["recommended"] and result["recommended"] in result["options"]
    assert result["recommended"]["free_seats"] == result["recommended"]["capacity"] - result["recommended"]["passengers"]
    nearby = public.get("/passenger/nearby", params={"lat":stop["lat"],"lon":stop["lon"],"region_id":"west"}).json()
    assert nearby["recommended"] and nearby["recommended"]["walking_minutes"] + .5 <= nearby["recommended"]["eta_minutes"]
    assert nearby["recommended"]["stop_id"] in {s["stop_id"] for s in nearby["nearest_stops"]}
    assert "journey-results" in public.get("/user.html").text


def test_team06_publisher_reading_survives_validation_processing_mapping_and_public_api(system):
    public, _, health, _ = system
    before = health.simulation.demo.buses["bus-302-1"]["local"]
    state = health.simulation.central_states[before["bus_id"]]
    checked = BusStateUpdate.model_validate(state)
    assert (checked.lat, checked.lon, checked.speed, checked.sequence_number) == (
        before["latitude"], before["longitude"], before["speed_kmh"], before["sequence_number"])
    assert checked.edge_metadata.passengers == before["passengers"]
    assert checked.edge_metadata.source_timestamp == before["timestamp"]
    view = next(b for b in public.get("/passenger/snapshot").json()["buses"] if b["bus_id"] == before["bus_id"])
    assert view["state"]["edge_metadata"]["passengers"] == checked.edge_metadata.passengers
    assert health.simulation.central_ack_count > 0


def test_team08_named_controls_change_shared_health_and_passenger_results(system):
    public, operator, health, _ = system
    assert operator.post("/health/simulation/scenarios/normal").status_code == 200
    advance(operator)
    assert operator.post("/scheduling/demand", json={"route_id":"302","profile":"custom","occupancy_percent":80}).status_code == 200
    advance(operator)
    row = next(b for b in public.get("/passenger/snapshot").json()["buses"] if b["bus_id"] == "bus-302-1")
    assert row["state"]["edge_metadata"]["occupancy_percent"] == 80
    operator.post("/health/simulation/scenarios/sensor_fault",json={"bus_id":"bus-302-1"})
    advance(operator)
    assert any(i["bus_id"] == "bus-302-1" and i["sensor"] == "occupancy" and i["status"] == "faulty"
               for i in operator.get("/health/overview?source=demo").json()["sensor_issues"])
    operator.post("/health/simulation/scenarios/recovery",json={"bus_id":"bus-302-1"})
    advance(operator, count=2)
    assert not any(i["bus_id"] == "bus-302-1" for i in operator.get("/health/overview?source=demo").json()["sensor_issues"])
    operator.post("/health/simulation/scenarios/traffic",json={"bus_id":"bus-302-1"})
    advance(operator)
    assert health.simulation.demo.buses["bus-302-1"]["condition"] == "traffic"
    assert public.post("/health/simulation/scenarios/normal").status_code == 401


def test_team14_demand_to_approved_dispatch_and_user_feedback_is_consistent(system):
    public, operator, health, _ = system
    operator.post("/health/simulation/scenarios/normal")
    advance(operator)
    operator.post("/health/simulation/scenarios/peak", json={"route_id":"302"})
    advance(operator, count=4)
    before = next(r for r in operator.get("/scheduling/status").json()["routes"] if r["route_id"] == "302")
    assert before["recommendation"]["status"] == "pending"
    result = operator.post("/scheduling/302/approve").json()
    assert result["headway_seconds"] < result["old_headway_seconds"] and result["dispatched_bus"]
    advance(operator, count=6)
    after = operator.get("/scheduling/status").json()
    entry = after["history"][-1]
    assert entry["execution"] == "confirmed by central telemetry" and entry["confirmation_sequence"] > 0
    bus = next(b for b in public.get("/passenger/snapshot").json()["buses"] if b["bus_id"] == result["dispatched_bus"])
    assert bus["state"]["bus_status"] != "out_of_service"
    assert bus["state"]["sequence_number"] == health.simulation.central_states[bus["bus_id"]]["sequence_number"]
    assert after["demand_measurements"]["302"]["waiting_passenger_seconds"] > 0
    assert len(after["history"]) == 1  # cooldown prevents immediate repeated dispatch
    assert operator.post("/health/simulation/scenarios/scheduling").status_code == 200
    advance(operator,count=8)
    automatic = operator.get("/scheduling/status").json()
    assert automatic["automatic"] and any(h["route_id"] == "302" and h["execution"] == "confirmed by central telemetry" for h in automatic["history"])

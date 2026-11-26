"""Combined realistic failure cases shared with the operator scenario table."""
import pytest
from services.central.simulation_scenarios import CASES


@pytest.fixture(autouse=True)
def clean_live_runner():
    from services.central.simulation_scenarios import stop_live
    stop_live()
    yield
    stop_live()


def real_clock(health):
    now = [0.0]
    health.simulation_clock.clock = lambda: now[0]
    health.simulation_clock.last = 0.0
    def elapsed(seconds):
        now[0] += seconds
        health.simulation_clock.tick()
    return elapsed


def test_recording_queue_is_visible_before_small_processing_batches():
    from services.edge.edge_server import SimulatedEdge
    worker = SimulatedEdge("recording-edge", "west", "DURHAM")
    worker.queue_visibility_seconds, worker.visible_jobs_per_second = 1.5, 8
    worker.queue.extend((i, "bus", 0.0) for i in range(40))
    worker.advance(1, 1)
    assert worker.completed == 0 and len(worker.queue) == 40
    worker.advance(1, 2)
    assert worker.completed == 8 and len(worker.queue) == 32
    worker.advance(0.1, 2.1)
    assert worker.completed == 8
    worker.advance(0.9, 3)
    assert worker.completed == 16 and len(worker.queue) == 24
    slow = SimulatedEdge("slow", "west", "DURHAM", processing_ms=250)
    slow.queue_visibility_seconds, slow.visible_jobs_per_second = 1.5, 8
    slow.queue.append((1, "bus", 0.0))
    for tick in range(1, 26):
        slow.advance(0.1, tick / 10)
    assert slow.completed == 1 and not slow.queue


@pytest.mark.parametrize("condition", ["sensor_fault", "sensors_stopped"])
def test_sensor_fault_freezes_only_affected_bus_processing_until_recovery(system, condition):
    _, operator, health, _ = system
    elapsed = real_clock(health)
    operator.post("/health/simulation/scenarios/normal")
    elapsed(3)
    def processed(bus):
        return sum(worker.completed_by_bus.get(bus, 0) for worker in health.simulation.workers.values())
    bus, peer = "bus-302-1", "bus-302-2"
    before, peer_before = processed(bus), processed(peer)
    sequence = health.simulation.central_states[bus]["sequence_number"]
    operator.patch("/health/simulation/demo", json={"bus_id":bus, "condition":condition})
    elapsed(5)
    assert processed(bus) == before and processed(peer) > peer_before
    assert health.simulation.central_states[bus]["sequence_number"] == sequence
    report = operator.get("/buses/" + bus + "/health?source=demo").json()
    assert report["pipeline"]["bus_processing"].startswith("paused")
    operator.patch("/health/simulation/demo", json={"bus_id":bus, "condition":"normal"})
    elapsed(5)
    assert processed(bus) > before
    assert health.simulation.central_states[bus]["sequence_number"] > sequence


def test_recording_all_cases_verify_recovery_with_new_readings_waiting(system):
    _, operator, health, _ = system
    from services.central import simulation_scenarios as cases
    operator.post("/health/simulation/live-cases/start?recording_pace=true")
    assert health.simulation_clock.speed == 0.5
    assert health.simulation.demo.movement_multiplier == 4
    cases.advance_live(sum(case["seconds"] * 3 + 3 for case in CASES))
    assert len(cases.results) == len(CASES)
    assert all(case["passed"] for case in cases.results.values())



@pytest.mark.parametrize("case",CASES,ids=[case["id"] for case in CASES])
def test_combined_edge_sensor_and_central_situations(system,case):
    _, operator, health, _ = system
    response = operator.post("/health/simulation/cases/"+case["id"])
    assert response.status_code == 200
    result = response.json()
    assert result["passed"], result["checks"]
    assert len(result["timeline"]) == case["seconds"]+1
    # Check independent invariants on evidence, not just the server's pass flag.
    for frame in result["timeline"]:
        owners = [bus for edge in frame["edges"] for bus in edge["assigned"]]
        assert len(owners) == len(set(owners)) == len(health.simulation.bus_regions)
        assert all(edge["queue"] <= edge["capacity"] for edge in frame["edges"])
        assert all(frame["owners"][bus] == edge["edge_id"] for edge in frame["edges"] for bus in edge["assigned"])
    if case["id"] == "busy_peer_failed":
        initial = {e["edge_id"]:e for e in result["timeline"][0]["edges"]}
        assert initial["edge-west-1"]["queue"] / initial["edge-west-1"]["capacity"] >= .7
        assert initial["edge-west-2"]["state"] == "failed"
        assert initial["edge-west-3"]["state"] == "healthy" and initial["edge-west-3"]["assigned"]
    assert health.simulation_clock.running is False  # Health displays the captured outcome.


def test_case_runner_requires_operator_and_rejects_unknown_cases(system):
    public, operator, health, _ = system
    original = health.simulation.session_id
    assert public.get("/health/simulation/cases").status_code == 401
    assert public.post("/health/simulation/cases/busy_peer_failed").status_code == 401
    assert health.simulation.session_id == original
    assert operator.post("/health/simulation/cases/unknown").status_code == 422
    assert health.simulation.session_id == original


def test_run_all_cases_reports_every_observed_outcome_and_preserves_evidence(system):
    _, operator, _, _ = system
    response = operator.post("/health/simulation/cases")
    assert response.status_code == 200
    results = response.json()
    assert {r["case_id"] for r in results} == {c["id"] for c in CASES}
    assert all(r["passed"] for r in results)
    catalogue = operator.get("/health/simulation/cases").json()
    assert all(c["result"]["passed"] for c in catalogue)
    evidence = operator.get("/health/simulation/cases/busy_peer_failed").json()
    assert evidence["timeline"][0]["edges"] and evidence["checks"]


def test_live_clock_drives_health_public_updates_and_continues_after_case(system):
    public, operator, health, _ = system
    elapsed = real_clock(health)
    from services.central.simulation_scenarios import live_status
    response = operator.post("/health/simulation/live-cases/central_outage/start")
    assert response.status_code == 200 and response.json()["elapsed_seconds"] == 0
    initial_time = health.simulation.time
    for _ in range(3):
        operator.get("/health/overview?source=demo")
        public.get("/passenger/snapshot")
    assert health.simulation.time == initial_time  # page polling never advances the clock
    elapsed(3)
    assert live_status()["phase"] == "observation" and not health.simulation.central_available
    saved = health.simulation.central_states["bus-302-1"]["sequence_number"]
    distance = health.simulation.demo.buses["bus-302-1"]["distance_m"]
    elapsed(4)
    overview = operator.get("/health/overview?source=demo").json()
    assert overview["live_scenario"]["running"] and overview["clock"]["speed"] == 1
    assert health.simulation.central_pending and health.simulation.demo.buses["bus-302-1"]["distance_m"] > distance
    bus = next(b for b in public.get("/passenger/snapshot").json()["buses"] if b["bus_id"] == "bus-302-1")
    assert bus["state"]["sequence_number"] == saved  # local movement is not invented central delivery
    elapsed(6)
    assert live_status()["status"] == "completed" and live_status()["passed"]
    assert health.simulation_clock.running and not health.simulation.central_pending
    sequence = health.simulation.central_states["bus-302-1"]["sequence_number"]
    elapsed(2)
    assert health.simulation.central_states["bus-302-1"]["sequence_number"] > sequence


def test_live_speed_and_pause_apply_to_scenario_and_bus_movement(system):
    _, operator, health, _ = system
    elapsed = real_clock(health)
    from services.central.simulation_scenarios import live_status
    operator.post("/health/simulation/live-cases/busy_peer_failed/start")
    elapsed(1)
    assert live_status()["elapsed_seconds"] == 1
    source_time = health.simulation.time
    operator.patch("/health/simulation/clock",json={"running":False,"speed":1.0})
    elapsed(10)
    assert health.simulation.time == source_time and live_status()["elapsed_seconds"] == 1
    operator.patch("/health/simulation/clock",json={"running":True,"speed":5.0})
    elapsed(1)
    assert live_status()["elapsed_seconds"] == 6
    assert health.simulation.time == pytest.approx(source_time+5)
    assert health.simulation.workers["edge-west-2"].state == "failed"


def test_live_sensor_fault_changes_health_and_passenger_suggestions_together(system):
    public, operator, health, _ = system
    elapsed = real_clock(health)
    operator.post("/health/simulation/live-cases/sensor_fault/start")
    elapsed(3)
    report = operator.get("/buses/bus-302-1/health?source=demo").json()
    sensors = operator.get("/buses/bus-302-1/sensors?source=demo").json()
    assert report["health"]["status"] == "degraded" and sensors["sensors"]["occupancy"]["health_status"] == "faulty"
    assert sensors["sensors"]["gps"]["health_status"] == "healthy"
    from shared.schema.network import load_network,route_stops
    stop = route_stops(load_network(),"302",0)[-1]["stop_id"]
    arrivals = public.get("/passenger/arrivals",params={"stop_id":stop,"region_id":"west"}).json()
    assert all(bus["bus_id"] != "bus-302-1" for bus in arrivals["arrivals"])
    elapsed(8)
    assert operator.get("/buses/bus-302-1/health?source=demo").json()["health"]["status"] == "healthy"
    assert any(bus["bus_id"] == "bus-302-1" for bus in public.get("/passenger/arrivals",params={"stop_id":stop,"region_id":"west"}).json()["arrivals"])


def test_live_sequence_all_cases_and_manual_reset_do_not_fast_forward_or_leak_actions(system):
    _, operator, health, _ = system
    elapsed = real_clock(health)
    from services.central.simulation_scenarios import live_status
    start = operator.post("/health/simulation/live-cases/start").json()
    assert start["running"] and start["queued"] == len(CASES)-1
    assert not any(case["result"] for case in operator.get("/health/simulation/cases").json())
    for _ in range(sum(c["seconds"]+3 for c in CASES)):
        elapsed(1)
    assert not live_status()["running"] and health.simulation_clock.running
    assert all(case["result"]["passed"] for case in operator.get("/health/simulation/cases").json())
    operator.post("/health/simulation/live-cases/standby_failure/start")
    assert operator.post("/health/simulation/reset",json={"fleet_size":5,"active_buses":50}).status_code == 422
    assert live_status()["running"]
    operator.post("/health/simulation/scenarios/normal")
    assert live_status()["status"] == "interrupted"
    elapsed(20)
    assert all(w.state != "failed" for w in health.simulation.workers.values())

"""TEAM-18: demo security and reliability acceptance; no production-security claim."""

def test_access_control_privacy_and_cross_origin_commands(system):
    public, operator, health, _ = system
    before = health.simulation.session_id
    for path in ("/health/overview", "/scheduling/status", "/openapi.json"):
        assert public.get(path).status_code == 401
    assert public.post("/health/simulation/scenarios/normal").status_code == 401
    assert operator.post("/health/simulation/scenarios/normal",headers={"Origin":"https://untrusted.example"}).status_code == 403
    assert health.simulation.session_id == before
    payload = public.get("/passenger/snapshot").json()
    assert not any(key in str(payload) for key in ("transit_operator", "password", "edge_id"))
    operator.post("/auth/logout")
    assert operator.get("/health/overview").status_code == 401


def test_threshold_grants_once_and_rearms_after_pressure_falls(system):
    _, _, _, _ = system
    from services.edge.main import EdgeClusterSimulation
    cluster = EdgeClusterSimulation()
    for w in cluster.workers.values(): w.processing_ms = 1000.0 if w.edge_id.endswith("-3") else 60000.0
    cluster.generate("bus-302-1",35)
    cluster.generate("bus-302-2",35)
    cluster.advance(6)
    third = cluster.workers["edge-west-3"]
    batches = [e for e in cluster.events if e["action"] == "assistance_batch"]
    assert third.active and len(batches) == 1
    old_assignments = set(cluster._assigned(third.edge_id))
    # No new work is admitted to the third between threshold grants.
    bus = next(iter(old_assignments))
    accepted = cluster.generate(bus,1)
    assert accepted["accepted"] == 0
    cluster.advance(2)
    assert len([e for e in cluster.events if e["action"] == "assistance_batch"]) == 1
    for w in cluster.workers.values(): w.processing_ms = 5.0
    cluster.advance(2)
    assert third.state == "standby" and not cluster._assigned(third.edge_id)
    assert cluster.assistance_armed["west"]
    for w in cluster.workers.values(): w.processing_ms = 1000.0 if w.edge_id.endswith("-3") else 60000.0
    cluster.generate("bus-302-1",35)
    cluster.generate("bus-302-2",35)
    cluster.advance(6)
    assert len([e for e in cluster.events if e["action"] == "assistance_batch"]) == 2
    assert all(len(w.queue) <= w.capacity for w in cluster.workers.values())


def test_failure_recovery_and_outage_preserve_sequence_and_queue_limits(system):
    _, operator, health, _ = system
    old = health.simulation.central_states["bus-302-1"]["sequence_number"]
    assert operator.post("/health/simulation/scenarios/failover",json={"bus_id":"bus-302-1"}).status_code == 200
    operator.post("/health/simulation/advance",json={"seconds":3.0})
    assert health.simulation.registry.get("bus-302-1").edge_id != "edge-west-1"
    operator.post("/health/simulation/scenarios/central_outage")
    operator.post("/health/simulation/advance",json={"seconds":4.0})
    assert health.simulation.central_pending
    operator.post("/health/simulation/scenarios/recovery",json={"bus_id":"bus-302-1"})
    operator.post("/health/simulation/advance",json={"seconds":5.0})
    assert not health.simulation.central_pending
    assert health.simulation.central_states["bus-302-1"]["sequence_number"] > old
    assert all(len(w.queue) <= w.capacity for w in health.simulation.workers.values())

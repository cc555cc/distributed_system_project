"""Repeatable combined failure cases. Runtime evidence and pytest use the same setup.

Each run uses accelerated source time, real edge processing and an isolated demo
cluster. No OS worker or live MQTT data is changed. The final demo is left paused
so the operator can inspect Health and the observed timeline.
"""
from copy import deepcopy
from services.edge.main import EdgeClusterSimulation

CASES = [
    dict(id="busy_peer_failed", title="One busy edge + partner failed", expected="Regional standby takes over; accepted readings reach central", seconds=8),
    dict(id="both_primaries_failed", title="Both primary edges failed", expected="Standby owns regional buses and keeps telemetry moving", seconds=6),
    dict(id="standby_failure", title="Standby fails during takeover", expected="Journal survives; a recovered primary resumes processing", seconds=12),
    dict(id="recovery_handback", title="Primary recovers while standby is busy", expected="Recovery window, synchronized handback, then standby", seconds=12),
    dict(id="owner_restart_full_peer", title="Owner restarts; peer full and standby failed", expected="Accepted journal is rebuilt and drained without sequence reset", seconds=8),
    dict(id="sensor_fault", title="One sensor faulty; GPS still works", expected="Only occupancy is faulty; restoration returns healthy data", seconds=8),
    dict(id="all_sensors_silent", title="All sensors silent on one bus", expected="Heartbeat timeout marks unavailable; recovery is observed", seconds=12),
    dict(id="central_outage", title="Central unavailable; edges still run", expected="Bounded pending updates retry and central sequence advances", seconds=10),
    dict(id="total_outage", title="Every edge and replica lost", expected="No invented recovery; last-known data is retained as unavailable", seconds=8),
]
results = {}
BUS = "bus-302-1"
A, B, C = "edge-west-1", "edge-west-2", "edge-west-3"


def _busy_failure(cluster):
    for worker in cluster.workers.values():
        if worker.region_id == "west": worker.processing_ms = 1000.0 if worker.edge_id != C else 100.0
    cluster.request_burst(80, "west")
    cluster.fail(B)


def _prepare(cluster, case_id):
    if case_id in {"busy_peer_failed", "standby_failure", "recovery_handback"}:
        _busy_failure(cluster)
    elif case_id == "both_primaries_failed":
        cluster.fail(A); cluster.fail(B)
    elif case_id == "owner_restart_full_peer":
        cluster.fail(C)
        for worker in cluster.workers.values():
            if worker.region_id == "west": worker.processing_ms = 60000.0
        cluster.request_burst(100,"west")
        cluster.fail(A)
    elif case_id == "sensor_fault":
        cluster.configure_demo(bus_id=BUS, condition="sensor_fault")
    elif case_id == "all_sensors_silent":
        cluster.configure_demo(bus_id=BUS, condition="sensors_stopped")
    elif case_id == "central_outage":
        cluster.configure_demo(central_available=False)
    elif case_id == "total_outage":
        for edge_id in cluster.workers: cluster.fail(edge_id)


def _actions(cluster, case_id, second):
    if case_id == "standby_failure":
        if second == 2: cluster.fail(C)
        if second == 4:
            cluster.workers[B].processing_ms = 5.0
            cluster.recover(B)
        if second == 8: cluster.recover(C)
    elif case_id == "owner_restart_full_peer" and second == 2:
        cluster.workers[A].processing_ms = 5.0
        cluster.recover(A)
    elif case_id == "recovery_handback" and second == 4:
        cluster.workers[A].processing_ms = cluster.workers[B].processing_ms = 5.0
        cluster.recover(B)
    elif case_id == "sensor_fault" and second == 4:
        cluster.configure_demo(bus_id=BUS, condition="normal")
    elif case_id == "all_sensors_silent" and second == 9:
        cluster.configure_demo(bus_id=BUS, condition="normal")
    elif case_id == "central_outage" and second == 6:
        cluster.configure_demo(central_available=True)


def _frame(cluster, second):
    state = cluster.snapshot()
    return dict(second=second, edges=[dict(edge_id=w["edge_id"], state=w["state"], queue=w["queue_size"], capacity=w["queue_capacity"],
                    assigned=list(w["assigned_buses"]), emergency=w["emergency_takeover"]) for w in state["workers"]],
        owners={bus:value["edge_id"] for bus,value in state["ownership"].items()},
        sequences={bus:value["sequence_number"] for bus,value in state["central_states"].items()},
        sensor_status=dict(state["demo_buses"][BUS]["sensors"]), bus_health=state["demo_buses"][BUS]["status"],
        local_gps=state["demo_buses"][BUS]["local"], central_pending=state["central_pending"],
        journal_pending=state["pending_telemetry"], acknowledged=state["central_ack_count"])


def _checks(cluster, case_id, timeline, baseline):
    last = timeline[-1]
    checks = []
    def check(label, passed): checks.append(dict(label=label, passed=bool(passed)))
    check("Every queue stays within capacity", all(0 <= e["queue"] <= e["capacity"] for f in timeline for e in f["edges"]))
    check("Central sequences never go backward", all(all(f["sequences"].get(bus,-1) >= sequence for bus,sequence in before["sequences"].items())
          for before,f in zip(timeline,timeline[1:])))
    check("Each bus has one queue owner", all(
        len([bus for e in f["edges"] for bus in e["assigned"]]) == len(set(f["owners"])) and
        all(f["owners"][bus] == e["edge_id"] for e in f["edges"] for bus in e["assigned"])
        for f in timeline))
    third = lambda f: next(e for e in f["edges"] if e["edge_id"] == C)
    peer = lambda f: next(e for e in f["edges"] if e["edge_id"] == B)
    if case_id in {"busy_peer_failed", "both_primaries_failed", "standby_failure", "recovery_handback"}:
        check("Standby actually takes ownership", any(third(f)["state"] == "healthy" and third(f)["assigned"] for f in timeline))
        west = [bus for bus,region in cluster.bus_regions.items() if region == "west"]
        check("Regional telemetry continues at central", all(last["sequences"].get(bus,0) > baseline[bus] for bus in west))
    if case_id == "busy_peer_failed":
        check("Failed partner stays failed during takeover", peer(last)["state"] == "failed")
        check("Standby is used during the emergency", third(last)["emergency"])
    elif case_id == "both_primaries_failed":
        check("Standby owns all regional buses", all(last["owners"][bus] == C for bus in cluster.bus_regions if cluster.bus_regions[bus] == "west"))
    elif case_id == "standby_failure":
        check("Standby failure was observed", any(third(f)["state"] == "failed" for f in timeline))
        check("Recovered primary passed through recovery", any(peer(f)["state"] == "recovering" for f in timeline) and peer(last)["state"] == "healthy")
        check("Accepted journal drained after recovery", last["journal_pending"] == 0)
    elif case_id == "recovery_handback":
        check("Recovery window was observed", any(peer(f)["state"] == "recovering" for f in timeline))
        check("Ownership returned before standby", third(last)["state"] == "standby" and not third(last)["assigned"])
        check("Accepted journal drained before handback", last["journal_pending"] == 0)
    elif case_id == "owner_restart_full_peer":
        owner = lambda f: next(e for e in f["edges"] if e["edge_id"] == A)
        check("Failed owner retained accepted journal", owner(timeline[0])["state"] == "failed" and timeline[0]["journal_pending"] > 0)
        check("Recovery rebuilt the owner's pending queue", any(owner(f)["state"] == "recovering" and owner(f)["queue"] > 0 for f in timeline))
        check("Recovered owner drained accepted work", owner(last)["state"] == "healthy" and last["journal_pending"] == 0)
        check("Central continued past the saved sequence", last["sequences"][BUS] > baseline[BUS])
    elif case_id == "sensor_fault":
        check("Occupancy fault observed while GPS stays healthy", any(f["sensor_status"]["occupancy"] == "faulty" and f["sensor_status"]["gps"] == "healthy" for f in timeline))
        check("Sensor restoration was observed", last["bus_health"] == "healthy" and all(v == "healthy" for v in last["sensor_status"].values()))
        check("Sensor fault did not invent an edge failure", all(e["state"] != "failed" for f in timeline for e in f["edges"]))
    elif case_id == "all_sensors_silent":
        check("Heartbeat timeout was observed", any(f["bus_health"] == "unavailable" for f in timeline))
        check("Valid telemetry returned after restoration", last["bus_health"] == "healthy" and last["sequences"][BUS] > baseline[BUS])
    elif case_id == "central_outage":
        check("Updates were pending during outage", any(f["central_pending"] > 0 for f in timeline))
        check("Retry cleared pending central delivery", last["central_pending"] == 0 and last["sequences"][BUS] > baseline[BUS])
    elif case_id == "total_outage":
        check("Every edge is failed", all(e["state"] == "failed" for e in last["edges"]))
        check("Central retains last known state", last["sequences"] == baseline)
        try:
            cluster.recover(A)
        except ValueError as error:
            rejected = "replica" in str(error)
        else:
            rejected = False
        check("Lost replicas do not produce fabricated recovery", rejected)
    return checks


def run_case(case_id):
    """Run deterministic source-time phases; keep evidence rather than asserting success in advance."""
    from services.central import health
    definition = next((case for case in CASES if case["id"] == case_id), None)
    if definition is None: raise ValueError("Unknown simulation case")
    with health.simulation_clock.lock:
        cluster = EdgeClusterSimulation(fleet_size=20)
        health.simulation = cluster
        from services.central import scheduling
        scheduling.demo_scheduler = scheduling.DemandScheduler()
        cluster.configure_demo(running=True)
        cluster.advance(2)
        baseline = {bus:state["sequence_number"] for bus,state in cluster.central_states.items()}
        timeline = []
        try:
            _prepare(cluster,case_id)
            timeline.append(_frame(cluster,0))
            for second in range(1,definition["seconds"]+1):
                _actions(cluster,case_id,second)
                cluster.advance(1)
                scheduling.observe_demo()
                timeline.append(_frame(cluster,second))
            checks = _checks(cluster,case_id,timeline,baseline)
        except (ValueError, RuntimeError, KeyError) as error:
            checks = [dict(label="Scenario completed: " + str(error),passed=False)]
        health.simulation = cluster
        health.simulation_clock.running = False
        health.simulation_clock.last = health.simulation_clock.clock()
        outcome = dict(case_id=case_id, passed=all(c["passed"] for c in checks), checks=checks, timeline=timeline,
                       baseline=baseline, model="accelerated simulation; live services unchanged")
        results[case_id] = deepcopy(outcome)
        return outcome


def catalogue():
    # Poll small summaries; detailed per-second evidence is loaded only on request.
    return [dict(case, result=dict(passed=results[case["id"]]["passed"], checks=results[case["id"]]["checks"])
                 if case["id"] in results else None) for case in CASES]


def evidence(case_id):
    if case_id not in results:
        raise ValueError("Run this case first")
    return deepcopy(results[case_id])

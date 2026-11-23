"""Repeatable combined failure cases. Runtime evidence and pytest use the same setup.

Interactive cases run on the shared elapsed-time clock; verification can also
replay the same phases deterministically. No OS worker or live MQTT data changes.
Health, passenger recommendations and evidence read the same running cluster.
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
_live = None
_live_queue = []
_live_fraction = 0.0
_last_live = None
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
        stop_live("Replaced by deterministic verification")
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
    # Poll small summaries; evidence remains available while a case is still running.
    status = live_status()
    return [dict(case, running=status.get("running",False) and status.get("case_id") == case["id"],
                 queued=case["id"] in _live_queue,
                 result=dict(passed=results[case["id"]]["passed"], checks=results[case["id"]]["checks"],status=results[case["id"]].get("status","completed"))
                 if case["id"] in results else None) for case in CASES]


def evidence(case_id):
    if _live and _live["definition"]["id"] == case_id:
        return dict(case_id=case_id,passed=None,checks=[],timeline=deepcopy(_live["timeline"]),status="running")
    if case_id not in results:
        raise ValueError("Run this case first")
    return deepcopy(results[case_id])


# Interactive cases advance only through SimulationClock.tick(), never through page polling.
def stop_live(reason="Stopped by operator"):
    global _live, _live_queue, _live_fraction, _last_live
    if _live:
        case_id = _live["definition"]["id"]
        _last_live = dict(running=False,status="interrupted",case_id=case_id,title=_live["definition"]["title"],message=reason)
        results[case_id] = dict(case_id=case_id,passed=False,status="interrupted",checks=[dict(label=reason,passed=False)],
                               timeline=deepcopy(_live["timeline"]),baseline=_live["baseline"])
    _live, _live_queue, _live_fraction = None, [], 0.0


def _start_live_case(case_id):
    global _live, _last_live
    from services.central import health, scheduling
    definition = next(case for case in CASES if case["id"] == case_id)
    cluster = EdgeClusterSimulation(fleet_size=20)
    cluster.configure_demo(running=True)
    cluster.advance(2)  # establish replicas and initial telemetry before the visible baseline
    health.simulation = cluster
    scheduling.demo_scheduler = scheduling.DemandScheduler()
    _live = dict(definition=definition,cluster=cluster,baseline={},timeline=[],warmup=0,second=0)
    _last_live = None
    results.pop(case_id,None)


def start_live(case_id=None):
    """Start one or all cases; return immediately so their transitions stay visible."""
    global _live_queue
    from services.central import health
    if case_id is not None and case_id not in {case["id"] for case in CASES}:
        raise ValueError("Unknown simulation case")
    with health.simulation_clock.lock:
        stop_live("Replaced by another live case")
        if case_id is None:
            results.clear()
            names = [case["id"] for case in CASES]
        else:
            names = [case_id]
        _start_live_case(names[0])
        _live_queue = names[1:]
        health.simulation_clock.running = True
        health.simulation_clock.last = health.simulation_clock.clock()
        return live_status()


def live_status():
    from services.central import health
    if _live and health.simulation is not _live["cluster"]:
        stop_live("Interrupted by a simulation reset")
    if not _live:
        return dict(_last_live or dict(running=False,status="idle"))
    definition = _live["definition"]
    return dict(running=True,status="running",case_id=definition["id"],title=definition["title"],
                phase="baseline" if _live["warmup"] < 3 else "observation",paused=not health.simulation_clock.running,
                elapsed_seconds=_live["warmup"]+_live["second"],duration_seconds=3+definition["seconds"],
                observation_seconds=_live["second"],queued=len(_live_queue))


def advance_live(seconds):
    """Consume elapsed source time; pause/speed are controlled by the shared real-time clock."""
    global _live, _live_fraction, _last_live
    from services.central import health, scheduling
    if not live_status()["running"]:
        return False
    _live_fraction += seconds
    while _live_fraction + 1e-9 >= 1:
        _live_fraction = max(0.0,_live_fraction-1)
        if _live is None:
            health.simulation.advance(1)
            scheduling.observe_demo()
            continue
        session = _live
        cluster, definition = session["cluster"], session["definition"]
        try:
            if session["warmup"] < 3:
                cluster.advance(1)
                scheduling.observe_demo()
                session["warmup"] += 1
                if session["warmup"] == 3:
                    session["baseline"] = {bus:state["sequence_number"] for bus,state in cluster.central_states.items()}
                    _prepare(cluster,definition["id"])
                    session["timeline"].append(_frame(cluster,0))
                continue
            second = session["second"]+1
            _actions(cluster,definition["id"],second)
            cluster.advance(1)
            scheduling.observe_demo()
            session["second"] = second
            session["timeline"].append(_frame(cluster,second))
            if second < definition["seconds"]:
                continue
            checks = _checks(cluster,definition["id"],session["timeline"],session["baseline"])
            outcome = dict(case_id=definition["id"],passed=all(c["passed"] for c in checks),checks=checks,
                           timeline=deepcopy(session["timeline"]),baseline=session["baseline"],status="completed",
                           model="elapsed real time multiplied by selected simulation speed")
        except (ValueError,RuntimeError,KeyError) as error:
            outcome = dict(case_id=definition["id"],passed=False,status="failed",checks=[dict(label=str(error),passed=False)],
                           timeline=deepcopy(session["timeline"]),baseline=session["baseline"])
        results[definition["id"]] = outcome
        _last_live = dict(running=False,status=outcome["status"],case_id=definition["id"],title=definition["title"],
                          passed=outcome["passed"],message="Observed checks complete; the simulation continues.")
        _live = None
        if _live_queue:
            _start_live_case(_live_queue.pop(0))
    if _live is None and _live_fraction > 0:
        health.simulation.advance(_live_fraction)
        scheduling.observe_demo()
        _live_fraction = 0.0
    return True

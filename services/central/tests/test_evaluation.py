"""CENTRAL-10: matched seeded runs, actual queue area/boarding and observed arrival errors."""
import json
import pytest


def measure(monkeypatch, automatic, traffic, duration=240):
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/evaluation")
    from services.central import health, scheduling
    from services.edge.main import EdgeClusterSimulation
    from services.central.journeys import transit_context
    cluster = EdgeClusterSimulation()
    cluster.configure_demo(running=True)
    for bus_id, bus in cluster.demo.buses.items():
        if bus_id != "bus-302-1":
            cluster.demo.configure(bus_id,"out_of_service",0)
            bus["reserved"] = True
    cluster.demo.set_demand("302","peak")
    engine = scheduling.DemandScheduler()
    monkeypatch.setattr(health,"simulation",cluster)
    monkeypatch.setattr(health,"simulation_clock",health.SimulationClock(clock=lambda:0.0))
    monkeypatch.setattr(scheduling,"demo_scheduler",engine)
    engine.observe(transit_context("demo"))
    engine.automatic = automatic
    occupancy, active_samples = [], []
    for _ in range(duration//2):
        if traffic:
            for bus in cluster.demo.buses.values():
                if not bus.get("reserved") and bus["condition"] != "out_of_service":
                    bus["condition"] = "traffic"
        cluster.advance(2)
        scheduling.observe_demo()
        route = next(m for m in engine.metrics(transit_context("demo")) if m["route_id"] == "302")
        if route["occupancy_percent"] is not None: occupancy.append(route["occupancy_percent"])
        active_samples.append(route["active_buses"])
    view = engine.view(transit_context("demo"))
    return dict(automatic=automatic, traffic=traffic, seconds=duration,
        waiting_passenger_seconds=cluster.demo.waiting_passenger_seconds["302"],
        boarded_passengers=cluster.demo.boarded_passengers["302"],
        waiting_at_end=sum(cluster.demo.waiting["302"].values()),
        mean_occupancy_percent=sum(occupancy)/len(occupancy),
        peak_active_buses=max(active_samples), confirmed_dispatches=sum(h["execution"] == "confirmed by central telemetry" for h in view["history"]),
        arrivals=view["eta_evaluation"])


@pytest.mark.parametrize("traffic",[False,True],ids=["normal-traffic","delayed-traffic"])
def test_measured_arrivals_compare_prediction_with_observed_source_time(monkeypatch,traffic,record_property):
    result = measure(monkeypatch,False,traffic)
    assert result["arrivals"]["sample_count"] > 0
    errors = result["arrivals"]["recent"]
    assert all(isinstance(item["error_seconds"],float) for item in errors)
    assert result["arrivals"]["mean_absolute_error_seconds"] >= 0
    record_property("arrival_measurements",json.dumps(result))


@pytest.mark.parametrize("traffic",[False,True],ids=["normal-traffic","delayed-traffic"])
def test_scheduling_on_off_compares_actual_waiting_boarding_and_crowding(monkeypatch,traffic,record_property):
    disabled = measure(monkeypatch,False,traffic)
    enabled = measure(monkeypatch,True,traffic)
    assert enabled["seconds"] == disabled["seconds"]
    assert disabled["confirmed_dispatches"] == 0 and enabled["confirmed_dispatches"] > 0
    assert enabled["peak_active_buses"] > disabled["peak_active_buses"]
    assert enabled["boarded_passengers"] >= disabled["boarded_passengers"]
    assert enabled["waiting_passenger_seconds"] <= disabled["waiting_passenger_seconds"]
    assert 0 <= enabled["mean_occupancy_percent"] <= 100
    # Occupancy need not improve in every run: report the observation rather than forcing success.
    comparison = dict(scheduling_off=disabled,scheduling_on=enabled)
    record_property("matched_demand_comparison",json.dumps(comparison))
    print(json.dumps(comparison))

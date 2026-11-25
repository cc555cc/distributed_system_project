"""Sustained demand signals and approved simulation dispatch, separate from edge balancing."""

from collections import deque
from threading import RLock


class DemandScheduler:
    def __init__(self):
        self.lock = RLock()
        self.generation = None
        self.samples, self.signatures, self.observed_at = {}, {}, {}
        self.decisions, self.applied_at = {}, {}
        self.history = deque(maxlen=100)
        self.profile_samples = {}
        self.changed_at = {}
        self.automatic = False
        from services.central.eta import ArrivalEvaluation
        self.evaluation = ArrivalEvaluation()

    def metrics(self, context):
        result = []
        for route in context["network"]["routes"]:
            values, signature = [], []
            speeds = []
            for bus in context["definitions"]:
                bus_id = bus["bus_id"]
                state = context["states"].get(bus_id)
                if bus["route_id"] != route["route_id"] or not state or context["ages"].get(bus_id, 30) >= 30:
                    continue
                if context["time"] - context["ages"].get(bus_id, 30) <= self.changed_at.get(route["route_id"], -1):
                    continue
                if context["status"].get(bus_id) not in {"healthy", "stationary"} or state.get("bus_status") in {"idle", "out_of_service"}:
                    continue
                metadata = state.get("edge_metadata") or {}
                if metadata.get("capacity", 0) > 0 and metadata.get("passengers") is not None:
                    values.append((metadata["passengers"], metadata["capacity"]))
                    signature.append((bus_id, state["sequence_number"]))
                    speeds.append(state["speed"])
            capacity = sum(value[1] for value in values)
            result.append(dict(route_id=route["route_id"], region_id=route["region_id"], active_buses=len(values),
                occupancy_percent=sum(value[0] for value in values) / capacity * 100 if capacity else None,
                signature=tuple(sorted(signature)), average_speed_kmh=sum(speeds)/len(speeds) if speeds else None,
                profile=context.get("snapshot", {}).get("route_demand", {}).get(route["route_id"], "live"),
                waiting_passengers=context.get("snapshot", {}).get("waiting_passengers", {}).get(route["route_id"])))
        return result

    def observe(self, context):
        with self.lock:
            if self.generation != context["generation"]:
                self.samples.clear(); self.signatures.clear(); self.observed_at.clear()
                self.decisions.clear(); self.applied_at.clear(); self.history.clear()
                self.profile_samples.clear()
                self.changed_at.clear()
                self.automatic = False
                self.generation = context["generation"]
                self.last_scan = -10  # a new clock must not wait for the previous scenario's elapsed time
                from services.central.eta import ArrivalEvaluation
                self.evaluation = ArrivalEvaluation()
            now = context["time"]
            self.evaluation.observe(context)
            for metric in self.metrics(context):
                route = metric["route_id"]
                if metric["occupancy_percent"] is None:
                    self.samples.pop(route, None)
                    self.decisions.pop(route, None)
                    continue
                if self.signatures.get(route) == metric["signature"] or now - self.observed_at.get(route, -10) < 2:
                    continue
                self.signatures[route], self.observed_at[route] = metric["signature"], now
                samples = self.samples.setdefault(route, deque(maxlen=3))
                samples.append(metric["occupancy_percent"])
                self.profile_samples.setdefault(route, {}).setdefault(metric["profile"], deque(maxlen=30)).append(dict(metric))
                if len(samples) < 3:
                    continue
                signal = "increase_service" if all(value >= 80 for value in samples) else "reduce_frequency" if all(value <= 20 for value in samples) else "normal"
                if signal == "normal":
                    self.decisions.pop(route, None)
                    continue
                if now - self.applied_at.get(route, -100) < 30:
                    continue
                old = self.decisions.get(route)
                if old and old["action"] == signal and old["status"] == "pending" and now - old["observed_at"] <= 30:
                    continue
                self.decisions[route] = dict(route_id=route, action=signal, status="pending", observed_at=now,
                    reason=f"Three distinct current samples {'at least 80%' if signal == 'increase_service' else 'at most 20%'} occupied; current {metric['occupancy_percent']:.1f}%.")

    def invalidate(self, route, now):
        with self.lock:
            self.samples.pop(route, None); self.signatures.pop(route, None); self.decisions.pop(route, None)
            self.changed_at[route] = now

    def approve(self, route_id, context):
        from services.central import health

        with health.simulation_clock.lock, self.lock:
            decision = self.decisions.get(route_id)
            now = context["time"]
            if not decision or decision["status"] != "pending" or now - decision["observed_at"] > 30:
                raise ValueError("No fresh pending scheduling recommendation")
            metric = next(metric for metric in self.metrics(context) if metric["route_id"] == route_id)
            high = decision["action"] == "increase_service"
            if metric["occupancy_percent"] is None or (high and metric["occupancy_percent"] < 80) or (not high and metric["occupancy_percent"] > 20):
                raise ValueError("Demand changed; wait for a current recommendation")
            if context["source"] != "demo":
                raise ValueError("Live dispatch needs a simulator scheduling consumer; use the controlled demonstration")
            with health.simulation_clock.lock:
                cluster = health.simulation
                current = cluster.headways.get(route_id, 10)
                wanted = max(1.0, current / 2) if high else min(3600.0, current * 1.5)
                cluster.schedule(route_id, "set_headway", headway_seconds=wanted)
                dispatched = None
                feedback = "Departure interval changed for future dispatches."
                if high:
                    spare = next((bus_id for bus_id, bus in cluster.demo.buses.items()
                                  if bus["definition"]["route_id"] == route_id and bus["condition"] == "out_of_service"
                                  and not any(bus_id in queue for queue in cluster.dispatch_queues.values())), None)
                    home_region = next(route["region_id"] for route in context["network"]["routes"] if route["route_id"] == route_id)
                    if spare is None and len(cluster.bus_definitions) < cluster.fleet_limit and cluster._eligible(home_region):
                        serial = len(cluster.bus_definitions) + 1
                        spare = f"reserve-{route_id}-{serial}"
                        while spare in cluster.bus_regions:
                            serial += 1; spare = f"reserve-{route_id}-{serial}"
                        cluster.register_bus(spare, route_id)
                    if spare:
                        cluster.schedule(route_id, "dispatch", bus_id=spare)
                        dispatched = spare
                        feedback = "Bus queued; bus_dispatched and central sequence updates confirm execution."
                    else:
                        feedback = "Fleet capacity reached; interval updated but no spare bus created."
                decision.update(status="applied", applied_at=now, old_headway_seconds=current,
                    headway_seconds=wanted, dispatched_bus=dispatched, feedback=feedback,
                    occupancy_before=metric["occupancy_percent"], waiting_before=metric["waiting_passengers"])
                self.applied_at[route_id] = now
                cluster.events.append(dict(time=cluster.time, action="scheduling_applied", route_id=route_id,
                                           bus_id=dispatched, headway_seconds=wanted))
                self.history.append(dict(decision))
                return dict(decision)

    def view(self, context):
        with self.lock:
            history = []
            current_metrics = {metric["route_id"]: metric for metric in self.metrics(context)}
            for decision in self.history:
                bus = context["states"].get(decision.get("dispatched_bus"), {})
                confirmed = bool(bus and bus.get("bus_status") != "out_of_service")
                history.append(dict(decision, execution="confirmed by central telemetry" if confirmed else "dispatch plan applied; awaiting vehicle update",
                    confirmation_sequence=bus.get("sequence_number"), occupancy_after=current_metrics[decision["route_id"]]["occupancy_percent"],
                    waiting_after=current_metrics[decision["route_id"]]["waiting_passengers"]))
                if not decision.get("dispatched_bus"):
                    history[-1]["execution"] = "departure interval plan applied"
            return dict(automatic=self.automatic, sample_interval_seconds=2, samples_required=3, cooldown_seconds=30,
                routes=[{**{key: value for key, value in metric.items() if key != "signature"},
                         "samples": len(self.samples.get(metric["route_id"], [])),
                         "headway_seconds": context.get("snapshot", {}).get("schedules", {}).get(metric["route_id"], {}).get("headway_seconds", 10),
                         "recommendation": self.decisions.get(metric["route_id"])} for metric in self.metrics(context)],
                history=history, eta_evaluation=self.evaluation.view(),
                demand_measurements=context.get("snapshot", {}).get("demand_measurements", {}),
                comparisons={route: {profile: dict(samples=len(samples),
                    occupancy_percent=sum(sample["occupancy_percent"] for sample in samples)/len(samples),
                    average_speed_kmh=sum(sample["average_speed_kmh"] for sample in samples)/len(samples),
                    waiting_passengers=sum(sample["waiting_passengers"] for sample in samples if sample["waiting_passengers"] is not None)/len(samples)
                        if any(sample["waiting_passengers"] is not None for sample in samples) else None)
                    for profile, samples in profiles.items()} for route, profiles in self.profile_samples.items()})


demo_scheduler = DemandScheduler()
live_scheduler = DemandScheduler()


def observe_demo():
    from services.central.journeys import transit_context
    from services.central import health
    if demo_scheduler.generation == health.simulation.session_id and health.simulation.time - getattr(demo_scheduler, "last_scan", -10) < 1:
        return
    context = transit_context("demo")
    demo_scheduler.observe(context)
    demo_scheduler.last_scan = context["time"]
    if demo_scheduler.automatic:
        for route, decision in list(demo_scheduler.decisions.items()):
            if decision["status"] == "pending":
                try:
                    demo_scheduler.approve(route, context)
                except ValueError:
                    continue

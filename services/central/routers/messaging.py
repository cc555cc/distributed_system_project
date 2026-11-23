"""Alerts and simulation commands carried over MQTT (MQTT-05).

GET /alerts lists alerts central has received on transit/alerts/#.
POST /commands publishes one simulation command to a bus or a whole region.
Operator-only access control for this endpoint is CENTRAL-09.
"""

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, ValidationError

from services.central.alert_folding import fold_repeats
from services.messaging import mqtt_client, topics
from services.messaging.commands import Action
from shared.schema.network import load_network

router = APIRouter(tags=["messaging"])

_network = load_network()
REGIONS = {region["region_id"] for region in _network["regions"]}
BUS_REGIONS = {bus["bus_id"]: bus["region_id"] for bus in _network["buses"]}


@router.get("/alerts")
def list_alerts(
    region_id: str | None = None,
    severity: Literal["info", "warning", "critical"] | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    source: Literal["live", "demo"] = "live",
    bus_id: str | None = None,
    fold: bool = True,
):
    if source == "live":
        store = mqtt_client.alert_store
        records = store.list(region_id=region_id, severity=severity, limit=store.limit)
        if fold:
            # One flapping source shows as one entry with a count, not dozens of rows.
            records = fold_repeats(records)
    else:
        from services.central import health

        with health.simulation_clock.lock:
            state = health.simulation.snapshot()
        records = []
        if not state["central_available"]:
            records.append(dict(alert_id="demo-central-down", source_id="central", severity="critical",
                                message="Injected central outage; delivery acknowledgements pending"))
        for bus in health.bus_health_snapshot("demo"):
            status = bus["health"]["status"]
            if status not in ("healthy", "stationary", "paused"):
                records.append(dict(alert_id="current-" + bus["bus_id"], bus_id=bus["bus_id"],
                    region_id=bus["region_id"], source_id=bus["edge_id"],
                    severity="info" if status == "unknown" else "warning",
                    message=status + ": " + "; ".join(bus["health"]["reasons"])))
        seen_events = set()
        for index, event in enumerate(reversed(state["events"])):
            action = event["action"]
            if action not in {"condition", "delivery_attempt", "central_retry", "central_expired", "central_fenced", "fail_instance", "recovery_complete", "central_available", "bus_dispatched"}:
                continue
            identity = event.get("bus_id")
            signature = (identity, event.get("edge_id"), action, event.get("reason"), event.get("condition"))
            if signature in seen_events:
                continue  # show the latest repeated warning, not one row per retry
            seen_events.add(signature)
            definition = next((bus for bus in state["registered_buses"] if bus["bus_id"] == identity), {})
            records.append(dict(alert_id="demo-event-" + str(index), bus_id=identity,
                region_id=definition.get("region_id"), source_id=event.get("edge_id", identity or "central"),
                severity="warning" if action in {"delivery_attempt", "central_retry", "central_expired", "central_fenced", "fail_instance"} else "info",
                simulation_seconds=event["time"], message=action + ": " + event.get("reason", event.get("condition", ""))))
    return [record for record in records
            if (bus_id is None or record.get("bus_id") in (None, bus_id))
            and (region_id is None or record.get("region_id") in (None, region_id))
            and (severity is None or record["severity"] == severity)][:limit]


class CommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    region_id: str
    bus_id: str | None = None  # omit to address every bus in the region
    action: Action
    params: dict[str, Any] = {}


@router.post("/commands", status_code=202)
def send_command(body: CommandRequest):
    if body.region_id not in REGIONS:
        raise HTTPException(404, f"Unknown region {body.region_id}")
    target = topics.ALL_BUSES
    if body.bus_id is not None:
        if BUS_REGIONS.get(body.bus_id) != body.region_id:
            raise HTTPException(
                404, f"Bus {body.bus_id} is not registered in region {body.region_id}"
            )
        target = body.bus_id
    try:
        command = mqtt_client.publish_command(
            body.region_id, body.action, body.params, target=target
        )
    except ValidationError as error:
        raise HTTPException(
            422, error.errors(include_url=False, include_context=False)
        ) from None
    except ConnectionError as error:
        raise HTTPException(503, str(error)) from None
    # 202: QoS 1 hands the command to MQTT; the bus may still be offline.
    return {
        "command_id": command.command_id,
        "topic": topics.command(command.region_id, command.target),
        "lamport_clock": command.lamport_clock,
        "status": "published",
    }

"""Alerts and simulation commands carried over MQTT (MQTT-05).

GET /alerts lists alerts central has received on transit/alerts/#.
POST /commands publishes one simulation command to a bus or a whole region.
Operator-only access control for this endpoint is CENTRAL-09.
"""

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, ValidationError

from services.messaging import mqtt_client, topics
from services.messaging.commands import Action
from shared.schema.network import load_network

router = APIRouter(tags=["messaging"])

_network = load_network()
REGIONS = {region["region_id"] for region in _network["regions"]}
BUS_REGIONS = {bus["bus_id"]: bus["region_id"] for bus in _network["buses"]}


@router.get("/alerts")
def list_alerts(region_id: str | None = None,
                severity: Literal["info", "warning", "critical"] | None = None,
                limit: int = Query(default=100, ge=1, le=500)):
    return mqtt_client.alert_store.list(region_id=region_id, severity=severity, limit=limit)


class CommandRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    region_id: str
    bus_id: str | None = None          # omit to address every bus in the region
    action: Action
    params: dict[str, Any] = {}


@router.post("/commands", status_code=202)
def send_command(body: CommandRequest):
    if body.region_id not in REGIONS:
        raise HTTPException(404, f"Unknown region {body.region_id}")
    target = topics.ALL_BUSES
    if body.bus_id is not None:
        if BUS_REGIONS.get(body.bus_id) != body.region_id:
            raise HTTPException(404, f"Bus {body.bus_id} is not registered in region {body.region_id}")
        target = body.bus_id
    try:
        command = mqtt_client.publish_command(body.region_id, body.action, body.params, target=target)
    except ValidationError as error:
        raise HTTPException(422, error.errors(include_url=False, include_context=False)) from None
    except ConnectionError as error:
        raise HTTPException(503, str(error)) from None
    # 202: QoS 1 hands the command to MQTT; the bus may still be offline.
    return {"command_id": command.command_id, "topic": topics.command(command.region_id, command.target),
            "lamport_clock": command.lamport_clock, "status": "published"}

"""Operator scheduling and demand feedback endpoints."""

from typing import Literal
import psycopg

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict

from services.central import health
from services.central.journeys import transit_context
from services.central import scheduling as scheduler_service

router = APIRouter(prefix="/scheduling", tags=["scheduling"])


class Policy(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    automatic: bool


class Demand(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    route_id: str
    profile: Literal["normal", "peak", "off_peak"]


@router.get("/status")
def status(source: Literal["demo", "live"] = "demo"):
    try:
        return (scheduler_service.demo_scheduler if source == "demo" else scheduler_service.live_scheduler).view(transit_context(source))
    except psycopg.Error as error:
        raise HTTPException(503, "Live demand data unavailable") from error


@router.post("/demand")
def change_demand(body: Demand):
    with health.simulation_clock.lock:
        health.simulation.configure_demo()
        scheduler_service.demo_scheduler.observe(transit_context("demo"))
        try:
            health.simulation.demo.set_demand(body.route_id, body.profile)
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        scheduler_service.demo_scheduler.invalidate(body.route_id, health.simulation.time)
        health.simulation.events.append(dict(time=health.simulation.time, action="demand_changed", route_id=body.route_id, condition=body.profile))
    return {"route_id": body.route_id, "profile": body.profile, "status": "applied"}


@router.post("/policy")
def set_policy(body: Policy):
    scheduler_service.observe_demo()
    scheduler_service.demo_scheduler.automatic = body.automatic
    return {"automatic": scheduler_service.demo_scheduler.automatic}


@router.post("/{route_id}/approve")
def approve(route_id: str):
    try:
        return scheduler_service.demo_scheduler.approve(route_id, transit_context("demo"))
    except ValueError as error:
        raise HTTPException(422, str(error)) from error

import logging
from pathlib import Path

from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

import asyncio
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from services.central.failover import run_round
from services.central.health import monitor
from services.central.health import simulation_clock
from services.central.health import router as health_router
from services.central.ownership import bus_ownership
from services.central.routers import bus_states, buses, eta, messaging, routes, stops
from services.messaging import mqtt_client

POLL_SECONDS = 1
logger = logging.getLogger(__name__)


# every second, central notice failed/recovered edges, move their buses accordingly, and tell the edges
async def poll_loop():
    while True:
        try:
            run_round(monitor, bus_ownership, mqtt_client.publish_assignment)
        except Exception:
            logger.exception(
                "Failover round failed"
            )  # it should keep polling even if one round fails
        await asyncio.sleep(POLL_SECONDS)


async def simulation_loop():
    while True:
        try:
            simulation_clock.tick()
        except Exception:
            logger.exception("Simulation clock step failed")
        await asyncio.sleep(0.1)


@asynccontextmanager
async def lifespan(app):
    mqtt_client.start()
    poll_task = asyncio.create_task(poll_loop())
    simulation_clock.last = simulation_clock.clock()
    clock_task = asyncio.create_task(simulation_loop())
    try:
        yield  # server pause here and handle request
    finally:
        poll_task.cancel()
        clock_task.cancel()
        await asyncio.gather(poll_task, clock_task, return_exceptions=True)
        mqtt_client.stop()  # execute on ctrl+C input or shutdown


app = FastAPI(lifespan=lifespan)

# allow the frontend dashboard to call this API from the browser
cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:5500,http://127.0.0.1:5500")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip() for origin in cors_origins.split(",") if origin.strip()
    ],
    allow_methods=["GET", "POST", "PUT", "PATCH"],
    allow_headers=["Content-Type"],
)

app.include_router(bus_states.router)
app.include_router(stops.router)
app.include_router(routes.router)
app.include_router(health_router)
app.include_router(eta.router)
app.include_router(buses.router)
app.include_router(messaging.router)

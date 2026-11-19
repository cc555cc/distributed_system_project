from pathlib import Path
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from fastapi import FastAPI
from services.central.routers import bus_states, stops, routes
from contextlib import asynccontextmanager
from services.messaging import mqtt_client
from services.central.health import router as health_router

@asynccontextmanager
async def lifespan(app):
    mqtt_client.start()
    yield #server pause here and handle request
    mqtt_client.stop() #execute on ctrl+C input or shutdown

app = FastAPI(lifespan=lifespan)
app.include_router(bus_states.router)
app.include_router(stops.router)
app.include_router(routes.router)
app.include_router(health_router)




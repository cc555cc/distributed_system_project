from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / "config" / ".env")

from fastapi import FastAPI
from app.routers import bus_states, stops, routes
from contextlib import asynccontextmanager
from app import mqtt_client

@asynccontextmanager
async def lifespan(app):
    mqtt_client.start()
    yield #server pause here and handle request
    mqtt_client.stop() #execute on ctrl+C input or shutdown

app = FastAPI(lifespan=lifespan)
app.include_router(bus_states.router)
app.include_router(stops.router)
app.include_router(routes.router)




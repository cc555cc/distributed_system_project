from pathlib import Path
import sys
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

from fastapi import FastAPI
from services.central.routers import bus_states, stops, routes
from contextlib import asynccontextmanager
from services.messaging import mqtt_client

@asynccontextmanager
async def lifespan(app):
    mqtt_client.start()
    yield #server pause here and handle request
    mqtt_client.stop() #execute on ctrl+C input or shutdown

app = FastAPI(lifespan=lifespan)
app.include_router(bus_states.router)
app.include_router(stops.router)
app.include_router(routes.router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

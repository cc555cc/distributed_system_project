from pathlib import Path
from fastapi import FastAPI
from app.routers import bus_states, stops, routes
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / "config" / ".env")

app = FastAPI()
app.include_router(bus_states.router)
app.include_router(stops.router)
app.include_router(routes.router)


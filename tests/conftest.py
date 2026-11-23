"""Global integration uses the real API and processing code with an injected clock."""
import pytest
from fastapi.testclient import TestClient
from services.central.tests.client import OperatorClient

@pytest.fixture
def system(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://unused/integration")
    from services.central import health, scheduling
    from services.central.main import app
    from services.edge.main import EdgeClusterSimulation
    monkeypatch.setattr(health, "simulation", EdgeClusterSimulation())
    monkeypatch.setattr(health, "simulation_clock", health.SimulationClock(clock=lambda: 0.0))
    monkeypatch.setattr(scheduling, "demo_scheduler", scheduling.DemandScheduler())
    operator = OperatorClient(app)
    operator.patch("/health/simulation/demo", json={"running": True})
    operator.post("/health/simulation/advance", json={"seconds": 2.0})
    return TestClient(app), operator, health, scheduling

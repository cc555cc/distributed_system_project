"""TEAM-06 native transport: real publishers, MQTT, edge processes, PostgreSQL and public UI data.

Uses the existing dedicated-service helper and isolated schema cleanup. Opt-in so
ordinary test runs do not require a broker or write to a developer database.
"""
import os
import pytest
from fastapi.testclient import TestClient


@pytest.mark.skipif(not os.getenv("EDGE_LIVE_DATABASE_URL"),reason="Set EDGE_LIVE_DATABASE_URL for real MQTT/PostgreSQL integration")
def test_team06_native_pipeline_reaches_public_passenger_payload(monkeypatch):
    from services.central.tests.test_live_edge_ingestion import test_live_four_instances_forward_and_report_failure_recovery as run_pipeline
    monkeypatch.setenv("DATABASE_URL", os.environ["EDGE_LIVE_DATABASE_URL"])
    from services.central.routers import buses

    def verify_public(app, scoped_url, sources):
        monkeypatch.setattr(buses,"db_url",scoped_url)
        public = TestClient(app)
        response = public.get("/passenger/snapshot?source=live")
        assert response.status_code == 200
        displayed = {b["bus_id"]:b for b in response.json()["buses"]}
        for source in sources:
            row = displayed[source["bus_id"]]["state"]
            assert row["lat"] == source["latitude"] and row["lon"] == source["longitude"]
            assert row["sequence_number"] == source["sequence_number"]
            assert row["edge_metadata"]["passengers"] == source["passengers"]
            assert "edge_id" not in row["edge_metadata"]  # management internals are private
        assert public.get("/user.html").status_code == 200
        assert public.get("/health/overview").status_code == 401
    run_pipeline(monkeypatch, public_probe=verify_public)

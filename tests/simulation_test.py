"""Combined realistic failure cases shared with the operator scenario table."""
import pytest
from services.central.simulation_scenarios import CASES


@pytest.mark.parametrize("case",CASES,ids=[case["id"] for case in CASES])
def test_combined_edge_sensor_and_central_situations(system,case):
    _, operator, health, _ = system
    response = operator.post("/health/simulation/cases/"+case["id"])
    assert response.status_code == 200
    result = response.json()
    assert result["passed"], result["checks"]
    assert len(result["timeline"]) == case["seconds"]+1
    # Check independent invariants on evidence, not just the server's pass flag.
    for frame in result["timeline"]:
        owners = [bus for edge in frame["edges"] for bus in edge["assigned"]]
        assert len(owners) == len(set(owners)) == len(health.simulation.bus_regions)
        assert all(edge["queue"] <= edge["capacity"] for edge in frame["edges"])
        assert all(frame["owners"][bus] == edge["edge_id"] for edge in frame["edges"] for bus in edge["assigned"])
    if case["id"] == "busy_peer_failed":
        initial = {e["edge_id"]:e for e in result["timeline"][0]["edges"]}
        assert initial["edge-west-1"]["queue"] / initial["edge-west-1"]["capacity"] >= .7
        assert initial["edge-west-2"]["state"] == "failed"
        assert initial["edge-west-3"]["state"] == "healthy" and initial["edge-west-3"]["assigned"]
    assert health.simulation_clock.running is False  # Health displays the captured outcome.


def test_case_runner_requires_operator_and_rejects_unknown_cases(system):
    public, operator, health, _ = system
    original = health.simulation.session_id
    assert public.get("/health/simulation/cases").status_code == 401
    assert public.post("/health/simulation/cases/busy_peer_failed").status_code == 401
    assert health.simulation.session_id == original
    assert operator.post("/health/simulation/cases/unknown").status_code == 422
    assert health.simulation.session_id == original


def test_run_all_cases_reports_every_observed_outcome_and_preserves_evidence(system):
    _, operator, _, _ = system
    response = operator.post("/health/simulation/cases")
    assert response.status_code == 200
    results = response.json()
    assert {r["case_id"] for r in results} == {c["id"] for c in CASES}
    assert all(r["passed"] for r in results)
    catalogue = operator.get("/health/simulation/cases").json()
    assert all(c["result"]["passed"] for c in catalogue)
    evidence = operator.get("/health/simulation/cases/busy_peer_failed").json()
    assert evidence["timeline"][0]["edges"] and evidence["checks"]

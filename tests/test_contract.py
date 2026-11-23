"""TEAM-02: the simulator, edge, central and UI share one checked wire format."""
from services.data_simulation.generate_data import generate_messages
from services.edge.main import EdgeClusterSimulation
from services.edge.validation.validator import parse_telemetry
from services.messaging import topics
from shared.schema.bus_state import BusStateUpdate
from shared.schema.network import load_network


def test_team02_all_configured_bus_contracts_and_topics_are_compatible():
    cluster = EdgeClusterSimulation()
    for bus in load_network()["buses"]:
        source = next(generate_messages(bus["route_id"],bus.get("direction_id",0),bus["bus_id"],max_updates=1,update_seconds=0))
        telemetry, errors = parse_telemetry(source, expected_region=bus["region_id"], expected_bus=bus["bus_id"], registry=cluster.registry)
        assert not errors and telemetry["schema_version"] == "bus-telemetry-v1"
        assert topics.telemetry(bus["region_id"],bus["bus_id"]).startswith("transit/buses/")
        assert topics.state(bus["region_id"],bus["bus_id"]) == f"transit/{bus['region_id']}/bus/{bus['bus_id']}/state"
        assert cluster.submit_telemetry(source)["accepted"]
    cluster.advance(1)
    for bus_id, state in cluster.central_states.items():
        parsed = BusStateUpdate.model_validate(state)
        assert parsed.edge_metadata.schema_version == "edge-central-v1"
        assert parsed.edge_metadata.route_id == cluster.registry.get(bus_id).route_id
        assert parsed.edge_metadata.passengers == state["occupancy"]


def test_team02_bad_contract_cannot_change_central_or_public_state(system):
    public, operator, health, _ = system
    before = health.simulation.central_states.copy()
    source = dict(health.simulation.demo.buses["bus-302-1"]["local"], region_id="east")
    assert not operator.post("/health/simulation/telemetry",json=source).json()["accepted"]
    assert health.simulation.central_states == before
    assert public.get("/passenger/nearby?lat=999&lon=0").status_code == 422

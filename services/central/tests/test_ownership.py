"""Central reassigns buses away from unhealthy edges and returns them home after recovery."""

import pytest

from services.central.ownership import BusOwnership


def network():
    #two regions, two edges each; every edge is home to some buses.
    return {
        "regions": [
            {"region_id": "west", "primary_edge": "west-1", "backup_edge": "west-2"},
            {"region_id": "east", "primary_edge": "east-1", "backup_edge": "east-2"},
        ],
        "buses": [
            {"bus_id": "w-a", "region_id": "west", "edge_id": "west-1"},
            {"bus_id": "w-b", "region_id": "west", "edge_id": "west-1"},
            {"bus_id": "w-c", "region_id": "west", "edge_id": "west-2"},
            {"bus_id": "e-a", "region_id": "east", "edge_id": "east-1"},
            {"bus_id": "e-b", "region_id": "east", "edge_id": "east-2"},
        ],
    }


ALL_HEALTHY = {"west-1": "healthy", "west-2": "healthy", "east-1": "healthy", "east-2": "healthy"}


def status(**overrides):
    #status(west_1="unavailable") -> ALL_HEALTHY with west-1 replaced
    return {**ALL_HEALTHY, **{key.replace("_", "-"): value for key, value in overrides.items()}}


@pytest.fixture
def ownership():
    return BusOwnership(network())


def test_starts_with_home_assignment(ownership):
    assert ownership.buses_of("west-1") == {"w-a", "w-b"}
    assert ownership.buses_of("west-2") == {"w-c"}
    assert ownership.owner("w-a") == "west-1"
    assert ownership.owner("unknown-bus") is None


def test_all_healthy_changes_nothing(ownership):
    epoch = ownership.assignment("west")["epoch"]
    assert ownership.reconcile(ALL_HEALTHY) == set()
    assert ownership.buses_of("west-1") == {"w-a", "w-b"}
    assert ownership.assignment("west")["epoch"] == epoch


def test_failover_moves_only_failed_edges_home_buses(ownership):
    epoch = ownership.assignment("west")["epoch"]
    assert ownership.reconcile(status(west_1="unavailable")) == {"west"}
    assert ownership.buses_of("west-1") == set()
    assert ownership.buses_of("west-2") == {"w-a", "w-b", "w-c"}
    assert ownership.assignment("west")["epoch"] == epoch + 1


def test_repeated_reconcile_is_idempotent(ownership):
    failed = status(west_1="unavailable")
    ownership.reconcile(failed)
    epoch = ownership.assignment("west")["epoch"]
    assert ownership.reconcile(failed) == set()
    assert ownership.buses_of("west-2") == {"w-a", "w-b", "w-c"}
    assert ownership.assignment("west")["epoch"] == epoch


def test_recovering_edge_does_not_get_buses_back_yet(ownership):
    ownership.reconcile(status(west_1="unavailable"))
    assert ownership.reconcile(status(west_1="recovering")) == set()
    assert ownership.buses_of("west-1") == set()


def test_failback_returns_only_home_buses(ownership):
    ownership.reconcile(status(west_1="unavailable"))
    assert ownership.reconcile(ALL_HEALTHY) == {"west"}
    assert ownership.buses_of("west-1") == {"w-a", "w-b"}
    assert ownership.buses_of("west-2") == {"w-c"}


def test_both_edges_down_moves_nothing(ownership):
    assert ownership.reconcile(status(west_1="unavailable", west_2="unavailable")) == set()
    assert ownership.buses_of("west-1") == {"w-a", "w-b"}
    assert ownership.buses_of("west-2") == {"w-c"}


def test_double_failure_then_home_recovers_takes_whole_region(ownership):
    ownership.reconcile(status(west_1="unavailable"))                     # west-2 takes everything
    ownership.reconcile(status(west_1="unavailable", west_2="unavailable"))  # nowhere to go
    assert ownership.buses_of("west-2") == {"w-a", "w-b", "w-c"}
    assert ownership.reconcile(status(west_2="unavailable")) == {"west"}  # west-1 back, west-2 still dead
    assert ownership.buses_of("west-1") == {"w-a", "w-b", "w-c"}
    assert ownership.buses_of("west-2") == set()


def test_startup_unknown_status_moves_nothing(ownership):
    unknown = {edge: "unknown" for edge in ALL_HEALTHY}
    assert ownership.reconcile(unknown) == set()
    assert ownership.reconcile({}) == set()
    assert ownership.buses_of("west-1") == {"w-a", "w-b"}


def test_staggered_startup_does_not_move_buses_to_the_first_healthy_edge(ownership):
    # west-1 has reported, west-2 has not been heard from yet (still starting)
    assert ownership.reconcile(status(west_2="unknown")) == set()
    assert ownership.buses_of("west-2") == {"w-c"}


def test_recovering_current_owner_keeps_its_buses(ownership):
    # w-c's home and current owner west-2 restarted; it is alive again, so nothing moves to west-1
    assert ownership.reconcile(status(west_2="recovering")) == set()
    assert ownership.buses_of("west-2") == {"w-c"}


def test_failure_never_crosses_regions(ownership):
    east_epoch = ownership.assignment("east")["epoch"]
    assert ownership.reconcile(status(west_1="unavailable", west_2="unavailable", east_1="unavailable")) == {"east"}
    assert ownership.buses_of("east-2") == {"e-a", "e-b"}
    assert ownership.buses_of("west-1") == {"w-a", "w-b"}  # west has no healthy edge; east-2 must not take them
    assert ownership.assignment("east")["epoch"] == east_epoch + 1


def test_assignment_describes_whole_region(ownership):
    ownership.reconcile(status(west_1="unavailable"))
    payload = ownership.assignment("west")
    assert payload["region_id"] == "west"
    assert payload["owners"] == {"w-a": "west-2", "w-b": "west-2", "w-c": "west-2"}
    assert isinstance(payload["epoch"], int)


def test_assignment_rejects_unknown_region(ownership):
    with pytest.raises(KeyError):
        ownership.assignment("north")

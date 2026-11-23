from services.central.alert_folding import fold_repeats


def alert(source, kind, second):
    """Alert received at 12:00:<second> (newest-first lists are built below)."""
    return {"source_id": source, "kind": kind,
            "received_at": f"2026-11-23T12:{second // 60:02d}:{second % 60:02d}+00:00"}


def test_repeats_within_the_window_fold_into_one_entry_with_a_count():
    newest_first = [alert("bus-407-1", "sensor_failure", s) for s in (50, 30, 10)]
    [entry] = fold_repeats(newest_first)
    assert entry["count"] == 3
    assert entry["received_at"].endswith("12:00:50+00:00")
    assert entry["first_received_at"].endswith("12:00:10+00:00")


def test_other_sources_and_kinds_are_not_merged():
    newest_first = [alert("bus-407-1", "sensor_recovered", 20),
                    alert("bus-302-1", "sensor_failure", 15),
                    alert("bus-407-1", "sensor_failure", 10)]
    assert [e["count"] for e in fold_repeats(newest_first)] == [1, 1, 1]


def test_a_gap_longer_than_the_window_opens_a_new_entry():
    newest_first = [alert("bus-407-1", "sensor_failure", s) for s in (130, 100, 20)]
    assert [e["count"] for e in fold_repeats(newest_first)] == [2, 1]
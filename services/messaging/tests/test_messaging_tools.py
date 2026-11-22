"""Delivery accounting (MQTT-06/10/11), broker security generation (MQTT-07) and the benchmark.

Offline except the last test, which runs a short benchmark when EDGE_LIVE_MQTT_HOST is set.
"""

import base64
import hashlib
import json
import os

import pytest

from services.data_simulation.run_buses import bus_credentials
from services.messaging import broker_security
from services.messaging.delivery import DUPLICATE, LATE, NEW, SequenceTracker
from shared.schema.network import load_network


# ==================== SEQUENCE TRACKER ====================
def test_tracker_classifies_new_duplicate_and_late_arrivals():
    tracker = SequenceTracker()
    assert [tracker.record("A", n) for n in (1, 2, 4, 2, 3, 5)] == [
        NEW,
        NEW,
        NEW,
        DUPLICATE,
        LATE,
        NEW,
    ]
    report = tracker.report({"A": [1, 2, 3, 4, 5]})
    assert (
        report["arrivals"],
        report["unique"],
        report["duplicates"],
        report["late"],
        report["lost"],
    ) == (6, 5, 1, 1, 0)


def test_tracker_lists_exactly_which_messages_never_arrived():
    tracker = SequenceTracker()
    for n in (1, 2, 6):
        tracker.record("A", n)
    tracker.record("B", 1)
    report = tracker.report({"A": list(range(1, 7)), "B": [1, 2]})
    assert report["missing"] == {"A": [3, 4, 5], "B": [2]} and report["lost"] == 4
    assert tracker.missing("A", range(1, 7)) == [3, 4, 5]


def test_tracker_reports_senders_nobody_expected():
    tracker = SequenceTracker()
    tracker.record("stranger", 1)
    assert tracker.report({})["unexpected"] == ["stranger"]


# ==================== BROKER SECURITY FILES ====================
def test_password_hash_uses_mosquitto_pbkdf2_sha512_format():
    salt = bytes(range(12))
    hashed = broker_security.mosquitto_hash("correct horse", salt=salt)
    kind, iterations, encoded_salt, encoded_hash = hashed.split("$")[1:]
    assert (kind, int(iterations)) == ("7", 101)
    assert base64.b64decode(encoded_salt) == salt
    expected = hashlib.pbkdf2_hmac("sha512", b"correct horse", salt, 101)
    assert base64.b64decode(encoded_hash) == expected
    assert broker_security.mosquitto_hash(
        "correct horse"
    ) != broker_security.mosquitto_hash("correct horse")


def test_generated_accounts_match_acl_users_and_env_file(tmp_path):
    network = load_network()
    passwords = broker_security.write(
        network, tmp_path / "secure", tmp_path / "secure.env"
    )
    acl_users = [
        line.split()[1]
        for line in (tmp_path / "secure" / "acl").read_text().splitlines()
        if line.startswith("user ")
    ]
    passwd_users = [
        line.split(":")[0]
        for line in (tmp_path / "secure" / "passwd").read_text().splitlines()
    ]
    assert acl_users == passwd_users == broker_security.accounts(network)
    assert len(set(passwords.values())) == len(passwords)  # unique per account
    env = dict(
        line.split("=", 1)
        for line in (tmp_path / "secure.env").read_text().splitlines()[1:]
    )
    assert env["CENTRAL_MQTT_PASSWORD"] == passwords["central"]
    assert env["EDGE_WEST_1_MQTT_PASSWORD"] == passwords["edge-west-1"]
    buses = json.loads(env["MQTT_BUS_PASSWORDS_JSON"].strip("'"))
    assert set(buses) == {bus["bus_id"] for bus in network["buses"]}
    for line in (
        (tmp_path / "secure" / "passwd").read_text().splitlines()
    ):  # no plaintext in passwd
        assert passwords[line.split(":")[0]] not in line


def test_acl_covers_alert_and_command_topics_without_cross_bus_access():
    from services.edge.auth_edge_server import mosquitto_acl

    acl = mosquitto_acl(load_network())
    central = acl.split("\n\n")[0]
    assert (
        "topic read transit/alerts/#" in central
        and "topic write transit/commands/#" in central
    )
    bus = acl.split("user bus-302-1\n", 1)[1].split("\n\n", 1)[0]
    assert (
        "topic read transit/commands/west/bus-302-1" in bus
        and "topic read transit/commands/west/all" in bus
    )
    assert (
        "topic write transit/alerts/west/bus-302-1" in bus
        and "commands/west/bus-302-2" not in bus
    )
    assert "write transit/commands" not in bus


def test_secure_compose_references_every_generated_secret():
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]
    compose = (root / "infrastructure" / "compose.secure.yaml").read_text()
    network = load_network()
    for user in broker_security.accounts(network):
        if not user.startswith("bus-"):
            assert "${" + broker_security.env_name(user) in compose, user
    assert "${MQTT_BUS_PASSWORDS_JSON" in compose


def test_bus_launcher_selects_each_bus_account(monkeypatch):
    monkeypatch.delenv("MQTT_BUS_PASSWORDS_JSON", raising=False)
    assert bus_credentials("bus-302-1") is None  # anonymous demo broker
    monkeypatch.setenv("MQTT_BUS_PASSWORDS_JSON", json.dumps({"bus-302-1": "secret-1"}))
    assert bus_credentials("bus-302-1") == ("bus-302-1", "secret-1")
    with pytest.raises(ValueError):
        bus_credentials("bus-302-2")  # no account: fail, never share


# ==================== BENCHMARK ====================
@pytest.mark.skipif(
    not os.getenv("EDGE_LIVE_MQTT_HOST"),
    reason="set EDGE_LIVE_MQTT_HOST to run the live benchmark",
)
@pytest.mark.parametrize("scenario", ["normal", "outage"])
def test_short_benchmark_delivers_every_message_in_order(scenario):
    from services.messaging.benchmark import run_once

    result = run_once(
        os.environ["EDGE_LIVE_MQTT_HOST"],
        int(os.getenv("EDGE_LIVE_MQTT_PORT", "1883")),
        3,
        scenario=scenario,
        rate=5,
        duration=6 if scenario == "outage" else 2,
        outage_s=1.0,
    )
    assert (
        result["lost"] == 0
        and result["late"] == 0
        and result["lamport_violations"] == 0
    )
    assert result["unique"] == result["published"]
    if scenario == "outage":
        assert result["reconnects"] == 3

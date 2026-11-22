"""Authenticate signed publishers and restrict edge MQTT operations to their scope.

Broker ACLs remain necessary to control the broker itself. These checks protect
edge ingestion even when the demo broker permits anonymous clients.
"""

import hashlib
import hmac
import json
import math
import os
import re
from threading import RLock
import time
from uuid import uuid4

from services.edge.data_to_json_converter import decode_json


def _segment(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]+", value) is not None


def _key(value):
    if not isinstance(value, str) or len(value.encode("utf-8")) < 32:
        raise ValueError("Publisher signing keys require at least 32 UTF-8 bytes")
    return value.encode("utf-8")


def _signed_bytes(topic, principal, timestamp, nonce, payload):
    return json.dumps([topic, principal, timestamp, nonce, payload], sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def sign_message(topic, payload, principal, secret, *, timestamp=None, nonce=None):
    """Publishers sign the exact topic and body; naming a bus is not authentication."""
    timestamp = time.time() if timestamp is None else timestamp
    nonce = uuid4().hex if nonce is None else nonce
    signature = hmac.new(_key(secret), _signed_bytes(topic, principal, timestamp, nonce, payload), hashlib.sha256).hexdigest()
    return dict(auth=dict(principal=principal, timestamp=timestamp, nonce=nonce, signature=signature), payload=payload)


def _environment_keys():
    keys = decode_json(os.getenv("EDGE_PUBLISHER_KEYS_JSON", "{}"))
    if not isinstance(keys, dict):
        raise ValueError("EDGE_PUBLISHER_KEYS_JSON must be a principal-to-secret object")
    for principal, secret in keys.items():
        if not isinstance(principal, str) or not principal.startswith("bus:") or not _segment(principal[4:]):
            raise ValueError("Publisher principals must use bus:<registered-id>")
        _key(secret)
    return keys


def _required(keys):
    setting = os.getenv("EDGE_AUTH_REQUIRED", "true" if keys else "false").lower()
    if setting not in ("true", "false"):
        raise ValueError("EDGE_AUTH_REQUIRED must be true or false")
    return setting == "true"


class EdgeAuth:
    def __init__(self, edge_id, region_id, registry, *, city_id=None, keys=None, required=True, clock=time.time):
        if not _segment(edge_id) or not _segment(region_id):
            raise ValueError("Edge and region IDs must be safe MQTT topic segments")
        self.edge_id, self.region_id, self.city_id = edge_id, region_id, city_id
        self.registry, self.required, self.clock = registry, required, clock
        self._keys = {principal: _key(secret) for principal, secret in (keys or {}).items()}
        if required and not self._keys:
            raise ValueError("Authentication enabled without publisher keys")
        self._seen = {}
        self._lock = RLock()

    @classmethod
    def from_environment(cls, edge_id, region_id, registry, city_id=None):
        keys = _environment_keys()
        return cls(edge_id, region_id, registry, city_id=city_id, keys=keys, required=_required(keys))

    def _owned(self, bus_id):
        if not _segment(bus_id):
            return False
        assignment = self.registry.get(bus_id)
        return assignment is not None and assignment.region_id == self.region_id and assignment.edge_id == self.edge_id and (
            self.city_id is None or assignment.city_id == self.city_id)

    def authorize_publish(self, topic):
        if topic in (f"transit/heartbeats/edge/{self.region_id}/{self.edge_id}",
                     f"transit/alerts/{self.region_id}/{self.edge_id}"):
            return
        parts = topic.split("/")
        if len(parts) == 5 and parts[:3] == ["transit", self.region_id, "bus"] and parts[4] == "state" and self._owned(parts[3]):
            return
        raise PermissionError("Edge cannot publish outside its assigned topics")

    def authorize_subscribe(self, topic):
        if topic not in (f"transit/buses/{self.region_id}/+/telemetry", f"transit/heartbeats/bus/{self.region_id}/+"):
            raise PermissionError("Edge cannot subscribe outside its region")

    def verify(self, topic, message):
        if not isinstance(message, dict) or "auth" not in message:
            if self.required:
                raise PermissionError("Signed publisher message required")
            return message
        if set(message) != {"auth", "payload"} or not isinstance(message["auth"], dict):
            raise PermissionError("Invalid authentication envelope")
        auth = message["auth"]
        if set(auth) != {"principal", "timestamp", "nonce", "signature"}:
            raise PermissionError("Invalid authentication metadata")
        principal, timestamp, nonce, signature = (auth[name] for name in ("principal", "timestamp", "nonce", "signature"))
        if not isinstance(principal, str) or principal not in self._keys or not principal.startswith("bus:"):
            raise PermissionError("Unknown publisher")
        bus_id = principal[4:]
        if not self._owned(bus_id) or topic not in (f"transit/buses/{self.region_id}/{bus_id}/telemetry",
                                                   f"transit/heartbeats/bus/{self.region_id}/{bus_id}"):
            raise PermissionError("Publisher is not authorized for this edge/topic")
        now = self.clock()
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)) or not math.isfinite(timestamp) or now - timestamp >= 30 or timestamp - now > 5:
            raise PermissionError("Expired or future authentication timestamp")
        if not isinstance(nonce, str) or re.fullmatch(r"[A-Za-z0-9_-]{8,128}", nonce) is None or not isinstance(signature, str) or re.fullmatch(r"[0-9a-f]{64}", signature) is None:
            raise PermissionError("Invalid nonce or signature")
        expected = hmac.new(self._keys[principal], _signed_bytes(topic, principal, timestamp, nonce, message["payload"]), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise PermissionError("Invalid publisher signature")
        with self._lock:
            self._seen = {key: expiry for key, expiry in self._seen.items() if expiry > now}
            key = principal, nonce
            if key in self._seen:
                raise PermissionError("Replayed publisher message")
            if len(self._seen) >= 4096:
                raise PermissionError("Replay protection is full; retry with a new message later")
            self._seen[key] = timestamp + 30
        return message["payload"]


def configure_mqtt_client(client, username=None, password=None):
    """Set broker credentials and verified TLS before connect; never log secrets.

    Credentials come from MQTT_USERNAME/MQTT_PASSWORD unless given explicitly,
    e.g. per-bus accounts selected by the simulator launcher (MQTT-07).
    """
    if username is None and password is None:
        username, password = os.getenv("MQTT_USERNAME"), os.getenv("MQTT_PASSWORD")
    if (username is None) != (password is None) or username == "" or password == "":
        raise ValueError("MQTT_USERNAME and MQTT_PASSWORD must both be nonempty")
    tls = os.getenv("MQTT_TLS", "false").lower()
    if tls not in ("true", "false"):
        raise ValueError("MQTT_TLS must be true or false")
    if tls == "true":
        import ssl
        context = ssl.create_default_context(cafile=os.getenv("MQTT_TLS_CA_FILE") or None)
        client.tls_set_context(context)
    if username is not None:
        if tls != "true" and os.getenv("EDGE_ALLOW_INSECURE_MQTT") != "true":
            raise ValueError("Broker passwords require TLS; local tests may explicitly allow insecure MQTT")
        client.username_pw_set(username, password)


def encode_bus_message(topic, payload, bus_id):
    """The existing simulator can publish the same signed wire format."""
    keys = _environment_keys()
    principal = "bus:" + bus_id
    if principal not in keys:
        if _required(keys) or keys:
            raise ValueError("Publisher has no signing key")
        return json.dumps(payload, allow_nan=False)
    return json.dumps(sign_message(topic, payload, principal, keys[principal]), allow_nan=False)



def mosquitto_acl(network):
    """Generate least-scope broker ACL text for the fixed MQTT-worker topology.

    An operator installs this alongside a password file and allow_anonymous false.
    It does not silently deploy broker-wide policy or create credentials.
    """
    lines = ["user central", "topic read transit/+/bus/+/state", "topic read transit/heartbeats/edge/+/+",
             "topic read transit/alerts/#", "topic write transit/commands/#"]
    edge_ids = set()
    for region in network["regions"]:
        if not _segment(region["region_id"]):
            raise ValueError("Unsafe region ID")
        for field in ("primary_edge", "backup_edge"):
            edge_id = region[field]
            if not _segment(edge_id) or ":" in edge_id or edge_id == "central" or edge_id in edge_ids:
                raise ValueError("Unsafe or duplicate edge ID")
            edge_ids.add(edge_id)
            lines.extend(["", "user " + edge_id,
                f"topic read transit/buses/{region['region_id']}/+/telemetry",
                f"topic read transit/heartbeats/bus/{region['region_id']}/+",
                f"topic write transit/heartbeats/edge/{region['region_id']}/{edge_id}",
                f"topic write transit/alerts/{region['region_id']}/{edge_id}"])
            for bus in network["buses"]:
                if bus["edge_id"] == edge_id:
                    if not _segment(bus["bus_id"]) or bus["region_id"] != region["region_id"]:
                        raise ValueError("Unsafe or mismatched bus assignment")
                    lines.append(f"topic write transit/{region['region_id']}/bus/{bus['bus_id']}/state")
    usernames = {"central"} | edge_ids
    for bus in network["buses"]:
        if not _segment(bus["bus_id"]) or ":" in bus["bus_id"] or bus["bus_id"] in usernames or bus["edge_id"] not in edge_ids:
            raise ValueError("Unsafe bus assignment")
        usernames.add(bus["bus_id"])
        lines.extend(["", "user " + bus["bus_id"],
            f"topic write transit/buses/{bus['region_id']}/{bus['bus_id']}/telemetry",
            f"topic write transit/heartbeats/bus/{bus['region_id']}/{bus['bus_id']}",
            f"topic write transit/alerts/{bus['region_id']}/{bus['bus_id']}",
            f"topic read transit/commands/{bus['region_id']}/{bus['bus_id']}",
            f"topic read transit/commands/{bus['region_id']}/all"])
    return "\n".join(lines) + "\n"

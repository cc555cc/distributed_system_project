"""Load route stops from the shared network instead of a separate hardcoded list."""

import argparse
import json

from shared.schema.network import DEFAULT_NETWORK_PATH, load_network, route_stops


def main(route_id: str, direction: int | None = None, *,
         network_path=DEFAULT_NETWORK_PATH, network: dict | None = None) -> list[dict]:
    """Both cities and the intercity route use this same ordered-stop lookup."""
    if network is None:
        network = load_network(network_path)
    return route_stops(network, route_id, 0 if direction is None else direction)


def seed_sql(network: dict) -> str:
    """Build central's seed from the same assignments; never touch a database here."""
    def literal(value):
        if isinstance(value, (int, float)):
            return str(value)
        return "'" + str(value).replace("'", "''") + "'"
    statements = ["-- Derived from shared/routes.json; preserve the configured directional stop orders.",
                  "-- Use this with a dedicated fresh development database."]
    def insert(table, fields, rows):
        values = ["  (" + ", ".join(literal(row[field]) for field in fields) + ")" for row in rows]
        statements.append("INSERT INTO " + table + " (" + ", ".join(fields) + ") VALUES\n" + ",\n".join(values) + ";")
    insert("regions", ("region_id", "name", "primary_edge", "backup_edge"), network["regions"])
    insert("routes", ("route_id", "name", "region_id"), network["routes"])
    insert("stops", ("stop_id", "name", "lat", "lon"), network["stops"])
    ordered=[]
    for route in network["routes"]:
        for direction in (0, 1):
            for stop in route_stops(network, route["route_id"], direction):
                ordered.append(dict(route_id=route["route_id"], direction_id=direction,
                                    position=stop["position"], stop_id=stop["stop_id"]))
    insert("route_stops", ("route_id", "direction_id", "position", "stop_id"), ordered)
    insert("buses", ("bus_id", "route_id"), network["buses"])
    return "\n\n".join(statements) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Show configured stops for one route.")
    parser.add_argument("route_id")
    parser.add_argument("--direction", type=int, choices=(0, 1), default=0)
    parser.add_argument("--network", default=str(DEFAULT_NETWORK_PATH))
    args = parser.parse_args()
    print(json.dumps(main(args.route_id, args.direction, network_path=args.network), indent=2))

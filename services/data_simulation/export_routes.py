import argparse
import json
from urllib.parse import quote
from urllib.request import urlopen


def main(route_id: str, direction: int | None = None) -> list[dict]:
    """Download all stop data for this bus's assigned route."""
    route_id = quote(str(route_id), safe="")
    url = f"http://localhost:8000/routes/{route_id}/stops"
    if direction is not None:
        url += f"?direction={int(direction)}"
    with urlopen(url, timeout=10) as response:
        return json.load(response)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download the stops for one bus route.")
    parser.add_argument("route_id", help="The bus's assigned route, such as 302.")
    args = parser.parse_args()
    stops = main(args.route_id)
    print(json.dumps(stops, indent=2))

"""Lightweight crowding calculations; faulty readings are not live estimates."""


def calculate_occupancy(telemetry: dict, medium: float, high: float) -> dict:
    """Use reported counts only; conservatively suppress faulty sensor results."""
    if telemetry.get("sensor_status", "healthy") != "healthy":
        return {"occupancy_percent": None, "crowding_status": "unavailable"}
    percent = telemetry["passengers"] / telemetry["capacity"] * 100
    status = "high" if percent >= high else "medium" if percent >= medium else "low"
    return {"occupancy_percent": percent, "crowding_status": status}

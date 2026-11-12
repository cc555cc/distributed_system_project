"""Run with: python3 services/data_simulation/run_buses.py"""

from multiprocessing import Process

if __package__:
    from . import generate_data
else:
    import generate_data

# ID, route, direction
BUSES = [
    ("bus-1", "302", 0),
    ("bus-2", "407", 0),
    ("bus-3", "409", 1),
]


SCENARIOS = {
    "bus-1": {"demand": "peak", "traffic_factor": 1.0, "sensor_failure_rate": 0.0},
    "bus-2": {"demand": "off_peak", "traffic_factor": 0.5, "sensor_failure_rate": 0.0},
    "bus-3": {"demand": "peak", "traffic_factor": 1.0, "sensor_failure_rate": 0.2},
}


def main():
    processes = []
    try:
        for bus_id, route_id, direction in BUSES:
            process = Process(
                target=generate_data.main,
                args=(route_id, direction, bus_id),
                kwargs=SCENARIOS.get(bus_id, {}),
                name=bus_id,
            )
            process.start()
            processes.append(process)

        for process in processes:
            process.join()
    except KeyboardInterrupt:
        pass
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
        for process in processes:
            process.join()

    if any(process.exitcode != 0 for process in processes):
        raise SystemExit(1)


if __name__ == "__main__":
    main()

DROP TABLE IF EXISTS bus_states;
DROP TABLE IF EXISTS buses;
DROP TABLE IF EXISTS route_stops;
DROP TABLE IF EXISTS stops;
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS regions;

CREATE TABLE regions (
    region_id TEXT PRIMARY KEY,
    name TEXT NOT NULL, 
    primary_edge TEXT NOT NULL,
    backup_edge TEXT NOT NULL,
    active_edge TEXT,
    last_updated TIMESTAMPTZ
);

CREATE TABLE routes (
    route_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    region_id TEXT REFERENCES regions(region_id)
);

CREATE TABLE stops (
    stop_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL
);

CREATE TABLE route_stops (
    route_id TEXT REFERENCES routes(route_id),
    direction_id INTEGER NOT NULL,
    stop_id TEXT REFERENCES stops(stop_id),
    position INTEGER NOT NULL,
    PRIMARY KEY (route_id, direction_id, position)
);

CREATE TABLE buses (
    bus_id TEXT PRIMARY KEY,
    route_id TEXT REFERENCES routes(route_id)
);

CREATE TABLE bus_states (
    bus_id TEXT PRIMARY KEY REFERENCES buses(bus_id),
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    speed DOUBLE PRECISION NOT NULL,
    direction_id INTEGER NOT NULL,
    current_stop_id TEXT REFERENCES stops(stop_id),
    next_stop_id TEXT REFERENCES stops(stop_id),
    bus_status TEXT NOT NULL,
    occupancy INTEGER NOT NULL,
    sequence_number INTEGER NOT NULL,
    lamport INTEGER NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL
);

CREATE TABLE ingestion_rejections (
    id           BIGSERIAL PRIMARY KEY,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    source       TEXT NOT NULL,          
    region_id    TEXT,                   
    bus_id       TEXT,                   
    reason       TEXT NOT NULL,         
    payload      JSONB                   
);

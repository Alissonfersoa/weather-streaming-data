CREATE TABLE IF NOT EXISTS hourly_weather (
    city TEXT NOT NULL,
    display_name TEXT,
    hour_start TIMESTAMP NOT NULL,

    avg_temperature_c DOUBLE PRECISION,
    min_temperature_c DOUBLE PRECISION,
    max_temperature_c DOUBLE PRECISION,

    avg_humidity_pct DOUBLE PRECISION,
    total_precipitation_mm DOUBLE PRECISION,

    avg_wind_speed_kmh DOUBLE PRECISION,
    max_wind_speed_kmh DOUBLE PRECISION,

    records_count BIGINT NOT NULL,

    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (city, hour_start)
);
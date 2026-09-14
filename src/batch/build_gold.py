import os
import time
from datetime import datetime, timezone

import duckdb
import psycopg
from psycopg.errors import DeadlockDetected


SILVER_GLOB = "/data/silver/weather/**/*.parquet"


def get_aggregates():
    query = f"""
        SELECT
            city,
            any_value(display_name) AS display_name,
            date_trunc('hour', observed_at) AS hour_start,
            avg(temperature_c) AS avg_temperature_c,
            min(temperature_c) AS min_temperature_c,
            max(temperature_c) AS max_temperature_c,
            avg(relative_humidity_pct) AS avg_humidity_pct,
            sum(coalesce(precipitation_mm, 0)) AS total_precipitation_mm,
            avg(wind_speed_kmh) AS avg_wind_speed_kmh,
            max(wind_speed_kmh) AS max_wind_speed_kmh,
            count(*) AS records_count
        FROM read_parquet(
            '{SILVER_GLOB}',
            hive_partitioning = true
        )
        GROUP BY
            city,
            date_trunc('hour', observed_at)
    """

    return duckdb.sql(query).fetchall()


def upsert(rows) -> None:
    dsn = os.environ["ANALYTICS_DB_DSN"]

    sql = """
        INSERT INTO hourly_weather (
            city,
            display_name,
            hour_start,
            avg_temperature_c,
            min_temperature_c,
            max_temperature_c,
            avg_humidity_pct,
            total_precipitation_mm,
            avg_wind_speed_kmh,
            max_wind_speed_kmh,
            records_count,
            updated_at
        )
        VALUES (
            %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s
        )
        ON CONFLICT (city, hour_start)
        DO UPDATE SET
            display_name = EXCLUDED.display_name,
            avg_temperature_c = EXCLUDED.avg_temperature_c,
            min_temperature_c = EXCLUDED.min_temperature_c,
            max_temperature_c = EXCLUDED.max_temperature_c,
            avg_humidity_pct = EXCLUDED.avg_humidity_pct,
            total_precipitation_mm = EXCLUDED.total_precipitation_mm,
            avg_wind_speed_kmh = EXCLUDED.avg_wind_speed_kmh,
            max_wind_speed_kmh = EXCLUDED.max_wind_speed_kmh,
            records_count = EXCLUDED.records_count,
            updated_at = EXCLUDED.updated_at;
    """

    now = datetime.now(timezone.utc)

    payload = [
        (*row, now)
        for row in rows
    ]

    payload.sort(key=lambda row: (row[0], row[2]))

    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        try:
            with psycopg.connect(dsn) as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(sql, payload)

                conn.commit()

            print(f"gold_rows_upserted={len(payload)}")
            return

        except DeadlockDetected:
            print(
                f"deadlock_detected=true "
                f"attempt={attempt} "
                f"max_attempts={max_attempts}"
            )

            if attempt == max_attempts:
                raise

            time.sleep(attempt * 2)


def main() -> None:
    rows = get_aggregates()

    print(f"gold_rows_calculated={len(rows)}")

    upsert(rows)

    print("gold_upsert_completed=true")


if __name__ == "__main__":
    main()
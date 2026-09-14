import argparse
import hashlib
import os

from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.api.open_meteo_archive_client import OpenMeteoArchiveClient
from src.config import load_cities


SILVER_ROOT = Path(
    os.getenv(
        "SILVER_ROOT",
        "/data/silver/weather",
    )
)


SILVER_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("city", pa.string()),
        ("display_name", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("observed_at", pa.timestamp("us")),
        ("collected_at", pa.timestamp("us")),
        ("temperature_c", pa.float64()),
        ("relative_humidity_pct", pa.float64()),
        ("apparent_temperature_c", pa.float64()),
        ("precipitation_mm", pa.float64()),
        ("rain_mm", pa.float64()),
        ("weather_code", pa.int32()),
        ("cloud_cover_pct", pa.float64()),
        ("surface_pressure_hpa", pa.float64()),
        ("wind_speed_kmh", pa.float64()),
        ("wind_direction_deg", pa.float64()),
    ]
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Historical weather backfill"
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date YYYY-MM-DD",
    )

    parser.add_argument(
        "--end-date",
        required=True,
        help="End date YYYY-MM-DD",
    )

    return parser.parse_args()


def safe_get(values, index):
    if values is None:
        return None

    if index >= len(values):
        return None

    return values[index]


def build_rows(
    city: dict,
    response: dict,
) -> list[dict]:

    hourly = response["hourly"]

    collected_at = datetime.now(
        timezone.utc
    ).replace(tzinfo=None)

    rows = []

    for index, timestamp in enumerate(hourly["time"]):

        observed_at = datetime.fromisoformat(timestamp)

        event_key = f"{city['name']}|{timestamp}"

        event_id = hashlib.sha256(
            event_key.encode("utf-8")
        ).hexdigest()

        rows.append(
            {
                "event_id": event_id,

                "city": city["name"],

                "display_name": city["display_name"],

                "latitude": float(city["latitude"]),

                "longitude": float(city["longitude"]),

                "observed_at": observed_at,

                "collected_at": collected_at,

                "temperature_c": safe_get(
                    hourly.get("temperature_2m"),
                    index,
                ),

                "relative_humidity_pct": safe_get(
                    hourly.get("relative_humidity_2m"),
                    index,
                ),

                "apparent_temperature_c": safe_get(
                    hourly.get("apparent_temperature"),
                    index,
                ),

                "precipitation_mm": safe_get(
                    hourly.get("precipitation"),
                    index,
                ),

                "rain_mm": safe_get(
                    hourly.get("rain"),
                    index,
                ),

                "weather_code": safe_get(
                    hourly.get("weather_code"),
                    index,
                ),

                "cloud_cover_pct": safe_get(
                    hourly.get("cloud_cover"),
                    index,
                ),

                "surface_pressure_hpa": safe_get(
                    hourly.get("surface_pressure"),
                    index,
                ),

                "wind_speed_kmh": safe_get(
                    hourly.get("wind_speed_10m"),
                    index,
                ),

                "wind_direction_deg": safe_get(
                    hourly.get("wind_direction_10m"),
                    index,
                ),
            }
        )

    return rows


def write_silver(
    city_name: str,
    rows: list[dict],
) -> int:

    rows_by_date = defaultdict(list)

    for row in rows:
        observation_date = row["observed_at"].date()

        rows_by_date[observation_date].append(row)

    written_rows = 0

    for observation_date, daily_rows in rows_by_date.items():

        partition_path = (
            SILVER_ROOT
            / f"observation_date={observation_date}"
        )

        partition_path.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_file = (
            partition_path
            / f"backfill-{city_name}.parquet"
        )

        table = pa.Table.from_pylist(
            daily_rows,
            schema=SILVER_SCHEMA,
        )

        pq.write_table(
            table,
            output_file,
            compression="snappy",
        )

        written_rows += len(daily_rows)

        print(
            f"silver_file_written="
            f"{output_file} "
            f"rows={len(daily_rows)}"
        )

    return written_rows


def main():
    args = parse_args()

    client = OpenMeteoArchiveClient()

    cities = load_cities()

    total_rows = 0

    print(
        f"historical_backfill_started=true "
        f"start_date={args.start_date} "
        f"end_date={args.end_date}"
    )

    for city in cities:

        print(
            f"backfill_city_started="
            f"{city['name']}"
        )

        response = client.get_history(
            latitude=city["latitude"],
            longitude=city["longitude"],
            start_date=args.start_date,
            end_date=args.end_date,
        )

        rows = build_rows(
            city=city,
            response=response,
        )

        written = write_silver(
            city_name=city["name"],
            rows=rows,
        )

        total_rows += written

        print(
            f"backfill_city_finished="
            f"{city['name']} "
            f"rows={written}"
        )
    print(
        f"historical_backfill_completed=true "
        f"total_rows={total_rows}"
    )

if __name__ == "__main__":
    main()
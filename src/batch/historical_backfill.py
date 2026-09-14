import argparse

from src.api.open_meteo_archive_client import OpenMeteoArchiveClient
from src.config import load_cities


def parse_args():
    parser = argparse.ArgumentParser(
        description="Historical weather backfill"
    )

    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format",
    )

    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format",
    )

    return parser.parse_args()


def main():
    args = parse_args()

    client = OpenMeteoArchiveClient()
    cities = load_cities()

    total_rows = 0

    for city in cities:
        print(
            f"backfill_started "
            f"city={city['name']} "
            f"start_date={args.start_date} "
            f"end_date={args.end_date}"
        )

        response = client.get_history(
            latitude=city["latitude"],
            longitude=city["longitude"],
            start_date=args.start_date,
            end_date=args.end_date,
        )

        hourly = response["hourly"]

        rows = len(hourly["time"])
        total_rows += rows

        print(
            f"backfill_finished "
            f"city={city['name']} "
            f"rows={rows}"
        )

    print(f"backfill_total_rows={total_rows}")


if __name__ == "__main__":
    main()
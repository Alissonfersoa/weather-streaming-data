from airflow.sdk import dag, task, Param
from datetime import datetime
import subprocess


@dag(
    dag_id="weather_historical_backfill",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    params={
        "start_date": Param(
            default="2026-09-01",
            type="string",
            description="Data inicial no formato YYYY-MM-DD",
        ),
        "end_date": Param(
            default="2026-09-03",
            type="string",
            description="Data final no formato YYYY-MM-DD",
        ),
    },
    tags=["weather", "backfill"],
)
def weather_historical_backfill():

    @task
    def run_backfill(**context):
        start_date = context["params"]["start_date"]
        end_date = context["params"]["end_date"]

        subprocess.run(
            [
                "python",
                "-m",
                "src.batch.historical_backfill",
                "--start-date",
                start_date,
                "--end-date",
                end_date,
            ],
            cwd="/opt/airflow/project",
            check=True,
        )

    run_backfill()

weather_historical_backfill()
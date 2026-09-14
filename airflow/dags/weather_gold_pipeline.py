import subprocess
import pendulum
from airflow.sdk import dag, task

@dag(
    dag_id="weather_gold_pipeline",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "gold"],
)
def weather_gold_pipeline():

    @task
    def build_gold():
        subprocess.run(
            [
                "python",
                "/opt/airflow/project/src/batch/build_gold.py",
            ],
            check=True,
        )
    build_gold()
weather_gold_pipeline()

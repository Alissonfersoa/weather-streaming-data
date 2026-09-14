import subprocess
import pendulum
from airflow.sdk import dag, task

@dag(
  dag_id="weather_quality_pipeline",
  schedule="0 * * * *",
  start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
  catchup=False,
  tags=["weather", "quality"],
)
def weather_quality_pipeline():

  @task
  def validate_silver():
    subprocess.run(
      [
        "python",
        "/opt/airflow/project/src/quality/check_silver.py",
      ],
      check=True,
    )
  validate_silver()
weather_quality_pipeline()
import os
import time

from src.api.open_meteo_client import OpenMeteoClient
from src.config import load_cities
from src.producer.event_builder import build_weather_event
from src.producer.kafka_producer import WeatherKafkaProducer


def run() -> None:
    interval = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

    client = OpenMeteoClient()
    producer = WeatherKafkaProducer()
    cities = load_cities()

    print(
        f"weather producer started "
        f"cities={len(cities)} "
        f"interval_seconds={interval}"
    )

    try:
        while True:
            for city in cities:
                try:
                    response = client.get_current_weather(
                        latitude=city["latitude"],
                        longitude=city["longitude"],
                    )

                    event = build_weather_event(
                        city=city,
                        api_response=response,
                    )

                    producer.send(event)

                    print(
                        f"collected city={city['name']} "
                        f"observed_at={event['observed_at']}"
                    )

                except Exception as exc:
                    print(
                        f"collection_error "
                        f"city={city['name']} "
                        f"error={exc}"
                    )

            producer.flush()
            time.sleep(interval)

    except KeyboardInterrupt:
        print("producer stopped")

    finally:
        producer.flush()


if __name__ == "__main__":
    run()

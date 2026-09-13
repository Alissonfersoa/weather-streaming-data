import json
import os
from typing import Any

from confluent_kafka import Producer


class WeatherKafkaProducer:
    def __init__(self) -> None:
        bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )

        self.topic = os.getenv(
            "KAFKA_TOPIC_WEATHER_RAW",
            "weather.raw",
        )

        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "weather-api-producer",
                "acks": "all",
            }
        )

    @staticmethod
    def _delivery_report(err, msg) -> None:
        if err is not None:
            print(f"Delivery failed: {err}")
            return

        print(
            "event delivered "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )

    def send(self, event: dict[str, Any]) -> None:
        key = event["city"]

        self.producer.produce(
            topic=self.topic,
            key=key.encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            callback=self._delivery_report,
        )

        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush()
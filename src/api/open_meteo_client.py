from __future__ import annotations

from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential


class OpenMeteoClient:
    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    CURRENT_VARIABLES = [
        "temperature_2m",
        "relative_humidity_2m",
        "apparent_temperature",
        "precipitation",
        "rain",
        "weather_code",
        "cloud_cover",
        "surface_pressure",
        "wind_speed_10m",
        "wind_direction_10m",
    ]

    def __init__(self, timeout_seconds: int = 15) -> None:
        self.timeout_seconds = timeout_seconds

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def get_current_weather(
        self,
        latitude: float,
        longitude: float,
    ) -> dict[str, Any]:
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current": ",".join(self.CURRENT_VARIABLES),
            "timezone": "UTC",
        }

        response = requests.get(
            self.BASE_URL,
            params=params,
            timeout=self.timeout_seconds,
        )

        response.raise_for_status()

        return response.json()
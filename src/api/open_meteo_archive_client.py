import requests

class OpenMeteoArchiveClient:
    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
    HOURLY_VARIABLES = [
        "temperature_2m",
        "relative_humidity_2m",
        "precipitation",
        "rain",
        "surface_pressure",
        "wind_speed_10m",
    ]

    def get_history(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
    ) -> dict:
        response = requests.get(
            self.BASE_URL,
            params={
                "latitude": latitude,
                "longitude": longitude,
                "start_date": start_date,
                "end_date": end_date,
                "hourly": ",".join(self.HOURLY_VARIABLES),
                "timezone": "UTC",
            },
            timeout=60,
        )

        response.raise_for_status()

        return response.json()

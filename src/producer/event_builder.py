from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any


def build_weather_event(
    city: dict[str, Any],
    api_response: dict[str, Any],
) -> dict[str, Any]:
  current = api_response["current"]
  observed_at = current["time"]

  event_key = f"{city['name']}|{observed_at}"
  event_id = hashlib.sha256(event_key.encode("utf-8")).hexdigest()

  return {
    "event_id": event_id,
    "city": city["name"],
    "display_name": city["display_name"] ,
    "latitude": city["latitude"],
    "longitude": city["longitude"],
    "observed_at": observed_at,
    "collected_at": datetime.now(timezone.utc).isoformat(),
    "temperature_c": current.get("temperature_2m"),
    "relative_humidity_pct": current.get("relative_humidity_2m"),
    "apparent_temperature_c": current.get("apparent_temperature"),
    "precipitation_mm": current.get("precipitation"),
    "rain_mm": current.get("rain"),
    "weather_code": current.get("weather_code"),
    "cloud_cover_pct": current.get("cloud_cover"),
    "surface_pressure_hpa": current.get("surface_pressure"),
    "wind_speed_kmh": current.get("wind_speed_10m"),
    "wind_direction_deg": current.get("wind_direction_10m"),
  }
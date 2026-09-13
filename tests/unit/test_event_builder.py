from src.producer.event_builder import build_weather_event


def test_event_id_is_deterministic():
  city = {
    "name": "sao_jose_dos_campos",
    "display_name": "São José dos Campos",
    "latitude": -23.1896,
    "longitude": -45.8841,
  }

  response = {
    "current":{
      "time": "2026-09-13T18:00",
      "temperature_2m": 22.0,
    }
  }

  first = build_weather_event(city, response)
  second = build_weather_event(city, response)

  assert first["event_id"] == second["event_id"]
  assert first["city"] == "sao_jose_dos_campos"

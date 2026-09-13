from pprint import pprint

from src.api.open_meteo_client import OpenMateoClient

client = OpenMateoClient()

data = client.get_current_weather(
  latitude=-23.1896,
  longitude=-45.8841,
)

pprint(data)
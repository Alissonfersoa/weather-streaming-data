from pathlib import Path
from typing import Any
import yaml

def load_cities(config_path: str = "config/cities.yaml") -> list[dict[str, Any]]:
  path = Path(config_path)

  if not path.exists():
    raise FileNotFoundError(f"Config file not found:{config_path}")

  with path.open("r", encoding="utf-8") as file:
    config = yaml.safe_load(file)

  cities = config.get("cities", [])

  if not cities:
    raise ValueError("No cities configured")
  return cities
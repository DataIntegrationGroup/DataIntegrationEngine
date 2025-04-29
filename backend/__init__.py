from enum import Enum
from os import environ


class OutputFormat(str, Enum):
    GEOJSON = "geojson"
    CSV = "csv"
    GEOSERVER = "geoserver"


def get_bool_env_variable(var: str) -> bool:
    env_var = environ.get(var, None)
    if env_var is None or env_var.strip().lower() not in ["true", "1", "yes"]:
        return False
    else:
        return True

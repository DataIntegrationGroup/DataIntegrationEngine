from enum import Enum
from os import environ


class OutputFormat(str, Enum):
    GEOJSON = "geojson"
    CSV = "csv"
    GEOSERVER = "geoserver"


def get_bool_env_variable(var) -> bool:
    if environ.get(var).lower() in ["true", "1", "yes"]:
        return True
    else:
        return False

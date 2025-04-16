from enum import Enum


class OutputFormat(str, Enum):
    GEOJSON = "geojson"
    CSV = "csv"
    GEOSERVER = "geoserver"
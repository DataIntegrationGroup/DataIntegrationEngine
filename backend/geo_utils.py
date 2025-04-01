# ===============================================================================
# Copyright 2023 ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
import pyproj
from shapely.ops import transform

PROJECTIONS = {}
TRANSFORMS = {}

ALLOWED_DATUMS = ["NAD27", "NAD83", "WGS84"]

# srids for NM
SRID_WGS84 = 4326
SRID_UTM_ZONE_13N = 26913


def transform_srid(geometry, source_srid, target_srid):
    """
    geometry must be a shapely geometry object, like Point, Polygon, or MultiPolygon
    """
    source_crs = pyproj.CRS(f"EPSG:{source_srid}")
    target_crs = pyproj.CRS(f"EPSG:{target_srid}")
    transformer = pyproj.Transformer.from_crs(source_crs, target_crs, always_xy=True)
    return transform(transformer.transform, geometry)


def datum_transform(x, y, in_datum, out_datum):
    """
    Transform x, y to a different datum

    Parameters
    --------
    x: float
        easting
    y: float
        northing
    datum: str
        datum name

    Returns
    --------
    tuple
        (easting, northing)
    """
    if in_datum == "NAD27":
        in_datum = "EPSG:4267"
    elif in_datum == "NAD83":
        in_datum = "EPSG:4269"

    if out_datum == "WGS84":
        out_datum = "EPSG:4326"

    name = f"datum{out_datum}{in_datum}"
    if name not in TRANSFORMS:
        pr = pyproj.Transformer.from_proj(
            pyproj.Proj(in_datum),
            pyproj.Proj(out_datum),
        )
        TRANSFORMS[name] = pr

    pr = TRANSFORMS[name]
    lng, lat = pr.transform(x, y)
    return lng, lat


def utm_to_lonlat(e, n, zone=13):
    """
    Converts easting and northing into longitude and latitude

    Parameters
    --------
    e: float
        easting
    n: float
        northing

    Returns
    --------
    tuple
        (longitude, latitude)
    """
    name = f"utm{zone}"
    if name not in PROJECTIONS:
        pr = pyproj.Proj(proj="utm", zone=int(zone), ellps="WGS84")
        PROJECTIONS[name] = pr
    pr = PROJECTIONS[name]
    lonlat = pr(e, n, inverse=True)
    return lonlat


def lonlat_to_utm(lon, lat, zone=13):
    """
    Converts longitude and latitude into easting and northing

    Parameters
    --------
    lon: float
        longitude in decimal degrees
    lat: float
        latitude in decimal degrees


    Returns
    --------
    tuple
        (easting, northing)
    """
    name = "lonlat"
    if name not in PROJECTIONS:
        pr = pyproj.Proj(proj="utm", ellps="WGS84", zone=zone)
        PROJECTIONS[name] = pr

    pr = PROJECTIONS[name]
    easting_northing = pr(lon, lat)
    return easting_northing


# ============= EOF =============================================

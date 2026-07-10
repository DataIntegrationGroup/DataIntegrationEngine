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

TRANSFORMS: dict = {}

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


_DATUM_EPSG = {"NAD27": "EPSG:4267", "NAD83": "EPSG:4269", "WGS84": "EPSG:4326"}


def datum_transform(x, y, in_datum, out_datum):
    """Reproject a lon/lat point from one geographic datum to another.

    ``x`` is longitude, ``y`` is latitude. Uses ``always_xy=True`` so the
    transformer takes and returns (lon, lat) — the geographic CRSs here
    (NAD27/NAD83/WGS84) have a native (lat, lon) axis order, so without it the
    coordinates are fed swapped and the datum shift is silently dropped
    (e.g. NAD27 -> WGS84 returned the input unchanged instead of the ~50-100 m
    shift). Transformers are cached per CRS pair.

    Returns (lon, lat).
    """
    in_crs = _DATUM_EPSG.get(in_datum, in_datum)
    out_crs = _DATUM_EPSG.get(out_datum, out_datum)

    name = f"{in_crs}->{out_crs}"
    if name not in TRANSFORMS:
        TRANSFORMS[name] = pyproj.Transformer.from_crs(
            in_crs, out_crs, always_xy=True
        )
    return TRANSFORMS[name].transform(x, y)


# ============= EOF =============================================

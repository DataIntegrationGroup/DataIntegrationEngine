# ===============================================================================
# Copyright 2024 Jake Ross
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
import json
import os

import click
import httpx
from shapely.geometry import shape

from backend.geo_utils import transform_srid, SRID_WGS84, SRID_UTM_ZONE_13N


def get_county_polygon(name, as_wkt=True):
    if ":" in name:
        state, county = name.split(":")
        statefp = _statelookup(state)
    else:
        state = "NM"
        county = name
        statefp = 35

    if statefp:

        obj = _get_cached_object(
            f"{state}.counties",
            f"{state} counties",
            f"https://reference.geoconnex.us"
            f"/collections/counties/items?statefp={statefp}&f=json",
        )

        county = county.lower()
        for f in obj["features"]:
            # get county name
            name = f["properties"].get("name")
            if name is None:
                name = f["properties"].get("NAME")

            if name is None:
                continue

            if name.lower() == county:
                return _make_shape(f, as_wkt)
        else:
            _warning(f"county '{county}' does not exist")
            _warning("---------- Valid county names -------------")
            for f in obj["features"]:
                _warning(f["properties"]["name"])
            _warning("--------------------------------------------")
    else:
        _warning(f"Invalid state. {state}")


def get_state_polygon(state: str, buffer: int | None = None):
    statefp = _statelookup(state)
    if statefp:
        obj = _get_cached_object(
            f"{state}.state",
            f"{state} state",
            f"https://reference.geoconnex.us/collections/states/items/{statefp}?&f=json",
        )
        geom_gcs = shape(obj["features"][0]["geometry"])

        if buffer:
            geom_utm = transform_srid(geom_gcs, SRID_WGS84, SRID_UTM_ZONE_13N)
            geom_utm = geom_utm.buffer(buffer)
            geom_gcs = transform_srid(geom_utm, SRID_UTM_ZONE_13N, SRID_WGS84)

        return geom_gcs


# private helpers ============================
def _make_shape(obj, as_wkt):
    poly = shape(obj["geometry"])
    poly = poly.simplify(0.1)
    if as_wkt:
        return poly.wkt
    return poly


def _warning(msg):
    click.secho(msg, fg="red")


def _cache_path(name):
    return os.path.join(os.path.expanduser("~"), f".die.{name}.json")


def _statelookup(shortname):
    obj = _get_cached_object(
        f"{shortname}.state",
        shortname,
        f"https://reference.geoconnex.us/collections/states/items?f=json&stusps={shortname}",
    )

    # return obj["features"][0]["properties"]["statefp"]
    shortname = shortname.lower()
    for f in obj["features"]:
        props = f["properties"]
        if props["stusps"].lower() == shortname:
            return props["statefp"]


def _get_statefp(state):
    if state is None:
        state = "NM"
        statefp = 35
    else:
        statefp = _statelookup(state)
    return state, statefp


def _get_cached_object(name, msg, url):
    path = _cache_path(name)

    if not os.path.isfile(path):
        click.secho(f"Caching {msg} to {path}")
        if callable(url):
            obj = url()
        else:
            resp = httpx.get(url, timeout=30)
            obj = resp.json()
        with open(path, "w") as wfile:
            json.dump(obj, wfile)
    else:
        click.secho(f"Using cached version of {msg}. Path={path}")

    with open(path, "r") as rfile:
        obj = json.load(rfile)
    return obj


NM_BOUNDARY_BUFFERED = get_state_polygon("NM", 25000)


if __name__ == "__main__":
    print(get_state_polygon("NM"))
# ============= EOF =============================================

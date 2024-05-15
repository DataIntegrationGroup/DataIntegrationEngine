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
from shapely import Polygon, box


def warning(msg):
    click.secho(msg, fg="red")


def cache_path(name):
    return os.path.join(os.path.expanduser("~"), f".sta.{name}.json")


def statelookup(shortname):
    p = cache_path("states")
    if not os.path.isfile(p):
        click.secho(f"Caching states to {p}")
        url = f"https://reference.geoconnex.us/collections/states/items?f=json"
        resp = httpx.get(url)
        with open(p, "w") as wfile:
            json.dump(resp.json(), wfile)

    with open(p, "r") as rfile:
        obj = json.load(rfile)

    shortname = shortname.lower()
    for f in obj["features"]:
        props = f["properties"]
        if props["STUSPS"].lower() == shortname:
            return props["STATEFP"]


def get_state_polygon(state):
    statefp = statelookup(state)
    if statefp:
        p = cache_path(state)
        if not os.path.isfile(p):
            click.secho(f"Caching {state} counties to {p}")
            url = f"https://reference.geoconnex.us/collections/states/items/{statefp}?&f=json"
            resp = httpx.get(url)

            obj = resp.json()
            with open(p, "w") as wfile:
                json.dump(obj, wfile)

        with open(p, "r") as rfile:
            obj = json.load(rfile)

        return Polygon(obj["geometry"]["coordinates"][0][0])


def get_state_bb(state):
    p = get_state_polygon(state)
    return box(*p.bounds).wkt


def get_county_polygon(name, as_wkt=True):
    if ":" in name:
        state, county = name.split(":")
        statefp = statelookup(state)
    else:
        state = "NM"
        county = name
        statefp = 35

    if statefp:
        p = cache_path(f"{state}.counties")
        if not os.path.isfile(p):
            click.secho(f"Caching {state} counties to {p}")
            url = f"https://reference.geoconnex.us/collections/counties/items?statefp={statefp}&f=json"
            resp = httpx.get(url)

            obj = resp.json()
            with open(p, "w") as wfile:
                json.dump(obj, wfile)

        with open(p, "r") as rfile:
            obj = json.load(rfile)

        county = county.lower()
        for f in obj["features"]:
            if f["properties"]["name"].lower() == county:
                poly = Polygon(f["geometry"]["coordinates"][0][0])
                if as_wkt:
                    return poly.wkt
                return poly
        else:
            warning(f"county '{county}' does not exist")
            warning("---------- Valid county names -------------")
            for f in obj["features"]:
                warning(f["properties"]["name"])
            warning("--------------------------------------------")
    else:
        warning(f"Invalid state. {state}")


# ============= EOF =============================================

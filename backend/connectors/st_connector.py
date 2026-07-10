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
from datetime import datetime
from typing import Optional

from shapely import MultiPolygon, unary_union

from backend.bounding_polygons import get_state_polygon
from backend.connectors._sensorthings import sta_query
from backend.source import (
    BaseSiteSource,
    BaseWaterLevelSource,
    BaseAnalyteSource,
    get_terminal_record,
)
from backend.transformer import SiteTransformer


class STClient:
    """Thin SensorThings client — builds the OData query and returns the JSON
    entities (dicts). Replaces frost_sta_client (see _sensorthings.py)."""

    def __init__(self, url: str):
        self._url = url

    def _get_things(self, site, expand="Locations,Datastreams", additional_filters=None):
        if self._url is None:
            raise ValueError("URL not set")
        fs = [f"Locations/id eq {site.id}"]
        if additional_filters:
            fs.extend(additional_filters)
        return sta_query(self._url, "Things", expand=expand, filter=" and ".join(fs))


def make_dt_filter(tag, start, end):
    if start:
        s = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        e = end
        if not e:
            e = datetime.now()
        e = e.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return f"overlaps({tag}, {s}/{e})"
    elif end:
        e = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return f"{tag} le {e}"
    return ""


class STSiteSource(BaseSiteSource):
    url: Optional[str] = None

    def __init__(self, transformer=None):
        super().__init__(transformer=transformer)
        self.client = STClient(self.url)

    def health(self):
        try:
            return bool(sta_query(self.url, "Locations", top=1))
        except Exception:
            return False

    def get_records(self, *args, **kw):
        config = self.config

        fs = []
        if config:
            if config.has_bounds():
                poly = config.bounding_wkt(as_wkt=False)
                if type(poly) == MultiPolygon:
                    if len(poly.geoms) == 1:
                        poly = unary_union(poly)
                    else:
                        state_boundary = get_state_polygon("NM")
                        for geom in poly:
                            if state_boundary.contains(geom):
                                poly = geom
                                break

                fs.append(f"st_within(location, geography'{poly}')")

            fi = make_dt_filter(
                "Things/Datastreams/phenomenonTime", config.start_dt, config.end_dt
            )
            if fi:
                fs.append(fi)

        fs = fs + self._get_filters()
        return sta_query(
            self.url,
            "Locations",
            expand="Things/Datastreams",
            filter=" and ".join(fs) if fs else None,
            top=kw.get("top"),
        )

    def _get_filters(self):
        return []


class STWaterLevelSource(BaseWaterLevelSource):
    url: Optional[str] = None

    def __init__(self, transformer=None):
        super().__init__(transformer=transformer)
        self.client = STClient(self.url)

    def _parse_result(self, result):
        return result

    def _extract_terminal_record(self, records, position):
        record = get_terminal_record(
            records, tag=lambda x: x["observation"]["phenomenonTime"], position=position
        )
        return {
            "value": self._parse_result(record["observation"]["result"]),
            "datetime": record["observation"]["phenomenonTime"],
            "source_parameter_units": record["datastream"]["unitOfMeasurement"]["symbol"],
            "source_parameter_name": record["datastream"]["name"],
        }


class STAnalyteSource(BaseAnalyteSource):
    url: Optional[str] = None

    def __init__(self, transformer=None):
        super().__init__(transformer=transformer)
        self.client = STClient(self.url)

    def _parse_result(self, result):
        return result

    def _extract_terminal_record(self, records, position):
        record = get_terminal_record(
            records, tag=lambda x: x["observation"]["phenomenonTime"], position=position
        )
        return {
            "value": self._parse_result(record["observation"]["result"]),
            "datetime": record["observation"]["phenomenonTime"],
            "source_parameter_units": record["datastream"]["unitOfMeasurement"]["symbol"],
            "source_parameter_name": record["datastream"]["name"],
        }


class STSiteTransformer(SiteTransformer):
    source_id: str
    check_contained = False

    def _transform_elevation(self, elevation, record):
        return elevation

    def _transform_hook(self, rec):
        return rec

    def _transform(self, record):
        if self.source_id is None:
            raise ValueError(f"{self.__class__.__name__} Source ID not set")

        coordinates = record["location"]["coordinates"]

        lat = coordinates[1]
        lng = coordinates[0]

        ele = None
        if len(coordinates) == 3:
            ele = coordinates[2]
            ele = self._transform_elevation(ele, record)

        rec = {
            "source": self.source_id,
            "id": record["@iot.id"],
            "name": record["name"],
            "latitude": lat,
            "longitude": lng,
            "elevation": ele,
            "elevation_units": "m",
            "horizontal_datum": "WGS84",
        }
        return self._transform_hook(rec)


# ============= EOF =============================================

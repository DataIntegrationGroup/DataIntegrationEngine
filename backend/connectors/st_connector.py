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

import frost_sta_client as fsc

from backend.source import (
    BaseSiteSource,
    BaseWaterLevelSource,
    BaseAnalyteSource,
    get_most_recent,
)
from backend.transformer import SiteTransformer


def get_service(url):
    s = fsc.SensorThingsService(url)
    return s


class STSource:
    url: str

    def get_service(self):
        if self.url is None:
            raise ValueError("URL not set")

        return get_service(self.url)

    def _get_things(
        self, service, site, expand="Locations,Datastreams", additional_filters=None
    ):

        things = service.things().query().expand(expand)
        fs = [f"Locations/id eq {site.id}"]
        if additional_filters is not None:
            for fi in additional_filters:
                fs.append(fi)
        if fs:
            things.filter(" and ".join(fs))

        return things.list()

    def _extract_most_recent(self, records):
        record = get_most_recent(
            records, tag=lambda x: x["observation"].phenomenon_time
        )

        return {
            "value": self._parse_result(record["observation"].result),
            "datetime": record["observation"].phenomenon_time,
            "units": record["datastream"].unit_of_measurement.symbol,
        }

    def _parse_result(self, result):
        return result


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


class STSiteSource(BaseSiteSource, STSource):
    def health(self):
        return self.get_records(top=10)

    def get_records(self, *args, **kw):
        service = self.get_service()

        config = self.config

        fs = []
        if config:
            if config.has_bounds():
                fs.append(
                    f"st_within(Location/location, geography'{config.bounding_wkt()}')"
                )

            fi = make_dt_filter(
                "Things/Datastreams/phenomenonTime", config.start_dt, config.end_dt
            )
            if fi:
                fs.append(fi)

        fs = fs + self._get_filters()
        q = (
            service.locations()
            .query()
            .expand("Things/Datastreams")
            .filter(" and ".join(fs))
        )
        if "top" in kw:
            q = q.top(kw["top"])

        return list(q.list())

    def _get_filters(self):
        return []


class STWaterLevelSource(STSource, BaseWaterLevelSource):
    pass


class STAnalyteSource(STSource, BaseAnalyteSource):
    pass


class STSiteTransformer(SiteTransformer):
    source_id: str
    check_contained = False  # API returns only records within the bounds

    def _transform_hook(self, rec):
        return rec

    def _transform(self, record):
        if self.source_id is None:
            raise ValueError(f"{self.__class__.__name__} Source ID not set")

        coordinates = record.location["coordinates"]

        lat = coordinates[1]
        lng = coordinates[0]
        # if not self.contained(lng, lat):
        #     print("not contained")
        #     return

        ele = None
        if len(coordinates) == 3:
            ele = coordinates[2]

        rec = {
            "source": self.source_id,
            "id": record.id,
            "name": record.name,
            "latitude": lat,
            "longitude": lng,
            "elevation": ele,
            "elevation_units": "m",
            "horizontal_datum": "WGS84",
        }
        return self._transform_hook(rec)


# ============= EOF =============================================

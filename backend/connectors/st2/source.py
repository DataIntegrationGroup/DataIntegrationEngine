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
import datetime

import frost_sta_client as fsc

from backend.connectors.st2.transformer import (
    PVACDSiteTransformer,
    EBIDSiteTransformer,
    PVACDWaterLevelTransformer,
    EBIDWaterLevelTransformer,
)
from backend.constants import DTW, DTW_UNITS, DTW_DT_MEASURED
from backend.source import BaseSiteSource, BaseWaterLevelSource, get_most_recent

URL = "https://st2.newmexicowaterdata.org/FROST-Server/v1.0"


class ST2Mixin:
    def get_service(self):
        return fsc.SensorThingsService(URL)


class ST2SiteSource(BaseSiteSource, ST2Mixin):
    agency = "ST2"

    def get_records(self, *args, **kw):
        service = self.get_service()
        config = self.config

        f = f"properties/agency eq '{self.agency}'"
        if config.has_bounds():
            f = f"{f} and st_within(Location/location, geography'{config.bounding_wkt()}')"

        q = service.locations().query().filter(f)
        return list(q.list())


class PVACDSiteSource(ST2SiteSource):
    transformer_klass = PVACDSiteTransformer
    agency = "PVACD"


class EBIDSiteSource(ST2SiteSource):
    transformer_klass = EBIDSiteTransformer
    agency = "EBID"


class ST2WaterLevelSource(BaseWaterLevelSource, ST2Mixin):
    def _extract_most_recent(self, records):
        record = get_most_recent(
            records, tag=lambda x: x["observation"].phenomenon_time
        )

        return {
            "value": record["observation"].result,
            "datetime": record["observation"].phenomenon_time,
            "units": record["datastream"].unit_of_measurement.symbol,
        }

    def _extract_parameter_record(self, record):
        record[DTW] = record["observation"].result
        record[DTW_UNITS] = record["datastream"].unit_of_measurement.symbol
        record[DTW_DT_MEASURED] = record["observation"].phenomenon_time
        return record

    def _extract_parameter_results(self, records):
        return [r["observation"].result for r in records]

    def get_records(self, parent_record, *args, **kw):
        service = self.get_service()
        config = self.config

        things = (
            service.things()
            .query()
            .expand("Locations,Datastreams")
            .filter(f"Locations/id eq {parent_record.id}")
        )
        records = []
        for t in things.list():
            if t.name == "Water Well":
                for di in t.datastreams:
                    q = di.get_observations().query()
                    if config.latest_water_level_only and not config.output_summary:
                        q = q.orderby("phenomenonTime", "desc").top(1)

                    for obs in q.list():
                        records.append(
                            {
                                "thing": t,
                                "location": parent_record,
                                "datastream": di,
                                "observation": obs,
                            }
                        )
                        if config.latest_water_level_only and not config.output_summary:
                            break
        return records


class PVACDWaterLevelSource(ST2WaterLevelSource):
    transformer_klass = PVACDWaterLevelTransformer
    agency = "PVACD"


class EBIDWaterLevelSource(ST2WaterLevelSource):
    transformer_klass = EBIDWaterLevelTransformer
    agency = "EBID"


# ============= EOF =============================================

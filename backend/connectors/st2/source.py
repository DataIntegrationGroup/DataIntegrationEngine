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

from backend.connectors import PVACD_BOUNDING_POLYGON, BERNCO_BOUNDING_POLYGON
from backend.connectors.st2.transformer import (
    PVACDSiteTransformer,
    EBIDSiteTransformer,
    PVACDWaterLevelTransformer,
    EBIDWaterLevelTransformer,
    BernCoSiteTransformer,
    BernCoWaterLevelTransformer,
)
from backend.connectors.st_connector import (
    STSiteSource,
    STWaterLevelSource,
    make_dt_filter,
)
from backend.constants import DTW, DTW_UNITS, DT_MEASURED
from backend.source import BaseSiteSource, BaseWaterLevelSource, get_most_recent

URL = "https://st2.newmexicowaterdata.org/FROST-Server/v1.0"


class ST2SiteSource(STSiteSource):
    agency: str
    url = URL

    def _get_filters(self):
        if self.agency is None:
            raise ValueError(f"{self.__class__.__name__}. Agency not set")

        return [f"properties/agency eq '{self.agency}'"]


class PVACDSiteSource(ST2SiteSource):
    transformer_klass = PVACDSiteTransformer
    agency = "PVACD"
    bounding_polygon = PVACD_BOUNDING_POLYGON


class EBIDSiteSource(ST2SiteSource):
    transformer_klass = EBIDSiteTransformer
    agency = "EBID"


class BernCoSiteSource(ST2SiteSource):
    agency = "BernCo"
    transformer_klass = BernCoSiteTransformer
    bounding_polygon = BERNCO_BOUNDING_POLYGON


class ST2WaterLevelSource(STWaterLevelSource):
    url = URL

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
        record[DT_MEASURED] = record["observation"].phenomenon_time
        return record

    def _extract_parameter_results(self, records):
        return [r["observation"].result for r in records]

    def get_records(self, parent_record, *args, **kw):
        service = self.get_service()
        config = self.config

        records = []
        for t in self._get_things(service, parent_record):
            if t.name == "Water Well":
                for di in t.datastreams:

                    q = di.get_observations().query()

                    fi = make_dt_filter(
                        "phenomenonTime", config.start_dt, config.end_dt
                    )
                    if fi:
                        q = q.filter(fi)

                    # if config.latest_water_level_only and not config.output_summary:
                    q = q.orderby("phenomenonTime", "desc")

                    for obs in q.list():
                        records.append(
                            {
                                "thing": t,
                                "location": parent_record,
                                "datastream": di,
                                "observation": obs,
                            }
                        )

                        # if config.latest_water_level_only and not config.output_summary:
                        #     break
        return records


class PVACDWaterLevelSource(ST2WaterLevelSource):
    transformer_klass = PVACDWaterLevelTransformer
    agency = "PVACD"


class EBIDWaterLevelSource(ST2WaterLevelSource):
    transformer_klass = EBIDWaterLevelTransformer
    agency = "EBID"


class BernCoWaterLevelSource(ST2WaterLevelSource):
    agency = "BernCo"
    transformer_klass = BernCoWaterLevelTransformer


# ============= EOF =============================================

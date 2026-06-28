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
from functools import partial

from backend.connectors import (
    PVACD_BOUNDING_POLYGON,
    BERNCO_BOUNDING_POLYGON,
    EBID_BOUNDING_POLYGON,
    CABQ_BOUNDING_POLYGON,
)
from backend.connectors.st2.transformer import (
    NMOSERoswellSiteTransformer,
    NMOSERoswellWaterLevelTransformer,
    PVACDSiteTransformer,
    PVACDWaterLevelTransformer,
    EBIDSiteTransformer,
    EBIDWaterLevelTransformer,
    BernCoSiteTransformer,
    BernCoWaterLevelTransformer,
    CABQSiteTransformer,
    CABQWaterLevelTransformer,
)
from backend.connectors.st_connector import (
    STSiteSource,
    STWaterLevelSource,
    make_dt_filter,
)
from backend.constants import (
    DTW,
    DT_MEASURED,
    PARAMETER_NAME,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
    SOURCE_PARAMETER_NAME,
    SOURCE_PARAMETER_UNITS,
    SOURCE_DATASTREAM_LINK,
)

URL = "https://st2.newmexicowaterdata.org/FROST-Server/v1.1"


class ST2SiteSource(STSiteSource):
    url = URL

    def __init__(self, agency: str, bounding_polygon=None, transformer=None):
        self.agency = agency
        if bounding_polygon is not None:
            self.bounding_polygon = bounding_polygon
        super().__init__(transformer=transformer)

    def __repr__(self):
        return f"ST2SiteSource(agency={self.agency!r})"

    def _get_filters(self):
        if self.agency is None:
            raise ValueError(f"{self.__class__.__name__}. Agency not set")
        return [f"properties/agency eq '{self.agency}'"]


NMOSERoswellSiteSource = partial(
    ST2SiteSource,
    agency="OSE-Roswell",
    transformer=NMOSERoswellSiteTransformer(),
)
PVACDSiteSource = partial(
    ST2SiteSource,
    agency="PVACD",
    bounding_polygon=PVACD_BOUNDING_POLYGON,
    transformer=PVACDSiteTransformer(),
)
EBIDSiteSource = partial(
    ST2SiteSource,
    agency="EBID",
    bounding_polygon=EBID_BOUNDING_POLYGON,
    transformer=EBIDSiteTransformer(),
)
BernCoSiteSource = partial(
    ST2SiteSource,
    agency="BernCo",
    bounding_polygon=BERNCO_BOUNDING_POLYGON,
    transformer=BernCoSiteTransformer(),
)
CABQSiteSource = partial(
    ST2SiteSource,
    agency="CABQ",
    bounding_polygon=CABQ_BOUNDING_POLYGON,
    transformer=CABQSiteTransformer(),
)


class ST2WaterLevelSource(STWaterLevelSource):
    url = URL

    def __init__(self, transformer=None):
        super().__init__(transformer=transformer)

    def _extract_parameter_record(self, record):
        record[PARAMETER_NAME] = DTW
        record[PARAMETER_VALUE] = record["observation"].result
        record[PARAMETER_UNITS] = self.config.waterlevel_output_units
        record[DT_MEASURED] = record["observation"].phenomenon_time
        record[SOURCE_PARAMETER_NAME] = record["datastream"].name
        record[SOURCE_PARAMETER_UNITS] = record["datastream"].unit_of_measurement.symbol
        # Link to the raw, non-normalized SensorThings datastream these
        # observations came from, so consumers can trace a feature back to the
        # provider's original series (before DIE normalization).
        record[SOURCE_DATASTREAM_LINK] = self._datastream_link(record["datastream"])
        return record

    def _datastream_link(self, datastream):
        ds_id = getattr(datastream, "id", None)
        if ds_id is None:
            return None
        return f"{self.url}/Datastreams({ds_id})"

    def _summary_extra(self, cleaned: list) -> dict:
        # All of a well's observations come from the same datastream; link the
        # summary feature back to it.
        for r in cleaned:
            link = self._datastream_link(r.get("datastream"))
            if link:
                return {SOURCE_DATASTREAM_LINK: link}
        return {}

    def _extract_source_parameter_results(self, records):
        return [r["observation"].result for r in records]

    def _extract_parameter_dates(self, records: list) -> list:
        return [r["observation"].phenomenon_time for r in records]

    def _extract_source_parameter_names(self, records: list) -> list:
        return [r["datastream"].name for r in records]

    def _clean_records(self, records: list) -> list:
        return [r for r in records if r["observation"].result is not None]

    def get_records(self, site_record, *args, **kw):
        service = self.client.get_service()
        config = self.config

        records = []
        for t in self.client._get_things(service, site_record):
            if t.name == "Water Well":
                for di in t.datastreams:

                    q = di.get_observations().query()

                    fi = make_dt_filter(
                        "phenomenonTime", config.start_dt, config.end_dt
                    )
                    if fi:
                        q = q.filter(fi)

                    q = q.orderby("phenomenonTime", "desc")

                    for obs in q.list():
                        records.append(
                            {
                                "thing": t,
                                "location": site_record,
                                "datastream": di,
                                "observation": obs,
                            }
                        )
        return records


class NMOSERoswellWaterLevelSource(ST2WaterLevelSource):
    agency = "OSE-Roswell"

    def __init__(self):
        super().__init__(transformer=NMOSERoswellWaterLevelTransformer())

    def __repr__(self):
        return "NMOSERoswellWaterLevelSource"


class PVACDWaterLevelSource(ST2WaterLevelSource):
    agency = "PVACD"

    def __init__(self):
        super().__init__(transformer=PVACDWaterLevelTransformer())

    def __repr__(self):
        return "PVACDWaterLevelSource"


class EBIDWaterLevelSource(ST2WaterLevelSource):
    agency = "EBID"

    def __init__(self):
        super().__init__(transformer=EBIDWaterLevelTransformer())

    def __repr__(self):
        return "EBIDWaterLevelSource"


class BernCoWaterLevelSource(ST2WaterLevelSource):
    agency = "BernCo"

    def __init__(self):
        super().__init__(transformer=BernCoWaterLevelTransformer())

    def __repr__(self):
        return "BernCoWaterLevelSource"


class CABQWaterLevelSource(ST2WaterLevelSource):
    agency = "CABQ"

    def __init__(self):
        super().__init__(transformer=CABQWaterLevelTransformer())

    def __repr__(self):
        return "CABQWaterLevelSource"


# ============= EOF =============================================

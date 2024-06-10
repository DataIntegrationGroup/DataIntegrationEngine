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
import os

import httpx

from backend.connectors import NM_STATE_BOUNDING_POLYGON
from backend.connectors.nmbgmr.transformer import (
    NMBGMRSiteTransformer,
    NMBGMRWaterLevelTransformer,
    NMBGMRAnalyteTransformer,
)
from backend.connectors.mappings import NMBGMR_ANALYTE_MAPPING
from backend.constants import (
    TDS,
    FEET,
    URANIUM,
    SULFATE,
    ARSENIC,
    CHLORIDE,
    FLUORIDE,
    DTW,
    DTW_UNITS,
    DT_MEASURED,
    PARAMETER,
    PARAMETER_UNITS,
    PARAMETER_VALUE,
)
from backend.source import (
    BaseWaterLevelSource,
    BaseSiteSource,
    BaseAnalyteSource,
    get_most_recent,
    get_analyte_search_param,
    make_site_list,
)


def _make_url(endpoint):
    if os.getenv("DEBUG") == "1":
        return f"http://localhost:8000/{endpoint}"
    return f"https://waterdata.nmt.edu/{endpoint}"


class NMBGMRSiteSource(BaseSiteSource):
    transformer_klass = NMBGMRSiteTransformer
    chunk_size = 100
    bounding_polygon = NM_STATE_BOUNDING_POLYGON

    def health(self):
        resp = self._execute_json_request(
            _make_url("locations"), tag="features", params={"limit": 1}
        )
        return bool(resp)

    def get_records(self):
        config = self.config
        params = {}
        if config.has_bounds():
            params["wkt"] = config.bounding_wkt()

        if config.site_limit:
            params["limit"] = config.site_limit

        if config.analyte:
            params["has_analyte"] = get_analyte_search_param(
                config.analyte, NMBGMR_ANALYTE_MAPPING
            )
        else:
            params["has_waterlevels"] = True

        return self._execute_json_request(
            _make_url("locations"), params, tag="features", timeout=30
        )


class NMBGMRAnalyteSource(BaseAnalyteSource):
    transformer_klass = NMBGMRAnalyteTransformer

    def get_records(self, parent_record):
        analyte = get_analyte_search_param(self.config.analyte, NMBGMR_ANALYTE_MAPPING)
        records = self._execute_json_request(
            _make_url("waterchemistry"),
            params={
                "pointid": ",".join(make_site_list(parent_record)),
                "analyte": analyte,
            },
            tag="",
        )
        return records[analyte]

    def _extract_parent_records(self, records, parent_record):
        return records.get(parent_record.id, [])

    def _extract_parameter_units(self, records):
        return [r["Units"] for r in records]

    def _extract_most_recent(self, records):
        record = get_most_recent(records, "info.CollectionDate")
        return {
            "value": record["SampleValue"],
            "datetime": record["info"]["CollectionDate"],
            "units": record["Units"],
        }

    def _extract_parameter_results(self, records):
        return [r["SampleValue"] for r in records]

    def _extract_parameter_record(self, record):
        record[PARAMETER] = self.config.analyte
        record[PARAMETER_VALUE] = record["SampleValue"]
        record[PARAMETER_UNITS] = record["Units"]
        record[DT_MEASURED] = record["info"]["CollectionDate"]
        return record


class NMBGMRWaterLevelSource(BaseWaterLevelSource):
    transformer_klass = NMBGMRWaterLevelTransformer

    def _clean_records(self, records):
        return [r for r in records if r["DepthToWaterBGS"] is not None]

    def _extract_parameter_record(self, record, *args, **kw):
        record[DTW] = record["DepthToWaterBGS"]
        record[DT_MEASURED] = (record["DateMeasured"], record["TimeMeasured"])
        record[DTW_UNITS] = FEET
        return record

    def _extract_most_recent(self, records):
        record = get_most_recent(records, "DateMeasured")
        return {
            "value": record["DepthToWaterBGS"],
            "datetime": (record["DateMeasured"], record["TimeMeasured"]),
            "units": FEET,
        }

    def _extract_parameter_results(self, records):
        return [r["DepthToWaterBGS"] for r in records]

    def _extract_parent_records(self, records, parent_record):
        return [ri for ri in records if ri["Well"]["PointID"] == parent_record.id]

    def get_records(self, parent_record):
        # if self.config.latest_water_level_only:
        #     params = {"pointids": parent_record.id}
        #     url = _make_url("waterlevels/latest")
        # else:
        params = {"pointid": ",".join(make_site_list(parent_record))}
        # just use manual waterlevels temporarily
        url = _make_url("waterlevels/manual")

        return self._execute_json_request(url, params)


# ============= EOF =============================================

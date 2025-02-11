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
        return f"http://localhost:8000/latest/{endpoint}"
    return f"https://waterdata.nmt.edu/latest/{endpoint}"


class NMBGMRSiteSource(BaseSiteSource):
    transformer_klass = NMBGMRSiteTransformer
    chunk_size = 100
    bounding_polygon = NM_STATE_BOUNDING_POLYGON

    def __repr__(self):
        return "NMBGMRSiteSource"

    def health(self):
        resp = self._execute_json_request(
            _make_url("locations"), tag="features", params={"limit": 1}
        )
        return bool(resp)

    def get_records(self):
        config = self.config
        params = {"site_type": "Groundwater other than spring (well)", "expand": False}
        if config.has_bounds():
            params["wkt"] = config.bounding_wkt()

        if config.site_limit:
            params["limit"] = config.site_limit

        if config.parameter.lower() != "waterlevels":
            params["parameter"] = get_analyte_search_param(
                config.parameter, NMBGMR_ANALYTE_MAPPING
            )
        else:
            params["parameter"] = "Manual groundwater levels"

        # tags="features" because the response object is a GeoJSON
        sites = self._execute_json_request(
            _make_url("locations"), params, tag="features", timeout=30
        )
        return sites

        # loop through the responses and add well information for each location
        # this may be slow because of the number of sites that need to be queried
        # but it is necessary to get the well information. With further
        # development, this could be faster if one can batch the requests
        # to /wells
        # for site in sites:
        #     well_info = self._execute_json_request(
        #         _make_url("/wells"),
        #         params={"pointid": site["properties"]["point_id"]},
        #         tag="",
        #     )
        #     site["properties"]["formation"] = well_info["formation"]
        #     site["properties"]["well_depth"] = well_info["well_depth_ftbgs"]
        #     site["properties"]["well_depth_units"] = "ft"


class NMBGMRAnalyteSource(BaseAnalyteSource):
    transformer_klass = NMBGMRAnalyteTransformer

    def __repr__(self):
        return "NMBGMRAnalyteSource"

    def get_records(self, site_record):
        analyte = get_analyte_search_param(
            self.config.parameter, NMBGMR_ANALYTE_MAPPING
        )
        records = self._execute_json_request(
            _make_url("waterchemistry"),
            params={
                "pointid": ",".join(make_site_list(site_record)),
                "analyte": analyte,
            },
            tag="",
        )
        records_sorted_by_pointid = {}
        for pointid in records.keys():
            records_sorted_by_pointid[pointid] = records[pointid][analyte]

        return records_sorted_by_pointid

    def _extract_site_records(self, records, site_record):
        return records.get(site_record.id, [])

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

    def _extract_parameter_dates(self, records: list) -> list:
        return [r["info"]["CollectionDate"] for r in records]

    def _extract_parameter_record(self, record):
        record[PARAMETER] = self.config.parameter
        record[PARAMETER_VALUE] = record["SampleValue"]
        record[PARAMETER_UNITS] = record["Units"]
        record[DT_MEASURED] = record["info"]["CollectionDate"]
        return record


class NMBGMRWaterLevelSource(BaseWaterLevelSource):
    transformer_klass = NMBGMRWaterLevelTransformer

    def __repr__(self):
        return "NMBGMRWaterLevelSource"

    def _clean_records(self, records):
        # remove records with no depth to water value
        return [r for r in records if r["DepthToWaterBGS"] is not None]

    def _extract_parameter_record(self, record, *args, **kw):
        record[PARAMETER] = DTW
        record[PARAMETER_VALUE] = record["DepthToWaterBGS"]
        record[PARAMETER_UNITS] = FEET
        record[DT_MEASURED] = (record["DateMeasured"], record["TimeMeasured"])
        return record

    def _extract_most_recent(self, records):
        record = get_most_recent(records, "DateMeasured")
        return {
            "value": record["DepthToWaterBGS"],
            "datetime": (record["DateMeasured"], record["TimeMeasured"]),
            "units": FEET,
        }

    def _extract_parameter_dates(self, records: list) -> list:
        return [(r["DateMeasured"], r["TimeMeasured"]) for r in records]

    def _extract_parameter_results(self, records):
        return [r["DepthToWaterBGS"] for r in records]

    def _extract_site_records(self, records, site_record):
        return [ri for ri in records if ri["Well"]["PointID"] == site_record.id]

    def get_records(self, site_record):
        # if self.config.latest_water_level_only:
        #     params = {"pointids": site_record.id}
        #     url = _make_url("waterlevels/latest")
        # else:
        params = {"pointid": ",".join(make_site_list(site_record))}
        # just use manual waterlevels temporarily
        url = _make_url("waterlevels/manual")

        return self._execute_json_request(url, params)


# ============= EOF =============================================

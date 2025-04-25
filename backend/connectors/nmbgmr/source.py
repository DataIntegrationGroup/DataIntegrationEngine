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

from backend import get_bool_env_variable
from backend.connectors import NM_STATE_BOUNDING_POLYGON
from backend.connectors.nmbgmr.transformer import (
    NMBGMRSiteTransformer,
    NMBGMRWaterLevelTransformer,
    NMBGMRAnalyteTransformer,
)
from backend.connectors.mappings import NMBGMR_ANALYTE_MAPPING
from backend.constants import (
    FEET,
    DTW,
    DT_MEASURED,
    PARAMETER_NAME,
    PARAMETER_UNITS,
    PARAMETER_VALUE,
    SOURCE_PARAMETER_NAME,
    SOURCE_PARAMETER_UNITS,
    EARLIEST,
    LATEST,
)
from backend.source import (
    BaseWaterLevelSource,
    BaseSiteSource,
    BaseAnalyteSource,
    get_terminal_record,
    get_analyte_search_param,
    make_site_list,
)


def _make_url(endpoint):
    if os.getenv("DEBUG") == "1":
        url = f"http://localhost:8000/latest/{endpoint}"
    else:
        url = f"https://waterdata.nmt.edu/latest/{endpoint}"
    return url


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

        if not config.sites_only:

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
        if not config.sites_only:
            for site in sites:
                if get_bool_env_variable("IS_TESTING_ENV"):
                    print(
                        f"Skipping well data for {site['properties']['point_id']} for testing (until well data can be retrieved in batches)"
                    )
                    site["properties"]["formation"] = None
                    site["properties"]["well_depth"] = None
                    site["properties"]["well_depth_units"] = FEET
                else:
                    print(f"Obtaining well data for {site['properties']['point_id']}")
                    well_data = self._execute_json_request(
                        _make_url("wells"),
                        params={"pointid": site["properties"]["point_id"]},
                        tag="",
                    )
                    site["properties"]["formation"] = well_data["formation"]
                    site["properties"]["well_depth"] = well_data["well_depth_ftbgs"]
                    site["properties"]["well_depth_units"] = FEET

        return sites


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

    def _extract_source_parameter_units(self, records):
        return [r["Units"] for r in records]

    def _extract_terminal_record(self, records, bookend):
        record = get_terminal_record(records, "info.CollectionDate", bookend=bookend)
        return {
            "value": record["SampleValue"],
            "datetime": record["info"]["CollectionDate"],
            "source_parameter_units": record["Units"],
            "source_parameter_name": record["AnalyteMeaning"],
        }

    def _extract_source_parameter_results(self, records):
        return [r["SampleValue"] for r in records]

    def _extract_parameter_dates(self, records: list) -> list:
        return [r["info"]["CollectionDate"] for r in records]

    def _extract_source_parameter_names(self, records: list) -> list:
        return [r["AnalyteMeaning"] for r in records]

    def _extract_parameter_record(self, record):
        record[PARAMETER_NAME] = self.config.parameter
        record[PARAMETER_VALUE] = record["SampleValue"]
        record[PARAMETER_UNITS] = self.config.analyte_output_units
        record[DT_MEASURED] = record["info"]["CollectionDate"]
        record[SOURCE_PARAMETER_NAME] = record["AnalyteMeaning"]
        record[SOURCE_PARAMETER_UNITS] = record["Units"]

        return record


class NMBGMRWaterLevelSource(BaseWaterLevelSource):
    transformer_klass = NMBGMRWaterLevelTransformer

    def __repr__(self):
        return "NMBGMRWaterLevelSource"

    def _clean_records(self, records):
        # remove records with no depth to water value
        return [
            r
            for r in records
            if r["DepthToWaterBGS"] is not None and r["DateMeasured"] is not None
        ]

    def _extract_parameter_record(self, record, *args, **kw):
        record[PARAMETER_NAME] = DTW
        record[PARAMETER_VALUE] = record["DepthToWaterBGS"]
        record[PARAMETER_UNITS] = self.config.waterlevel_output_units
        record[DT_MEASURED] = (record["DateMeasured"], record["TimeMeasured"])
        record[SOURCE_PARAMETER_NAME] = "DepthToWaterBGS"
        record[SOURCE_PARAMETER_UNITS] = record["DepthToWaterBGSUnits"]
        return record

    def _extract_terminal_record(self, records, bookend):
        record = get_terminal_record(records, "DateMeasured", bookend=bookend)
        return {
            "value": record["DepthToWaterBGS"],
            "datetime": (record["DateMeasured"], record["TimeMeasured"]),
            "source_parameter_units": record["DepthToWaterBGSUnits"],
            "source_parameter_name": "DepthToWaterBGS",
        }

    def _extract_parameter_dates(self, records: list) -> list:
        return [(r["DateMeasured"], r["TimeMeasured"]) for r in records]

    def _extract_source_parameter_results(self, records):
        return [r["DepthToWaterBGS"] for r in records]

    def _extract_site_records(self, records, site_record):
        return [ri for ri in records if ri["PointID"] == site_record.id]

    def _extract_source_parameter_names(self, records):
        return ["DepthToWaterBGS" for r in records]

    def _extract_source_parameter_units(self, records):
        return [r["DepthToWaterBGSUnits"] for r in records]

    def get_records(self, site_record):
        # if self.config.latest_water_level_only:
        #     params = {"pointids": site_record.id}
        #     url = _make_url("waterlevels/latest")
        # else:
        params = {"pointid": ",".join(make_site_list(site_record))}
        # just use manual waterlevels temporarily
        url = _make_url("waterlevels/manual")

        paginated_records = self._execute_json_request(url, params, tag="")
        items = paginated_records["items"]
        page = paginated_records["page"]
        pages = paginated_records["pages"]

        while page < pages:
            page += 1
            params["page"] = page
            new_records = self._execute_json_request(url, params, tag="")
            items.extend(new_records["items"])
            pages = new_records["pages"]

        return items


# ============= EOF =============================================

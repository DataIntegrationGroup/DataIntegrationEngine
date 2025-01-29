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
import pprint

import httpx

from backend.connectors import NM_STATE_BOUNDING_POLYGON
from backend.connectors.mappings import WQP_ANALYTE_MAPPING
from backend.constants import (
    TDS,
    URANIUM,
    NITRATE,
    SULFATE,
    ARSENIC,
    CHLORIDE,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
    DT_MEASURED,
)
from backend.connectors.wqp.transformer import WQPSiteTransformer, WQPAnalyteTransformer
from backend.source import (
    BaseSource,
    BaseSiteSource,
    BaseAnalyteSource,
    make_site_list,
    get_most_recent,
    get_analyte_search_param,
)


def parse_tsv(text):
    rows = text.split("\n")
    header = rows[0].split("\t")
    return [dict(zip(header, row.split("\t"))) for row in rows[1:]]


def get_date_range(config):
    params = {}
    if config.start_date:
        params["startDateLo"] = config.start_dt.strftime("%m-%d-%Y")
    if config.end_date:
        params["end"] = config.end_dt.strftime("%m-%d-%Y")
    return params


class WQPSiteSource(BaseSiteSource):
    transformer_klass = WQPSiteTransformer
    chunk_size = 100

    bounding_polygon = NM_STATE_BOUNDING_POLYGON

    def __repr__(self):
        return "WQPSiteSource"

    def health(self):
        try:
            r = httpx.get(
                "https://www.waterqualitydata.us/data/Station/search",
                params={"mimeType": "tsv", "siteid": "325754103461301"},
            )
            return r.status_code == 200
        except Exception as e:
            return False

    def get_records(self):
        config = self.config
        params = {
            "mimeType": "tsv",
            "siteType": "Well",
            "sampleMedia": "Water",
            "statecode": "US:35",
        }
        if config.has_bounds():
            params["bBox"] = ",".join([str(b) for b in config.bbox_bounding_points()])

        if config.parameter.lower() != "waterlevels":
            params["characteristicName"] = get_analyte_search_param(
                config.parameter, WQP_ANALYTE_MAPPING
            )

        params.update(get_date_range(config))

        text = self._execute_text_request(
            "https://www.waterqualitydata.us/data/Station/search?", params, timeout=30
        )
        if text:
            return parse_tsv(text)


class WQPAnalyteSource(BaseAnalyteSource):
    transformer_klass = WQPAnalyteTransformer

    def __repr__(self):
        return "WQPAnalyteSource"

    def _extract_parameter_record(self, record):
        record[PARAMETER_VALUE] = record["ResultMeasureValue"]
        record[PARAMETER_UNITS] = record["ResultMeasure/MeasureUnitCode"]
        record[DT_MEASURED] = record["ActivityStartDate"]
        return record

    def _extract_site_records(self, records, site_record):
        return [
            ri for ri in records if ri["MonitoringLocationIdentifier"] == site_record.id
        ]

    def _extract_parameter_results(self, records):
        return [ri["ResultMeasureValue"] for ri in records]

    def _clean_records(self, records):
        return [ri for ri in records if ri["ResultMeasureValue"]]

    def _extract_parameter_units(self, records):
        return [ri["ResultMeasure/MeasureUnitCode"] for ri in records]

    def _extract_parameter_dates(self, records):
        return [ri["ActivityStartDate"] for ri in records]

    def _extract_most_recent(self, records):
        ri = get_most_recent(records, "ActivityStartDate")
        return {
            "value": ri["ResultMeasureValue"],
            "datetime": ri["ActivityStartDate"],
            "units": ri["ResultMeasure/MeasureUnitCode"],
        }

    def get_records(self, site_record):
        sites = make_site_list(site_record)

        params = {
            "siteid": sites,
            "mimeType": "tsv",
            "characteristicName": get_analyte_search_param(
                self.config.parameter, WQP_ANALYTE_MAPPING
            ),
        }
        params.update(get_date_range(self.config))

        text = self._execute_text_request(
            "https://www.waterqualitydata.us/data/Result/search?", params
        )
        if text:
            return parse_tsv(text)


# ============= EOF =============================================

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

from backend.connectors.mappings import WQP_ANALYTE_MAPPING
from backend.constants import TDS, URANIUM, NITRATE, SULFATE, ARSENIC, CHLORIDE
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


class WQPSiteSource(BaseSiteSource):
    transformer_klass = WQPSiteTransformer
    chunk_size = 100

    def get_records(self):
        config = self.config
        params = {"mimeType": "tsv", "siteType": "Well"}
        if config.has_bounds():
            params["bBox"] = ",".join([str(b) for b in config.bbox_bounding_points()])

        if config.analyte:
            params["characteristicName"] = get_analyte_search_param(
                config.analyte, WQP_ANALYTE_MAPPING
            )

        resp = httpx.get(
            "https://www.waterqualitydata.us/data/Station/search?", params=params
        )
        return parse_tsv(resp.text)


class WQPAnalyteSource(BaseAnalyteSource):
    transformer_klass = WQPAnalyteTransformer

    def _extract_parent_records(self, records, parent_record):
        return [
            ri
            for ri in records
            if ri["MonitoringLocationIdentifier"] == parent_record.id
        ]

    def _extract_parameter_results(self, records):
        return [ri["ResultMeasureValue"] for ri in records]

    def _clean_records(self, records):
        return [ri for ri in records if ri["ResultMeasureValue"]]

    def _extract_parameter_units(self, records):
        return [ri["ResultMeasure/MeasureUnitCode"] for ri in records]

    def _extract_most_recent(self, records):
        ri = get_most_recent(records, "ActivityStartDate")
        return {
            "value": ri["ResultMeasureValue"],
            "datetime": ri["ActivityStartDate"],
            "units": ri["ResultMeasure/MeasureUnitCode"],
        }

    def get_records(self, parent_record):
        sites = make_site_list(parent_record)

        params = {
            "siteid": sites,
            "mimeType": "tsv",
            "characteristicName": get_analyte_search_param(
                self.config.analyte, WQP_ANALYTE_MAPPING
            ),
        }

        resp = httpx.get(
            "https://www.waterqualitydata.us/data/Result/search?",
            params=params,
            timeout=10,
        )
        return parse_tsv(resp.text)


# ============= EOF =============================================

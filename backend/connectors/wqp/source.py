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

from backend.connectors.constants import TDS, URANIUM, NITRATE, SULFATE
from backend.connectors.wqp.transformer import WQPSiteTransformer, WQPAnalyteTransformer
from backend.source import BaseSource, BaseSiteSource, BaseAnalyteSource, make_site_list


class WQPSiteSource(BaseSiteSource):
    transformer_klass = WQPSiteTransformer
    chunk_size = 100

    def get_records(self, config):
        params = {"mimeType": "tsv", "siteType": "Well"}
        # if config.bbox:
        #     bbox = config.bounding_points()
        #     params["bBox"] = ",".join([str(b) for b in bbox])
        if config.has_bounds():
            params["bBox"] = ",".join([str(b) for b in config.bounding_points()])
        if config.analyte:
            params["characteristicName"] = get_characteristic_names(config.analyte)

        resp = httpx.get(
            "https://www.waterqualitydata.us/data/Station/search?", params=params
        )
        result = resp.text
        rows = result.split("\n")
        header = rows[0].split("\t")
        return [dict(zip(header, row.split("\t"))) for row in rows[1:]]


def get_characteristic_names(parameter):
    if parameter == "Arsenic":
        characteristic_names = ["Arsenic"]
    elif parameter == "Chloride":
        characteristic_names = ["Chloride"]
    elif parameter == "Fluoride":
        characteristic_names = ["Fluoride"]
    elif parameter == NITRATE:
        characteristic_names = ["Nitrate", "Nitrate-N", "Nitrate as N"]
    elif parameter == SULFATE:
        characteristic_names = [
            "Sulfate",
            "Sulfate as SO4",
            "Sulfur Sulfate",
            "Sulfate as S",
            "Total Sulfate",
        ]
    elif parameter == TDS:
        characteristic_names = ["Total dissolved solids"]
    elif parameter == URANIUM:
        characteristic_names = ["Uranium", "Uranium-238"]
    else:
        raise ValueError(f"Invalid parameter name {parameter}")
    return characteristic_names


class WQPAnalyteSource(BaseAnalyteSource):
    transformer_klass = WQPAnalyteTransformer

    def _extract_parent_records(self, records, parent_record):
        return [
            ri
            for ri in records
            if ri["MonitoringLocationIdentifier"] == parent_record.id
        ]

    def _extract_analyte_results(self, records):
        return [ri["ResultMeasureValue"] for ri in records if ri["ResultMeasureValue"]]

    def _extract_analyte_units(self, records):
        return [
            ri["ResultMeasure/MeasureUnitCode"]
            for ri in records
            if ri["ResultMeasureValue"]
        ]

    def _extract_most_recent(self, records):
        return sorted([ri["ActivityStartDate"] for ri in records], reverse=True)[0]

    def get_records(self, parent_record, config):
        sites = make_site_list(parent_record)

        params = {
            "siteid": sites,
            "mimeType": "tsv",
            "characteristicName": get_characteristic_names(config.analyte),
        }

        resp = httpx.get(
            "https://www.waterqualitydata.us/data/Result/search?",
            params=params,
            timeout=10,
        )
        result = resp.text
        rows = result.split("\n")
        header = rows[0].split("\t")
        return [dict(zip(header, row.split("\t"))) for row in rows[1:]]


# ============= EOF =============================================

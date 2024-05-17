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

from backend.connectors.bor.transformer import BORSiteTransformer, BORAnalyteTransformer
from backend.connectors.mappings import BOR_ANALYTE_MAPPING
from backend.constants import TDS, URANIUM, ARSENIC, SULFATE, FLUORIDE, CHLORIDE

from backend.source import (
    BaseSource,
    BaseSiteSource,
    BaseAnalyteSource,
    get_most_recent,
    get_analyte_search_param,
)


class BORSiteSource(BaseSiteSource):
    transformer_klass = BORSiteTransformer

    def get_records(self):
        # locationTypeId 10 is for wells
        params = {"stateId": "NM", "locationTypeId": 10}
        resp = httpx.get("https://data.usbr.gov/rise/api/location", params=params)
        return resp.json()["data"]


class BORAnalyteSource(BaseAnalyteSource):
    transformer_klass = BORAnalyteTransformer
    _catalog_item_idx = None

    def _extract_parameter_results(self, rs):
        return [ri["attributes"]["result"] for ri in rs]

    def _extract_parameter_units(self, records):
        return [ri["attributes"]["resultAttributes"]["units"] for ri in records]

    def _extract_most_recent(self, rs):
        def parse_dt(dt):
            return tuple(dt.split("T"))

        record = get_most_recent(rs, "attributes.dateTime")
        return {
            "value": record["attributes"]["result"],
            "datetime": parse_dt(record["attributes"]["dateTime"]),
            "units": record["attributes"]["resultAttributes"]["units"],
        }

    def _extract_parent_records(self, records, parent_record):
        return [
            ri for ri in records if ri["attributes"]["locationId"] == parent_record.id
        ]

    def _reorder_catalog_items(self, items):
        if self._catalog_item_idx:
            # rotate list so catalog_item_idx is the first item
            items = items[self._catalog_item_idx :] + items[: self._catalog_item_idx]
        return items

    def get_records(self, parent_record):
        code = get_analyte_search_param(self.config.analyte, BOR_ANALYTE_MAPPING)

        for i, item in enumerate(
            self._reorder_catalog_items(parent_record.catalogItems)
        ):
            resp = httpx.get(
                f'https://data.usbr.gov{item["id"]}',
            )
            data = resp.json()["data"]
            pcode = data["attributes"]["parameterSourceCode"]
            if pcode == code:
                if not self._catalog_item_idx:
                    self._catalog_item_idx = i

                params = {
                    "itemId": data["attributes"]["_id"],
                }
                resp = httpx.get("https://data.usbr.gov/rise/api/result", params=params)
                return resp.json()["data"]


# class BORWaterLevelSource(BaseWaterLevelSource):
#     transformer_klass = BORWaterLevelTransformer

# def get_records(self, parent_record, config):
#     for item in parent_record.catalogItems:
#         print("get records", item)
#         resp = httpx.get(
#             f'https://data.usbr.gov{item["id"]}',
#         )
#         data = resp.json()["data"]
#         # pprint.pprint(data)
#         print("asdf", data["attributes"]["parameterName"])
#
#     # print('get records', parent_record.catalogItems)
#     # crec = parent_record.catalogItems[0]['id']
#     # pprint.pprint(resp.json())
#     # print('get records', parent_record)
#     # params = {
#     #     "format": "rdb",
#     #     "siteType": "GW",
#     #     "sites": parent_record.id,
#     #     # "startDT": config.start_date,
#     #     # "endDT": config.end_date,
#     # }
#     #
#     # resp = httpx.get(
#     #     "https://waterservices.BOR.gov/nwis/gwlevels/", params=params, timeout=10
#     # )
#     # records = parse_rdb(resp.text)
#     # return records
#
# def _extract_parameter_results(self, records):
#     return [float(r["lev_va"]) for r in records if r["lev_va"] is not None]
#
# def _extract_most_recent(self, records):
#
#     return [(r["lev_dt"], r["lev_tm"]) for r in records if r["lev_dt"] is not None][
#         -1
#     ]


# ============= EOF =============================================

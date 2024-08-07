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
from json import JSONDecodeError

import httpx

from backend.connectors.bor.transformer import BORSiteTransformer, BORAnalyteTransformer
from backend.connectors.mappings import BOR_ANALYTE_MAPPING
from backend.constants import (
    TDS,
    URANIUM,
    ARSENIC,
    SULFATE,
    FLUORIDE,
    CHLORIDE,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
    DT_MEASURED,
)

from backend.source import (
    BaseSource,
    BaseSiteSource,
    BaseAnalyteSource,
    get_most_recent,
    get_analyte_search_param,
)


class BORSiteSource(BaseSiteSource):
    transformer_klass = BORSiteTransformer

    def health(self):
        try:
            self.get_records()
            return True
        except Exception:
            return False

    def get_records(self):
        # locationTypeId 10 is for wells
        url = "https://data.usbr.gov/rise/api/location"
        params = {"stateId": "NM", "locationTypeId": 10}
        return self._execute_json_request(url, params)


def parse_dt(dt):
    return tuple(dt.split("T"))


class BORAnalyteSource(BaseAnalyteSource):
    transformer_klass = BORAnalyteTransformer
    _catalog_item_idx = None

    def _extract_parameter_record(self, record):
        record[PARAMETER_VALUE] = record["attributes"]["result"]
        record[PARAMETER_UNITS] = record["attributes"]["resultAttributes"]["units"]
        record[DT_MEASURED] = parse_dt(record["attributes"]["dateTime"])
        return record

    def _extract_parameter_results(self, rs):
        return [ri["attributes"]["result"] for ri in rs]

    def _extract_parameter_units(self, records):
        return [ri["attributes"]["resultAttributes"]["units"] for ri in records]

    def _extract_most_recent(self, rs):

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

            data = self._execute_json_request(f'https://data.usbr.gov{item["id"]}')
            if not data:
                continue

            pcode = data["attributes"]["parameterSourceCode"]
            if pcode == code:
                if not self._catalog_item_idx:
                    self._catalog_item_idx = i

                return self._execute_json_request(
                    "https://data.usbr.gov/rise/api/result",
                    params={"itemId": data["attributes"]["_id"]},
                )


# ============= EOF =============================================

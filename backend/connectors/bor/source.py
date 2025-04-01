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
    PARAMETER_NAME,
    PARAMETER_VALUE,
    PARAMETER_UNITS,
    SOURCE_PARAMETER_NAME,
    SOURCE_PARAMETER_UNITS,
    DT_MEASURED,
    EARLIEST,
    LATEST,
)

from backend.source import (
    BaseSource,
    BaseSiteSource,
    BaseAnalyteSource,
    get_terminal_record,
    get_analyte_search_param,
)


class BORSiteSource(BaseSiteSource):
    transformer_klass = BORSiteTransformer

    def __repr__(self):
        return "BORSiteSource"

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
    _source_parameter_name = None

    def __repr__(self):
        return "BORAnalyteSource"

    def _extract_parameter_record(self, record):
        record[PARAMETER_NAME] = self.config.parameter
        record[PARAMETER_VALUE] = record["attributes"]["result"]
        record[PARAMETER_UNITS] = self.config.analyte_output_units
        record[DT_MEASURED] = parse_dt(record["attributes"]["dateTime"])
        record[SOURCE_PARAMETER_NAME] = self._source_parameter_name
        record[SOURCE_PARAMETER_UNITS] = record["attributes"]["resultAttributes"][
            "units"
        ]
        return record

    def _extract_source_parameter_results(self, rs):
        return [ri["attributes"]["result"] for ri in rs]

    def _extract_source_parameter_units(self, records):
        return [ri["attributes"]["resultAttributes"]["units"] for ri in records]

    def _extract_parameter_dates(self, records):
        return [parse_dt(ri["attributes"]["dateTime"]) for ri in records]

    def _extract_source_parameter_names(self, records):
        return [self._source_parameter_name for ri in records]

    def _extract_terminal_record(self, records, bookend):
        record = get_terminal_record(records, "attributes.dateTime", bookend=bookend)
        return {
            "value": record["attributes"]["result"],
            "datetime": parse_dt(record["attributes"]["dateTime"]),
            "source_parameter_units": record["attributes"]["resultAttributes"]["units"],
            "source_parameter_name": self._source_parameter_name,
        }

    def _extract_site_records(self, records, site_record):
        return [
            ri for ri in records if ri["attributes"]["locationId"] == site_record.id
        ]

    def _reorder_catalog_items(self, items):
        if self._catalog_item_idx:
            # rotate list so catalog_item_idx is the first item
            items = items[self._catalog_item_idx :] + items[: self._catalog_item_idx]
        return items

    def get_records(self, site_record):
        code = get_analyte_search_param(self.config.parameter, BOR_ANALYTE_MAPPING)

        catalog_record_data = self._execute_json_request(
            f"https://data.usbr.gov{site_record.catalogRecords[0]['id']}"
        )
        catalog_items = catalog_record_data["relationships"]["catalogItems"]["data"]

        for i, item in enumerate(self._reorder_catalog_items(catalog_items)):

            data = self._execute_json_request(f'https://data.usbr.gov{item["id"]}')
            if not data:
                continue

            pcode = data["attributes"]["parameterSourceCode"]
            if pcode == code:
                if not self._catalog_item_idx:
                    self._catalog_item_idx = i

                if self._source_parameter_name is None:
                    self._source_parameter_name = data["attributes"][
                        "parameterSourceCode"
                    ]

                return self._execute_json_request(
                    "https://data.usbr.gov/rise/api/result",
                    params={"itemId": data["attributes"]["_id"]},
                )


# ============= EOF =============================================

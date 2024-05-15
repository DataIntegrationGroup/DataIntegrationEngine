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

from backend.connectors.bor.transformer import (
    BORSiteTransformer,
    BORWaterLevelTransformer,
)
from backend.source import BaseSource, BaseWaterLevelSource, BaseSiteSource


class BORSiteSource(BaseSiteSource):
    transformer_klass = BORSiteTransformer

    def get_records(self, config):
        # locationTypeId 10 is for wells
        params = {"stateId": "NM", "locationTypeId": 10}
        resp = httpx.get("https://data.usbr.gov/rise/api/location", params=params)
        print(resp.url)
        return resp.json()["data"]


class BORWaterLevelSource(BaseWaterLevelSource):
    transformer_klass = BORWaterLevelTransformer

    def get_records(self, parent_record, config):
        for item in parent_record.catalogItems:
            print("get records", item)
            resp = httpx.get(
                f'https://data.usbr.gov{item["id"]}',
            )
            data = resp.json()["data"]
            # pprint.pprint(data)
            print("asdf", data["attributes"]["parameterName"])

        # print('get records', parent_record.catalogItems)
        # crec = parent_record.catalogItems[0]['id']
        # pprint.pprint(resp.json())
        # print('get records', parent_record)
        # params = {
        #     "format": "rdb",
        #     "siteType": "GW",
        #     "sites": parent_record.id,
        #     # "startDT": config.start_date,
        #     # "endDT": config.end_date,
        # }
        #
        # resp = httpx.get(
        #     "https://waterservices.BOR.gov/nwis/gwlevels/", params=params, timeout=10
        # )
        # records = parse_rdb(resp.text)
        # return records

    def _extract_waterlevels(self, records):
        return [float(r["lev_va"]) for r in records if r["lev_va"] is not None]

    def _extract_most_recent(self, records):

        return [(r["lev_dt"], r["lev_tm"]) for r in records if r["lev_dt"] is not None][
            -1
        ]


# ============= EOF =============================================

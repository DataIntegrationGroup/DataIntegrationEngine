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
from datetime import datetime

import httpx

from backend.connectors.constants import TDS, FEET
from backend.connectors.isc_seven_rivers.transformer import (
    ISCSevenRiversSiteTransformer,
    ISCSevenRiversWaterLevelTransformer,
    ISCSevenRiversAnalyteTransformer,
)
from backend.source import (
    BaseSource,
    BaseSiteSource,
    BaseWaterLevelSource,
    BaseAnalyteSource,
    get_most_recent,
)


def _make_url(endpoint):
    return f"https://nmisc-wf.gladata.com/api/{endpoint}"


class ISCSevenRiversSiteSource(BaseSiteSource):
    transformer_klass = ISCSevenRiversSiteTransformer

    def get_records(self):
        resp = httpx.get(_make_url("getMonitoringPoints.ashx"))
        return resp.json()["data"]


class ISCSevenRiversAnalyteSource(BaseAnalyteSource):
    transformer_klass = ISCSevenRiversAnalyteTransformer
    _analyte_ids = None

    def _get_analyte_id(self, analyte):
        if self._analyte_ids is None:
            resp = httpx.get(_make_url("getAnalytes.ashx"))
            self._analyte_ids = {r["name"]: r["id"] for r in resp.json()["data"]}

        if analyte == TDS:
            analyte = "TDS calc"

        return self._analyte_ids.get(analyte)

    def _extract_most_recent(self, records):
        record = get_most_recent(records, "dateTime")

        return {
            "value": record["result"],
            "datetime": datetime.fromtimestamp(record["dateTime"] / 1000),
            "units": record["units"],
        }

    def _extract_parameter_results(self, records):
        return [r["result"] for r in records]

    def _extract_parameter_units(self, records):
        return [r["units"] for r in records]

    def get_records(self, parent_record):
        config = self.config
        resp = httpx.get(
            _make_url("getReadings.ashx"),
            params={
                "monitoringPointId": parent_record.id,
                "analyteId": self._get_analyte_id(config.analyte),
                "start": 0,
                "end": config.now_ms(days=1),
            },
        )
        return resp.json()["data"]


class ISCSevenRiversWaterLevelSource(BaseWaterLevelSource):
    transformer_klass = ISCSevenRiversWaterLevelTransformer

    def get_records(self, parent_record):
        resp = httpx.get(
            _make_url("getWaterLevels.ashx"),
            params={
                "id": parent_record.id,
                "start": 0,
                "end": self.config.now_ms(days=1),
            },
        )
        return resp.json()["data"]

    def _clean_records(self, records):
        return [r for r in records if r["depthToWaterFeet"] is not None]

    def _extract_parameter_results(self, records):
        return [
            r["depthToWaterFeet"] for r in records if not r["invalid"] and not r["dry"]
        ]

    def _extract_most_recent(self, records):
        record = get_most_recent(records, "dateTime")
        t = datetime.fromtimestamp(record["dateTime"] / 1000)
        return {"value": record["depthToWaterFeet"], "datetime": t, "units": FEET}


# ============= EOF =============================================

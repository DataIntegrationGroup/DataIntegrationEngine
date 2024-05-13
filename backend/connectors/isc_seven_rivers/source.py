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

from backend.connectors.isc_seven_rivers.transformer import (
    ISCSevenRiversSiteTransformer, ISCSevenRiversWaterLevelTransformer,
)
from backend.source import BaseSource, BaseSiteSource, BaseWaterLevelsSource


def _make_url(endpoint):
    return f"https://nmisc-wf.gladata.com/api/{endpoint}"


class ISCSevenRiversSiteSource(BaseSiteSource):
    transformer_klass = ISCSevenRiversSiteTransformer

    def get_records(self, config):
        resp = httpx.get(_make_url("getMonitoringPoints.ashx"))
        for record in resp.json()["data"]:
            yield record


class ISCSevenRiversWaterLevelSource(BaseWaterLevelsSource):
    transformer_klass = ISCSevenRiversWaterLevelTransformer

    def get_records(self, parent_record, config):
        resp = httpx.get(
            _make_url("getWaterLevels.ashx"),
            params={"id": parent_record.id,
                    "start": 0,
                    "end": config.now_ms(days=1)},
        )
        for record in resp.json()["data"]:
            yield record

    def _extract_waterlevels(self, records):
        return [r['depthToWaterFeet'] for r in records
                if r["depthToWaterFeet"] is not None
                and not r['invalid']
                and not r['dry']]

    def _extract_most_recent(self, records):
        t = max(records, key=lambda x: x["dateTime"])['dateTime']
        t = datetime.fromtimestamp(t / 1000)
        return t.isoformat()
# ============= EOF =============================================

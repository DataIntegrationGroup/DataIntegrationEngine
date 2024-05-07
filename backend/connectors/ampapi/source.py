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
import httpx

from backend.connectors.ampapi.transformer import (
    AMPAPISiteTransformer,
    AMPAPIWaterLevelTransformer,
)
from backend.source import BaseWaterLevelsSource, BaseSiteSource

DEBUG = False


def _make_url(endpoint):
    if DEBUG:
        return f"http://localhost:8000/{endpoint}"
    return f"https://waterdata.nmt.edu/{endpoint}"


class AMPAPISiteSource(BaseSiteSource):
    transformer_klass = AMPAPISiteTransformer

    def get_records(self, config):
        params = {}
        if config.has_bounds():
            params["wkt"] = config.bounding_wkt()

        resp = httpx.get(_make_url("locations"), params=params)
        for site in resp.json()["features"]:
            yield site


class AMPAPIWaterLevelSource(BaseWaterLevelsSource):
    transformer_klass = AMPAPIWaterLevelTransformer

    def _extract_most_recent(self, records):
        return records[0]["DateMeasured"]

    def _extract_waterlevels(self, records):
        return [r["DepthToWaterBGS"] for r in records]

    def get_records(self, parent_record, config):
        if config.latest_water_level_only:
            params = {"pointids": parent_record.id}
            url = _make_url("waterlevels/latest")
        else:
            params = {"pointid": parent_record.id}
            # just use manual waterlevels temporarily
            url = _make_url("waterlevels/manual")

        resp = httpx.get(url, params=params)
        for wl in resp.json():
            yield wl


# ============= EOF =============================================

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

from backend.connectors.wqp.transformer import WQPSiteTransformer
from backend.source import BaseSource, BaseSiteSource


class WQPSiteSource(BaseSiteSource):
    transformer_klass = WQPSiteTransformer

    def get_records(self, config):
        params = {"mimeType": "tsv", "siteType": "Well"}
        if config.bbox:
            bbox = config.bounding_points()
            params["bBox"] = ",".join([str(b) for b in bbox])

        resp = httpx.get(
            "https://www.waterqualitydata.us/data/Station/search?", params=params
        )
        result = resp.text
        rows = result.split("\n")
        header = rows[0].split("\t")
        for row in rows[1:]:
            vals = row.split("\t")
            yield dict(zip(header, vals))


class WQPAnalyteSource(BaseSource):
    transformer_klass = WQPSiteTransformer

    def get_records(self, config):
        params = {"mimeType": "tsv", "siteType": "Well"}
        if config.bbox:
            bbox = config.bounding_points()
            params["bBox"] = ",".join([str(b) for b in bbox])

        resp = httpx.get(
            "https://www.waterqualitydata.us/data/Result/search?", params=params
        )
        result = resp.text
        rows = result.split("\n")
        header = rows[0].split("\t")
        for row in rows[1:]:
            vals = row.split("\t")
            yield dict(zip(header, vals))


# ============= EOF =============================================

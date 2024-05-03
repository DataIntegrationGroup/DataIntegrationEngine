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

from backend.connectors.ampapi.transformer import AMPAPISiteTransformer
from backend.source import BaseSource


class AMPAPISiteSource(BaseSource):
    transformer_klass = AMPAPISiteTransformer

    def get_records(self, config):

        params = {}
        if config.bbox:

            # need to update api to use lon/lat pairs
            # params["wkt"] = config.bounding_wkt()

            x1, y1, x2, y2 = config.bounding_points()
            w = f"POLYGON(({y1} {x1},{y1} {x2},{y2} {x2},{y2} {x1},{y1} {x1}))"
            params["wkt"] = w

        resp = httpx.get(self._make_url("locations"), params=params)
        for site in resp.json()["features"]:
            yield site

    def _make_url(self, endpoint):
        return f"https://waterdata.nmt.edu/{endpoint}"


# ============= EOF =============================================

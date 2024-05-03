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

from backend.connectors.ampapi.transformer import AMPAPISiteTransformer, AMPAPIWaterLevelTransformer
from backend.source import BaseSource


def _make_url(endpoint):
    return f"https://waterdata.nmt.edu/{endpoint}"


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

        resp = httpx.get(_make_url("locations"), params=params)
        for site in resp.json()["features"]:
            yield site


class AMPAPIWaterLevelSource(BaseSource):
    transformer_klass = AMPAPIWaterLevelTransformer

    def read(self, parent_record, config):
        self.log(f"Gathering records for record {parent_record.id}")
        n = 0
        for record in self.get_records(parent_record, config):
            record = self.transformer.transform(record, parent_record, config)
            if record:
                n += 1
                yield record

        self.log(f"nrecords={n}")

    def get_records(self, parent_record, config):
        params = {'pointid': parent_record.id}
        # just use manual waterlevels temporarily
        resp = httpx.get(_make_url("waterlevels/manual"), params=params)
        for wl in resp.json():
            yield wl
# ============= EOF =============================================

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

from backend.connectors.usgs.transformer import USGSSiteTransformer
from backend.source import BaseSource


class USGSSiteSource(BaseSource):
    transformer_klass = USGSSiteTransformer

    def get_records(self):
        resp = httpx.get(
            "https://waterservices.usgs.gov/nwis/site/?format=rdb&siteOutput=expanded&siteType=GW&stateCd=NM"
        )
        for line in resp.text.split("\n"):
            if line.startswith("#"):
                continue
            elif line.startswith("agency_cd"):
                header = [h.strip() for h in line.split("\t")]
                continue
            elif line.startswith("5s"):
                continue
            elif line == "":
                continue

            vals = [v.strip() for v in line.split("\t")]
            yield dict(zip(header, vals))


# ============= EOF =============================================

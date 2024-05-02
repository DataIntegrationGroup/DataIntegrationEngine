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
from backend.record import SiteRecord
from backend.transformer import BaseTransformer


class ISCSevenRiversSiteTransformer(BaseTransformer):
    def transform(self, record):
        rec = {
            "source": "ISCSevenRivers",
            "id": record["id"],
            "name": record["name"],
            "latitude": record["latitude"],
            "longitude": record["longitude"],
            "elevation_feet": record["groundSurfaceElevationFeet"],
        }

        return SiteRecord(rec)


# ============= EOF =============================================

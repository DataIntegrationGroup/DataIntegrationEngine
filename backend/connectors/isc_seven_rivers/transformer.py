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
import shapely.wkt
from shapely import Point

from backend.record import SiteRecord
from backend.transformer import BaseTransformer


class ISCSevenRiversSiteTransformer(BaseTransformer):
    _cached_polygon = None

    def transform(self, record, config):
        lat = record['latitude']
        lng = record['longitude']

        if config.bbox:
            if not self._cached_polygon:
                poly = shapely.wkt.loads(config.bounding_wkt())
                self._cached_polygon = poly
            else:
                poly = self._cached_polygon

            pt = Point(lng, lat)
            if poly.contains(pt):
                return

        rec = {
            "source": "ISCSevenRivers",
            "id": record["id"],
            "name": record["name"],
            "latitude": lat,
            "longitude": lng,
            "elevation_feet": record["groundSurfaceElevationFeet"],
        }

        return SiteRecord(rec)

# ============= EOF =============================================

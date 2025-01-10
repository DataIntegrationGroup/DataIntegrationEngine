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
from backend.transformer import (
    BaseTransformer,
    WaterLevelTransformer,
    SiteTransformer,
    AnalyteTransformer,
)


class ISCSevenRiversSiteTransformer(SiteTransformer):
    def _transform(self, record):
        lat = record["latitude"]
        lng = record["longitude"]

        # if not self.contained(lng, lat):
        #     return

        rec = {
            "source": "ISCSevenRivers",
            "id": record["id"],
            "name": record["name"],
            "latitude": lat,
            "longitude": lng,
            "elevation": record["groundSurfaceElevationFeet"],
            "elevation_units": "ft",
        }

        return rec


class ISCSevenRiversAnalyteTransformer(AnalyteTransformer):
    source_tag = "ISCSevenRivers"


class ISCSevenRiversWaterLevelTransformer(WaterLevelTransformer):
    source_tag = "ISCSevenRivers"

    # def _transform_hook(self, record, config, site_record):
    #     rec = {
    #         "source": "ISCSevenRivers",
    #         "id": site_record.id,
    #         "location": site_record.name,
    #         "latitude": site_record.latitude,
    #         "longitude": site_record.longitude,
    #         "elevation": site_record.elevation,
    #         "elevation_units": "ft",
    #     }
    #     if config.output_summary_waterlevel_stats:
    #         rec.update(record)
    #     else:
    #         rec["date_measured"] = record["dateTime"]
    #         rec["depth_to_water_ft_below_ground_surface"] = record["depthToWaterFeet"]
    #
    #     return rec


# ============= EOF =============================================

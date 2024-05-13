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
from backend.transformer import BaseTransformer, WaterLevelTransformer


class ISCSevenRiversSiteTransformer(BaseTransformer):
    def transform(self, record, config):
        lat = record["latitude"]
        lng = record["longitude"]

        if not self.contained(lng, lat, config):
            return

        rec = {
            "source": "ISCSevenRivers",
            "id": record["id"],
            "name": record["name"],
            "latitude": lat,
            "longitude": lng,
            "elevation": record["groundSurfaceElevationFeet"],
            "elevation_unit": "ft",
        }

        return SiteRecord(rec)


class ISCSevenRiversWaterLevelTransformer(WaterLevelTransformer):
    def transform(self, record, parent_record, config):
        rec = {
            "source": "ISCSevenRivers",
            "id": parent_record.id,
            "location": parent_record.name,
            "latitude": parent_record.latitude,
            "longitude": parent_record.longitude,
            "surface_elevation_ft": parent_record.elevation,
        }

        if config.output_summary_waterlevel_stats:
            rec["nrecords"] = record["nrecords"]
            rec["min"] = record["min"]
            rec["max"] = record["max"]
            rec["mean"] = record["mean"]
            rec["date_measured"] = record["most_recent_date"]
        else:
            rec["date_measured"] = record["DateMeasured"]
            rec["time_measured"] = record["TimeMeasured"]
            rec["depth_to_water_ft_below_ground_surface"] = record["DepthToWaterBGS"]

        klass = self._get_record_klass(config)
        return klass(rec)
# ============= EOF =============================================

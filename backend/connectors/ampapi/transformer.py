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
from backend.record import SiteRecord, WaterLevelRecord
from backend.transformer import BaseTransformer, WaterLevelTransformer


class AMPAPISiteTransformer(BaseTransformer):
    def transform(self, record, config):
        props = record["properties"]
        print(props)
        rec = {
            "source": "AMPAPI",
            "id": props["point_id"],
            "name": props["point_id"],
            "latitude": record["geometry"]["coordinates"][1],
            "longitude": record["geometry"]["coordinates"][0],
            "elevation": record["geometry"]["coordinates"][2],
            "elevation_unit": "m",
            "horizontal_datum": props["lonlat_datum"],
            "vertical_datum": props["altitude_datum"],
            "usgs_site_id": props["site_id"],
            "alternate_site_id": props["alternate_site_id"],
            "formation": props["formation"],
            "well_depth": props["well_depth"]["value"],
            "well_depth_unit": props["well_depth"]["units"],
        }
        return SiteRecord(rec)


class AMPAPIWaterLevelTransformer(WaterLevelTransformer):
    def transform(self, record, parent_record, config):
        rec = {
            "source": "AMPAPI",
            "id": parent_record.id,
            "location": parent_record.name,
            "usgs_site_id": parent_record.usgs_site_id,
            "alternate_site_id": parent_record.alternate_site_id,
            "latitude": parent_record.latitude,
            "longitude": parent_record.longitude,
            "surface_elevation_ft": parent_record.elevation,
            "well_depth_ft_below_ground_surface": parent_record.well_depth_ft_below_ground_surface,
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

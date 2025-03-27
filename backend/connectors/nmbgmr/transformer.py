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
from backend.constants import DTW
from backend.record import SiteRecord, WaterLevelRecord
from backend.transformer import (
    BaseTransformer,
    WaterLevelTransformer,
    SiteTransformer,
    AnalyteTransformer,
)


class NMBGMRSiteTransformer(SiteTransformer):
    def _transform(self, record):
        props = record["properties"]
        rec = {
            "source": "NMBGMR",
            "id": props["point_id"],
            "name": props["point_id"],
            "latitude": record["geometry"]["coordinates"][1],
            "longitude": record["geometry"]["coordinates"][0],
            "elevation": record["geometry"]["coordinates"][2],
            "elevation_units": "m",
            "horizontal_datum": props["lonlat_datum"],
            "vertical_datum": props["altitude_datum"],
            "usgs_site_id": props["site_id"],
            "alternate_site_id": props["alternate_site_id"],
            "formation": props.get("formation", ""),
            "well_depth": props.get("well_depth", ""),
            "well_depth_units": props.get("well_depth_units", ""),
        }
        return rec


class NMBGMRAnalyteTransformer(AnalyteTransformer):
    source_tag = "NMBGMR"


class NMBGMRWaterLevelTransformer(WaterLevelTransformer):
    source_tag = "NMBGMR"
    # def _transform_hook(self, record, config, site_record):
    #     rec = {
    #         "source": "NMBGMR",
    #         "id": site_record.id,
    #         "location": site_record.name,
    #         "usgs_site_id": site_record.usgs_site_id,
    #         "alternate_site_id": site_record.alternate_site_id,
    #         "latitude": site_record.latitude,
    #         "longitude": site_record.longitude,
    #         "well_depth": site_record.well_depth,
    #         "well_depth_units": site_record.well_depth_units,
    #         "elevation": site_record.elevation,
    #         "elevation_units": site_record.elevation_units,
    #     }
    #
    #     if config.output_summary_waterlevel_stats:
    #         rec.update(record)
    #     else:
    #         rec["date_measured"] = record["DateMeasured"]
    #         rec["time_measured"] = record["TimeMeasured"]
    #         rec["depth_to_water_ft_below_ground_surface"] = record["DepthToWaterBGS"]
    #
    #     return rec


# ============= EOF =============================================

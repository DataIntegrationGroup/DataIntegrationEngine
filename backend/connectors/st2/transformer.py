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
import pprint

from backend.connectors.st_connector import STSiteTransformer
from backend.record import SiteRecord, WaterLevelRecord
from backend.transformer import BaseTransformer, WaterLevelTransformer, SiteTransformer


class PVACDSiteTransformer(STSiteTransformer):
    source_id = "ST2/PVACD"

    def _transform_hook(self, rec):
        # if rec["id"] in [9402, 9403, 9404, 9405, 9406, 9408, 9409, 9410, 9411, 9417]:
        if rec["id"] in [
            9640,
            9641,
            9642,
            9643,
            9644,
            9645,
            9646,
            9647,
            9648,
            9649,
            9650,
        ]:
            return rec


class BernCoSiteTransformer(STSiteTransformer):
    source_id = "ST2/BernCo"

    def _transform_hook(self, rec):
        if rec["id"] not in [
            9652,
        ]:
            return rec


class EBIDSiteTransformer(STSiteTransformer):
    source_tag = "ST2/EBID"


# class ST2WaterLevelTransformer(WaterLevelTransformer):
#     source_tag = "ST2"

# def _transform_hook(self, record, config, site_record, *args, **kw):
#     rec = {
#         "source": self.source_id,
#         "id": site_record.id,
#         "location": site_record.name,
#         "latitude": site_record.latitude,
#         "longitude": site_record.longitude,
#         "surface_elevation_ft": site_record.elevation,
#         "well_depth_ft_below_ground_surface": site_record.well_depth,
#     }
#
#     if config.output_summary_waterlevel_stats:
#         rec.update(record)
#     else:
#         dt = record["observation"].phenomenon_time
#         dtw = record["observation"].result
#         rec["depth_to_water_ft_below_ground_surface"] = dtw
#         rec["datetime_measured"] = dt
#
#     return rec


class PVACDWaterLevelTransformer(WaterLevelTransformer):
    source_tag = "ST2/PVACD"


class EBIDWaterLevelTransformer(WaterLevelTransformer):
    source_tag = "ST2/EBID"


class BernCoWaterLevelTransformer(WaterLevelTransformer):
    source_tag = "ST2/BernCo"


# ============= EOF =============================================

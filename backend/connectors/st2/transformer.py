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

from backend.record import SiteRecord, WaterLevelRecord
from backend.transformer import BaseTransformer


class ST2SiteTransformer(BaseTransformer):
    source_id = "ST2"

    def transform(self, record, config):
        lat = record.location["coordinates"][1]
        lng = record.location["coordinates"][0]
        if not self.contained(lng, lat, config):
            return

        rec = {
            "source": self.source_id,
            "id": record.id,
            "name": record.name,
            "latitude": lat,
            "longitude": lng,
            "horizontal_datum": "WGS84",
        }
        #     # "elevation": record['VerticalMeasure/MeasureValue'],
        #     # "elevation_unit": record['VerticalMeasure/MeasureUnitCode'],
        #     # "horizontal_datum": record["HorizontalCoordinateReferenceSystemDatumName"],
        #     # "vertical_datum": record["VerticalCoordinateReferenceSystemDatumName"],
        #     # 'aquifer': record['AquiferName'],
        #     # 'well_depth': record["WellDepthMeasure/MeasureValue"],
        #     # 'well_depth_unit': record["WellDepthMeasure/MeasureUnitCode"],
        # }
        # rec=  {}
        rec = self._transform_hook(rec)
        if rec:
            return SiteRecord(rec)

    def _transform_hook(self, rec):
        return rec


class PVACDSiteTransformer(ST2SiteTransformer):
    source_id = "ST2/PVACD"

    def _transform_hook(self, rec):
        if rec["id"] in [9402, 9403, 9404, 9405, 9406, 9408, 9409, 9410, 9411, 9417]:
            return rec


class EBIDSiteTransformer(ST2SiteTransformer):
    source_id = "ST2/EBID"


class ST2WaterLevelTransformer(BaseTransformer):
    source_id = "ST2"

    def transform(self, record, parent_record, config, *args, **kw):
        dstr, tstr = self._standardize_datetime(record["observation"].phenomenon_time)

        rec = {
            "source": self.source_id,
            "id": parent_record.id,
            "location": parent_record.name,
            "surface_elevation_ft": parent_record.elevation,
            "well_depth_ft_below_ground_surface": parent_record.well_depth,
            "depth_to_water_ft_below_ground_surface": record["observation"].result,
            "date_measured": dstr,
            "time_measured": tstr,
        }

        return WaterLevelRecord(rec)


class PVACDWaterLevelTransformer(ST2WaterLevelTransformer):
    source_id = "ST2/PVACD"


class EBIDWaterLevelTransformer(ST2WaterLevelTransformer):
    source_id = "ST2/EBID"


# ============= EOF =============================================

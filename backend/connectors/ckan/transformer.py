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
from backend.transformer import BaseTransformer, WaterLevelTransformer, SiteTransformer


class OSERoswellSiteTransformer(SiteTransformer):
    def _transform(self, record):
        # pprint.pprint(record)
        lat = float(record["DD_lat"])
        lng = float(record["DD_lon"])
        # if not self.contained(lng, lat):
        #     return

        rec = {
            "source": f"CKAN/OSERoswell",
            "id": record["Site_ID"],
            "name": record["Location"],
            "latitude": lat,
            "longitude": lng,
            "horizontal_datum": "WGS84",
            # "elevation": record['VerticalMeasure/MeasureValue'],
            # "elevation_unit": record['VerticalMeasure/MeasureUnitCode'],
            # "horizontal_datum": record["HorizontalCoordinateReferenceSystemDatumName"],
            # "vertical_datum": record["VerticalCoordinateReferenceSystemDatumName"],
            # 'aquifer': record['AquiferName'],
            # 'well_depth': record["WellDepthMeasure/MeasureValue"],
            # 'well_depth_unit': record["WellDepthMeasure/MeasureUnitCode"],
        }
        return rec


class OSERoswellWaterLevelTransformer(WaterLevelTransformer):
    source_tag = "CKAN/OSERoswell"

    # def _transform_hook(self, record, config, site_record):
    #     rec = {
    #         "id": site_record.id,
    #         "source": "CKAN/OSERoswell",
    #         "location": site_record.name,
    #         "usgs_site_id": site_record.id,
    #         "latitude": site_record.latitude,
    #         "longitude": site_record.longitude,
    #         "elevation": site_record.elevation,
    #         "elevation_units": "ft",
    #         "well_depth": site_record.well_depth,
    #         "well_depth_units": "ft",
    #     }
    #     if config.output_summary_waterlevel_stats:
    #         rec.update(record)
    #
    #     return rec


# ============= EOF =============================================

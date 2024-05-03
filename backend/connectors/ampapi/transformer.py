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
from datetime import datetime

from backend.record import SiteRecord, WaterLevelRecord
from backend.transformer import BaseTransformer


class AMPAPISiteTransformer(BaseTransformer):
    def transform(self, record, config):
        props = record["properties"]
        rec = {
            "source": "AMPAPI",
            "id": props["point_id"],
            "name": props["point_id"],
            "latitude": record["geometry"]["coordinates"][1],
            "longitude": record["geometry"]["coordinates"][0],
            "elevation": record["geometry"]["coordinates"][2] * 3.28084,
            "horizontal_datum": props["lonlat_datum"],
            "vertical_datum": props["altitude_datum"],
            "usgs_site_id": props["site_id"],
            "alternate_site_id": props["alternate_site_id"],
            "formation": props["formation"],
        }
        return SiteRecord(rec)


class AMPAPIWaterLevelTransformer(BaseTransformer):
    def transform(self, record, parent_record, config):
        dt = record['DateMeasured']
        tt = record['TimeMeasured']

        # ts = datetime.strptime(f'{dt} {tt}', '%Y-%m-%d %H:%M:%S')
        rec = {
            'source': 'AMPAPI',
            'id': parent_record.id,
            'depth_to_water_below_ground_surface_ft': record['DepthToWaterBGS'],
            'date_measured': dt,
            'time_measured': tt
        }
        return WaterLevelRecord(rec)
# ============= EOF =============================================

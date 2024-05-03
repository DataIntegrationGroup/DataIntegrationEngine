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


class USGSSiteTransformer(BaseTransformer):
    def transform(self, record, config):
        elevation = record["alt_va"]
        try:
            elevation = float(elevation)
        except (ValueError, TypeError):
            elevation = None

        lng = float(record["dec_long_va"])
        lat = float(record["dec_lat_va"])
        datum = record["coord_datum_cd"]

        rec = {
            "source": "USGS-NWIS",
            "id": record["site_no"],
            "name": record["station_nm"],
            "latitude": lat,
            "longitude": lng,
            "elevation": elevation,
            "horizontal_datum": datum,
            "vertical_datum": record["alt_datum_cd"],
            "aquifer": record["nat_aqfr_cd"],
        }
        return SiteRecord(rec)


# ============= EOF =============================================

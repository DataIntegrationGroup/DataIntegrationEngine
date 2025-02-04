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
import json

from backend.record import SiteRecord, WaterLevelRecord, AnalyteSummaryRecord
from backend.transformer import (
    BaseTransformer,
    WaterLevelTransformer,
    SiteTransformer,
    AnalyteTransformer,
)

WELL_DEPTHS = {
    3243: 1340,  # well 1
    3244: 190,  # well 2
    3245: 220,  # well 3
    3246: 185,  # well 4
}


class BORSiteTransformer(SiteTransformer):
    def _transform(self, record):
        props = record["attributes"]

        elevation = props["elevation"]
        try:
            elevation = float(elevation)
        except (ValueError, TypeError):
            elevation = None

        lng = float(props["locationCoordinates"]["coordinates"][0])
        lat = float(props["locationCoordinates"]["coordinates"][1])

        rec = {
            "source": "BOR-RISE",
            "id": props["_id"],
            "name": props["locationName"],
            "latitude": lat,
            "longitude": lng,
            "elevation": elevation,
            "elevation_units": "ft",
            "horizontal_datum": props["horizontalDatum"]["_id"],
            "vertical_datum": props["verticalDatum"]["_id"],
            "well_depth": WELL_DEPTHS.get(props["_id"]),
            "well_depth_units": "ft",
            "catalogRecords": record["relationships"]["catalogRecords"]["data"],
        }
        return rec


class BORAnalyteTransformer(AnalyteTransformer):
    source_tag = "BOR-RISE"


# ============= EOF =============================================

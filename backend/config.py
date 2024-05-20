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
import time
from datetime import datetime, timedelta

import shapely.wkt

from backend.bounding_polygons import get_county_polygon
from backend.connectors.ampapi.source import (
    AMPAPISiteSource,
    AMPAPIWaterLevelSource,
    AMPAPIAnalyteSource,
)
from backend.connectors.bor.source import BORSiteSource, BORAnalyteSource
from backend.connectors.ckan import (
    HONDO_RESOURCE_ID,
    FORT_SUMNER_RESOURCE_ID,
    ROSWELL_RESOURCE_ID,
)
from backend.connectors.ckan.source import (
    OSERoswellSiteSource,
    OSERoswellWaterLevelSource,
)
from backend.constants import MILLIGRAMS_PER_LITER, WGS84, FEET
from backend.connectors.isc_seven_rivers.source import (
    ISCSevenRiversSiteSource,
    ISCSevenRiversWaterLevelSource,
    ISCSevenRiversAnalyteSource,
)
from backend.connectors.st2.source import (
    ST2SiteSource,
    PVACDSiteSource,
    EBIDSiteSource,
    PVACDWaterLevelSource,
)
from backend.connectors.usgs.source import USGSSiteSource, USGSWaterLevelSource
from backend.connectors.wqp.source import WQPSiteSource, WQPAnalyteSource


class Config:
    # spatial
    bbox = None  # dict or str
    county = None
    wkt = None

    # sources
    use_source_ampapi = True
    use_source_wqp = True
    use_source_isc_seven_rivers = True
    use_source_nwis = True
    use_source_ose_roswell = True
    use_source_st2 = True
    use_source_bor = True

    analyte = None

    # output
    output_path = "output"
    output_horizontal_datum = WGS84
    output_elevation_units = FEET
    output_well_depth_units = FEET
    output_summary = False
    # output_summary_waterlevel_stats = False
    # output_summary_analyte_stats = False
    latest_water_level_only = False

    analyte_output_units = MILLIGRAMS_PER_LITER
    waterlevel_output_units = FEET

    use_csv = True
    use_geojson = False

    def __init__(self, model=None):
        if model:
            if model.wkt:
                self.wkt = model.wkt
            else:
                self.county = model.county
                if not self.county:
                    self.bbox = model.bbox.model_dump()

    def analyte_sources(self):
        sources = []

        # if self.use_source_wqp:
        # sources.append((WQPSiteSource, WQPAnalyteSource))
        if self.use_source_bor:
            sources.append((BORSiteSource(), BORAnalyteSource()))
        if self.use_source_wqp:
            sources.append((WQPSiteSource(), WQPAnalyteSource()))
        if self.use_source_isc_seven_rivers:
            sources.append((ISCSevenRiversSiteSource(), ISCSevenRiversAnalyteSource()))
        if self.use_source_ampapi:
            sources.append((AMPAPISiteSource(), AMPAPIAnalyteSource()))

        for s, ss in sources:
            s.set_config(self)
            ss.set_config(self)

            # s.config = self
            # ss.config = self

        return sources

    def water_level_sources(self):
        sources = []
        if self.use_source_ampapi:
            sources.append((AMPAPISiteSource(), AMPAPIWaterLevelSource()))

        if self.use_source_isc_seven_rivers:
            sources.append(
                (ISCSevenRiversSiteSource(), ISCSevenRiversWaterLevelSource())
            )

        if self.use_source_nwis:
            sources.append((USGSSiteSource(), USGSWaterLevelSource()))

        if self.use_source_ose_roswell:
            sources.append(
                (
                    OSERoswellSiteSource(HONDO_RESOURCE_ID),
                    OSERoswellWaterLevelSource(HONDO_RESOURCE_ID),
                )
            )
            sources.append(
                (
                    OSERoswellSiteSource(FORT_SUMNER_RESOURCE_ID),
                    OSERoswellWaterLevelSource(FORT_SUMNER_RESOURCE_ID),
                )
            )
            sources.append(
                (
                    OSERoswellSiteSource(ROSWELL_RESOURCE_ID),
                    OSERoswellWaterLevelSource(ROSWELL_RESOURCE_ID),
                )
            )
        if self.use_source_st2:
            sources.append((PVACDSiteSource(), PVACDWaterLevelSource()))
            # sources.append((EBIDSiteSource, EBIDWaterLevelSource))

        # if self.use_source_bor:
        #     sources.append((BORSiteSource(), BORWaterLevelSource()))

        for s, ss in sources:
            s.set_config(self)
            ss.set_config(self)

        return sources

    def site_sources(self):
        sources = []
        if self.use_source_ampapi:
            sources.append(AMPAPISiteSource)
        if self.use_source_isc_seven_rivers:
            sources.append(ISCSevenRiversSiteSource)
        if self.use_source_ose_roswell:
            sources.append(OSERoswellSiteSource)
        if self.use_source_nwis:
            sources.append(USGSSiteSource)
        if self.use_source_st2:
            sources.append(PVACDSiteSource)
            sources.append(EBIDSiteSource)
        if self.use_source_bor:
            sources.append(BORSiteSource)
        return sources

    def bbox_bounding_points(self):
        if isinstance(self.bbox, str):
            p1, p2 = self.bbox.split(",")
            x1, y1 = [float(a) for a in p1.strip().split(" ")]
            x2, y2 = [float(a) for a in p2.strip().split(" ")]
        else:
            shp = None
            if self.county:
                shp = get_county_polygon(self.county, as_wkt=False)
            elif self.wkt:
                shp = shapely.wkt.loads(self.wkt)

            if shp:
                x1, y1, x2, y2 = shp.bounds
            else:
                x1 = self.bbox["minLng"]
                x2 = self.bbox["maxLng"]
                y1 = self.bbox["minLat"]
                y2 = self.bbox["maxLat"]

        if x1 > x2:
            x1, x2 = x2, x1
        if y1 > y2:
            y1, y2 = y2, y1

        return round(x1, 7), round(y1, 7), round(x2, 7), round(y2, 7)

    def bounding_wkt(self):
        if self.wkt:
            return self.wkt
        elif self.bbox:
            x1, y1, x2, y2 = self.bbox_bounding_points()
            pts = f"{x1} {y1},{x1} {y2},{x2} {y2},{x2} {y1},{x1} {y1}"
            return f"POLYGON({pts})"
        elif self.county:
            return get_county_polygon(self.county)

    def has_bounds(self):
        return self.bbox or self.county or self.wkt

    def now_ms(self, days=0):
        td = timedelta(days=days)
        # return current time in milliseconds
        return int((datetime.now() - td).timestamp() * 1000)


# ============= EOF =============================================

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
from backend.bounding_polygons import get_county_polygon
from backend.connectors.ampapi.source import AMPAPISiteSource, AMPAPIWaterLevelSource
from backend.connectors.ckan import HONDO_RESOURCE_ID, FORT_SUMNER_RESOURCE_ID, ROSWELL_RESOURCE_ID
from backend.connectors.ckan.source import OSERoswellSiteSource, OSERoswellWaterLevelSource
from backend.connectors.isc_seven_rivers.source import ISCSevenRiversSiteSource, ISCSevenRiversWaterLevelSource
from backend.connectors.st2.source import ST2SiteSource, PVACDSiteSource, EBIDSiteSource
from backend.connectors.usgs.source import USGSSiteSource


class Config:
    bbox = None
    county = None
    output_path = "output"
    use_csv = True
    use_geojson = False

    use_source_ampapi = False
    use_source_wqp = False
    use_source_isc_seven_rivers = False
    use_source_nwis = False
    use_source_ose_roswell = False
    use_source_st2 = True

    output_horizontal_datum = "WGS84"
    output_elevation_unit = "ft"
    output_well_depth_unit = "ft"

    def water_level_sources(self):
        sources = []
        if self.use_source_ampapi:
            sources.append((AMPAPISiteSource, AMPAPIWaterLevelSource))

        if self.use_source_isc_seven_rivers:
            sources.append(
                (ISCSevenRiversSiteSource, ISCSevenRiversWaterLevelSource)
            )

        if self.use_source_nwis:
            pass

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
        return sources

    def bounding_points(self):
        p1, p2 = self.bbox.split(",")
        x1, y1 = [float(a) for a in p1.strip().split(" ")]
        x2, y2 = [float(a) for a in p2.strip().split(" ")]

        if x1 > x2:
            x1, x2 = x2, x1
        if y1 > y2:
            y1, y2 = y2, y1

        return x1, y1, x2, y2

    def bounding_wkt(self):
        if self.bbox:
            x1, y1, x2, y2 = self.bounding_points()
            pts = f'{x1} {y1},{x1} {y2},{x2} {y2},{x2} {y1},{x1} {y1}'
            return f"POLYGON({pts})"
        elif self.county:
            return get_county_polygon(self.county)

    def has_bounds(self):
        return self.bbox or self.county

# ============= EOF =============================================

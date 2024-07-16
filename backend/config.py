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
import os
import sys
import time
from datetime import datetime, timedelta

import click
import shapely.wkt

from .bounding_polygons import get_county_polygon
from .connectors.nmbgmr.source import (
    NMBGMRSiteSource,
    NMBGMRWaterLevelSource,
    NMBGMRAnalyteSource,
)
from .connectors.bor.source import BORSiteSource, BORAnalyteSource
from .connectors.ckan import (
    HONDO_RESOURCE_ID,
    FORT_SUMNER_RESOURCE_ID,
    ROSWELL_RESOURCE_ID,
)
from .connectors.ckan.source import (
    OSERoswellSiteSource,
    OSERoswellWaterLevelSource,
)
from .connectors.nmenv.source import DWBSiteSource, DWBAnalyteSource
from .constants import MILLIGRAMS_PER_LITER, WGS84, FEET
from .connectors.isc_seven_rivers.source import (
    ISCSevenRiversSiteSource,
    ISCSevenRiversWaterLevelSource,
    ISCSevenRiversAnalyteSource,
)
from .connectors.st2.source import (
    ST2SiteSource,
    PVACDSiteSource,
    EBIDSiteSource,
    PVACDWaterLevelSource,
    BernCoSiteSource,
    BernCoWaterLevelSource,
)
from .connectors.usgs.source import NWISSiteSource, NWISWaterLevelSource
from .connectors.wqp.source import WQPSiteSource, WQPAnalyteSource

SOURCE_KEYS = (
    "nmbgmr",
    "wqp",
    "iscsevenrivers",
    "nwis",
    "oseroswell",
    "pvacd",
    "bor",
    "dwb",
    "bernco",
)


def get_source(source):
    if source == "nmbgmr":
        return NMBGMRSiteSource()
    elif source == "wqp":
        return WQPSiteSource()
    elif source == "iscsevenrivers":
        return ISCSevenRiversSiteSource()
    elif source == "nwis":
        return NWISSiteSource()
    elif source == "oseroswell":
        return OSERoswellSiteSource(HONDO_RESOURCE_ID)
    elif source == "pvacd":
        return PVACDSiteSource()
    elif source == "bor":
        return BORSiteSource()
    elif source == "dwb":
        return DWBSiteSource()
    elif source == "bernco":
        return BernCoSiteSource()

    return None


class Config(object):
    site_limit: int = 0
    dry: bool = False

    # date
    start_date: str = ""
    end_date: str = ""

    # spatial
    bbox: dict  # dict or str
    county: str = ""
    wkt: str = ""

    # sources
    use_source_nmbgmr: bool = True
    use_source_wqp: bool = True
    use_source_iscsevenrivers: bool = True
    use_source_nwis: bool = True
    use_source_oseroswell: bool = True
    use_source_pvacd: bool = True
    use_source_bor: bool = True
    use_source_dwb: bool = True
    use_source_bernco: bool = True

    analyte: str = ""

    # output
    use_cloud_storage: bool = False
    output_dir: str = ""
    output_name: str = "output"
    output_horizontal_datum: str = WGS84
    output_elevation_units: str = FEET
    output_well_depth_units: str = FEET
    output_summary: bool = False

    latest_water_level_only: bool = False

    analyte_output_units: str = MILLIGRAMS_PER_LITER
    waterlevel_output_units: str = FEET

    use_csv: bool = True
    use_geojson: bool = False

    def __init__(self, model=None, payload=None):
        self.bbox = {}
        if model:
            if model.wkt:
                self.wkt = model.wkt
            else:
                self.county = model.county
                if not self.county:
                    if model.bbox:
                        self.bbox = model.bbox.model_dump()

            if model.sources:
                for s in SOURCE_KEYS:
                    setattr(self, f"use_source_{s}", s in model.sources)
        elif payload:
            self.wkt = payload.get("wkt", "")
            self.county = payload.get("county", "")
            self.output_summary = payload.get("output_summary", False)
            self.output_name = payload.get("output_name", "output")
            self.start_date = payload.get("start_date", "")
            self.end_date = payload.get("end_date", "")
            self.analyte = payload.get("analyte", "")

            for s in SOURCE_KEYS:
                setattr(self, f"use_source_{s}", s in payload.get("sources", []))

    def analyte_sources(self):
        sources = []

        # if self.use_source_wqp:
        # sources.append((WQPSiteSource, WQPAnalyteSource))
        if self.use_source_bor:
            sources.append((BORSiteSource(), BORAnalyteSource()))
        if self.use_source_wqp:
            sources.append((WQPSiteSource(), WQPAnalyteSource()))
        if self.use_source_iscsevenrivers:
            sources.append((ISCSevenRiversSiteSource(), ISCSevenRiversAnalyteSource()))
        if self.use_source_nmbgmr:
            sources.append((NMBGMRSiteSource(), NMBGMRAnalyteSource()))
        if self.use_source_dwb:
            sources.append((DWBSiteSource(), DWBAnalyteSource()))

        for s, ss in sources:
            s.set_config(self)
            ss.set_config(self)

            # s.config = self
            # ss.config = self

        return sources

    def water_level_sources(self):
        sources = []
        if self.use_source_nmbgmr:
            sources.append((NMBGMRSiteSource(), NMBGMRWaterLevelSource()))

        if self.use_source_iscsevenrivers:
            sources.append(
                (ISCSevenRiversSiteSource(), ISCSevenRiversWaterLevelSource())
            )

        if self.use_source_nwis:
            sources.append((NWISSiteSource(), NWISWaterLevelSource()))

        if self.use_source_oseroswell:
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
        if self.use_source_pvacd:
            sources.append((PVACDSiteSource(), PVACDWaterLevelSource()))
            # sources.append((EBIDSiteSource, EBIDWaterLevelSource))
        if self.use_source_bernco:
            sources.append((BernCoSiteSource(), BernCoWaterLevelSource()))
        # if self.use_source_bor:
        #     sources.append((BORSiteSource(), BORWaterLevelSource()))

        for s, ss in sources:
            s.set_config(self)
            ss.set_config(self)

        return sources

    # def site_sources(self):
    #     sources = [
    #         NMBGMRSiteSource(),
    #         WQPSiteSource(),
    #         ISCSevenRiversSiteSource(),
    #         NWISSiteSource(),
    #         DWBSiteSource(),
    #         BORSiteSource(),
    #         PVACDSiteSource(),
    #         EBIDSiteSource(),
    #         OSERoswellSiteSource(HONDO_RESOURCE_ID),
    #         OSERoswellSiteSource(FORT_SUMNER_RESOURCE_ID),
    #         OSERoswellSiteSource(ROSWELL_RESOURCE_ID),
    #     ]
    #
    #     # if self.use_source_nmbgmr:
    #     #     sources.append(NMBGMRSiteSource)
    #     # if self.use_source_isc_seven_rivers:
    #     #     sources.append(ISCSevenRiversSiteSource)
    #     # if self.use_source_ose_roswell:
    #     #     sources.append(OSERoswellSiteSource)
    #     # if self.use_source_nwis:
    #     #     sources.append(USGSSiteSource)
    #     # if self.use_source_st2:
    #     #     sources.append(PVACDSiteSource)
    #     #     sources.append(EBIDSiteSource)
    #     # if self.use_source_bor:
    #     #     sources.append(BORSiteSource)
    #     return sources

    def bbox_bounding_points(self, bbox=None):
        if bbox is None:
            bbox = self.bbox

        if isinstance(bbox, str):
            p1, p2 = bbox.split(",")
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
                x1 = bbox["minLng"]
                x2 = bbox["maxLng"]
                y1 = bbox["minLat"]
                y2 = bbox["maxLat"]

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
            return f"POLYGON(({pts}))"
        elif self.county:
            return get_county_polygon(self.county)

    def has_bounds(self):
        return self.bbox or self.county or self.wkt

    def now_ms(self, days=0):
        td = timedelta(days=days)
        # return current time in milliseconds
        return int((datetime.now() - td).timestamp() * 1000)

    def report(self):
        def _report_attributes(title, attrs):
            click.secho(
                f"---- {title} --------------------------------------------------",
                fg="yellow",
            )
            for k in attrs:
                v = getattr(self, k)
                click.secho(f"{k}: {v}", fg="yellow")
            click.secho("", fg="yellow")

        click.secho(
            "---- Begin configuration -------------------------------------\n",
            fg="yellow",
        )
        sources = [f"use_source_{s}" for s in SOURCE_KEYS]
        attrs = [
            "start_date",
            "end_date",
            "county",
            "bbox",
            "wkt",
            "analyte",
            "site_limit",
        ] + sources
        # inputs
        _report_attributes(
            "Inputs",
            attrs,
        )

        # outputs
        _report_attributes(
            "Outputs",
            (
                "output_dir",
                "output_name",
                "output_summary",
                "output_horizontal_datum",
                "output_elevation_units",
            ),
        )

        click.secho(
            "---- End configuration -------------------------------------", fg="yellow"
        )

    def validate(self):
        if not self._validate_bbox():
            click.secho("Invalid bounding box", fg="red")
            sys.exit(2)

        if not self._validate_county():
            click.secho("Invalid county", fg="red")
            sys.exit(2)

        if not self._validate_date(self.start_date):
            click.secho(f"Invalid start date {self.start_date}", fg="red")
            sys.exit(2)

        if not self._validate_date(self.end_date):
            click.secho("Invalid end date", fg="red")
            sys.exit(2)

    def _extract_date(self, d):
        if d:
            for fmt in (
                "%Y",
                "%Y-%m",
                "%Y-%m-%d",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%fZ",
            ):
                try:
                    return datetime.strptime(d, fmt)
                except ValueError:
                    pass

    def _validate_date(self, d):
        if d:
            return bool(self._extract_date(d))
        return True

    def _validate_bbox(self):
        try:
            if self.bbox:
                self.bbox_bounding_points()
            return True
        except ValueError:
            return False

    def _validate_county(self):
        if self.county:
            return bool(get_county_polygon(self.county))

        return True

    @property
    def start_dt(self):
        return self._extract_date(self.start_date)

    @property
    def end_dt(self):
        return self._extract_date(self.end_date)

    @property
    def output_path(self):
        return os.path.join(self.output_dir, f"{self.output_name}")


# ============= EOF =============================================

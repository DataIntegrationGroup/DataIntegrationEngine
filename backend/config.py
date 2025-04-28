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
from datetime import datetime, timedelta
from enum import Enum
import shapely.wkt
import yaml

from . import OutputFormat
from .bounding_polygons import get_county_polygon
from .connectors.nmbgmr.source import (
    NMBGMRSiteSource,
    NMBGMRWaterLevelSource,
    NMBGMRAnalyteSource,
)
from .connectors.bor.source import BORSiteSource, BORAnalyteSource
from .connectors.nmenv.source import DWBSiteSource, DWBAnalyteSource
from .connectors.nmose.source import NMOSEPODSiteSource
from .constants import (
    MILLIGRAMS_PER_LITER,
    WGS84,
    FEET,
    WATERLEVELS,
    ARSENIC,
    BICARBONATE,
    CALCIUM,
    CARBONATE,
    CHLORIDE,
    FLUORIDE,
    MAGNESIUM,
    NITRATE,
    PH,
    POTASSIUM,
    SILICA,
    SODIUM,
    SULFATE,
    TDS,
    URANIUM,
)
from .connectors.isc_seven_rivers.source import (
    ISCSevenRiversSiteSource,
    ISCSevenRiversWaterLevelSource,
    ISCSevenRiversAnalyteSource,
)
from .connectors.st2.source import (
    PVACDSiteSource,
    PVACDWaterLevelSource,
    EBIDSiteSource,
    EBIDWaterLevelSource,
    BernCoSiteSource,
    BernCoWaterLevelSource,
    CABQSiteSource,
    CABQWaterLevelSource,
    NMOSERoswellSiteSource,
    NMOSERoswellWaterLevelSource,
)
from .connectors.usgs.source import NWISSiteSource, NWISWaterLevelSource
from .connectors.wqp.source import WQPSiteSource, WQPAnalyteSource, WQPWaterLevelSource
from backend.logger import Loggable


SOURCE_DICT = {
    "bernco": BernCoSiteSource,
    "bor": BORSiteSource,
    "cabq": CABQSiteSource,
    "ebid": EBIDSiteSource,
    "nmbgmr_amp": NMBGMRSiteSource,
    "nmed_dwb": DWBSiteSource,
    "nmose_isc_seven_rivers": ISCSevenRiversSiteSource,
    "nmose_pod": NMOSEPODSiteSource,
    "nmose_roswell": NMOSERoswellSiteSource,
    "nwis": NWISSiteSource,
    "pvacd": PVACDSiteSource,
    "wqp": WQPSiteSource,
}

SOURCE_KEYS = sorted(list(SOURCE_DICT.keys()))


def get_source(source):
    try:
        klass = SOURCE_DICT[source]
    except KeyError:
        raise ValueError(f"Unknown source {source}")

    if klass:
        return klass()


class Config(Loggable):
    site_limit: int = 0
    dry: bool = False

    # date
    start_date: str = ""
    end_date: str = ""

    # spatial
    bbox: str = ""
    county: str = ""
    wkt: str = ""

    sites_only = False

    # sources
    use_source_bernco: bool = True
    use_source_bor: bool = True
    use_source_cabq: bool = True
    use_source_ebid: bool = True
    use_source_nmbgmr_amp: bool = True
    use_source_nmed_dwb: bool = True
    use_source_nmose_isc_seven_rivers: bool = True
    use_source_nmose_pod: bool = True
    use_source_nmose_roswell: bool = True
    use_source_nwis: bool = True
    use_source_pvacd: bool = True
    use_source_wqp: bool = True

    # parameter
    parameter: str = ""

    # output
    use_cloud_storage: bool = False
    output_dir: str = "."
    output_name: str = "output"
    output_horizontal_datum: str = WGS84
    output_elevation_units: str = FEET
    output_well_depth_units: str = FEET
    output_summary: bool = False
    output_timeseries_unified: bool = False
    output_timeseries_separated: bool = False

    latest_water_level_only: bool = False

    analyte_output_units: str = MILLIGRAMS_PER_LITER
    waterlevel_output_units: str = FEET

    output_format: str = OutputFormat.CSV

    yes: bool = False

    def __init__(self, model=None, payload=None, path=None):
        # need to initialize logger
        super().__init__()

        if path:
            payload = self._load_from_yaml(path)

        self._payload = payload

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
            sources = payload.get("sources", [])
            if sources:
                for sk in SOURCE_KEYS:
                    value = sources.get(sk)
                    if value is not None:
                        setattr(self, f"use_source_{sk}", value)

            for attr in (
                "wkt",
                "county",
                "bbox",
                "output_summary",
                "output_timeseries_unified",
                "output_timeseries_separated",
                "start_date",
                "end_date",
                "parameter",
                "output_name",
                "dry",
                "latest_water_level_only",
                "output_format",
                "use_cloud_storage",
                "yes",
            ):
                if attr in payload:
                    setattr(self, attr, payload[attr])

    def _load_from_yaml(self, path):
        path = os.path.abspath(path)
        if os.path.exists(path):
            self.log(f"Loading config from {path}")
            with open(path, "r") as f:
                data = yaml.safe_load(f)
            return data
        else:
            self.warn(f"Config file {path} not found")

    def get_config_and_false_agencies(self):
        if self.parameter == WATERLEVELS:
            config_agencies = [
                "bernco",
                "cabq",
                "ebid",
                "nmbgmr_amp",
                "nmose_isc_seven_rivers",
                "nmose_roswell",
                "nwis",
                "pvacd",
                "wqp",
            ]
            false_agencies = ["bor", "nmose_pod", "nmed_dwb"]
        elif self.parameter == CARBONATE:
            config_agencies = ["nmbgmr_amp", "wqp"]
            false_agencies = [
                "bor",
                "bernco",
                "cabq",
                "ebid",
                "nmed_dwb",
                "nmose_isc_seven_rivers",
                "nmose_pod",
                "nmose_roswell",
                "nwis",
                "pvacd",
            ]
        elif self.parameter in [ARSENIC, URANIUM]:
            config_agencies = ["bor", "nmbgmr_amp", "nmed_dwb", "wqp"]
            false_agencies = [
                "bernco",
                "cabq",
                "ebid",
                "nmose_isc_seven_rivers",
                "nmose_roswell",
                "nmose_pod",
                "nwis",
                "pvacd",
            ]
        elif self.parameter in [
            BICARBONATE,
            CALCIUM,
            CHLORIDE,
            FLUORIDE,
            MAGNESIUM,
            NITRATE,
            PH,
            POTASSIUM,
            SILICA,
            SODIUM,
            SULFATE,
            TDS,
        ]:
            config_agencies = [
                "bor",
                "nmbgmr_amp",
                "nmed_dwb",
                "nmose_isc_seven_rivers",
                "wqp",
            ]
            false_agencies = [
                "bernco",
                "cabq",
                "ebid",
                "nmose_roswell",
                "nmose_pod",
                "nwis",
                "pvacd",
            ]
        return config_agencies, false_agencies

    def finalize(self):
        self._update_output_units()
        if self.output_format != OutputFormat.GEOSERVER:
            self.update_output_name()

        self.make_output_directory()
        self.make_output_path()

    def all_site_sources(self):
        sources = []
        for s in SOURCE_KEYS:
            if getattr(self, f"use_source_{s}"):
                source = get_source(s)
                source.set_config(self)
                sources.append((source, None))

        # pods = NMOSEPODSiteSource()
        # pods.set_config(self)
        # sources.append((pods, None))
        return sources

    def analyte_sources(self):
        sources = []

        if self.use_source_bor:
            sources.append((BORSiteSource(), BORAnalyteSource()))
        if self.use_source_wqp:
            sources.append((WQPSiteSource(), WQPAnalyteSource()))
        if self.use_source_nmose_isc_seven_rivers:
            sources.append((ISCSevenRiversSiteSource(), ISCSevenRiversAnalyteSource()))
        if self.use_source_nmbgmr_amp:
            sources.append((NMBGMRSiteSource(), NMBGMRAnalyteSource()))
        if self.use_source_nmed_dwb:
            sources.append((DWBSiteSource(), DWBAnalyteSource()))

        for s, ss in sources:
            s.set_config(self)
            ss.set_config(self)

        return sources

    def water_level_sources(self):
        sources = []
        if self.use_source_nmbgmr_amp:
            sources.append((NMBGMRSiteSource(), NMBGMRWaterLevelSource()))

        if self.use_source_nmose_isc_seven_rivers:
            sources.append(
                (ISCSevenRiversSiteSource(), ISCSevenRiversWaterLevelSource())
            )

        if self.use_source_nwis:
            sources.append((NWISSiteSource(), NWISWaterLevelSource()))

        if self.use_source_nmose_roswell:
            sources.append((NMOSERoswellSiteSource(), NMOSERoswellWaterLevelSource()))
        if self.use_source_pvacd:
            sources.append((PVACDSiteSource(), PVACDWaterLevelSource()))
        if self.use_source_bernco:
            sources.append((BernCoSiteSource(), BernCoWaterLevelSource()))
        if self.use_source_ebid:
            sources.append((EBIDSiteSource(), EBIDWaterLevelSource()))
        if self.use_source_cabq:
            sources.append((CABQSiteSource(), CABQWaterLevelSource()))
        if self.use_source_wqp:
            sources.append((WQPSiteSource(), WQPWaterLevelSource()))

        for s, ss in sources:
            s.set_config(self)
            ss.set_config(self)

        return sources

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

    def bounding_wkt(self, as_wkt=True):
        if self.wkt:
            return self.wkt
        elif self.bbox:
            x1, y1, x2, y2 = self.bbox_bounding_points()
            pts = f"{x1} {y1},{x1} {y2},{x2} {y2},{x2} {y1},{x1} {y1}"
            return f"POLYGON(({pts}))"
        elif self.county:
            return get_county_polygon(self.county, as_wkt=as_wkt)

    def has_bounds(self):
        return self.bbox or self.county or self.wkt

    def now_ms(self, days=0):
        td = timedelta(days=days)
        # return current time in milliseconds
        return int((datetime.now() - td).timestamp() * 1000)

    def report(self):
        def _report_attributes(title, attrs):
            s = f"---- {title} --------------------------------------------------"
            self.log(s)

            for k in attrs:
                v = getattr(self, k)
                s = f"{k}: {v}"
                self.log(s)

            s = ""
            self.log(s)

        s = "---- Begin configuration -------------------------------------\n"
        self.log(s)

        sources = [f"use_source_{s}" for s in SOURCE_KEYS]
        attrs = [
            "start_date",
            "end_date",
            "county",
            "bbox",
            "wkt",
            "parameter",
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
                "output_path",
                "output_summary",
                "output_timeseries_unified",
                "output_timeseries_separated",
                "output_horizontal_datum",
                "output_elevation_units",
                "use_cloud_storage",
                "output_format",
            ),
        )

        s = "---- End configuration -------------------------------------\n"
        self.log(s)

    def validate(self):
        if not self._validate_bbox():
            self.warn("Invalid bounding box")
            sys.exit(2)

        if not self._validate_county():
            self.warn("Invalid county")
            sys.exit(2)

        if not self._validate_date(self.start_date):
            self.warn(f"Invalid start date {self.start_date}")
            sys.exit(2)

        if not self._validate_date(self.end_date):
            self.warn(f"Invalid end date {self.end_date}")
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

    def make_output_directory(self):
        """
        Create the output directory if it doesn't exist.
        """
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)

    def update_output_name(self):
        """
        Generate a unique output name based on existing directories in the output directory.

        If there are no directories with the string "output" in their name, the output name will be "output".

        If there is a directory called "output", then output_name will be "output_1".

        If there are directories called "output_{n}" where n is an integer, then output_name will be "output_{m+1}"
        where m is the highest integer in the existing directories.
        """
        output_name = self.output_name

        # find if there are already directories with the string "output" their names
        output_names = [
            name
            for name in os.listdir(self.output_dir)
            if os.path.isdir(name) and output_name in name
        ]

        if len(output_names) > 0:
            max_count = 0
            # find the highest number appended to directories with "output" in their name
            counts = [
                name.split("_")[-1]
                for name in output_names
                if name.split("_")[-1].isdigit()
            ]
            counts = [int(count) for count in counts]
            if len(counts) > 0:
                max_count = max(counts)
            output_name = f"{output_name}_{max_count + 1}"

        self.output_name = output_name

    def make_output_path(self):
        if not os.path.exists(self.output_path):
            os.mkdir(self.output_path)

    def _update_output_units(self):
        parameter = self.parameter.lower()
        if parameter == "ph":
            self.analyte_output_units = ""

    @property
    def start_dt(self):
        return self._extract_date(self.start_date)

    @property
    def end_dt(self):
        return self._extract_date(self.end_date)

    @property
    def output_path(self):
        return os.path.join(self.output_dir, f"{self.output_name}")

    def get(self, attr):
        if self._payload:
            return self._payload.get(attr)


# ============= EOF =============================================

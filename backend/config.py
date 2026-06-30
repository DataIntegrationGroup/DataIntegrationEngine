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
from dataclasses import dataclass
from datetime import datetime, timedelta
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
    MICROSIEMENS_PER_CENTIMETER,
    WGS84,
    FEET,
    WATERLEVELS,
    ARSENIC,
    BICARBONATE,
    CALCIUM,
    CARBONATE,
    CHLORIDE,
    CONDUCTIVITY,
    FLUORIDE,
    MAGNESIUM,
    NITRATE,
    PH,
    POTASSIUM,
    SILICA,
    SODIUM,
    SPECIFIC_CONDUCTANCE,
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
from backend.logger import make_logger


# Which sources report each parameter (empirical availability). A plain
# parameter -> [source_key, ...] map; the waterlevels list mirrors the sources
# with a waterlevel class in the SOURCES registry (asserted by
# tests/test_source_registry.py), while the analyte lists are authored because
# they encode which analytes each agency actually reports.
PARAMETER_SOURCE_MAP = {
    WATERLEVELS: ["bernco", "cabq", "ebid", "nmbgmr_amp", "nmose_isc_seven_rivers", "nmose_roswell", "nwis", "pvacd", "wqp"],
    CARBONATE: ["nmbgmr_amp", "wqp"],
    ARSENIC: ["bor", "nmbgmr_amp", "nmed_dwb", "wqp"],
    URANIUM: ["bor", "nmbgmr_amp", "nmed_dwb", "wqp"],
    SPECIFIC_CONDUCTANCE: ["nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    CONDUCTIVITY: ["bor", "nmose_isc_seven_rivers", "wqp"],
    BICARBONATE: ["nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    CALCIUM: ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    CHLORIDE: ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    FLUORIDE: ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    MAGNESIUM: ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    NITRATE: ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    PH: ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    POTASSIUM: ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    SILICA: ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    SODIUM: ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    SULFATE: ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
    TDS: ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"],
}

@dataclass(frozen=True)
class SourceDef:
    """One data source's class wiring, declared in a single place.

    ``site`` is the site-source class (every source has one). ``waterlevel`` and
    ``analyte`` are the parameter-source classes for each group, or ``None`` when
    the source doesn't serve that group (e.g. ``bor`` is analyte-only; ``nmose_pod``
    is site-only). The ``SOURCE_DICT`` / ``*_SOURCE_PAIRS`` lookup tables below are
    derived from this, so adding a source is one ``SourceDef`` entry here (plus
    listing it under the parameters it serves in ``PARAMETER_SOURCE_MAP``)."""

    key: str
    site: type
    waterlevel: type | None = None
    analyte: type | None = None


# The single registry of sources. Order is the source-key order; it drives the
# iteration order of water_level_sources()/analyte_sources(). A consistency test
# (tests/test_source_registry.py) asserts this stays in sync with
# PARAMETER_SOURCE_MAP so a source can't be wired in one place but not the other.
SOURCES = (
    SourceDef("bernco", BernCoSiteSource, waterlevel=BernCoWaterLevelSource),
    SourceDef("bor", BORSiteSource, analyte=BORAnalyteSource),
    SourceDef("cabq", CABQSiteSource, waterlevel=CABQWaterLevelSource),
    SourceDef("ebid", EBIDSiteSource, waterlevel=EBIDWaterLevelSource),
    SourceDef(
        "nmbgmr_amp",
        NMBGMRSiteSource,
        waterlevel=NMBGMRWaterLevelSource,
        analyte=NMBGMRAnalyteSource,
    ),
    SourceDef("nmed_dwb", DWBSiteSource, analyte=DWBAnalyteSource),
    SourceDef(
        "nmose_isc_seven_rivers",
        ISCSevenRiversSiteSource,
        waterlevel=ISCSevenRiversWaterLevelSource,
        analyte=ISCSevenRiversAnalyteSource,
    ),
    SourceDef("nmose_pod", NMOSEPODSiteSource),
    SourceDef("nmose_roswell", NMOSERoswellSiteSource, waterlevel=NMOSERoswellWaterLevelSource),
    SourceDef("nwis", NWISSiteSource, waterlevel=NWISWaterLevelSource),
    SourceDef("pvacd", PVACDSiteSource, waterlevel=PVACDWaterLevelSource),
    SourceDef("wqp", WQPSiteSource, waterlevel=WQPWaterLevelSource, analyte=WQPAnalyteSource),
)

# Lookup tables derived from the registry — keep these read-only/derived; edit
# SOURCES (and PARAMETER_SOURCE_MAP) instead.
SOURCE_DICT = {s.key: s.site for s in SOURCES}
SOURCE_KEYS = sorted(SOURCE_DICT)

ANALYTE_SOURCE_PAIRS = {
    s.key: (s.site, s.analyte) for s in SOURCES if s.analyte is not None
}
WATERLEVEL_SOURCE_PAIRS = {
    s.key: (s.site, s.waterlevel) for s in SOURCES if s.waterlevel is not None
}


def get_source(source):
    try:
        klass = SOURCE_DICT[source]
    except KeyError:
        raise ValueError(f"Unknown source {source}")

    if klass:
        return klass()


class Config:
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

    output_format: str = OutputFormat.CSV.value

    yes: bool = False

    def __init__(self, model=None, payload=None, path=None):
        _l = make_logger(self.__class__.__name__)
        self.log = _l.log
        self.warn = _l.warn
        self.debug = _l.debug

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
        config_agencies = PARAMETER_SOURCE_MAP.get(self.parameter)
        if config_agencies is None:
            raise ValueError(f"Unknown parameter {self.parameter!r}. Valid parameters: {sorted(PARAMETER_SOURCE_MAP)}")
        false_agencies = [a for a in SOURCE_KEYS if a not in config_agencies]
        return config_agencies, false_agencies

    def _build_source_pair(self, site_klass, param_klass):
        s, ss = site_klass(), param_klass()
        s.set_config(self)
        ss.set_config(self)
        return s, ss

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

        return sources

    def analyte_sources(self):
        return [
            self._build_source_pair(s, ss)
            for key, (s, ss) in ANALYTE_SOURCE_PAIRS.items()
            if getattr(self, f"use_source_{key}")
        ]

    def water_level_sources(self):
        return [
            self._build_source_pair(s, ss)
            for key, (s, ss) in WATERLEVEL_SOURCE_PAIRS.items()
            if getattr(self, f"use_source_{key}")
        ]

    def source_pair(self, source_key):
        """Return the (site_source, parameter_source) pair for a single source
        key, respecting the current parameter. Returns None if the source does
        not provide the parameter."""
        table = (
            WATERLEVEL_SOURCE_PAIRS
            if self.parameter == WATERLEVELS
            else ANALYTE_SOURCE_PAIRS
        )
        entry = table.get(source_key)
        if entry is None:
            return None
        return self._build_source_pair(*entry)

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

        if not self._validate_parameter():
            self.warn(
                f"Unknown parameter {self.parameter!r}. "
                f"Valid parameters: {sorted(PARAMETER_SOURCE_MAP)}"
            )
            sys.exit(2)

        # Advisory only: these states are accepted (the code picks one) but are
        # almost always a mistake, so surface them instead of failing silently.
        self._warn_spatial_exclusivity()
        self._warn_output_mode_exclusivity()

    def _validate_parameter(self):
        # An empty parameter is valid: sites-only flows don't need one. A set
        # parameter must be one the source map knows, otherwise no source can
        # ever be resolved for it.
        if self.parameter:
            return self.parameter in PARAMETER_SOURCE_MAP
        return True

    def _warn_spatial_exclusivity(self):
        # bbox/county/wkt are resolved with inconsistent precedence across
        # bbox_bounding_points (bbox first) and bounding_wkt (wkt first), so
        # setting more than one silently does different things in different code
        # paths. Exactly one (or none, meaning statewide) is intended.
        set_filters = [n for n in ("bbox", "county", "wkt") if getattr(self, n)]
        if len(set_filters) > 1:
            self.warn(
                f"Multiple spatial filters set ({', '.join(set_filters)}); set "
                "exactly one — resolution precedence differs between code paths."
            )

    def _warn_output_mode_exclusivity(self):
        modes = [
            n
            for n in (
                "output_summary",
                "output_timeseries_unified",
                "output_timeseries_separated",
            )
            if getattr(self, n)
        ]
        if len(modes) > 1:
            self.warn(
                f"Multiple output modes set ({', '.join(modes)}); only the first "
                "is used at dump time. Set exactly one."
            )

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
        if parameter == PH:
            self.analyte_output_units = ""
        elif parameter in [CONDUCTIVITY, SPECIFIC_CONDUCTANCE]:
            self.analyte_output_units = MICROSIEMENS_PER_CENTIMETER

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

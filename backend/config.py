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
from datetime import datetime, timedelta
import shapely.wkt

from .exceptions import ConfigError
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


PARAMETER_SOURCE_MAP = {
    WATERLEVELS: {"agencies": ["bernco", "cabq", "ebid", "nmbgmr_amp", "nmose_isc_seven_rivers", "nmose_roswell", "nwis", "pvacd", "wqp"]},
    CARBONATE: {"agencies": ["nmbgmr_amp", "wqp"]},
    ARSENIC: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "wqp"]},
    URANIUM: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "wqp"]},
    SPECIFIC_CONDUCTANCE: {"agencies": ["nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    CONDUCTIVITY: {"agencies": ["bor", "nmose_isc_seven_rivers", "wqp"]},
    BICARBONATE: {"agencies": ["nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    CALCIUM: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    CHLORIDE: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    FLUORIDE: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    MAGNESIUM: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    NITRATE: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    PH: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    POTASSIUM: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    SILICA: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    SODIUM: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    SULFATE: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
    TDS: {"agencies": ["bor", "nmbgmr_amp", "nmed_dwb", "nmose_isc_seven_rivers", "wqp"]},
}

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

# Per-source (site_source, parameter_source) class pairs, keyed by source key.
# Insertion order mirrors the historical order of analyte_sources()/
# water_level_sources(). source_pair() and the *_sources() methods build from
# these so per-source unification can resolve a single source by key.
ANALYTE_SOURCE_PAIRS = {
    "bor": (BORSiteSource, BORAnalyteSource),
    "wqp": (WQPSiteSource, WQPAnalyteSource),
    "nmose_isc_seven_rivers": (ISCSevenRiversSiteSource, ISCSevenRiversAnalyteSource),
    "nmbgmr_amp": (NMBGMRSiteSource, NMBGMRAnalyteSource),
    "nmed_dwb": (DWBSiteSource, DWBAnalyteSource),
}

WATERLEVEL_SOURCE_PAIRS = {
    "nmbgmr_amp": (NMBGMRSiteSource, NMBGMRWaterLevelSource),
    "nmose_isc_seven_rivers": (ISCSevenRiversSiteSource, ISCSevenRiversWaterLevelSource),
    "nwis": (NWISSiteSource, NWISWaterLevelSource),
    "nmose_roswell": (NMOSERoswellSiteSource, NMOSERoswellWaterLevelSource),
    "pvacd": (PVACDSiteSource, PVACDWaterLevelSource),
    "bernco": (BernCoSiteSource, BernCoWaterLevelSource),
    "ebid": (EBIDSiteSource, EBIDWaterLevelSource),
    "cabq": (CABQSiteSource, CABQWaterLevelSource),
    "wqp": (WQPSiteSource, WQPWaterLevelSource),
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

    # Number of chunks fetched concurrently per source (network-bound I/O).
    # 1 = serial (legacy behavior). Higher values speed up multi-chunk sources
    # but issue more simultaneous requests, which can trip per-source API rate
    # limits (e.g. USGS) — tune down if you see 429s.
    fetch_workers: int = 4

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

    # output — transform-facing units/datum + summary-vs-timeseries mode.
    # output_summary is toggled by unify_source_both and read live by the
    # transformer to pick SummaryRecord vs ParameterRecord.
    output_horizontal_datum: str = WGS84
    output_elevation_units: str = FEET
    output_well_depth_units: str = FEET
    output_summary: bool = False

    analyte_output_units: str = MILLIGRAMS_PER_LITER
    waterlevel_output_units: str = FEET

    def __init__(self, payload=None):
        _l = make_logger(self.__class__.__name__)
        self.log = _l.log
        self.warn = _l.warn
        self.debug = _l.debug

        if payload:
            for attr in (
                "wkt",
                "county",
                "bbox",
                "output_summary",
                "start_date",
                "end_date",
                "parameter",
            ):
                if attr in payload:
                    setattr(self, attr, payload[attr])

    def _build_source_pair(self, site_klass, param_klass):
        s, ss = site_klass(), param_klass()
        s.set_config(self)
        ss.set_config(self)
        return s, ss

    def finalize(self):
        # Resolve the parameter-dependent output units (ph -> "", conductivity ->
        # uS/cm) the converter needs. The old output-name/dir setup served the
        # removed CLI file output.
        self._update_output_units()

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

        if isinstance(bbox, str) and bbox.strip():
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

    def validate(self):
        # Raise (don't sys.exit) so callers control failure: the CLI converts
        # this to exit(2); a Dagster asset catches it and soft-fails just that
        # source. sys.exit() here would kill the whole run's process.
        if not self._validate_bbox():
            raise ConfigError("Invalid bounding box")

        if not self._validate_county():
            raise ConfigError("Invalid county")

        if not self._validate_date(self.start_date):
            raise ConfigError(f"Invalid start date {self.start_date}")

        if not self._validate_date(self.end_date):
            raise ConfigError(f"Invalid end date {self.end_date}")

        if not self._validate_parameter():
            raise ConfigError(
                f"Unknown parameter {self.parameter!r}. "
                f"Valid parameters: {sorted(PARAMETER_SOURCE_MAP)}"
            )

        # Advisory only: multiple spatial filters are accepted (the code picks
        # one) but almost always a mistake, so surface it.
        self._warn_spatial_exclusivity()

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

    def _update_output_units(self):
        parameter = self.parameter.lower()
        if parameter == "ph":
            self.analyte_output_units = ""
        elif parameter in [CONDUCTIVITY, SPECIFIC_CONDUCTANCE]:
            self.analyte_output_units = MICROSIEMENS_PER_CENTIMETER

    @property
    def start_dt(self):
        return self._extract_date(self.start_date)

    @property
    def end_dt(self):
        return self._extract_date(self.end_date)

# ============= EOF =============================================

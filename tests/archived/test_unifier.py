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
import datetime
import os

import pytest
import shapely.wkt

from backend.config import Config
from backend.connectors.ckan import HONDO_RESOURCE_ID
from backend.unifier import unify_analytes, unify_waterlevels


def config_factory():
    cfg = Config()
    cfg.county = "eddy"
    cfg.bbox = "-104.5 32.5,-104 33"
    cfg.start_date = "2020-01-01"
    cfg.end_date = "2024-5-01"
    cfg.output_summary = False

    cfg.use_source_nmbgmr = False
    cfg.use_source_wqp = False
    cfg.use_source_iscsevenrivers = False
    cfg.use_source_nwis = False
    cfg.use_source_oseroswell = False
    cfg.use_source_pvacd = False
    cfg.use_source_bor = False
    cfg.use_source_dwb = False
    cfg.use_source_bernco = False

    cfg.site_limit = 10
    return cfg


@pytest.fixture
def waterlevel_summary_cfg():
    cfg = config_factory()
    cfg.output_summary = True
    return cfg


@pytest.fixture
def waterlevel_timeseries_cfg():
    cfg = config_factory()
    cfg.output_summary = False
    return cfg


@pytest.fixture
def analyte_summary_cfg():
    cfg = config_factory()
    cfg.output_summary = True
    cfg.analyte = "TDS"
    return cfg


# def test_unify_analytes(cfg):
#     unify_analytes(cfg)


def _setup(tmp_path, cfg, source, tag):
    d = tmp_path / tag
    d.mkdir()
    cfg.output_dir = str(d)
    for stag in (
        "nmbgmr",
        "nwis",
        "pvacd",
        "bor",
        "dwb",
        "wqp",
        "iscsevenrivers",
        "oseroswell",
        "bernco",
    ):
        if stag == source:
            setattr(cfg, f"use_source_{stag}", True)
    return d


def _setup_waterlevels(tmp_path, cfg, source):
    d = _setup(tmp_path, cfg, source, "waterlevels")
    unify_waterlevels(cfg)
    return d


def _setup_analytes(tmp_path, cfg, source):
    d = _setup(tmp_path, cfg, source, "analyte")
    unify_analytes(cfg)
    return d


def _test_analytes_summary(tmp_path, cfg, source):
    d = _setup_analytes(tmp_path, cfg, source)
    assert (d / "output.csv").is_file()


def _test_waterlevels_summary(tmp_path, cfg, source):
    d = _setup_waterlevels(tmp_path, cfg, source)
    assert (d / "output.csv").is_file()


def _test_waterlevels_timeseries(
    tmp_path, cfg, source, combined_flag=True, timeseries_flag=False
):
    d = _setup_waterlevels(tmp_path, cfg, source)
    combined = d / "output.combined.csv"
    timeseries = d / "output_timeseries"
    print(combined_flag)

    print("combined", combined.is_file(), combined_flag)
    assert combined.is_file() == combined_flag
    print("timeseries", timeseries.is_dir(), timeseries_flag)
    assert timeseries.is_dir() == timeseries_flag

    return combined, timeseries


def _test_waterelevels_timeseries_date_range(
    tmp_path, cfg, source, timeseries_flag=True, combined_flag=False
):
    combined, timeseries = _test_waterlevels_timeseries(
        tmp_path,
        cfg,
        source,
        timeseries_flag=timeseries_flag,
        combined_flag=combined_flag,
    )

    for p in timeseries.iterdir():
        if os.path.basename(p) == "sites.csv":
            continue

        with open(p, "r") as rfile:
            lines = rfile.readlines()
            for l in lines[1:]:
                vs = l.split(",")
                dd = vs[3]
                dd = datetime.datetime.strptime(dd, "%Y-%m-%d")
                assert dd.year >= 2020 and dd.year <= 2024


def test_nwis_site_health_check():
    from backend.connectors.usgs.source import NWISSiteSource

    n = NWISSiteSource()
    assert n.health()


def test_nmbgmr_site_health_check():
    from backend.connectors.nmbgmr.source import NMBGMRSiteSource

    n = NMBGMRSiteSource()
    assert n.health()


def test_wqp_site_health_check():
    from backend.connectors.wqp.source import WQPSiteSource

    n = WQPSiteSource()
    assert n.health()


def test_bor_site_health_check():
    from backend.connectors.bor.source import BORSiteSource

    n = BORSiteSource()
    assert n.health()


def test_dwb_site_health_check():
    from backend.connectors.nmenv.source import DWBSiteSource

    n = DWBSiteSource()
    assert n.health()


def test_isc_seven_rivers_site_health_check():
    from backend.connectors.isc_seven_rivers.source import ISCSevenRiversSiteSource

    n = ISCSevenRiversSiteSource()
    assert n.health()


def test_ckan_site_health_check():
    from backend.connectors.ckan.source import OSERoswellSiteSource

    n = OSERoswellSiteSource(HONDO_RESOURCE_ID)
    assert n.health()


def test_pvacd_site_health_check():
    from backend.connectors.st2.source import PVACDSiteSource

    n = PVACDSiteSource()
    assert n.health()


def test_bernco_site_health_check():
    from backend.connectors.st2.source import BernCoSiteSource

    n = BernCoSiteSource()
    assert n.health()


# def test_ose_roswell_site_health_check():
#     from backend.connectors.ose_roswell.source import OSESiteSource
#     n = OSESiteSource()
#     assert n.health()


# Source tests ========================================================================================================
def test_source_bounds_nmbgmr():
    from backend.unifier import get_source_bounds
    from backend.connectors import NM_STATE_BOUNDING_POLYGON

    sourcekey = "nmbgmr"
    bounds = get_source_bounds(sourcekey)
    assert bounds
    assert bounds.is_valid
    assert bounds.geom_type == "Polygon"
    assert bounds == NM_STATE_BOUNDING_POLYGON


def test_source_bounds_is_seven_rivers():
    from backend.unifier import get_source_bounds
    from backend.connectors import ISC_SEVEN_RIVERS_BOUNDING_POLYGON

    sourcekey = "iscsevenrivers"
    bounds = get_source_bounds(sourcekey)
    assert bounds
    assert bounds.is_valid
    assert bounds.geom_type == "Polygon"
    assert bounds == ISC_SEVEN_RIVERS_BOUNDING_POLYGON


def test_source_bounds_oser():
    from backend.unifier import get_source_bounds
    from backend.connectors import (
        OSE_ROSWELL_HONDO_BOUNDING_POLYGON,
        OSE_ROSWELL_ROSWELL_BOUNDING_POLYGON,
        OSE_ROSWELL_FORT_SUMNER_BOUNDING_POLYGON,
    )

    sourcekey = "oseroswell"
    bounds = get_source_bounds(sourcekey)
    assert bounds
    assert bounds.is_valid
    assert bounds.geom_type == "GeometryCollection"
    assert bounds == shapely.GeometryCollection(
        [
            OSE_ROSWELL_HONDO_BOUNDING_POLYGON,
            OSE_ROSWELL_FORT_SUMNER_BOUNDING_POLYGON,
            OSE_ROSWELL_ROSWELL_BOUNDING_POLYGON,
        ]
    )


def test_sources_socorro(tmp_path):
    cfg = Config()
    cfg.county = "socorro"

    from backend.unifier import get_sources

    sources = get_sources(cfg)
    assert sources
    assert len(sources) == 2
    assert sorted([s.__class__.__name__ for s in sources]) == sorted(
        ["NMBGMRSiteSource", "NWISSiteSource"]
    )


def test_sources_eddy_dtw(tmp_path):
    cfg = Config()
    cfg.county = "eddy"

    from backend.unifier import get_sources

    sources = get_sources(cfg)
    assert sources
    assert len(sources) == 5
    assert sorted([s.__class__.__name__ for s in sources]) == sorted(
        [
            "ISCSevenRiversSiteSource",
            "NMBGMRSiteSource",
            "OSERoswellSiteSource",
            "PVACDSiteSource",
            "NWISSiteSource",
        ]
    )


def test_sources_eddy_tds(tmp_path):
    cfg = Config()
    cfg.county = "eddy"
    cfg.analyte = "TDS"

    from backend.unifier import get_sources

    sources = get_sources(cfg)
    assert sources
    assert len(sources) == 5
    assert sorted([s.__class__.__name__ for s in sources]) == sorted(
        [
            "BORSiteSource",
            "DWBSiteSource",
            "ISCSevenRiversSiteSource",
            "NMBGMRSiteSource",
            "WQPSiteSource",
        ]
    )


# Waterlevel Summary tests  ===========================================================================================
def test_unify_waterlevels_bernco_summary(tmp_path, waterlevel_summary_cfg):
    waterlevel_summary_cfg.county = "bernalillo"
    waterlevel_summary_cfg.bbox = None
    _test_waterlevels_summary(tmp_path, waterlevel_summary_cfg, "bernco")


def test_unify_waterlevels_nwis_summary(tmp_path, waterlevel_summary_cfg):
    _test_waterlevels_summary(tmp_path, waterlevel_summary_cfg, "nwis")


def test_unify_waterlevels_amp_summary(tmp_path, waterlevel_summary_cfg):
    _test_waterlevels_summary(tmp_path, waterlevel_summary_cfg, "nmbgmr")


def test_unify_waterlevels_pvacd_summary(tmp_path, waterlevel_summary_cfg):
    _test_waterlevels_summary(tmp_path, waterlevel_summary_cfg, "pvacd")


def test_unify_waterlevels_isc_seven_rivers_summary(tmp_path, waterlevel_summary_cfg):
    _test_waterlevels_summary(tmp_path, waterlevel_summary_cfg, "iscsevenrivers")


def test_unify_waterlevels_ose_roswell_summary(tmp_path, waterlevel_summary_cfg):
    _test_waterlevels_summary(tmp_path, waterlevel_summary_cfg, "oseroswell")


# Waterlevel timeseries tests =========================================================================================
def test_unify_waterlevels_nwis_timeseries(tmp_path, waterlevel_timeseries_cfg):
    # there are one or more locations within the bounding box that have only
    # one record, so there is a combined file
    _test_waterlevels_timeseries(
        tmp_path,
        waterlevel_timeseries_cfg,
        "nwis",
        combined_flag=True,
        timeseries_flag=True,
    )


def test_unify_waterlevels_amp_timeseries(tmp_path, waterlevel_timeseries_cfg):
    _test_waterlevels_timeseries(tmp_path, waterlevel_timeseries_cfg, "nmbgmr")


def test_unify_waterlevels_pvacd_timeseries(tmp_path, waterlevel_timeseries_cfg):
    # all locations within the bounding box have more than one record
    # so there is no combined file
    _test_waterlevels_timeseries(
        tmp_path,
        waterlevel_timeseries_cfg,
        "pvacd",
        combined_flag=False,
        timeseries_flag=True,
    )


def test_unify_waterlevels_isc_seven_rivers_timeseries(
    tmp_path, waterlevel_timeseries_cfg
):
    # all locations within the bounding box have more than one record
    # so there is no combined file
    _test_waterlevels_timeseries(
        tmp_path,
        waterlevel_timeseries_cfg,
        "iscsevenrivers",
        combined_flag=False,
        timeseries_flag=True,
    )


def test_unify_waterlevels_ose_roswell_timeseries(tmp_path, waterlevel_timeseries_cfg):
    _test_waterlevels_timeseries(
        tmp_path, waterlevel_timeseries_cfg, "oseroswell", timeseries_flag=True
    )


# Waterlevel summary date range tests =================================================================================
def test_waterlevels_nwis_summary_date_range(tmp_path, waterlevel_summary_cfg):
    d = _setup_waterlevels(tmp_path, waterlevel_summary_cfg, "nwis")
    assert (d / "output.csv").is_file()


# Waterlevel timeseries date range ====================================================================================
def test_waterlevels_nwis_timeseries_date_range(tmp_path, waterlevel_timeseries_cfg):
    # there are one or more locations within the bounding box and date range
    # that have only one record, so there is a combined file
    _test_waterelevels_timeseries_date_range(
        tmp_path,
        waterlevel_timeseries_cfg,
        "nwis",
        timeseries_flag=True,
        combined_flag=True,
    )


def test_waterlevels_isc_seven_rivers_timeseries_date_range(
    tmp_path, waterlevel_timeseries_cfg
):
    # all locations within the bounding box and date rangehave more than one
    # record so there is no combined file
    _test_waterelevels_timeseries_date_range(
        tmp_path,
        waterlevel_timeseries_cfg,
        "iscsevenrivers",
        timeseries_flag=True,
        combined_flag=False,
    )


def test_waterlevels_pvacd_timeseries_date_range(tmp_path, waterlevel_timeseries_cfg):
    # all locations within the bounding box and date rangehave more than one
    # record so there is no combined file
    _test_waterelevels_timeseries_date_range(
        tmp_path,
        waterlevel_timeseries_cfg,
        "pvacd",
        timeseries_flag=True,
        combined_flag=False,
    )


# Analyte summary tests ===============================================================================================
def test_unify_analytes_wqp_summary(tmp_path, analyte_summary_cfg):
    _test_analytes_summary(tmp_path, analyte_summary_cfg, "wqp")


def test_unify_analytes_amp_summary(tmp_path, analyte_summary_cfg):
    _test_analytes_summary(tmp_path, analyte_summary_cfg, "nmbgmr")


def test_unify_analytes_bor_summary(tmp_path, analyte_summary_cfg):
    # BOR locations are found within Otero County
    analyte_summary_cfg.county = "otero"
    analyte_summary_cfg.bbox = None
    _test_analytes_summary(tmp_path, analyte_summary_cfg, "bor")


def test_unify_analytes_isc_seven_rivers_summary(tmp_path, analyte_summary_cfg):
    _test_analytes_summary(tmp_path, analyte_summary_cfg, "iscsevenrivers")


def test_unify_analytes_dwb_summary(tmp_path, analyte_summary_cfg):
    _test_analytes_summary(tmp_path, analyte_summary_cfg, "dwb")


# ============= EOF =============================================

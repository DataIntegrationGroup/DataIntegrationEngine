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

from backend.config import Config
from backend.unifier import unify_analytes, unify_waterlevels


def config_factory():
    cfg = Config()
    cfg.county = "eddy"
    cfg.bbox = "-104.5 32.5,-104 33"
    cfg.start_date = "2020-01-01"
    cfg.end_date = "2024-5-01"
    cfg.output_summary = False

    cfg.use_source_ampapi = False
    cfg.use_source_wqp = False
    cfg.use_source_isc_seven_rivers = False
    cfg.use_source_nwis = False
    cfg.use_source_ose_roswell = False
    cfg.use_source_st2 = False
    cfg.use_source_bor = False
    cfg.use_source_dwb = False

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
        "ampapi",
        "nwis",
        "st2",
        "bor",
        "dwb",
        "wqp",
        "isc_seven_rivers",
        "ose_roswell",
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

    print("combined", combined.is_file(), combined_flag)
    assert combined.is_file() == combined_flag
    print("timeseries", timeseries.is_dir(), timeseries_flag)
    assert timeseries.is_dir() == timeseries_flag

    return combined, timeseries


def _test_waterelevels_timeseries_date_range(tmp_path, cfg, source):
    combined, timeseries = _test_waterlevels_timeseries(
        tmp_path,
        cfg,
        source,
        timeseries_flag=True,
        combined_flag=False,
    )

    for p in timeseries.iterdir():
        if os.path.basename(p) == "sites.csv":
            continue

        with open(p, "r") as rfile:
            lines = rfile.readlines()
            for l in lines[1:]:
                vs = l.split(",")
                dd = vs[1]
                dd = datetime.datetime.strptime(dd, "%Y-%m-%d")
                assert dd.year >= 2020 and dd.year <= 2024


# Waterlevel Summary tests  ===========================================================================================
def test_unify_waterlevels_nwis_summary(tmp_path, waterlevel_summary_cfg):
    _test_waterlevels_summary(tmp_path, waterlevel_summary_cfg, "nwis")


def test_unify_waterlevels_amp_summary(tmp_path, waterlevel_summary_cfg):
    _test_waterlevels_summary(tmp_path, waterlevel_summary_cfg, "ampapi")


def test_unify_waterlevels_st2_summary(tmp_path, waterlevel_summary_cfg):
    _test_waterlevels_summary(tmp_path, waterlevel_summary_cfg, "st2")


def test_unify_waterlevels_isc_seven_rivers_summary(tmp_path, waterlevel_summary_cfg):
    _test_waterlevels_summary(tmp_path, waterlevel_summary_cfg, "isc_seven_rivers")


def test_unify_waterlevels_ose_roswell_summary(tmp_path, waterlevel_summary_cfg):
    _test_waterlevels_summary(tmp_path, waterlevel_summary_cfg, "ose_roswell")


# Waterlevel timeseries tests =========================================================================================
def test_unify_waterlevels_nwis_timeseries(tmp_path, waterlevel_timeseries_cfg):
    _test_waterlevels_timeseries(
        tmp_path,
        waterlevel_timeseries_cfg,
        "nwis",
        combined_flag=False,
        timeseries_flag=True,
    )


def test_unify_waterlevels_amp_timeseries(tmp_path, waterlevel_timeseries_cfg):
    _test_waterlevels_timeseries(tmp_path, waterlevel_timeseries_cfg, "ampapi")


def test_unify_waterlevels_st2_timeseries(tmp_path, waterlevel_timeseries_cfg):
    _test_waterlevels_timeseries(
        tmp_path,
        waterlevel_timeseries_cfg,
        "st2",
        combined_flag=False,
        timeseries_flag=True,
    )


def test_unify_waterlevels_isc_seven_rivers_timeseries(
    tmp_path, waterlevel_timeseries_cfg
):
    _test_waterlevels_timeseries(
        tmp_path,
        waterlevel_timeseries_cfg,
        "isc_seven_rivers",
        combined_flag=False,
        timeseries_flag=True,
    )


def test_unify_waterlevels_ose_roswell_timeseries(tmp_path, waterlevel_timeseries_cfg):
    _test_waterlevels_timeseries(
        tmp_path, waterlevel_timeseries_cfg, "ose_roswell", timeseries_flag=True
    )


# Waterlevel summary date range tests =================================================================================
def test_waterlevels_nwis_summary_date_range(tmp_path, waterlevel_summary_cfg):
    d = _setup_waterlevels(tmp_path, waterlevel_summary_cfg, "nwis")
    assert (d / "output.csv").is_file()


# Waterlevel timeseries date range ====================================================================================
def test_waterlevels_nwis_timeseries_date_range(tmp_path, waterlevel_timeseries_cfg):
    _test_waterelevels_timeseries_date_range(
        tmp_path, waterlevel_timeseries_cfg, "nwis"
    )


def test_waterlevels_isc_seven_rivers_timeseries_date_range(
    tmp_path, waterlevel_timeseries_cfg
):
    _test_waterelevels_timeseries_date_range(
        tmp_path, waterlevel_timeseries_cfg, "isc_seven_rivers"
    )


def test_waterlevels_st2_timeseries_date_range(tmp_path, waterlevel_timeseries_cfg):
    _test_waterelevels_timeseries_date_range(tmp_path, waterlevel_timeseries_cfg, "st2")


# Analyte summary tests ===============================================================================================
def test_unify_analytes_wqp_summary(tmp_path, analyte_summary_cfg):
    _test_analytes_summary(tmp_path, analyte_summary_cfg, "wqp")


def test_unify_analytes_amp_summary(tmp_path, analyte_summary_cfg):
    _test_analytes_summary(tmp_path, analyte_summary_cfg, "ampapi")


def test_unify_analytes_bor_summary(tmp_path, analyte_summary_cfg):
    _test_analytes_summary(tmp_path, analyte_summary_cfg, "bor")


def test_unify_analytes_isc_seven_rivers_summary(tmp_path, analyte_summary_cfg):
    _test_analytes_summary(tmp_path, analyte_summary_cfg, "isc_seven_rivers")


def test_unify_analytes_dwb_summary(tmp_path, analyte_summary_cfg):
    _test_analytes_summary(tmp_path, analyte_summary_cfg, "dwb")


# ============= EOF =============================================

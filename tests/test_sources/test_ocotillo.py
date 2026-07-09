"""
Live integration tests for the Ocotillo OGC API - Features connector.

Ocotillo is SUMMARY-ONLY: the API publishes pre-aggregated "latest"/"summary"
collections and exposes no raw observation time series. It therefore does not
fit the shared BaseSourceTestClass (which exercises time series output), so the
supported surface is tested directly here:

  * health check
  * water-level summary (count/min/max/latest populated; mean/earliest null)
  * analyte summary (latest value only; count/min/max/mean/earliest null)
  * time series requests return nothing (graceful no-op)

A small bounding box is used to keep the requests fast and deterministic.
"""
from pathlib import Path

import pytest

from backend.config import Config, SOURCE_KEYS
from backend.constants import WATERLEVELS, CALCIUM, MILLIGRAMS_PER_LITER, FEET
from backend.logger import setup_logging
from backend.record import SummaryRecord
from backend.unifier import unify_analytes, unify_waterlevels
from tests import recursively_clean_directory

# Chaves/Eddy county area with several NMBGMR monitoring wells.
BBOX = "-104.15 32.55,-103.9 32.7"
SUMMARY_HEADERS = list(SummaryRecord.keys)


def _make_config(parameter):
    config = Config()
    for agency in SOURCE_KEYS:
        setattr(config, f"use_source_{agency}", False)
    config.use_source_ocotillo = True
    config.parameter = parameter
    config.bbox = BBOX
    config.output_summary = True
    config.finalize()
    setup_logging(path=config.output_path)
    return config


@pytest.fixture
def waterlevel_config():
    config = _make_config(WATERLEVELS)
    yield config
    recursively_clean_directory(Path(config.output_path))


@pytest.fixture
def analyte_config():
    config = _make_config(CALCIUM)
    yield config
    recursively_clean_directory(Path(config.output_path))


def _read_summary_rows(config):
    summary_file = Path(config.output_path) / "summary.csv"
    assert summary_file.exists()
    with open(summary_file) as f:
        lines = [ln.strip() for ln in f.readlines()]
    headers = lines[0].split(",")
    assert headers == SUMMARY_HEADERS
    return [dict(zip(headers, ln.split(","))) for ln in lines[1:] if ln]


def test_health():
    config = _make_config(WATERLEVELS)
    try:
        source = config.all_site_sources()[0][0]
        assert source.health()
    finally:
        recursively_clean_directory(Path(config.output_path))


def test_waterlevel_summary(waterlevel_config):
    unify_waterlevels(waterlevel_config)
    rows = _read_summary_rows(waterlevel_config)

    assert rows, "expected at least one water-level summary row"
    for row in rows:
        assert row["source"] == "NMBGMR-Ocotillo"
        assert row["parameter_name"] == "depth_to_water_below_ground_surface"
        # latest is always populated; count/min/max come from water_well_summary
        assert row["latest_value"] != ""
        assert row["nrecords"] != ""
        # Ocotillo exposes neither mean nor the earliest observation.
        assert row["mean"] == ""
        assert row["earliest_date"] == ""
        assert row["earliest_value"] == ""


def test_analyte_summary(analyte_config):
    unify_analytes(analyte_config)
    rows = _read_summary_rows(analyte_config)

    assert rows, "expected at least one analyte summary row"
    for row in rows:
        assert row["source"] == "NMBGMR-Ocotillo"
        assert row["parameter_name"] == CALCIUM
        assert row["parameter_units"] == MILLIGRAMS_PER_LITER
        assert row["latest_value"] != ""
        # Chemistry collections carry only the latest value.
        assert row["min"] == ""
        assert row["max"] == ""
        assert row["mean"] == ""
        assert row["earliest_value"] == ""


def test_timeseries_unsupported():
    """Time series output is unsupported and must be a graceful no-op, not an
    error or partial file."""
    config = _make_config(WATERLEVELS)
    config.output_summary = False
    config.output_timeseries_unified = True
    try:
        unify_waterlevels(config)
        # No timeseries file is produced because the source yields no records.
        assert not (Path(config.output_path) / "timeseries_unified.csv").exists()
    finally:
        recursively_clean_directory(Path(config.output_path))

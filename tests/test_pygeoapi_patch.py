"""Tests for orchestration/pygeoapi/patch_pygeoapi.py: each fix applies exactly
once, and the build fails when the pygeoapi source it targets has changed."""

import importlib.util
from pathlib import Path

import pytest

PATCH = Path(__file__).resolve().parents[1] / "orchestration" / "pygeoapi" / "patch_pygeoapi.py"
spec = importlib.util.spec_from_file_location("patch_pygeoapi", PATCH)
pp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pp)

# The lines patch_pygeoapi.py targets, as they appear in pygeoapi 0.25.dev0.
SOURCE = """
                if '/' in datetime_:
                    begin, end = datetime_.split('/')
                    if begin != '..':
                        begin = isoparse(begin)
                    if end != '..':
                        end = isoparse(end)
                else:
                    target_time = isoparse(datetime_)
            for batch in batches:
                read += batch.num_rows
                if read > limit:
                    batches_list.append(batch.slice(0, limit + 1))
                    break
            return gdf.__geo_interface__['features'][0]
            number_matched = offset + len(rp)
            result = gdf.__geo_interface__
"""


def test_all_fixes_apply():
    out = pp.patch(SOURCE)
    assert "limit + 1 - (read - batch.num_rows)" in out
    assert out.count("_utc(isoparse(") == 3
    assert "def _utc(value):" in out
    assert "__geo_interface__" not in out
    assert out.count("show_bbox=False") == 2
    assert "number_matched = scanner.count_rows()" in out


def test_changed_source_fails():
    with pytest.raises(SystemExit, match="paging"):
        pp.patch(SOURCE.replace("limit + 1))", "limit))"))


def test_already_patched_source_fails():
    with pytest.raises(SystemExit):
        pp.patch(pp.patch(SOURCE))


def test_utc_helper_treats_naive_as_utc():
    from datetime import datetime, timedelta, timezone

    ns = {}
    exec(pp.UTC_HELPER, ns)
    assert ns["_utc"](datetime(2020, 1, 1)).tzinfo == timezone.utc
    mst = timezone(timedelta(hours=-7))
    assert ns["_utc"](datetime(2020, 1, 1, tzinfo=mst)).tzinfo == mst

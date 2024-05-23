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

from click.testing import CliRunner
from frontend.cli import analytes, waterlevels


def _tester(source, func, county, bbox, args=None):
    runner = CliRunner()

    nosources = [
        f
        for f in (
            "--no-amp",
            "--no-nwis",
            "--no-st2",
            "--no-bor",
            "--no-dwb",
            "--no-wqp",
            "--no-isc-seven-rivers",
            "--no-ckan",
        )
        if f != f"--no-{source}"
    ]

    dargs = nosources + ["--site-limit", 10]

    if args:
        args += dargs
    else:
        args = dargs

    if county:
        args.extend(("--county", county))
    elif bbox:
        args.extend(("--bbox", bbox))

    print(" ".join([str(f) for f in args]))
    result = runner.invoke(func, args)

    return result


def _summary_tester(source, func, county=None, bbox=None, args=None):
    if not (county or bbox):
        county = "eddy"

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = _tester(source, func, county, bbox, args)
        assert result.exit_code == 0
        assert os.path.isfile("output.csv")


def _timeseries_tester(
    source,
    func,
    combined_flag=True,
    timeseries_flag=True,
    county=None,
    bbox=None,
    args=None,
):
    if args is None:
        args = []
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = _tester(source, func, county, bbox, args=args + ["--timeseries"])
        assert result.exit_code == 0
        print("combined", os.path.isfile("output.combined.csv"), combined_flag)
        assert os.path.isfile("output.combined.csv") == combined_flag
        print("timeseries", os.path.isdir("output_timeseries"), timeseries_flag)
        assert os.path.isdir("output_timeseries") == timeseries_flag


# ====== Analyte Tests =======================================================
def _analyte_summary_tester(key):
    _summary_tester(key, analytes, args=["TDS"])


def _analyte_county_tester(source, **kw):
    _timeseries_tester(source, analytes, args=["TDS"], county="eddy", **kw)


def test_unify_analytes_amp():
    _analyte_county_tester("amp", timeseries_flag=False)


def test_unify_analytes_wqp():
    _analyte_county_tester("wqp")


def test_unify_analytes_bor():
    _analyte_county_tester("bor", combined_flag=False)


def test_unify_analytes_isc_seven_rivers():
    _analyte_county_tester("isc-seven-rivers")


def test_unify_analytes_dwb():
    _analyte_county_tester("dwb", timeseries_flag=False)


def test_unify_analytes_wqp_summary():
    _analyte_summary_tester("wqp")


def test_unify_analytes_bor_summary():
    _analyte_summary_tester("bor")


def test_unify_analytes_amp_summary():
    _analyte_summary_tester("amp")


def test_unify_analytes_dwb_summary():
    _analyte_summary_tester("dwb")


def test_unify_analytes_isc_seven_rivers_summary():
    _analyte_summary_tester("isc-seven-rivers")


# ====== End Analyte Tests =======================================================


# ====== Water Level Tests =======================================================
def _waterlevel_county_tester(source, **kw):
    _timeseries_tester(source, waterlevels, county="eddy", **kw)


def _waterlevel_bbox_tester(source, **kw):
    _timeseries_tester(source, waterlevels, bbox="-104.5 32.5,-104 33", **kw)


def test_unify_waterlevels_nwis():
    _waterlevel_county_tester("nwis", timeseries_flag=False)


def test_unify_waterlevels_amp():
    _waterlevel_county_tester("amp", timeseries_flag=False)


def test_unify_waterlevels_st2():
    _waterlevel_county_tester("st2", combined_flag=False)


def test_unify_waterlevels_isc_seven_rivers():
    _waterlevel_county_tester("isc-seven-rivers")


def test_unify_waterlevels_ckan():
    _waterlevel_county_tester("ckan")


def test_unify_waterlevels_nwis_summary():
    _summary_tester("nwis", waterlevels)


def test_unify_waterlevels_amp_summary():
    _summary_tester("amp", waterlevels)


def test_unify_waterlevels_st2_summary():
    _summary_tester("st2", waterlevels)


def test_unify_waterlevels_isc_seven_rivers_summary():
    _summary_tester("isc-seven-rivers", waterlevels)


def test_unify_waterlevels_nwis_bbox():
    _waterlevel_bbox_tester("nwis", timeseries_flag=False)


def test_unify_waterlevels_amp_bbox():
    _waterlevel_bbox_tester("amp")


def test_unify_waterlevels_st2_bbox():
    _waterlevel_bbox_tester("st2", combined_flag=False)


def test_unify_waterlevels_isc_seven_rivers_bbox():
    _waterlevel_bbox_tester("isc-seven-rivers", combined_flag=False)


def test_unify_waterlevels_ckan_bbox():
    _waterlevel_bbox_tester("ckan")


# ====== End Water Level Tests =======================================================
# ============= EOF =============================================

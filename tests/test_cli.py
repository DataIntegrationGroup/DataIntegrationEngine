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


def _tester(function, args, fail=False):
    runner = CliRunner()
    print(f"invoked with {args}")
    result = runner.invoke(function, args)
    print(f"result.exit_code={result.exit_code}")
    print(f"result.output=\n{result.output}")

    if fail:
        assert result.exit_code != 0
    else:
        assert result.exit_code == 0


def _make_args(source):
    args = []
    if source:
        nosources = [
            f
            for f in (
                "--no-amp",
                "--no-nwis",
                "--no-pvacd",
                "--no-bor",
                "--no-dwb",
                "--no-wqp",
                "--no-isc-seven-rivers",
                "--no-ckan",
            )
            if f != f"--no-{source}"
        ]
        args += nosources

    args += ["--site-limit", 10, "--dry"]

    return args


def _make_tds_args(source):
    return ["TDS"] + _make_args(source)


def _make_wl_args(source=None):
    return _make_args(source)


def test_waterlevels_nwis():
    args = _make_wl_args("nwis")
    _tester(waterlevels, args)


def test_waterlevels_pvacd():
    args = _make_wl_args("pvacd")
    _tester(waterlevels, args)


def test_waterlevels_nmbgmr():
    args = _make_wl_args("nmbgmr")
    _tester(waterlevels, args)


def test_waterlevels_isc_seven_rivers():
    args = _make_wl_args("iscsevenrivers")
    _tester(waterlevels, args)


def test_waterlevels_invalid_source():
    args = _make_wl_args()
    args.append("--no-foo")
    _tester(waterlevels, args, fail=True)


def test_waterlevels_invalid_bbox():
    args = _make_wl_args()
    args.append("--bbox")
    _tester(waterlevels, args, fail=True)


def test_waterlevels_invalid_bbox_format():
    args = _make_wl_args()
    args.extend(["--bbox", "1 2 3"])
    _tester(waterlevels, args, fail=True)


def test_waterlevels_valid_bbox_format():
    args = _make_wl_args()
    args.extend(["--bbox", "1 2,3 4"])
    _tester(waterlevels, args)


def test_waterlevels_invalid_county():
    args = _make_wl_args()
    args.append("--county")
    _tester(waterlevels, args, fail=True)


def test_waterlevels_invalid_county_name():
    args = _make_wl_args()
    args.extend(["--county", "foo"])
    _tester(waterlevels, args, fail=True)


# Analyte Tests =======================================================
def test_analytes_wqp():
    args = _make_tds_args("wqp")
    _tester(analytes, args)


def test_analytes_bor():
    args = _make_tds_args("bor")
    _tester(analytes, args)


def test_analytes_amp():
    args = _make_tds_args("amp")
    _tester(analytes, args)


def test_analytes_dwb():
    args = _make_tds_args("dwb")
    _tester(analytes, args)


def test_analytes_isc_seven_rivers():
    args = _make_tds_args("isc-seven-rivers")
    _tester(analytes, args)


def test_analytes_invalid_analyte():
    args = _make_args("wqp")
    args[0] = "Foo"
    _tester(analytes, args, fail=True)


def test_analytes_invalid_source():
    args = _make_tds_args("wqp")
    args.append("--no-foo")
    _tester(analytes, args, fail=True)


def test_analytes_invalid_bbox():
    args = _make_tds_args("wqp")
    args.append("--bbox")
    _tester(analytes, args, fail=True)


def test_analytes_invalid_bbox_format():
    args = _make_tds_args("wqp")
    args.extend(["--bbox", "1 2 3"])
    _tester(analytes, args, fail=True)


def test_analytes_valid_bbox_format():
    args = _make_tds_args("wqp")
    args.extend(["--bbox", "1 2,3 4"])
    _tester(analytes, args)


def test_analytes_invalid_county():
    args = _make_tds_args("wqp")
    args.append("--county")
    _tester(analytes, args, fail=True)


def test_analytes_invalid_county_name():
    args = _make_tds_args("wqp")
    args.extend(["--county", "foo"])
    _tester(analytes, args, fail=True)


def test_waterlevels_date_range_YMD():
    args = _make_wl_args()
    args.extend(["--start-date", "2020-01-01", "--end-date", "2020-05-01"])
    _tester(waterlevels, args)


def test_waterlevels_date_range_YM():
    args = _make_wl_args()
    args.extend(["--start-date", "2020-01", "--end-date", "2020-05"])
    _tester(waterlevels, args)


def test_waterlevels_date_range_Y():
    args = _make_wl_args()
    args.extend(["--start-date", "2020", "--end-date", "2021"])
    _tester(waterlevels, args)


def test_waterlevels_invalid_start():
    args = _make_wl_args()
    args.extend(["--start-date", "x-01-01", "--end-date", "2019-05-01"])
    _tester(waterlevels, args, fail=True)


def test_waterlevels_invalid_end():
    args = _make_wl_args()
    args.extend(["--start-date", "2020-01-01", "--end-date", "x-05-01"])
    _tester(waterlevels, args, fail=True)


#
# def _tester(source, func, county, bbox, args=None):
#     runner = CliRunner()
#
#     nosources = [
#         f
#         for f in (
#             "--no-amp",
#             "--no-nwis",
#             "--no-st2",
#             "--no-bor",
#             "--no-dwb",
#             "--no-wqp",
#             "--no-isc-seven-rivers",
#             "--no-ckan",
#         )
#         if f != f"--no-{source}"
#     ]
#
#     dargs = nosources + ["--site-limit", 10]
#
#     if args:
#         args += dargs
#     else:
#         args = dargs
#
#     if county:
#         args.extend(("--county", county))
#     elif bbox:
#         args.extend(("--bbox", bbox))
#
#     print(" ".join([str(f) for f in args]))
#     result = runner.invoke(func, args)
#
#     return result


# def _summary_tester(source, func, county=None, bbox=None, args=None):
#     if not (county or bbox):
#         county = "eddy"
#
#     runner = CliRunner()
#     # with runner.isolated_filesystem():
#     #     result = _tester(source, func, county, bbox, args)
#     #     assert result.exit_code == 0
#     #     assert os.path.isfile("output.csv")
#
#
# def _timeseries_tester(
#     source,
#     func,
#     combined_flag=True,
#     timeseries_flag=True,
#     county=None,
#     bbox=None,
#     args=None,
# ):
#     if args is None:
#         args = []
#     # runner = CliRunner()
#     # with runner.isolated_filesystem():
#     #     result = _tester(source, func, county, bbox, args=args + ["--timeseries"])
#     #     assert result.exit_code == 0
#     #     print("combined", os.path.isfile("output.combined.csv"), combined_flag)
#     #     assert os.path.isfile("output.combined.csv") == combined_flag
#     #     print("timeseries", os.path.isdir("output_timeseries"), timeseries_flag)
#     #     assert os.path.isdir("output_timeseries") == timeseries_flag
#
#
# # ====== Analyte Tests =======================================================
# def _analyte_summary_tester(key):
#     _summary_tester(key, analytes, args=["TDS"])
#
#
# def _analyte_county_tester(source, **kw):
#     _timeseries_tester(source, analytes, args=["TDS"], county="eddy", **kw)
#
#
# def test_unify_analytes_amp():
#     _analyte_county_tester("amp", timeseries_flag=False)
#
#
# def test_unify_analytes_wqp():
#     _analyte_county_tester("wqp")
#
#
# def test_unify_analytes_bor():
#     _analyte_county_tester("bor", combined_flag=False)
#
#
# def test_unify_analytes_isc_seven_rivers():
#     _analyte_county_tester("isc-seven-rivers")
#
#
# def test_unify_analytes_dwb():
#     _analyte_county_tester("dwb", timeseries_flag=False)
#
#
# def test_unify_analytes_wqp_summary():
#     _analyte_summary_tester("wqp")
#
#
# def test_unify_analytes_bor_summary():
#     _analyte_summary_tester("bor")
#
#
# def test_unify_analytes_amp_summary():
#     _analyte_summary_tester("amp")
#
#
# def test_unify_analytes_dwb_summary():
#     _analyte_summary_tester("dwb")
#
#
# def test_unify_analytes_isc_seven_rivers_summary():
#     _analyte_summary_tester("isc-seven-rivers")


# ====== End Analyte Tests =======================================================


# ====== Water Level Tests =======================================================
# def _waterlevel_county_tester(source, **kw):
#     _timeseries_tester(source, waterlevels, county="eddy", **kw)
#
#
# def _waterlevel_bbox_tester(source, **kw):
#     _timeseries_tester(source, waterlevels, bbox="-104.5 32.5,-104 33", **kw)

#
# def test_unify_waterlevels_nwis():
#     _waterlevel_county_tester("nwis", timeseries_flag=False)
#
#
# def test_unify_waterlevels_amp():
#     _waterlevel_county_tester("amp", timeseries_flag=False)
#
#
# def test_unify_waterlevels_st2():
#     _waterlevel_county_tester("st2", combined_flag=False)
#
#
# def test_unify_waterlevels_isc_seven_rivers():
#     _waterlevel_county_tester("isc-seven-rivers")
#
#
# def test_unify_waterlevels_ckan():
#     _waterlevel_county_tester("ckan")
#
#
# def test_unify_waterlevels_nwis_summary():
#     _summary_tester("nwis", waterlevels)
#
#
# def test_unify_waterlevels_amp_summary():
#     _summary_tester("amp", waterlevels)
#
#
# def test_unify_waterlevels_st2_summary():
#     _summary_tester("st2", waterlevels)
#
#
# def test_unify_waterlevels_isc_seven_rivers_summary():
#     _summary_tester("isc-seven-rivers", waterlevels)
#
#
# def test_unify_waterlevels_nwis_bbox():
#     _waterlevel_bbox_tester("nwis", timeseries_flag=False)
#
#
# def test_unify_waterlevels_amp_bbox():
#     _waterlevel_bbox_tester("amp")
#
#
# def test_unify_waterlevels_st2_bbox():
#     _waterlevel_bbox_tester("st2", combined_flag=False)
#
#
# def test_unify_waterlevels_isc_seven_rivers_bbox():
#     _waterlevel_bbox_tester("isc-seven-rivers", combined_flag=False)
#
#
# def test_unify_waterlevels_ckan_bbox():
#     _waterlevel_bbox_tester("ckan")


# ====== End Water Level Tests =======================================================
# ============= EOF =============================================

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


def _timeseries_tester(
    source, func, args=None, combined_flag=True, timeseries_flag=True
):
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

    with runner.isolated_filesystem():
        dargs = nosources + ["--site-limit", 10, "--timeseries", "--county", "eddy"]
        if args:
            args += dargs
        else:
            args = dargs
        print(" ".join([str(f) for f in args]))
        result = runner.invoke(func, args)

        assert result.exit_code == 0
        assert os.path.isfile("output.combined.csv") == combined_flag
        assert os.path.isdir("output_timeseries") == timeseries_flag


# ====== Analyte Tests =======================================================
def _analyte_tester(source, **kw):
    _timeseries_tester(source, analytes, ["TDS"], **kw)


def test_unify_analytes_amp():
    _analyte_tester("amp", timeseries_flag=False)


def test_unify_analytes_wqp():
    _analyte_tester("wqp")


def test_unify_analytes_bor():
    _analyte_tester("bor", combined_flag=False)


def test_unify_analytes_isc_seven_rivers():
    _analyte_tester("isc-seven-rivers", combined_flag=False)


def test_unify_analytes_dwb():
    _analyte_tester("dwb", timeseries_flag=False)


# ====== End Analyte Tests =======================================================


# ====== Waterlevel Tests =======================================================
def test_unify_waterlevels_nwis():
    _timeseries_tester("nwis", waterlevels, timeseries_flag=False)


def test_unify_waterlevels_amp():
    _timeseries_tester("amp", waterlevels, timeseries_flag=False)


def test_unify_waterlevels_st2():
    _timeseries_tester("st2", waterlevels, combined_flag=False)


def test_unify_waterlevels_isc_seven_rivers():
    _timeseries_tester("isc-seven-rivers", waterlevels)


def test_unify_waterlevels_ckan():
    _timeseries_tester("ckan", waterlevels)


# ====== End Waterlevel Tests =======================================================
# ============= EOF =============================================

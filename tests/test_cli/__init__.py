from click.testing import CliRunner
from logging import shutdown as logger_shutdown
from pathlib import Path
import pytest
from typing import List

from backend.config import SOURCE_KEYS
from backend.constants import (
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
from frontend.cli import weave
from tests import recursively_clean_directory


class BaseCLITestClass:

    runner: CliRunner
    agency: str
    agency_reports_parameter: dict
    output_dir: Path

    @pytest.fixture(autouse=True)
    def setup(self):
        # SETUP CODE -----------------------------------------------------------
        self.runner = CliRunner()

        # RUN TESTS ------------------------------------------------------------
        yield

        # TEARDOWN CODE ---------------------------------------------------------
        logger_shutdown()
        recursively_clean_directory(self.output_dir)

    def _test_weave(
        self,
        parameter: str,
        output_type: str,
        output_format: str = "csv",
        site_limit: int = 4,
        start_date: str = "1990-08-10",
        end_date: str = "1990-08-11",
        bbox: str | None = None,
        county: str | None = None,
        wkt: str | None = None,
    ):
        # Arrange
        # turn off all sources except for the one being tested
        no_agencies = []
        for source in SOURCE_KEYS:
            source_with_dash = source.replace("_", "-")
            if source_with_dash == self.agency:
                continue
            else:
                no_agencies.append(f"--no-{source_with_dash}")

        geographic_filter_name: str | None = None
        geographic_filter_value: str | None = None
        if bbox:
            geographic_filter_name = "bbox"
            geographic_filter_value = bbox
        elif county:
            geographic_filter_name = "county"
            geographic_filter_value = county
        elif wkt:
            geographic_filter_name = "wkt"
            geographic_filter_value = wkt

        arguments = [
            parameter,
            "--output-type",
            output_type,
            "--dry",
            "--site-limit",
            str(site_limit),
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--output-format",
            output_format,
        ]

        if geographic_filter_name and geographic_filter_value:
            arguments.extend([f"--{geographic_filter_name}", geographic_filter_value])

        arguments.extend(no_agencies)

        # Act
        result = self.runner.invoke(weave, arguments, standalone_mode=False)

        # Assert
        assert result.exit_code == 0

        """
        For the config, check that

        0. (set output dir to clean up tests results even in event of failure)
        1. The parameter is set correctly
        2. The agencies are set correctly
        3. The output types are set correctly
        4. The site limit is set correctly
        5. The dry is set correctly
        6. The start date is set correctly
        7. The end date is set correctly
        8. The geographic filter is set correctly
        9. The site output type is set correctly
        """
        config = result.return_value

        # 0
        self.output_dir = Path(config.output_path)

        # 1
        assert getattr(config, "parameter") == parameter

        # 2
        agency_with_underscore = self.agency.replace("-", "_")
        if self.agency_reports_parameter[parameter]:
            assert getattr(config, f"use_source_{agency_with_underscore}") is True
        else:
            assert getattr(config, f"use_source_{agency_with_underscore}") is False

        for no_agency in no_agencies:
            no_agency_with_underscore = no_agency.replace("--no-", "").replace("-", "_")
            assert getattr(config, f"use_source_{no_agency_with_underscore}") is False

        # 3
        output_types = ["summary", "timeseries_unified", "timeseries_separated"]
        for ot in output_types:
            if ot == output_type:
                assert getattr(config, f"output_{ot}") is True
            else:
                assert getattr(config, f"output_{ot}") is False

        # 4
        assert getattr(config, "site_limit") == 4

        # 5
        assert getattr(config, "dry") is True

        # 6
        assert getattr(config, "start_date") == start_date

        # 7
        assert getattr(config, "end_date") == end_date

        # 8
        if geographic_filter_name and geographic_filter_value:
            for _geographic_filter_name in ["bbox", "county", "wkt"]:
                if _geographic_filter_name == geographic_filter_name:
                    assert (
                        getattr(config, _geographic_filter_name)
                        == geographic_filter_value
                    )
                else:
                    assert getattr(config, _geographic_filter_name) == ""

        # 9
        assert getattr(config, "output_format") == output_format

    def test_weave_summary(self):
        self._test_weave(parameter=WATERLEVELS, output_type="summary")

    def test_weave_timeseries_unified(self):
        self._test_weave(parameter=WATERLEVELS, output_type="timeseries_unified")

    def test_weave_timeseries_separated(self):
        self._test_weave(parameter=WATERLEVELS, output_type="timeseries_separated")

    def test_weave_csv(self):
        self._test_weave(
            parameter=WATERLEVELS, output_type="summary", output_format="csv"
        )

    def test_weave_geojson(self):
        self._test_weave(
            parameter=WATERLEVELS, output_type="summary", output_format="geojson"
        )

    def test_weave_bbox(self):
        self._test_weave(
            parameter=WATERLEVELS, output_type="summary", bbox="32.0,-106.0,36.0,-102.0"
        )

    def test_weave_county(self):
        self._test_weave(
            parameter=WATERLEVELS, output_type="summary", county="Bernalillo"
        )

    def test_weave_wkt(self):
        self._test_weave(
            parameter=WATERLEVELS,
            output_type="summary",
            wkt="POLYGON((-106.0 32.0, -102.0 32.0, -102.0 36.0, -106.0 36.0, -106.0 32.0))",
        )

    def test_weave_waterlevels(self):
        self._test_weave(parameter=WATERLEVELS, output_type="summary")

    def test_weave_arsenic(self):
        self._test_weave(parameter=ARSENIC, output_type="summary")

    def test_weave_bicarbonate(self):
        self._test_weave(parameter=BICARBONATE, output_type="summary")

    def test_weave_calcium(self):
        self._test_weave(parameter=CALCIUM, output_type="summary")

    def test_weave_carbonate(self):
        self._test_weave(parameter=CARBONATE, output_type="summary")

    def test_weave_chloride(self):
        self._test_weave(parameter=CHLORIDE, output_type="summary")

    def test_weave_fluoride(self):
        self._test_weave(parameter=FLUORIDE, output_type="summary")

    def test_weave_magnesium(self):
        self._test_weave(parameter=MAGNESIUM, output_type="summary")

    def test_weave_nitrate(self):
        self._test_weave(parameter=NITRATE, output_type="summary")

    def test_weave_ph(self):
        self._test_weave(parameter=PH, output_type="summary")

    def test_weave_potassium(self):
        self._test_weave(parameter=POTASSIUM, output_type="summary")

    def test_weave_silica(self):
        self._test_weave(parameter=SILICA, output_type="summary")

    def test_weave_sodium(self):
        self._test_weave(parameter=SODIUM, output_type="summary")

    def test_weave_sulfate(self):
        self._test_weave(parameter=SULFATE, output_type="summary")

    def test_weave_tds(self):
        self._test_weave(parameter=TDS, output_type="summary")

    def test_weave_uranium(self):
        self._test_weave(parameter=URANIUM, output_type="summary")

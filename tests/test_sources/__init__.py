import json
from logging import shutdown as logger_shutdown
from pathlib import Path
import pytest
from shapely import Geometry

from backend.config import Config, SOURCE_KEYS
from backend.constants import WATERLEVELS
from backend.logger import setup_logging
from backend.record import SummaryRecord, SiteRecord, ParameterRecord
from backend.unifier import unify_analytes, unify_waterlevels
from tests import recursively_clean_directory

EXCLUDED_GEOJSON_KEYS = ["latitude", "longitude", "elevation"]

SUMMARY_RECORD_CSV_HEADERS = list(SummaryRecord.keys)
SUMMARY_RECORD_GEOJSON_KEYS = [
    k for k in SUMMARY_RECORD_CSV_HEADERS if k not in EXCLUDED_GEOJSON_KEYS
]

SITE_RECORD_CSV_HEADERS = list(SiteRecord.keys)
SITE_RECORD_GEOJSON_KEYS = [
    k for k in SITE_RECORD_CSV_HEADERS if k not in EXCLUDED_GEOJSON_KEYS
]

PARAMETER_RECORD_HEADERS = list(ParameterRecord.keys)


class BaseSourceTestClass:
    parameter: str
    units: str
    agency: str
    bounds: Geometry

    # set site_limit for tests
    site_limit: int = 3

    @pytest.fixture(autouse=True)
    def setup(self):
        # SETUP CODE  ----------------------------------------------------------
        # 1: setup test/config attributes
        self.config = Config()
        for agency in SOURCE_KEYS:
            setattr(self.config, f"use_source_{agency}", False)
        setattr(self.config, "site_limit", self.site_limit)
        setattr(self.config, "parameter", self.parameter)
        setattr(self.config, "units", self.units)
        setattr(self.config, f"use_source_{self.agency}", True)
        self.config.finalize()

        # 2: initiate logger
        setup_logging(path=self.config.output_path)

        # RUN TESTS ------------------------------------------------------------
        yield

        # UNIVERSAL ASSERTIONS -------------------------------------------------
        # 1: log file exists
        log_path = Path(self.config.output_path) / "die.log"
        assert log_path.exists()

        # TEARDOWN CODE --------------------------------------------------------
        # 1: close logger to delete log file
        logger_shutdown()

        # 2: delete newly created dirs and files
        path_to_clean = Path(self.config.output_path)
        print(f"Cleaning and removing {path_to_clean}")
        # recursively_clean_directory(path_to_clean)

        # reset test attributes
        self.dirs_to_delete = []
        self.config = None
        self.unifier = None

    def _run_unifier(self):
        if self.parameter == WATERLEVELS:
            unify_waterlevels(self.config)
        else:
            unify_analytes(self.config)

    def _check_summary_file(self, extension: str):
        summary_file = Path(self.config.output_path) / f"summary.{extension}"
        assert summary_file.exists()

        if extension == "csv":
            with open(summary_file, "r") as f:
                headers = f.readline().strip().split(",")
                assert headers == SUMMARY_RECORD_CSV_HEADERS

            # +1 for the header
            with open(summary_file, "r") as f:
                lines = f.readlines()
                assert len(lines) == self.site_limit + 1
        elif extension == "geojson":
            with open(summary_file, "r") as f:
                summary = json.load(f)
                assert len(summary["features"]) == self.site_limit
                assert summary["type"] == "FeatureCollection"
                for feature in summary["features"]:
                    assert feature["geometry"]["type"] == "Point"
                    assert len(feature["geometry"]["coordinates"]) == 3
                    assert sorted(feature["properties"].keys()) == sorted(
                        SUMMARY_RECORD_GEOJSON_KEYS
                    )
                assert summary["features"][0]["type"] == "Feature"
        else:
            raise ValueError(f"Unsupported file extension: {extension}")

    def _check_sites_file(self, extension: str):
        sites_file = Path(self.config.output_path) / f"sites.{extension}"
        assert sites_file.exists()

        if extension == "csv":
            with open(sites_file, "r") as f:
                headers = f.readline().strip().split(",")
                assert headers == SITE_RECORD_CSV_HEADERS

            # +1 for the header
            with open(sites_file, "r") as f:
                lines = f.readlines()
                assert len(lines) == self.site_limit + 1
        elif extension == "geojson":
            with open(sites_file, "r") as f:
                sites = json.load(f)
                assert len(sites["features"]) == self.site_limit
                assert sites["type"] == "FeatureCollection"
                for feature in sites["features"]:
                    assert feature["geometry"]["type"] == "Point"
                    assert len(feature["geometry"]["coordinates"]) == 3
                    assert sorted(feature["properties"].keys()) == sorted(
                        SITE_RECORD_GEOJSON_KEYS
                    )
                assert sites["features"][0]["type"] == "Feature"
        else:
            raise ValueError(f"Unsupported file extension: {extension}")

    def _check_timeseries_file(self, timeseries_dir, timeseries_file_name):
        timeseries_file = Path(timeseries_dir) / timeseries_file_name
        assert timeseries_file.exists()

        with open(timeseries_file, "r") as f:
            headers = f.readline().strip().split(",")
            assert headers == PARAMETER_RECORD_HEADERS

    def test_health(self):
        # do a health check for the agency
        source = self.config.all_site_sources()[0][0]
        assert source.health()

    def test_summary_csv(self):
        # Arrange --------------------------------------------------------------
        self.config.output_summary = True
        self.config.report()

        # Act ------------------------------------------------------------------
        self._run_unifier()

        # Assert ---------------------------------------------------------------
        self._check_summary_file("csv")

    def test_summary_geojson(self):
        # Arrange --------------------------------------------------------------
        self.config.output_summary = True
        self.config.output_format = "geojson"
        self.config.report()

        # Act ------------------------------------------------------------------
        self._run_unifier()

        # Assert ---------------------------------------------------------------
        self._check_summary_file("geojson")

    def test_timeseries_unified_csv(self):
        # Arrange --------------------------------------------------------------
        self.config.output_timeseries_unified = True
        self.config.report()

        # Act ------------------------------------------------------------------
        self._run_unifier()

        # Assert ---------------------------------------------------------------
        # Check the sites file
        self._check_sites_file("csv")

        # Check the timeseries file
        timeseries_dir = Path(self.config.output_path)
        timeseries_file_name = "timeseries_unified.csv"
        self._check_timeseries_file(timeseries_dir, timeseries_file_name)

    def test_timeseries_unified_geojson(self):
        # Arrange --------------------------------------------------------------
        self.config.output_timeseries_unified = True
        self.config.output_format = "geojson"
        self.config.report()

        # Act ------------------------------------------------------------------
        self._run_unifier()

        # Assert ---------------------------------------------------------------
        # Check the sites file
        self._check_sites_file("geojson")

        # Check the timeseries file
        timeseries_dir = Path(self.config.output_path)
        timeseries_file_name = "timeseries_unified.csv"
        self._check_timeseries_file(timeseries_dir, timeseries_file_name)

    def test_timeseries_separated_csv(self):
        # Arrange --------------------------------------------------------------
        self.config.output_timeseries_separated = True
        self.config.report()

        # Act ------------------------------------------------------------------
        self._run_unifier()

        # Assert ---------------------------------------------------------------
        # Check the sites file
        self._check_sites_file("csv")

        # Check the timeseries files
        timeseries_dir = Path(self.config.output_path) / "timeseries"
        assert len([f for f in timeseries_dir.iterdir()]) == self.site_limit

        for timeseries_file in timeseries_dir.iterdir():
            self._check_timeseries_file(timeseries_dir, timeseries_file.name)

    def test_timeseries_separated_geojson(self):
        # Arrange --------------------------------------------------------------
        self.config.output_timeseries_separated = True
        self.config.output_format = "geojson"
        self.config.report()

        # Act ------------------------------------------------------------------
        self._run_unifier()

        # Assert ---------------------------------------------------------------
        # Check the sites file
        self._check_sites_file("geojson")

        # Check the timeseries files
        timeseries_dir = Path(self.config.output_path) / "timeseries"
        assert len([f for f in timeseries_dir.iterdir()]) == self.site_limit

        for timeseries_file in timeseries_dir.iterdir():
            self._check_timeseries_file(timeseries_dir, timeseries_file.name)

    @pytest.mark.skip(reason="test_date_range not implemented yet")
    def test_date_range(self):
        pass

    @pytest.mark.skip(reason="test_bounds not implemented yet")
    def test_bounds(self):
        pass

    @pytest.mark.skip(reason="test_wkt not implemented yet")
    def test_wkt(self):
        pass

    @pytest.mark.skip(reason="test_county not implemented yet")
    def test_county(self):
        pass

    @pytest.mark.skip(reason="test_huc not implemented yet")
    def test_huc(self):
        pass

    @pytest.mark.skip(reason="test_bbox not implemented yet")
    def text_bbox(self):
        pass

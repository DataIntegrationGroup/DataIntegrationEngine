from logging import shutdown as logger_shutdown
from pathlib import Path
import pytest

from backend.config import Config, SOURCE_KEYS
from backend.constants import WATERLEVELS
from backend.logger import setup_logging
from backend.record import SummaryRecord, SiteRecord, ParameterRecord
from backend.unifier import unify_analytes, unify_waterlevels

SUMMARY_RECORD_HEADERS = list(SummaryRecord.keys)
SITE_RECORD_HEADERS = list(SiteRecord.keys)
PARAMETER_RECORD_HEADERS = list(ParameterRecord.keys)


def recursively_clean_directory(path):
    """Recursively delete all files and directories in the given path."""
    for item in path.iterdir():
        if item.is_dir():
            recursively_clean_directory(item)
        else:
            item.unlink()
    path.rmdir()


class BaseTestClass:

    parameter = None
    units = None
    agency = None

    # set set_limit for tests
    site_limit = 3

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
        recursively_clean_directory(path_to_clean)

        # reset test attributes
        self.dirs_to_delete = []
        self.config = None
        self.unifier = None

    def _run_unifier(self):
        if self.parameter == WATERLEVELS:
            unify_waterlevels(self.config)
        else:
            unify_analytes(self.config)

    def _check_sites_file(self):
        sites_file = Path(self.config.output_path) / "sites.csv"
        assert sites_file.exists()

        with open(sites_file, "r") as f:
            headers = f.readline().strip().split(",")
            assert headers == SITE_RECORD_HEADERS

        # +1 for the header
        with open(sites_file, "r") as f:
            lines = f.readlines()
            assert len(lines) == self.site_limit + 1

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

    def test_summary(self):
        # Arrange --------------------------------------------------------------
        self.config.output_summary = True
        self.config.report()

        # Act ------------------------------------------------------------------
        self._run_unifier()

        # Assert ---------------------------------------------------------------
        # Check the summary file
        summary_file = Path(self.config.output_path) / "summary.csv"
        assert summary_file.exists()

        # Check the column headers
        with open(summary_file, "r") as f:
            headers = f.readline().strip().split(",")
            assert headers == SUMMARY_RECORD_HEADERS

        # +1 for the header
        with open(summary_file, "r") as f:
            lines = f.readlines()
            assert len(lines) == self.site_limit + 1

    def test_timeseries_unified(self):
        # Arrange --------------------------------------------------------------
        self.config.output_timeseries_unified = True
        self.config.report()

        # Act ------------------------------------------------------------------
        self._run_unifier()

        # Assert ---------------------------------------------------------------
        # Check the sites file
        self._check_sites_file()

        # Check the timeseries file
        timeseries_dir = Path(self.config.output_path)
        timeseries_file_name = "timeseries_unified.csv"
        self._check_timeseries_file(timeseries_dir, timeseries_file_name)

    def test_timeseries_separated(self):
        # Arrange --------------------------------------------------------------
        self.config.output_timeseries_separated = True
        self.config.report()

        # Act ------------------------------------------------------------------
        self._run_unifier()

        # Assert ---------------------------------------------------------------
        # Check the sites file
        self._check_sites_file()

        # Check the timeseries files
        timeseries_dir = Path(self.config.output_path) / "timeseries"
        assert len([f for f in timeseries_dir.iterdir()]) == self.site_limit

        for timeseries_file in timeseries_dir.iterdir():
            self._check_timeseries_file(timeseries_dir, timeseries_file.name)

    @pytest.mark.skip(reason="Not implemented yet")
    def test_date_range(self):
        pass

    @pytest.mark.skip(reason="Not implemented yet")
    def test_wkt(self):
        pass

    @pytest.mark.skip(reason="Not implemented yet")
    def test_county(self):
        pass

    @pytest.mark.skip(reason="Not implemented yet")
    def test_huc(self):
        pass

    @pytest.mark.skip(reason="Not implemented yet")
    def text_bbox(self):
        pass

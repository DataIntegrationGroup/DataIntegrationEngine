from pathlib import Path
import pytest

from backend.config import Config, SOURCE_KEYS, get_source
from backend.constants import WATERLEVELS
from backend.unifier import unify_analytes, unify_waterlevels


class BaseTestClass:

    parameter = None
    units = None
    agency = None

    # set set_limit for tests
    site_limit = 8

    @pytest.fixture(autouse=True)
    def setup(self):
        # Setup code
        self.config = Config()

        for agency in SOURCE_KEYS:
            setattr(self.config, f"use_source_{agency}", False)

        setattr(self.config, "site_limit", self.site_limit)
        setattr(self.config, "parameter", self.parameter)
        setattr(self.config, "units", self.units)
        setattr(self.config, f"use_source_{self.agency}", True)

        self.config.finalize()

        # run test
        yield

        # Teardown code        
        path_to_clean = Path(self.config.output_path)
        print(f"Cleaning and removing {path_to_clean}")
        for f in Path(path_to_clean).iterdir():
            f.unlink()
        path_to_clean.rmdir()
        self.dirs_to_delete = []
        self.config = None
        self.unifier = None

    def _unify(self):
        self.unifier(self.config)

    def _test_health(self):
        # do a health check for the agency
        source = self.config.all_site_sources()[0][0]
        assert source.health()

    def _test_summary(self):
        # Arrange
        self.config.output_summary = True
        self.config.report()

        # Act
        if self.parameter == WATERLEVELS:
            unify_waterlevels(self.config)
        else:
            unify_analytes(self.config)

        # Assert
        # Check the summary file
        summary_file = Path(self.config.output_path) / "summary.csv"
        assert summary_file.exists()

        # Check the column headers
        with open(summary_file, "r") as f:
            headers = f.readline().strip().split(",")
            expected_headers = [
                "source",
                "id",
                "name",
                "usgs_site_id",
                "alternate_site_id",
                "latitude",
                "longitude",
                "horizontal_datum",
                "elevation",
                "elevation_units",
                "well_depth",
                "well_depth_units",
                "parameter_name",
                "parameter_units",
                "nrecords",
                "min",
                "max",
                "mean",
                "earliest_date",
                "earliest_time",
                "earliest_value",
                "earliest_units",
                "latest_date",
                "latest_time",
                "latest_value",
                "latest_units",
            ]
            assert headers == expected_headers

    def _test_timeseries_unified(self):
        pass

    def _test_timeseries_separated(self):
        pass

    def _test_date_range(self):
        pass

    def _test_wkt(self):
        pass

    def _test_county(self):
        pass

    def _test_huc(self):
        pass

    def _text_bbox(self):
        pass

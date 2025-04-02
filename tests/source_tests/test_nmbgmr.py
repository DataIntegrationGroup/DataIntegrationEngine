from backend.constants import WATERLEVELS
from tests import BaseTestClass

import pytest

class TestNMBGMRWaterlevels(BaseTestClass):

    parameter = WATERLEVELS
    units = "ft"
    agency = "nmbgmr_amp"

    def test_health(self):
        self._test_health()

    def test_summary(self):
        self._test_summary()

    @pytest.mark.skip(reason="Not implemented yet")
    def test_timeseries_unified(self):
        self._test_timeseries_unified()

    @pytest.mark.skip(reason="Not implemented yet")
    def test_timeseries_separated(self):
        self._test_timeseries_separated()

    @pytest.mark.skip(reason="Not implemented yet")
    def test_date_range(self):
        self._test_date_range()

    @pytest.mark.skip(reason="Not implemented yet")
    def test_wkt(self):
        self._test_wkt()

    @pytest.mark.skip(reason="Not implemented yet")
    def test_county(self):
        self._test_county()

    @pytest.mark.skip(reason="Not implemented yet")
    def test_huc(self):
        self._test_huc()
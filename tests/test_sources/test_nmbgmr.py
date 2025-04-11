from backend.constants import WATERLEVELS, CALCIUM, MILLIGRAMS_PER_LITER, FEET
from tests.test_sources import BaseSourceTestClass


class TestNMBGMRWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nmbgmr_amp"


class TestNMBGMRAnalyte(BaseSourceTestClass):

    parameter = CALCIUM
    units = MILLIGRAMS_PER_LITER
    agency = "nmbgmr_amp"

from backend.constants import WATERLEVELS, CALCIUM, MILLIGRAMS_PER_LITER, FEET
from tests import BaseTestClass


class TestNMBGMRWaterlevels(BaseTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nmbgmr_amp"


class TestNMBGMRAnalyte(BaseTestClass):

    parameter = CALCIUM
    units = MILLIGRAMS_PER_LITER
    agency = "nmbgmr_amp"

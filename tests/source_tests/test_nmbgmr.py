from backend.constants import WATERLEVELS, CALCIUM
from tests import BaseTestClass

import pytest


class TestNMBGMRWaterlevels(BaseTestClass):

    parameter = WATERLEVELS
    units = "ft"
    agency = "nmbgmr_amp"

class TestNMBGMRAnalyte(BaseTestClass):

    parameter = CALCIUM
    units = "mg/l"
    agency = "nmbgmr_amp"
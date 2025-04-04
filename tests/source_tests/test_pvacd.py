from backend.constants import WATERLEVELS, FEET
from tests import BaseTestClass


class TestPVACDWaterlevels(BaseTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "pvacd"
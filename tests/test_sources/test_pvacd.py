from backend.constants import WATERLEVELS, FEET
from tests.test_sources import BaseSourceTestClass


class TestPVACDWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "pvacd"

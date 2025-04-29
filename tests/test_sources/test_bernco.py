from backend.constants import WATERLEVELS, FEET
from tests.test_sources import BaseSourceTestClass


class TestBernCoWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "bernco"

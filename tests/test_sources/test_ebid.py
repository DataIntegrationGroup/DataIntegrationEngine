from backend.constants import WATERLEVELS, FEET
from tests.test_sources import BaseSourceTestClass


class TestEBIDWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "ebid"

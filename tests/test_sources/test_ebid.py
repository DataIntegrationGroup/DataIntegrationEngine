from backend.constants import WATERLEVELS, FEET
from teststest_sources import BaseSourceTestClass


class TestEBIDWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "ebid"

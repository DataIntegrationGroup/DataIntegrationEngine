from backend.constants import WATERLEVELS, FEET
from tests.test_sources import BaseSourceTestClass


class TestNWISWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nwis"

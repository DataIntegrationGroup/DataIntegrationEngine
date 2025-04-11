from backend.constants import WATERLEVELS, FEET
from tests.test_sources import BaseSourceTestClass


class TestCABQWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "cabq"

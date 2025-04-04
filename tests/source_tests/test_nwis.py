from backend.constants import WATERLEVELS, FEET
from tests import BaseTestClass


class TestNWISWaterlevels(BaseTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nwis"

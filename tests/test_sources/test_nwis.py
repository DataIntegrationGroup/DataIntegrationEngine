from backend.constants import WATERLEVELS, FEET
from tests import BaseSourceTestClass


class TestNWISWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nwis"

from backend.constants import WATERLEVELS, FEET
from tests import BaseSourceTestClass


class TestPVACDWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "pvacd"

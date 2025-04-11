from backend.constants import WATERLEVELS, FEET
from tests import BaseSourceTestClass


class TestBernCoWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "bernco"

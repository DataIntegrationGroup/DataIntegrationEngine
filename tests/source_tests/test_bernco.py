from backend.constants import WATERLEVELS, FEET
from tests import BaseTestClass


class TestBernCoWaterlevels(BaseTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "bernco"

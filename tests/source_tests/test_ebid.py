from backend.constants import WATERLEVELS, FEET
from tests import BaseTestClass


class TestEBIDWaterlevels(BaseTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "ebid"

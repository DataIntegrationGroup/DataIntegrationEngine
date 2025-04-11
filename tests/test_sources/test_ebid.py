from backend.constants import WATERLEVELS, FEET
from tests import BaseSourceTestClass


class TestEBIDWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "ebid"

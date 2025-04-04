from backend.constants import WATERLEVELS, FEET
from tests import BaseTestClass


class TestCABQWaterlevels(BaseTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "cabq"
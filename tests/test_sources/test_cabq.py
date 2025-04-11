from backend.constants import WATERLEVELS, FEET
from tests import BaseSourceTestClass


class TestCABQWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "cabq"

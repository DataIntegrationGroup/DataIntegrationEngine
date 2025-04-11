from backend.constants import WATERLEVELS, CALCIUM, MILLIGRAMS_PER_LITER, FEET
from tests import BaseSourceTestClass


class TestWQPWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "wqp"


class TestWQPAnalyte(BaseSourceTestClass):

    parameter = CALCIUM
    units = MILLIGRAMS_PER_LITER
    agency = "wqp"

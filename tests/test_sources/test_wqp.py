from backend.constants import WATERLEVELS, CALCIUM, MILLIGRAMS_PER_LITER, FEET
from tests import BaseTestClass


class TestWQPWaterlevels(BaseTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "wqp"


class TestWQPAnalyte(BaseTestClass):

    parameter = CALCIUM
    units = MILLIGRAMS_PER_LITER
    agency = "wqp"

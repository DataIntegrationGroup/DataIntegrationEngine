from backend.constants import CALCIUM, MILLIGRAMS_PER_LITER
from tests import BaseSourceTestClass


class TestBoRAnalyte(BaseSourceTestClass):

    parameter = CALCIUM
    units = MILLIGRAMS_PER_LITER
    agency = "bor"

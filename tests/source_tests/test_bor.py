from backend.constants import CALCIUM, MILLIGRAMS_PER_LITER
from tests import BaseTestClass

class TestBoRAnalyte(BaseTestClass):

    parameter = CALCIUM
    units = MILLIGRAMS_PER_LITER
    agency = "bor"
from backend.constants import CALCIUM, MILLIGRAMS_PER_LITER
from tests import BaseTestClass


class TestNMEDDWBAnalyte(BaseTestClass):

    parameter = CALCIUM
    units = MILLIGRAMS_PER_LITER
    agency = "nmed_dwb"
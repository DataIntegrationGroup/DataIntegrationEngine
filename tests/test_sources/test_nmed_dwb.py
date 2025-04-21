from backend.constants import CALCIUM, MILLIGRAMS_PER_LITER
from tests.test_sources import BaseSourceTestClass


class TestNMEDDWBAnalyte(BaseSourceTestClass):

    parameter = CALCIUM
    units = MILLIGRAMS_PER_LITER
    agency = "nmed_dwb"

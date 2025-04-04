from backend.constants import WATERLEVELS, CALCIUM, FEET, MILLIGRAMS_PER_LITER
from tests import BaseTestClass


class TestNMOSEISCSevenRiversWaterlevels(BaseTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nmose_isc_seven_rivers"

class TestNMOSEISCSevenRiversAnalyte(BaseTestClass):

    parameter = CALCIUM
    units = MILLIGRAMS_PER_LITER
    agency = "nmose_isc_seven_rivers"
from backend.constants import WATERLEVELS, CALCIUM, FEET, MILLIGRAMS_PER_LITER
from tests import BaseSourceTestClass


class TestNMOSEISCSevenRiversWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nmose_isc_seven_rivers"


class TestNMOSEISCSevenRiversAnalyte(BaseSourceTestClass):

    parameter = CALCIUM
    units = MILLIGRAMS_PER_LITER
    agency = "nmose_isc_seven_rivers"

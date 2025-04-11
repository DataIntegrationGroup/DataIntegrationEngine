from backend.constants import WATERLEVELS, FEET
from tests.test_sources import BaseSourceTestClass


class TestNMOSERoswellWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nmose_roswell"

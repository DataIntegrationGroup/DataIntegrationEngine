from backend.constants import WATERLEVELS, FEET
from tests import BaseTestClass


class TestNMOSERoswellWaterlevels(BaseTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nmose_roswell"
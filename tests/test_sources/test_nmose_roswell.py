from backend.constants import WATERLEVELS, FEET
from tests import BaseSourceTestClass


class TestNMOSERoswellWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nmose_roswell"

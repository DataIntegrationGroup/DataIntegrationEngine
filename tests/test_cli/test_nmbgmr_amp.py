from backend.constants import (
    WATERLEVELS,
    ARSENIC,
    BICARBONATE,
    CALCIUM,
    CARBONATE,
    CHLORIDE,
    FLUORIDE,
    MAGNESIUM,
    NITRATE,
    PH,
    POTASSIUM,
    SILICA,
    SODIUM,
    SULFATE,
    TDS,
    URANIUM,
)
from tests.test_cli import BaseCLITestClass


class TestNMBGMRCLI(BaseCLITestClass):

    agency = "nmbgmr-amp"
    agency_reports_parameter = {
        WATERLEVELS: True,
        ARSENIC: True,
        BICARBONATE: True,
        CALCIUM: True,
        CARBONATE: True,
        CHLORIDE: True,
        FLUORIDE: True,
        MAGNESIUM: True,
        NITRATE: True,
        PH: True,
        POTASSIUM: True,
        SILICA: True,
        SODIUM: True,
        SULFATE: True,
        TDS: True,
        URANIUM: True,
    }   



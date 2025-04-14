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


class TestNMEDDWBCLI(BaseCLITestClass):

    agency = "nmed-dwb"
    agency_reports_parameter = {
        WATERLEVELS: False,
        ARSENIC: True,
        BICARBONATE: True,
        CALCIUM: True,
        CARBONATE: False,
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

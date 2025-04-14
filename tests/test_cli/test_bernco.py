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


class TestBernCoCLI(BaseCLITestClass):

    agency = "bernco"
    agency_reports_parameter = {
        WATERLEVELS: True,
        ARSENIC: False,
        BICARBONATE: False,
        CALCIUM: False,
        CARBONATE: False,
        CHLORIDE: False,
        FLUORIDE: False,
        MAGNESIUM: False,
        NITRATE: False,
        PH: False,
        POTASSIUM: False,
        SILICA: False,
        SODIUM: False,
        SULFATE: False,
        TDS: False,
        URANIUM: False,
    }

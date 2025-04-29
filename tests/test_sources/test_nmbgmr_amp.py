import os
import pytest

from backend.constants import WATERLEVELS, CALCIUM, MILLIGRAMS_PER_LITER, FEET
from tests.test_sources import BaseSourceTestClass

os.environ["IS_TESTING_ENV"] = "True"


@pytest.fixture(autouse=True)
def setup():
    # SETUP CODE -----------------------------------------------------------
    os.environ["IS_TESTING_ENV"] = "True"

    # RUN TESTS ------------------------------------------------------------
    yield

    # TEARDOWN CODE ---------------------------------------------------------
    os.environ["IS_TESTING_ENV"] = "False"


class TestNMBGMRWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nmbgmr_amp"


class TestNMBGMRAnalyte(BaseSourceTestClass):

    parameter = CALCIUM
    units = MILLIGRAMS_PER_LITER
    agency = "nmbgmr_amp"

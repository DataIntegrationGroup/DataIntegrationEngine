import os
from dotenv import load_dotenv
import pytest

from backend.constants import WATERLEVELS, FEET
from tests.test_sources import BaseSourceTestClass

@pytest.fixture(autouse=True)
def setup_nwis():
    # SETUP CODE -----------------------------------------------------------
    load_dotenv(override=True)

    # RUN TESTS ------------------------------------------------------------
    yield

    # TEARDOWN CODE ---------------------------------------------------------
    os.environ["USGS_API_KEY"] = ""

class TestNWISWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nwis"

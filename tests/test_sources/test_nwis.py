import os
from dotenv import load_dotenv
import pytest

from backend.constants import WATERLEVELS, FEET
from tests.test_sources import BaseSourceTestClass

@pytest.fixture(autouse=True)
def setup_nwis():
    # SETUP CODE -----------------------------------------------------------
    had_usgs_api_key = "USGS_API_KEY" in os.environ
    original_usgs_api_key = os.environ.get("USGS_API_KEY")
    load_dotenv(override=True)

    # RUN TESTS ------------------------------------------------------------
    yield

    # TEARDOWN CODE ---------------------------------------------------------
    if had_usgs_api_key:
         os.environ["USGS_API_KEY"] = original_usgs_api_key
    else:
        os.environ.pop("USGS_API_KEY", None)

class TestNWISWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nwis"

import os
from dotenv import load_dotenv
import pytest

from backend.constants import WATERLEVELS, FEET
from tests.test_sources import BaseSourceTestClass

@pytest.fixture(autouse=True)
def setup_nwis():
    # SETUP CODE -----------------------------------------------------------
    original_environ = os.environ.copy()
    load_dotenv(override=True)

    # RUN TESTS ------------------------------------------------------------
    yield

    # TEARDOWN CODE ---------------------------------------------------------
    os.environ.clear()
    os.environ.update(original_environ)

class TestNWISWaterlevels(BaseSourceTestClass):

    parameter = WATERLEVELS
    units = FEET
    agency = "nwis"

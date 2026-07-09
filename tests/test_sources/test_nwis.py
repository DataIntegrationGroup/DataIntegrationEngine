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


def _site_feature(loc_id, data_type):
    return {
        "properties": {
            "monitoring_location_id": loc_id,
            "data_type": data_type,
            "monitoring_location_name": "TOME SITE",
        },
        "geometry": {"type": "Point", "coordinates": [-106.6, 34.7]},
    }


def test_nwis_site_source_dedups_duplicate_timeseries_features():
    # combined-metadata returns one feature per time series, so a well with
    # several series (field measurements + daily mean/max/min) shows up multiple
    # times with the same monitoring_location_id. get_records must collapse these
    # to one site so its readings aren't re-emitted once per series downstream.
    from backend.config import Config
    from backend.connectors.usgs.source import NWISSiteSource

    features = [
        _site_feature("USGS-344431106393403", "Field measurements"),
        _site_feature("USGS-344431106393403", "Daily values"),
        _site_feature("USGS-344431106393403", "Continuous values"),
        _site_feature("USGS-999900000000000", "Field measurements"),
    ]

    source = NWISSiteSource()
    source.set_config(Config())
    source._requester.request = lambda *a, **k: {"features": features, "links": []}

    records = source.get_records()

    ids = [f["properties"]["monitoring_location_id"] for f in records]
    assert ids == ["USGS-344431106393403", "USGS-999900000000000"]

"""NMBGMR AMP carries release status + qualifier through to the parameter
record, for both water levels and analytes. Network-free."""
from backend.config import Config
from backend.connectors.nmbgmr.source import (
    NMBGMRWaterLevelSource,
    NMBGMRAnalyteSource,
)


def test_waterlevel_maps_publicrelease_and_levelstatus():
    s = NMBGMRWaterLevelSource()
    c = Config()
    c.parameter = "waterlevels"
    s.set_config(c)
    raw = {
        "DepthToWaterBGS": 12.3,
        "DepthToWaterBGSUnits": "ft",
        "DateMeasured": "2024-01-15",
        "TimeMeasured": "08:30:00",
        "PublicRelease": True,
        "LevelStatus": "Water level affected by pumping",
    }
    out = s._extract_parameter_record(raw)
    assert out["approval_status"] is True
    assert out["qualifier"] == "Water level affected by pumping"


def test_analyte_maps_symbol_and_nested_publicrelease():
    s = NMBGMRAnalyteSource()
    c = Config()
    c.parameter = "arsenic"
    s.set_config(c)
    raw = {
        "SampleValue": 1.2,
        "Units": "ug/L",
        "AnalyteMeaning": "Arsenic",
        "Symbol": "<",
        "info": {"CollectionDate": "2024-01-15", "PublicRelease": False},
    }
    out = s._extract_parameter_record(raw)
    assert out["approval_status"] is False
    assert out["qualifier"] == "<"


def test_analyte_missing_metadata_is_none():
    s = NMBGMRAnalyteSource()
    c = Config()
    c.parameter = "arsenic"
    s.set_config(c)
    raw = {
        "SampleValue": 1.2,
        "Units": "ug/L",
        "AnalyteMeaning": "Arsenic",
        "Symbol": "",
        "info": {"CollectionDate": "2024-01-15"},
    }
    out = s._extract_parameter_record(raw)
    assert out["approval_status"] is None and out["qualifier"] is None

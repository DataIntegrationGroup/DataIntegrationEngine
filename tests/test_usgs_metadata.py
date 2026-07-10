"""USGS/NWIS carries per-observation quality metadata (provisional/approved
status + qualifier) through to the timeseries record. Network-free."""
from backend.config import Config
from backend.connectors.usgs.source import NWISWaterLevelSource


def _raw_feature(approval="Provisional", qualifier=None):
    return {
        "properties": {
            "monitoring_location_id": "USGS-123",
            "value": 42.5,
            "time": "2024-01-15T08:30:00Z",
            "unit_of_measure": "ft",
            "approval_status": approval,
            "qualifier": qualifier,
        }
    }


def test_standardize_record_keeps_approval_and_qualifier():
    s = NWISWaterLevelSource()
    rec = s._standardize_record(_raw_feature("Approved", ["Static"]))
    assert rec["approval_status"] == "Approved"
    assert rec["qualifier"] == ["Static"]


def test_standardize_record_missing_metadata_is_none():
    s = NWISWaterLevelSource()
    feature = {
        "properties": {
            "monitoring_location_id": "USGS-123",
            "value": 1.0,
            "time": "2024-01-15T00:00:00Z",
            "unit_of_measure": "ft",
        }
    }
    rec = s._standardize_record(feature)
    assert rec["approval_status"] is None and rec["qualifier"] is None


def test_extract_parameter_record_populates_metadata():
    s = NWISWaterLevelSource()
    c = Config()
    c.parameter = "waterlevels"
    s.set_config(c)
    std = s._standardize_record(_raw_feature("Provisional", ["Ice"]))
    out = s._extract_parameter_record(std)
    assert out["approval_status"] == "Provisional"
    assert out["qualifier"] == ["Ice"]


def test_parameter_record_schema_includes_metadata_keys():
    from backend.record import ParameterRecord

    assert "approval_status" in ParameterRecord.keys
    assert "qualifier" in ParameterRecord.keys

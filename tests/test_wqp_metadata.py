"""WQP carries result status + qualifier through to the parameter record.
Network-free."""
from backend.config import Config
from backend.connectors.wqp.source import WQPAnalyteSource


def _raw(status="Accepted", qualifier=""):
    return {
        "CharacteristicName": "Arsenic",
        "ResultMeasureValue": "1.2",
        "ResultMeasure/MeasureUnitCode": "ug/l",
        "ActivityStartDate": "2024-01-15",
        "ActivityStartTime/Time": "08:30:00",
        "ResultStatusIdentifier": status,
        "MeasureQualifierCode": qualifier,
    }


def _source():
    s = WQPAnalyteSource()
    c = Config()
    c.parameter = "arsenic"
    s.set_config(c)
    return s


def test_status_and_qualifier_mapped():
    out = _source()._extract_parameter_record(_raw("Historical", "J"))
    assert out["approval_status"] == "Historical"
    assert out["qualifier"] == "J"


def test_empty_strings_normalized_to_none():
    # WQP TSV uses "" for missing fields
    out = _source()._extract_parameter_record(_raw("", ""))
    assert out["approval_status"] is None and out["qualifier"] is None

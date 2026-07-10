"""BOR (RISE) carries the result status through to the parameter record.
RISE has no separate qualifier field, so qualifier stays None. Network-free."""
from backend.config import Config
from backend.connectors.bor.source import BORAnalyteSource


def _source():
    s = BORAnalyteSource()
    c = Config()
    c.parameter = "arsenic"
    s.set_config(c)
    s._source_parameter_name = "Arsenic"
    return s


def _raw(status):
    return {
        "attributes": {
            "result": 1.2,
            "dateTime": "2024-01-15T00:00:00",
            "status": status,
            "resultAttributes": {"units": "mg/L"},
        }
    }


def test_status_mapped_to_approval_status():
    out = _source()._extract_parameter_record(_raw("Provisional"))
    assert out["approval_status"] == "Provisional"
    # RISE has no qualifier field -> not set here; the record defaults it to None
    assert out.get("qualifier") is None

def test_missing_status_is_none():
    out = _source()._extract_parameter_record(_raw(None))
    assert out["approval_status"] is None

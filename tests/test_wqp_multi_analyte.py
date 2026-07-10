"""Multi-analyte fetch: characteristic-name flattening + per-analyte filtering
(the partition step that lets one WQP query serve N analyte products).

Network-free — the live cross-analyte parity is exercised manually against WQP.
"""
from backend.config import Config
from backend.connectors.wqp.source import WQPAnalyteSource, _wqp_characteristic_names


class _Site:
    def __init__(self, sid):
        self.id = sid
        self.chunk_size = 1


def _rec(site, name):
    return {"MonitoringLocationIdentifier": site, "CharacteristicName": name}


def test_characteristic_names_flatten_and_dedup():
    names = _wqp_characteristic_names(["nitrate", "arsenic"])
    # nitrate maps to several names; arsenic to one; order preserved, no dups
    assert names == ["Nitrate", "Nitrate-N", "Nitrate as N", "Arsenic"]


def test_shared_name_conductivity_specific_conductance():
    # both map to the same WQP name; a record for it belongs to both passes
    assert _wqp_characteristic_names(["conductivity"]) == ["Specific conductance"]
    assert _wqp_characteristic_names(["specific_conductance"]) == ["Specific conductance"]


def test_multi_mode_filters_records_to_active_analyte():
    s = WQPAnalyteSource()
    c = Config()
    c.parameter = "arsenic"
    s.set_config(c)
    s.set_parameters(["arsenic", "nitrate"])  # multi-analyte fetch

    records = [
        _rec("W1", "Arsenic"),
        _rec("W1", "Nitrate"),      # other analyte, same well -> dropped this pass
        _rec("W1", "Nitrate as N"),  # other analyte -> dropped
        _rec("W2", "Arsenic"),      # other site -> dropped by site filter
    ]
    got = s._extract_site_records(records, _Site("W1"))
    assert [r["CharacteristicName"] for r in got] == ["Arsenic"]


def test_multi_mode_nitrate_pass_keeps_all_nitrate_names():
    s = WQPAnalyteSource()
    c = Config()
    c.parameter = "nitrate"
    s.set_config(c)
    s.set_parameters(["arsenic", "nitrate"])

    records = [
        _rec("W1", "Nitrate"),
        _rec("W1", "Nitrate as N"),
        _rec("W1", "Arsenic"),
    ]
    got = s._extract_site_records(records, _Site("W1"))
    assert {r["CharacteristicName"] for r in got} == {"Nitrate", "Nitrate as N"}


def test_single_mode_does_not_analyte_filter():
    # _parameters is None -> original behavior: site filter only, no analyte
    # filter (the single-analyte query already scoped the fetch)
    s = WQPAnalyteSource()
    c = Config()
    c.parameter = "arsenic"
    s.set_config(c)
    records = [_rec("W1", "Nitrate")]  # would be filtered in multi mode
    got = s._extract_site_records(records, _Site("W1"))
    assert len(got) == 1

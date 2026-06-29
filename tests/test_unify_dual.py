"""Gate for the single-fetch dual unification (unify_source_both).

Proves the new path produces output identical to running unify_source twice
(once per mode) while pulling each source's records only once. Uses lightweight
fake sources so the test is offline and deterministic; the transform internals
are unchanged and covered by the connector tests.
"""
from collections import namedtuple

import pytest

from backend.config import Config
from backend.record import ParameterRecord, SiteRecord, SummaryRecord
from backend.source import BaseParameterSource, BaseSiteSource, BaseTransformer
from backend.unifier import unify_source, unify_source_both

_Site = namedtuple("_Site", "id")


class _FakeSiteSource(BaseSiteSource):
    chunk_size = 1

    def __init__(self):
        super().__init__(transformer=BaseTransformer())
        self.get_records_calls = 0

    def get_records(self, *a, **k):
        self.get_records_calls += 1
        return [{"id": "W1"}]

    def _transform_sites(self, records):
        s = SiteRecord({"source": "fake", "id": "W1", "latitude": 34.0, "longitude": -106.0})
        s.chunk_size = self.chunk_size
        return [s]


class _FakeParamSource(BaseParameterSource):
    def __init__(self):
        super().__init__(transformer=BaseTransformer())
        self.get_records_calls = 0

    def get_records(self, site_record, *a, **k):
        # The mode-agnostic API pull. Counting proves it runs once when shared.
        self.get_records_calls += 1
        return [{"id": "W1", "value": 1.0, "date": "2020-01-01"}]

    # Mode-specific transforms, kept trivial but driven off the shared fetch so
    # the fetch cache is exercised exactly as the real reads exercise it.
    def read_summary(self, site_record, start_ind, end_ind):
        obs = self._fetch_records(site_record)
        return [
            SummaryRecord({"source": "fake", "id": "W1", "nrecords": len(obs), "mean": 1.0})
        ]

    def read_timeseries(self, site_record):
        obs = self._fetch_records(site_record)
        site = SiteRecord({"source": "fake", "id": "W1"})
        recs = [ParameterRecord({"source": "fake", "id": "W1", "parameter_value": o["value"]}) for o in obs]
        return [(site, recs)]


def _config():
    cfg = Config(payload={"yes": True})
    cfg.parameter = "waterlevels"
    return cfg


@pytest.fixture
def patched_pair(monkeypatch):
    """Make config.source_pair return fresh fakes, and report the param source
    so a test can read its fetch count."""
    holder = {}

    def fake_pair(self, source_key):
        site = _FakeSiteSource()
        param = _FakeParamSource()
        site.set_config(self)
        param.set_config(self)
        holder["site"] = site
        holder["param"] = param
        return site, param

    monkeypatch.setattr(Config, "source_pair", fake_pair)
    return holder


class TestUnifySourceBoth:
    def test_fetches_source_once(self, patched_pair):
        cfg = _config()
        summary_p, ts_p = unify_source_both(cfg, "fake")
        # One observation fetch and one site fetch shared across both passes.
        assert patched_pair["param"].get_records_calls == 1
        assert patched_pair["site"].get_records_calls == 1
        # Both outputs populated.
        assert len(summary_p.records) == 1
        assert len(ts_p.sites) == 1 and len(ts_p.timeseries) == 1

    def test_output_identical_to_two_single_runs(self, patched_pair):
        # dual
        dual_summary, dual_ts = unify_source_both(_config(), "fake")
        # two separate runs (cache off path)
        cfg_s = _config(); cfg_s.output_summary = True
        single_summary = unify_source(cfg_s, "fake")
        cfg_t = _config(); cfg_t.output_summary = False
        single_ts = unify_source(cfg_t, "fake")

        def payloads(recs):
            return [r._payload for r in recs]

        assert payloads(dual_summary.records) == payloads(single_summary.records)
        assert payloads(dual_ts.sites) == payloads(single_ts.sites)
        assert [payloads(t) for t in dual_ts.timeseries] == [
            payloads(t) for t in single_ts.timeseries
        ]

    def test_single_run_fetches_each_time(self, patched_pair):
        # Control: a normal single unify (cache disabled) fetches on its one pass.
        cfg = _config(); cfg.output_summary = True
        unify_source(cfg, "fake")
        assert patched_pair["param"].get_records_calls == 1


class TestFetchCache:
    def test_disabled_calls_get_records_each_time(self):
        src = _FakeParamSource()
        src._fetch_records([_Site("W1")])
        src._fetch_records([_Site("W1")])
        assert src.get_records_calls == 2

    def test_enabled_shares_by_site_key(self):
        src = _FakeParamSource()
        src._fetch_cache_enabled = True
        a = src._fetch_records([_Site("W1")])
        b = src._fetch_records([_Site("W1")])
        assert src.get_records_calls == 1
        assert a is b

    def test_enabled_distinct_keys_refetch(self):
        src = _FakeParamSource()
        src._fetch_cache_enabled = True
        src._fetch_records([_Site("W1")])
        src._fetch_records([_Site("W2")])
        assert src.get_records_calls == 2

    def test_site_source_read_cached(self):
        site = _FakeSiteSource()
        site._fetch_cache_enabled = True
        r1 = site.read()
        r2 = site.read()
        assert site.get_records_calls == 1
        assert r1 is r2

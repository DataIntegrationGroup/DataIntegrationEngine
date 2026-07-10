"""Live per-connector integration harness.

Excluded from the default suite (network; see pyproject norecursedirs). Run
explicitly:

    uv run pytest tests/test_sources --override-ini="norecursedirs="

Migrated off the removed CLI dump path: each agency is exercised through
``unify_source_both`` (the production Dagster path) and asserted on the returned
persisters' records/sites/timeseries, not on dumped files.
"""
import pytest

from backend.config import Config, SOURCE_KEYS
from backend.unifier import unify_source_both


class BaseSourceTestClass:
    parameter: str
    agency: str
    units: str = ""  # kept for the per-connector subclasses; unused here

    # cap sites so a statewide run stays bounded
    site_limit: int = 3

    @pytest.fixture(autouse=True)
    def setup(self):
        self.config = Config()
        for agency in SOURCE_KEYS:
            setattr(self.config, f"use_source_{agency}", False)
        setattr(self.config, f"use_source_{self.agency}", True)
        self.config.site_limit = self.site_limit
        self.config.parameter = self.parameter
        yield
        self.config = None

    def test_health(self):
        source = self.config.all_site_sources()[0][0]
        assert source.health()

    def test_unify(self):
        """One fetch yields both summary + timeseries; both must carry data with
        the expected identity/parameter fields."""
        summary_persister, timeseries_persister = unify_source_both(
            self.config, self.agency
        )

        # --- summary ---
        records = summary_persister.records
        assert records, f"{self.agency}: no summary records"
        assert len(records) <= self.site_limit
        r = records[0]
        assert r.source and r.id
        assert r.parameter_name
        assert r.nrecords and r.nrecords > 0
        for field in ("min", "max", "mean"):
            assert getattr(r, field) is not None

        # --- timeseries (site[i] <-> observations[i]) ---
        sites = timeseries_persister.sites
        timeseries = timeseries_persister.timeseries
        assert sites, f"{self.agency}: no timeseries sites"
        assert timeseries, f"{self.agency}: no timeseries observations"
        assert len(sites) == len(timeseries) <= self.site_limit

        site = sites[0]
        assert site.source and site.id
        assert site.latitude is not None and site.longitude is not None

        obs = timeseries[0][0]
        assert obs.source and obs.id
        assert obs.parameter_value is not None
        assert obs.date_measured

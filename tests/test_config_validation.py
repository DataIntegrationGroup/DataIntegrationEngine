"""Tests for Config.validate() parameter check and advisory exclusivity guards."""
import pytest

from backend.config import Config, PARAMETER_SOURCE_MAP, SOURCE_KEYS
from backend.exceptions import ConfigError


def _cfg(**attrs):
    c = Config()
    for k, v in attrs.items():
        setattr(c, k, v)
    return c


class TestParameterValidation:
    def test_valid_parameter_passes(self):
        c = _cfg(parameter="waterlevels")
        c.validate()  # no exit

    def test_empty_parameter_ok(self):
        # sites-only flows carry no parameter
        c = _cfg(parameter="")
        c.validate()

    def test_unknown_parameter_raises_configerror(self):
        # Must raise (not sys.exit) so a Dagster asset soft-fails the source
        # instead of killing the run process.
        c = _cfg(parameter="not_a_real_parameter")
        with pytest.raises(ConfigError):
            c.validate()

    def test_invalid_bbox_raises_configerror(self):
        c = _cfg(parameter="waterlevels", bbox="not-a-bbox")
        with pytest.raises(ConfigError):
            c.validate()

    def test_validate_parameter_helper(self):
        assert _cfg(parameter="arsenic")._validate_parameter() is True
        assert _cfg(parameter="")._validate_parameter() is True
        assert _cfg(parameter="bogus")._validate_parameter() is False


class TestExclusivityGuards:
    def test_single_spatial_filter_no_warn(self, capsys):
        # county only — should not trip the multi-filter advisory
        c = _cfg(parameter="waterlevels", county="Bernalillo")
        c._warn_spatial_exclusivity()  # must not raise

    def test_multiple_spatial_filters_advisory(self):
        c = _cfg(county="Bernalillo", wkt="POLYGON((0 0,0 1,1 1,1 0,0 0))")
        # advisory only — does not raise/exit
        c._warn_spatial_exclusivity()

    def test_validate_passes_with_summary_mode(self):
        c = _cfg(parameter="waterlevels", output_summary=True)
        c.validate()


class TestSourceKeysCanonical:
    def test_source_keys_cover_parameter_map(self):
        # every agency referenced by the parameter map is a real source key
        for entry in PARAMETER_SOURCE_MAP.values():
            for agency in entry["agencies"]:
                assert agency in SOURCE_KEYS

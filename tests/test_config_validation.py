"""Tests for Config.validate() parameter check and advisory exclusivity guards."""
import pytest

from backend.config import Config, PARAMETER_SOURCE_MAP, SOURCE_KEYS


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

    def test_unknown_parameter_exits(self):
        c = _cfg(parameter="not_a_real_parameter")
        with pytest.raises(SystemExit):
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

    def test_multiple_output_modes_advisory(self):
        c = _cfg(output_summary=True, output_timeseries_unified=True)
        c._warn_output_mode_exclusivity()  # advisory, no raise

    def test_validate_passes_with_one_mode(self):
        c = _cfg(parameter="waterlevels", output_summary=True)
        c.validate()


class TestSourceKeysCanonical:
    def test_source_keys_cover_parameter_map(self):
        # every agency referenced by the parameter map is a real source key
        for agencies in PARAMETER_SOURCE_MAP.values():
            for agency in agencies:
                assert agency in SOURCE_KEYS

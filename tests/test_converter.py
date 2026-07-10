"""Unit tests for StandardUnitConverter — the unit-normalization science that
every parameter record passes through. Network-free."""
import pytest

from backend.converter import StandardUnitConverter, _coerce_value


@pytest.fixture
def conv():
    return StandardUnitConverter()


def _c(conv, value, in_u, out_u, src="x", die="x"):
    return conv.convert(value, in_u, out_u, src, die)


class TestCoerceValue:
    def test_numeric(self):
        assert _coerce_value("12.5") == (12.5, True)
        assert _coerce_value(3) == (3.0, True)

    @pytest.mark.parametrize("marker", ["ND", "not detected", "< MRL", "<mdl"])
    def test_non_detect_markers_become_zero(self, marker):
        assert _coerce_value(marker) == (0.0, True)

    def test_unparseable_non_marker(self):
        assert _coerce_value("garbage") == (None, False)
        assert _coerce_value(None) == (None, False)


class TestPassthroughAndSpecial:
    def test_ph_is_identity(self, conv):
        val, factor, warn = _c(conv, 7.2, "", "", die="ph")
        assert (val, factor, warn) == (7.2, 1.0, "")

    def test_non_detect_converts_to_zero(self, conv):
        val, factor, warn = _c(conv, "< mrl", "mg/L", "mg/L", die="arsenic")
        assert val == 0.0 and factor == 1.0 and warn == ""

    def test_non_numeric_is_skipped_not_crashed(self, conv):
        val, factor, warn = _c(conv, "abc", "mg/L", "mg/L")
        assert val == "abc" and factor is None and "Non-numeric" in warn

    def test_conductivity_known_unit_identity(self, conv):
        val, factor, warn = _c(conv, 500, "uS/cm", "", die="conductivity")
        assert val == 500.0 and factor == 1.0


class TestChemistryConversions:
    def test_mg_l_as_n_scales_to_no3(self, conv):
        val, factor, _ = _c(conv, 10, "mg/l as N", "mg/L", die="nitrate")
        assert factor == 4.427 and val == pytest.approx(44.27)

    def test_ppb_to_mgl(self, conv):
        val, factor, _ = _c(conv, 1000, "ppb", "mg/L", die="arsenic")
        assert factor == 0.001 and val == pytest.approx(1.0)

    def test_ppm_to_mgl_identity(self, conv):
        val, factor, _ = _c(conv, 5, "ppm", "mg/L", die="calcium")
        assert factor == 1.0 and val == 5.0

    @pytest.mark.parametrize(
        "die,factor", [("bicarbonate", 1.22), ("calcium", 0.4), ("carbonate", 0.6)]
    )
    def test_caco3_factors(self, conv, die, factor):
        val, f, _ = _c(conv, 100, "mg/l caco3", "mg/L", die=die)
        assert f == factor and val == pytest.approx(100 * factor)

    def test_same_units_identity(self, conv):
        val, factor, _ = _c(conv, 42, "mg/L", "mg/L", die="sulfate")
        assert factor == 1.0 and val == 42.0

    def test_nitrate_as_n_source_name_rescales_even_when_units_match(self, conv):
        # source reports "nitrate as n" already in mg/L -> still needs the 4.427
        val, factor, _ = _c(conv, 10, "mg/L", "mg/L", src="nitrate as n", die="nitrate")
        assert factor == 4.427 and val == pytest.approx(44.27)


class TestLengthConversions:
    def test_ft_to_m(self, conv):
        val, factor, _ = _c(conv, 100, "ft", "m", die="waterlevels")
        assert factor == 0.3048 and val == pytest.approx(30.48)

    def test_m_to_ft(self, conv):
        val, factor, _ = _c(conv, 100, "m", "ft", die="waterlevels")
        assert factor == 3.28084 and val == pytest.approx(328.084)

    def test_ft_identity(self, conv):
        val, factor, _ = _c(conv, 12, "ft", "ft", die="waterlevels")
        assert factor == 1.0 and val == 12.0


class TestFailedConversion:
    def test_unknown_units_warn_and_no_factor(self, conv):
        val, factor, warn = _c(conv, 5, "furlongs", "mg/L", die="calcium")
        assert val == 5 and factor is None and "Failed to convert" in warn

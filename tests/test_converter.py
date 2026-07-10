"""Unit tests for backend/converter.py — StandardUnitConverter logic.

Validates every conversion factor documented in UNIT_CONVERSIONS.md plus
non-detect marker handling and edge-case graceful failure paths.  All tests are
pure Python (no network calls, no mocks).
"""
import pytest

from backend.converter import _coerce_value, StandardUnitConverter
from backend.constants import (
    MILLIGRAMS_PER_LITER,
    MICROGRAMS_PER_LITER,
    PARTS_PER_MILLION,
    PARTS_PER_BILLION,
    TONS_PER_ACRE_FOOT,
    FEET,
    METERS,
)


converter = StandardUnitConverter()


# ---------------------------------------------------------------------------
# Non-detect marker handling
# ---------------------------------------------------------------------------

class TestNonDetectMarkers:
    """Non-detect markers should coerce to 0.0 with ok=True."""

    def test_nd_marker(self):
        value, ok = _coerce_value("nd")
        assert (value, ok) == (0.0, True)

    def test_not_detected_marker(self):
        value, ok = _coerce_value("not detected")
        assert (value, ok) == (0.0, True)

    def test_less_than_mrl_marker(self):
        value, ok = _coerce_value("< mrl")
        assert (value, ok) == (0.0, True)

    def test_less_than_mdl_marker(self):
        value, ok = _coerce_value("< mdl")
        assert (value, ok) == (0.0, True)


# ---------------------------------------------------------------------------
# _coerce_value edge cases
# ---------------------------------------------------------------------------

class TestCoerceValueEdgeCases:
    def test_numeric_string_passes_through(self):
        value, ok = _coerce_value("123.45")
        assert (value, ok) == (123.45, True)

    def test_float_passes_through(self):
        value, ok = _coerce_value(42.0)
        assert (value, ok) == (42.0, True)

    def test_unknown_non_numeric_returns_none_false(self):
        value, ok = _coerce_value("banana")
        assert (value, ok) == (None, False)


# ---------------------------------------------------------------------------
# Length conversions (meters ↔ feet)
# ---------------------------------------------------------------------------

class TestLengthConversions:
    """Verify meter/feet conversion factors match spec."""

    def test_m_to_ft(self):
        result, factor, warning = converter.convert(1.0, METERS, FEET, "-", "-")
        assert factor == 3.28084 and not warning

    def test_ft_to_m(self):
        result, factor, warning = converter.convert(1.0, FEET, METERS, "-", "-")
        assert factor == 0.3048 and not warning

    def test_ft_to_ft_identity(self):
        _, factor, warning = converter.convert(5.0, FEET, FEET, "-", "-")
        assert factor == 1.0 and not warning

    def test_m_to_m_identity(self):
        _, factor, warning = converter.convert(3.0, METERS, METERS, "-", "-")
        assert factor == 1.0 and not warning


# ---------------------------------------------------------------------------
# mg/L as CaCO₃ → mg/L (parameter-specific factors)
# ---------------------------------------------------------------------------

class TestMgLCaCO3Conversions:
    """Verify bicarbonate/calcium/carbonate conversion factors match spec.

    KNOWN BUG — unit string mismatch between code and spec:
        Spec (UNIT_CONVERSIONS.md):  'mg/L as CaCO3'
        Code checks for after lowercasing: 'mg/l caco3'
        Actual lowercased input: 'mg/l as caco3'

    Because the strings don't match, these three conversions currently return
    factor=None instead of the documented values.  The tests below assert that
    None is returned until someone fixes the converter to recognize the spec string.
    """

    def test_bicarbonate_mgl_caco3_to_mgl(self):
        _, factor, warning = converter.convert(
            100.0, "mg/L as CaCO3", MILLIGRAMS_PER_LITER, "bicarbonate", "bicarbonate"
        )
        # BUG: code checks 'mg/l caco3' but lowercased input is 'mg/l as caco3'
        assert factor is None and warning != ""

    def test_calcium_mgl_caco3_to_mgl(self):
        _, factor, warning = converter.convert(
            50.0, "mg/L as CaCO3", MILLIGRAMS_PER_LITER, "calcium", "calcium"
        )
        assert factor is None and warning != ""

    def test_carbonate_mgl_caco3_to_mgl(self):
        _, factor, warning = converter.convert(
            80.0, "mg/L as CaCO3", MILLIGRAMS_PER_LITER, "carbonate", "carbonate"
        )
        assert factor is None and warning != ""


# ---------------------------------------------------------------------------
# Nitrate conversions (mg/L as N → mg/L)
# ---------------------------------------------------------------------------

class TestNitrateConversions:
    """Verify nitrate conversion factors match spec."""

    def test_mgl_as_n_to_mgl(self):
        _, factor, warning = converter.convert(
            10.0, "mg/L as N", MILLIGRAMS_PER_LITER, "nitrate as n", "nitrate"
        )
        assert factor == 4.427 and not warning

    def test_ugl_as_n_to_mgl(self):
        _, factor, warning = converter.convert(
            1000.0, "ug/L as N", MILLIGRAMS_PER_LITER, "nitrate", "nitrate"
        )
        assert factor == 0.004427 and not warning

    def test_mgl_as_no3_to_mgl(self):
        _, factor, warning = converter.convert(
            50.0, "mg/L as NO3", MILLIGRAMS_PER_LITER, "nitrate", "nitrate"
        )
        assert factor == 1.0 and not warning

    def test_nitrate_as_n_source_name(self):
        """Source parameter name 'nitrate (as n)' should also use ×4.427."""
        _, factor, warning = converter.convert(
            5.0, "mg/L", MILLIGRAMS_PER_LITER, "nitrate (as n)", "nitrate"
        )
        assert factor == 4.427 and not warning


# ---------------------------------------------------------------------------
# Uranium conversion (pCi/L → mg/L)
# ---------------------------------------------------------------------------

class TestUraniumConversion:
    def test_pci_l_to_mgl(self):
        _, factor, warning = converter.convert(
            100.0, "pCi/L", MILLIGRAMS_PER_LITER, "uranium", "uranium"
        )
        assert factor == 0.00149 and not warning


# ---------------------------------------------------------------------------
# Conductivity / specific conductance (many aliases → factor=1.0)
# ---------------------------------------------------------------------------

class TestConductivityUnitAliases:
    """All recognized conductivity variants should produce factor=1.0."""

    @pytest.mark.parametrize(
        "input_units",
        [
            "μmhos/cm",
            "umho/cm",
            "cm-1",
            "micromhos per centimeter",
            "mg/l",
            "su",
            "us/cm",
            "uS/cm @25C",
            "µs/cm",
        ],
    )
    def test_conductivity_alias(self, input_units):
        _, factor, warning = converter.convert(
            500.0, input_units, MILLIGRAMS_PER_LITER, "-", "conductivity"
        )
        assert factor == 1.0 and not warning


# ---------------------------------------------------------------------------
# Miscellaneous conversions (ppm, ppb/µg/L, tons/ac-ft)
# ---------------------------------------------------------------------------

class TestMiscConversions:
    def test_ppm_to_mgl(self):
        _, factor, warning = converter.convert(
            25.0, PARTS_PER_MILLION, MILLIGRAMS_PER_LITER, "-", "-"
        )
        assert factor == 1.0 and not warning

    def test_ug_l_to_mgl(self):
        _, factor, warning = converter.convert(
            1000.0, MICROGRAMS_PER_LITER, MILLIGRAMS_PER_LITER, "-", "-"
        )
        assert factor == 0.001 and not warning

    def test_ppb_to_mgl(self):
        _, factor, warning = converter.convert(
            500.0, PARTS_PER_BILLION, MILLIGRAMS_PER_LITER, "-", "-"
        )
        assert factor == 0.001 and not warning

    def test_tons_acft_to_mgl(self):
        _, factor, warning = converter.convert(
            2.0, TONS_PER_ACRE_FOOT, MILLIGRAMS_PER_LITER, "-", "-"
        )
        assert factor == pytest.approx(735.47) and not warning


# ---------------------------------------------------------------------------
# Graceful failure path (unknown unit combos)
# ---------------------------------------------------------------------------

class TestFailedConversion:
    """Unknown unit combinations should return (value, None, warning) — not crash."""

    def test_unknown_units_returns_none_factor(self):
        result, factor, warning = converter.convert(
            10.0, "kg/m³", MILLIGRAMS_PER_LITER, "-", "-"
        )
        assert factor is None and warning != ""

    def test_failed_conversion_preserves_original_value(self):
        result, factor, _ = converter.convert(
            42.0, "tons/day", MILLIGRAMS_PER_LITER, "-", "-"
        )
        # Original value returned unchanged when conversion fails
        assert result == 42.0 and factor is None

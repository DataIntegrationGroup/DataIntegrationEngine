"""Unit tests for the transformer's pure helpers — datum reprojection, length
unit conversion, datetime standardization, and the NM containment check.
Network-free (the NM boundary is loaded from a local cache)."""
import pytest

from backend.constants import FEET, METERS
from backend.transformer import (
    BaseTransformer,
    standardize_datetime,
    transform_horizontal_datum,
    transform_length_units,
)


class TestTransformHorizontalDatum:
    def test_same_datum_passthrough(self):
        assert transform_horizontal_datum(-106.5, 34.0, "WGS84", "WGS84") == (
            -106.5,
            34.0,
            "WGS84",
        )

    def test_nad27_reproject_applies_shift(self):
        # NAD27 -> WGS84 is a real ~50-100 m shift in NM. Regression guard: the
        # datum_transform axis-order bug used to return the input unchanged.
        x, y, datum = transform_horizontal_datum(-106.5, 34.0, "NAD27", "WGS84")
        assert datum == "WGS84"
        # shifted, but still the same point (~0.0006 deg), and NOT axis-swapped
        assert x == pytest.approx(-106.5006, abs=1e-3) and x != -106.5
        assert y == pytest.approx(34.0001, abs=1e-3)

    def test_nad83_reproject_negligible_shift(self):
        # NAD83 ~= WGS84 (sub-meter); returns essentially the same coords
        x, y, datum = transform_horizontal_datum(-106.5, 34.0, "NAD83", "WGS84")
        assert datum == "WGS84"
        assert (x, y) == pytest.approx((-106.5, 34.0), abs=1e-5)


class TestTransformLengthUnits:
    def test_ft_to_m(self):
        assert transform_length_units(100, FEET, METERS) == pytest.approx((30.48, METERS))

    def test_m_to_ft(self):
        v, u = transform_length_units(100, METERS, FEET)
        assert v == pytest.approx(328.084) and u == FEET

    def test_same_unit_passthrough(self):
        assert transform_length_units(12.0, FEET, FEET) == (12.0, FEET)

    def test_word_input_unit_normalized(self):
        assert transform_length_units(100, "feet", METERS) == pytest.approx((30.48, METERS))

    def test_non_numeric_returns_none(self):
        assert transform_length_units("nope", FEET, METERS) == (None, METERS)


class TestStandardizeDatetime:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("2024-01-15T08:30:00", ("2024-01-15", "08:30:00")),
            ("2024-01-15T08:30:00.123Z", ("2024-01-15", "08:30:00")),
            ("2024-01-15 08:30:00", ("2024-01-15", "08:30:00")),
            ("2024-01-15", ("2024-01-15", "")),
            ("2024-01-15 00:00:00", ("2024-01-15", "")),  # midnight -> no time
            ("2024/01/15", ("2024-01-15", "")),
            ("01/15/2024", ("2024-01-15", "")),
            ("2024-01", ("2024-01", "")),
        ],
    )
    def test_formats(self, raw, expected):
        assert standardize_datetime(raw, "site1") == expected

    def test_tuple_input(self):
        assert standardize_datetime(("2024-01-15", "08:30:00"), "s") == (
            "2024-01-15",
            "08:30:00",
        )

    def test_excel_serial_number(self):
        # OSE Roswell (Ft Sumner) reports Excel date serials
        assert standardize_datetime("45000", "s") == ("2023-03-17", "")

    def test_unparseable_raises(self):
        with pytest.raises(ValueError):
            standardize_datetime("not-a-date", "site1")


class TestInNM:
    @pytest.fixture
    def t(self):
        return BaseTransformer()

    def test_point_inside_nm(self, t):
        assert t.in_nm(-106.0, 34.0) is True

    def test_point_outside_nm(self, t):
        assert t.in_nm(-74.0, 40.7) is False  # NYC

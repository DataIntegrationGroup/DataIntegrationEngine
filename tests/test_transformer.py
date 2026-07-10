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

    def test_different_datum_returns_out_datum(self):
        # Reproject NAD27 -> WGS84; the output datum is the target and the coords
        # stay in NM (the shift magnitude is a pyproj/datum-grid detail).
        x, y, datum = transform_horizontal_datum(-106.5, 34.0, "NAD27", "WGS84")
        assert datum == "WGS84"
        assert x == pytest.approx(-106.5, abs=0.01)
        assert y == pytest.approx(34.0, abs=0.01)


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

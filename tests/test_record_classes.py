"""Tests for backend/record.py — record class behavior, defaults, and sigfig rounding.

Validates that SiteRecord applies its default values correctly, that sigfig
rounding is applied to the documented fields (latitude, longitude, elevation,
min/max/mean), and that ParameterRecord / SummaryRecord expose their expected
key sets without missing entries.
"""
import pytest

from backend.record import BaseRecord, ParameterRecord, SiteRecord, SummaryRecord


class TestSiteRecordDefaults:
    """SiteRecord should apply defaults for optional fields."""

    def test_default_elevation_units_and_datum(self):
        rec = SiteRecord({"source": "test", "id": "W1", "latitude": 34.0, "longitude": -106.0})
        assert rec.__getattr__("elevation_units") == "ft"
        assert rec.__getattr__("horizontal_datum") == "WGS84"

    def test_explicit_values_override_defaults(self):
        payload = {
            "source": "test",
            "id": "W2",
            "latitude": 35.0,
            "longitude": -107.0,
            "elevation_units": "m",
            "horizontal_datum": "NAD83",
        }
        rec = SiteRecord(payload)
        assert rec.__getattr__("elevation_units") == "m"
        assert rec.__getattr__("horizontal_datum") == "NAD83"


class TestSigFigRounding:
    """Latitude/longitude round to 6 decimal places; min/max/mean to 2."""

    def test_latitude_longitude_rounded_to_six(self):
        payload = {
            "source": "test",
            "id": "W1",
            "latitude": 34.12345678901,
            "longitude": -106.98765432100,
        }
        rec = SiteRecord(payload)
        assert rec.__getattr__("latitude") == pytest.approx(34.123457, abs=1e-6)
        assert rec.__getattr__("longitude") == pytest.approx(-106.987654, abs=1e-6)

    def test_summary_min_max_mean_rounded_to_two(self):
        payload = {
            "source": "test",
            "id": "W1",
            "min": 123.456789,
            "max": 987.654321,
            "mean": 555.555555,
        }
        rec = SummaryRecord(payload)
        assert rec.__getattr__("min") == pytest.approx(123.46, abs=1e-2)
        assert rec.__getattr__("max") == pytest.approx(987.65, abs=1e-2)
        assert rec.__getattr__("mean") == pytest.approx(555.56, abs=1e-2)

    def test_elevation_rounded_to_two(self):
        payload = {
            "source": "test",
            "id": "W1",
            "latitude": 34.0,
            "longitude": -106.0,
            "elevation": 5280.987654,
        }
        rec = SiteRecord(payload)
        assert rec.__getattr__("elevation") == pytest.approx(5280.99, abs=1e-2)


class TestSummaryRecordKeys:
    """Verify SummaryRecord exposes all expected keys."""

    def test_expected_keys_present(self):
        # These are the documented fields from README.md summary table spec.
        required = {
            "source",
            "id",
            "name",
            "latitude",
            "longitude",
            "horizontal_datum",
            "elevation",
            "elevation_units",
            "well_depth",
            "well_depth_units",
            "parameter_name",
            "parameter_units",
            "nrecords",
            "min",
            "max",
            "mean",
            "earliest_date",
            "latest_value",
            "latest_units",
        }
        assert required.issubset(set(SummaryRecord.keys))


class TestParameterRecordKeys:
    """Verify ParameterRecord exposes all expected keys."""

    def test_expected_keys_present(self):
        # Documented fields from README.md time series table spec.
        required = {
            "source",
            "id",
            "parameter_name",
            "parameter_value",
            "parameter_units",
            "date_measured",
            "source_parameter_name",
            "source_parameter_units",
            "conversion_factor",
        }
        assert required.issubset(set(ParameterRecord.keys))

"""Cross-source approval-status normalization."""
import pytest

from backend.quality import (
    normalize_approval_status,
    APPROVED,
    PROVISIONAL,
    UNKNOWN,
)


class TestNormalizeApprovalStatus:
    @pytest.mark.parametrize(
        "raw",
        ["Approved", "Accepted", "Final", "Validated", "Historical", "PUBLIC RELEASE"],
    )
    def test_approved_terms(self, raw):
        assert normalize_approval_status(raw) == APPROVED

    @pytest.mark.parametrize(
        "raw", ["Provisional", "Preliminary", "Working", "estimated", "Not Released"]
    )
    def test_provisional_terms(self, raw):
        assert normalize_approval_status(raw) == PROVISIONAL

    def test_boolean_public_release_flag(self):
        # NMBGMR AMP PublicRelease
        assert normalize_approval_status(True) == APPROVED
        assert normalize_approval_status(False) == PROVISIONAL

    @pytest.mark.parametrize("raw", [None, "", "  ", "something weird"])
    def test_unknown(self, raw):
        assert normalize_approval_status(raw) == UNKNOWN

    def test_provisional_wins_in_compound_string(self):
        # substring fallback must not read "unapproved" as approved
        assert normalize_approval_status("provisional data") == PROVISIONAL


class TestNormalizedFieldOnRecord:
    def test_extract_parameter_adds_normalized_field(self):
        # a source's raw approval_status is normalized centrally in _extract_parameter
        from backend.config import Config
        from backend.connectors.usgs.source import NWISWaterLevelSource

        s = NWISWaterLevelSource()
        c = Config()
        c.parameter = "waterlevels"
        s.set_config(c)
        std = s._standardize_record(
            {
                "properties": {
                    "monitoring_location_id": "USGS-1",
                    "value": 10.0,
                    "time": "2024-01-15T00:00:00Z",
                    "unit_of_measure": "ft",
                    "approval_status": "Provisional",
                    "qualifier": None,
                }
            }
        )
        out = s._extract_parameter(std)
        assert out["approval_status"] == "Provisional"  # raw preserved
        assert out["approval_status_normalized"] == PROVISIONAL  # derived

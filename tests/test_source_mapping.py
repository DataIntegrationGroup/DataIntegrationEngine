"""Tests for backend/config.py — parameter-source mapping completeness.

Validates that PARAMETER_SOURCE_MAP and ANALYTE_OPTIONS from constants.py are
in sync, catching drift between the two configuration sources.
"""
from backend.config import Config, PARAMETER_SOURCE_MAP, SOURCE_KEYS
from backend.constants import (
    PARAMETER_OPTIONS,
    ANALYTE_OPTIONS,
)


class TestParameterSourceMapCompleteness:
    """Every parameter in PARAMETER_OPTIONS should have ≥1 source defined."""

    def test_every_parameter_has_at_least_one_source(self):
        for param in PARAMETER_OPTIONS:
            assert param in PARAMETER_SOURCE_MAP, (
                f"Parameter '{param}' missing from PARAMETER_SOURCE_MAP"
            )

    def test_no_empty_agency_lists(self):
        """No parameter should map to an empty agencies list."""
        for param, info in PARAMETER_SOURCE_MAP.items():
            assert len(info["agencies"]) >= 1, (
                f"Parameter '{param}' has zero agencies in mapping"
            )


class TestAnalyteOptionsSync:
    """ANALYTE_OPTIONS should match the analytes defined in PARAMETER_SOURCE_MAP."""

    def test_analyte_options_covered_by_source_map(self):
        """Every entry in ANALYTE_OPTIONS must appear as a key in PARAMETER_SOURCE_MAP."""
        for analyte in ANALYTE_OPTIONS:
            assert analyte in PARAMETER_SOURCE_MAP, (
                f"Analyte '{analyte}' in ANALYTE_OPTIONS but not in "
                f"PARAMETER_SOURCE_MAP"
            )

    def test_source_map_agencies_are_valid_keys(self):
        """Every agency referenced by the parameter map is a real source key."""
        for param, info in PARAMETER_SOURCE_MAP.items():
            for agency in info["agencies"]:
                assert agency in SOURCE_KEYS, (
                    f"Agency '{agency}' used for parameter '{param}' but not "
                    f"in SOURCE_KEYS"
                )

"""Consistency tests for the source registry (backend.config.SOURCES).

These tie the derived lookup tables and the empirical PARAMETER_SOURCE_MAP back
to the single SOURCES registry, so a source wired in one place but not another
fails fast instead of silently dropping out of a parameter's source list.
"""
from backend.config import (
    SOURCES,
    SOURCE_DICT,
    SOURCE_KEYS,
    ANALYTE_SOURCE_PAIRS,
    WATERLEVEL_SOURCE_PAIRS,
    PARAMETER_SOURCE_MAP,
)
from backend.constants import WATERLEVELS


def test_keys_unique():
    keys = [s.key for s in SOURCES]
    assert len(keys) == len(set(keys))


def test_derived_tables_match_registry():
    assert SOURCE_DICT == {s.key: s.site for s in SOURCES}
    assert SOURCE_KEYS == sorted(s.key for s in SOURCES)
    assert WATERLEVEL_SOURCE_PAIRS == {
        s.key: (s.site, s.waterlevel) for s in SOURCES if s.waterlevel
    }
    assert ANALYTE_SOURCE_PAIRS == {
        s.key: (s.site, s.analyte) for s in SOURCES if s.analyte
    }


def test_waterlevel_agencies_match_registry():
    # Every source the parameter map lists for waterlevels must have a
    # waterlevel source class — and vice versa.
    registry_wl = {s.key for s in SOURCES if s.waterlevel}
    map_wl = set(PARAMETER_SOURCE_MAP[WATERLEVELS]["agencies"])
    assert map_wl == registry_wl


def test_analyte_agencies_have_analyte_source():
    # Every agency listed for any analyte must actually have an analyte source
    # class in the registry (the map is a subset per analyte; the registry is
    # the universe of analyte-capable sources).
    analyte_keys = {s.key for s in SOURCES if s.analyte}
    for parameter, entry in PARAMETER_SOURCE_MAP.items():
        if parameter == WATERLEVELS:
            continue
        missing = set(entry["agencies"]) - analyte_keys
        assert not missing, f"{parameter}: agencies without an analyte source: {missing}"


def test_every_map_agency_is_a_known_source():
    for entry in PARAMETER_SOURCE_MAP.values():
        for agency in entry["agencies"]:
            assert agency in SOURCE_DICT

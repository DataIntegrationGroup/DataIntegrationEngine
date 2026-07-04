from backend.well_correlation import (
    correlate_wells,
    haversine_m,
    site_key,
    _match_keys,
    _parse_references,
    _normalize_id,
    _usgs_station_keys,
)


def _site(source, rid, lat=None, lon=None, **kw):
    d = {"source": source, "id": rid, "latitude": lat, "longitude": lon}
    d.update(kw)
    return d


def _by_key(results):
    return {f"{_normalize_id(r['source'])}:{r['id']}": r for r in results}


class TestHelpers:
    def test_match_keys_strips_usgs_prefix(self):
        assert _match_keys("USGS-08313000") == {"USGS-08313000", "08313000"}
        assert _match_keys("  abc123 ") == {"ABC123"}
        assert _match_keys(None) == set()
        assert _match_keys("") == set()

    def test_parse_references_splits_delimiters(self):
        assert _parse_references("a, b; c|d/e") == ["a", "b", "c", "d", "e"]
        assert _parse_references(None) == []
        assert _parse_references("  ") == []

    def test_site_key(self):
        assert site_key({"source": "nmbgmr", "id": "X1"}) == "NMBGMR:X1"

    def test_haversine_known_distance(self):
        # ~111 km per degree of latitude.
        d = haversine_m(34.0, -106.0, 35.0, -106.0)
        assert abs(d - 111_195) < 500

    def test_usgs_station_keys_canonicalizes(self):
        # Spaced, spaceless, and name-embedded forms all yield the canonical id.
        assert _usgs_station_keys("330519 104134001") == {"330519104134001"}
        assert _usgs_station_keys("330519104134001") == {"330519104134001"}
        assert _usgs_station_keys("Smith Well 330519 104134001") == {"330519104134001"}
        assert _usgs_station_keys("USGS-330519104134001") == {"330519104134001"}
        # Shorter surface-water gage numbers must not be treated as stations.
        assert _usgs_station_keys("08313000") == set()
        assert _usgs_station_keys(None) == set()


class TestExplicitLinking:
    def test_usgs_site_id_links_across_prefix(self):
        sites = [
            _site("NMBGMR", "WELL1", usgs_site_id="08313000"),
            _site("USGS-NWIS", "USGS-08313000"),
        ]
        res = _by_key(correlate_wells(sites))
        a = res["NMBGMR:WELL1"]
        b = res["USGS-NWIS:USGS-08313000"]
        assert a["cluster_id"] == b["cluster_id"]
        assert a["cluster_size"] == 2
        assert a["match_method"] == "explicit"
        assert a["match_confidence"] == 0.95
        assert b["linked_site_ids"] == ["NMBGMR:WELL1"]
        assert a["linked_by_agency"]["USGS-NWIS"] == ["USGS-08313000"]

    def test_usgs_station_number_in_name_links_to_nwis(self):
        # NMBGMR records the USGS station number (spaced) in its name; NWIS
        # stores the canonical spaceless id. They must link explicitly.
        sites = [
            _site("NMBGMR", "WELL1", name="330519 104134001"),
            _site("USGS-NWIS", "USGS-330519104134001"),
        ]
        res = _by_key(correlate_wells(sites))
        a = res["NMBGMR:WELL1"]
        b = res["USGS-NWIS:USGS-330519104134001"]
        assert a["cluster_id"] == b["cluster_id"]
        assert a["cluster_size"] == 2
        assert a["match_method"] == "explicit"
        assert a["match_confidence"] == 0.95

    def test_usgs_station_number_spaced_in_alternate_id_links(self):
        sites = [
            _site("NMBGMR", "W", alternate_site_id="330519 104134001"),
            _site("USGS-NWIS", "USGS-330519104134001"),
        ]
        res = _by_key(correlate_wells(sites))
        assert res["NMBGMR:W"]["cluster_size"] == 2
        assert res["NMBGMR:W"]["match_method"] == "explicit"

    def test_surface_gage_not_matched_as_station(self):
        # An 8-digit gage in a name must not spuriously cluster with a well.
        sites = [
            _site("NMBGMR", "WELL1", name="near gage 08313000"),
            _site("USGS-NWIS", "USGS-330519104134001"),
        ]
        res = _by_key(correlate_wells(sites))
        assert res["NMBGMR:WELL1"]["cluster_size"] == 1
        assert res["USGS-NWIS:USGS-330519104134001"]["cluster_size"] == 1

    def test_alternate_site_id_multi_token(self):
        sites = [
            _site("NMBGMR", "W", alternate_site_id="POD-9; OTHER-1"),
            _site("NMOSEPOD", "POD-9"),
            _site("ISC", "OTHER-1"),
        ]
        res = _by_key(correlate_wells(sites))
        assert res["NMBGMR:W"]["cluster_size"] == 3
        assert res["NMBGMR:W"]["n_agencies"] == 3


class TestSpatialLinking:
    def test_close_across_agencies_with_depth(self):
        # ~30 m apart (within threshold/3 -> close bonus), depths agree.
        sites = [
            _site("NMBGMR", "A", 34.00000, -106.00000, well_depth=100),
            _site("PVACD", "B", 34.00027, -106.00000, well_depth=110),
        ]
        res = _by_key(correlate_wells(sites))
        assert res["NMBGMR:A"]["cluster_size"] == 2
        assert res["NMBGMR:A"]["match_method"] == "spatial"
        # base 0.35 + close 0.10 + depth 0.25
        assert res["NMBGMR:A"]["match_confidence"] == 0.7

    def test_spatial_only_without_depth_lower_confidence(self):
        sites = [
            _site("NMBGMR", "A", 34.00000, -106.00000),
            _site("PVACD", "B", 34.00027, -106.00000),
        ]
        res = _by_key(correlate_wells(sites))
        # base 0.35 + close 0.10, no corroborating attributes
        assert res["NMBGMR:A"]["match_confidence"] == 0.45

    def test_elevation_agreement_adds_confidence(self):
        base = [
            _site("NMBGMR", "A", 34.0, -106.0, elevation=1600.0),
            _site("PVACD", "B", 34.00027, -106.0, elevation=1605.0),
        ]
        c_elev = _by_key(correlate_wells(base))["NMBGMR:A"]["match_confidence"]
        no_elev = _by_key(
            correlate_wells(
                [
                    _site("NMBGMR", "A", 34.0, -106.0),
                    _site("PVACD", "B", 34.00027, -106.0),
                ]
            )
        )["NMBGMR:A"]["match_confidence"]
        assert c_elev > no_elev
        # base 0.35 + close 0.10 + elevation 0.12
        assert c_elev == 0.57

    def test_elevation_disagreement_rejects(self):
        sites = [
            _site("NMBGMR", "A", 34.0, -106.0, elevation=1600.0),
            _site("PVACD", "B", 34.00027, -106.0, elevation=1900.0),
        ]
        res = _by_key(correlate_wells(sites))
        assert res["NMBGMR:A"]["cluster_size"] == 1

    def test_name_token_overlap_adds_confidence(self):
        sites = [
            _site("NMBGMR", "SMITH RANCH 1", 34.0, -106.0, name="Smith Ranch"),
            _site("PVACD", "SMITH-RANCH", 34.00027, -106.0, name="Smith Ranch Well"),
        ]
        res = _by_key(correlate_wells(sites))
        a = res["NMBGMR:SMITH RANCH 1"]
        assert a["cluster_size"] == 2
        # base 0.35 + close 0.10 + name 0.10
        assert a["match_confidence"] == 0.55

    def test_confidence_capped_below_explicit(self):
        # Every corroborating signal present -> capped at 0.9 (< explicit 0.95).
        sites = [
            _site(
                "NMBGMR",
                "RANCHWELL",
                34.0,
                -106.0,
                well_depth=100,
                elevation=1600.0,
                name="Ranch Well",
            ),
            _site(
                "PVACD",
                "RANCHWELL",
                34.00009,
                -106.0,
                well_depth=105,
                elevation=1602.0,
                name="Ranch Well",
            ),
        ]
        res = _by_key(correlate_wells(sites))
        assert res["NMBGMR:RANCHWELL"]["match_confidence"] == 0.9

    def test_disagreeing_depth_blocks_link(self):
        sites = [
            _site("NMBGMR", "A", 34.0, -106.0, well_depth=100),
            _site("PVACD", "B", 34.00027, -106.0, well_depth=900),
        ]
        res = _by_key(correlate_wells(sites))
        assert res["NMBGMR:A"]["cluster_size"] == 1
        assert res["NMBGMR:A"]["match_method"] == "unmatched"

    def test_far_apart_not_linked(self):
        sites = [
            _site("NMBGMR", "A", 34.0, -106.0),
            _site("PVACD", "B", 34.1, -106.0),  # ~11 km
        ]
        res = _by_key(correlate_wells(sites))
        assert res["NMBGMR:A"]["cluster_size"] == 1

    def test_same_source_never_spatially_linked(self):
        sites = [
            _site("NMBGMR", "A", 34.0, -106.0),
            _site("NMBGMR", "B", 34.00009, -106.0),  # ~10 m, same agency
        ]
        res = _by_key(correlate_wells(sites))
        assert res["NMBGMR:A"]["cluster_size"] == 1


class TestPODLinking:
    def test_pod_in_cluster_explicit(self):
        sites = [
            _site("NMBGMR", "W", usgs_site_id=None, alternate_site_id="P1"),
            _site("NMOSEPOD", "P1"),
        ]
        res = _by_key(correlate_wells(sites))
        w = res["NMBGMR:W"]
        assert w["ose_pod_ids"] == ["P1"]
        assert w["ose_pod_link_method"] == "explicit"
        pod = res["NMOSEPOD:P1"]
        assert pod["is_ose_pod"] is True

    def test_pod_linked_spatially_when_no_explicit(self):
        sites = [
            _site("NMBGMR", "W", 34.0, -106.0),
            _site("NMOSEPOD", "P1", 34.00027, -106.0),  # ~30 m, no explicit ref
        ]
        res = _by_key(correlate_wells(sites))
        w = res["NMBGMR:W"]
        # NMBGMR and NMOSEPOD are different agencies within distance -> they also
        # cluster spatially, so POD is a member (explicit-membership) link.
        assert "P1" in w["ose_pod_ids"]

    def test_pod_spatial_association_without_clustering(self):
        # POD far enough that it does not cluster (>150 m) but within POD link
        # distance widened here, so it is only an advisory spatial POD link.
        sites = [
            _site("NMBGMR", "W", 34.0, -106.0),
            _site("NMOSEPOD", "P1", 34.0027, -106.0),  # ~300 m
        ]
        res = _by_key(
            correlate_wells(sites, max_link_distance_m=150, pod_link_distance_m=500)
        )
        w = res["NMBGMR:W"]
        assert w["cluster_size"] == 1  # not clustered
        assert w["ose_pod_ids"] == ["P1"]
        assert w["ose_pod_link_method"] == "spatial"


class TestGeneral:
    def test_every_input_well_emitted(self):
        sites = [
            _site("A", "1", 34.0, -106.0),
            _site("B", "2", 35.0, -107.0),
            _site("C", "3"),
        ]
        res = correlate_wells(sites)
        assert len(res) == 3

    def test_duplicate_collapsed(self):
        sites = [
            _site("A", "1", 34.0, -106.0),
            _site("A", "1", 34.0, -106.0),
        ]
        res = correlate_wells(sites)
        assert len(res) == 1

    def test_order_independent(self):
        sites = [
            _site("NMBGMR", "W", usgs_site_id="08313000"),
            _site("USGS-NWIS", "USGS-08313000"),
            _site("PVACD", "B", 34.0, -106.0),
        ]
        r1 = correlate_wells(sites)
        r2 = correlate_wells(list(reversed(sites)))
        assert r1 == r2

    def test_transitive_clustering(self):
        # A-B explicit, B-C spatial => all one cluster (mixed method).
        sites = [
            _site("NMBGMR", "A", 34.0, -106.0, alternate_site_id="BID"),
            _site("PVACD", "BID", 34.0, -106.0, well_depth=100),
            _site("ISC", "C", 34.00027, -106.0, well_depth=100),
        ]
        res = _by_key(correlate_wells(sites))
        cids = {
            res["NMBGMR:A"]["cluster_id"],
            res["PVACD:BID"]["cluster_id"],
            res["ISC:C"]["cluster_id"],
        }
        assert len(cids) == 1
        assert res["NMBGMR:A"]["match_method"] == "mixed"
        assert res["NMBGMR:A"]["match_confidence"] == 0.95

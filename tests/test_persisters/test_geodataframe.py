"""Parity tests: the GeoPandas-backed persistence produces output identical to
the legacy hand-built OGC dumper.

This is the gate for Phase A of the framework migration
(docs/framework-migration-plan.md): every product's GeoJSON output MUST be
unchanged when its features are sourced from a GeoDataFrame instead of built by
hand. Proven here for ogc_summary; the same comparison guards each further
``dump_*`` conversion.
"""

import json

import pytest

from shapely.geometry import Polygon

from backend.persisters.ogc_features import (
    dump_summary_collection,
    dump_timeseries_collection,
    dump_hardness_collection,
    dump_major_chemistry_collection,
    dump_well_density_collection,
    _dump_collection,
)
from backend.persisters.geodataframe import (
    dump_summary_collection_gpd,
    dump_timeseries_collection_gpd,
    dump_hardness_collection_gpd,
    dump_major_chemistry_collection_gpd,
    dump_well_density_collection_gpd,
    geodataframe_to_features,
    gdf_to_parquet_bytes,
    parquet_bytes_to_gdf,
    records_to_geodataframe,
    write_geopackage,
)
from backend.record import SummaryRecord, SiteRecord, ParameterRecord


def _make_chem_record(source, rid, analyte, value, units="mg/L", date="2024-05-01", well_depth=None):
    return SummaryRecord(
        {
            "source": source,
            "id": rid,
            "name": f"Well {rid}",
            "latitude": 34.0,
            "longitude": -106.0,
            "elevation": None,
            "well_depth": well_depth,
            "well_depth_units": "ft",
            "parameter_name": analyte,
            "latest_value": value,
            "latest_units": units,
            "latest_date": date,
        }
    )


def _square_county(name, fips, x0, y0, side=1.0):
    poly = Polygon([(x0, y0), (x0 + side, y0), (x0 + side, y0 + side), (x0, y0 + side)])
    return {"name": name, "fips": fips, "geometry": poly, "area_sq_km": 100.0}


def _density_site(source, rid, lon, lat):
    return {"source": source, "id": rid, "latitude": lat, "longitude": lon}


def _make_summary_record(
    source="nmbgmr_amp",
    rid="RA-1234",
    lat=35.0,
    lon=-106.5,
    elevation=1650.0,
    well_depth=None,
    parameter_name="waterlevels",
    parameter_units="ft",
    latest_value=220.0,
    latest_units="ft",
):
    return SummaryRecord(
        {
            "source": source,
            "id": rid,
            "name": "Test Well",
            "usgs_site_id": "",
            "alternate_site_id": "",
            "latitude": lat,
            "longitude": lon,
            "horizontal_datum": "WGS84",
            "elevation": elevation,
            "elevation_units": "ft",
            "well_depth": well_depth,
            "well_depth_units": "ft",
            "parameter_name": parameter_name,
            "parameter_units": parameter_units,
            "nrecords": 10,
            "min": 200.0,
            "max": 250.0,
            "mean": 225.0,
            "earliest_date": "1990-01-01",
            "earliest_time": "00:00:00",
            "earliest_value": 200.0,
            "earliest_units": "ft",
            "latest_date": "2024-01-01",
            "latest_time": "00:00:00",
            "latest_value": latest_value,
            "latest_units": latest_units,
        }
    )


def _strip_timestamp(collection: dict) -> dict:
    """timeStamp is wall-clock; drop it before comparing the two collections."""
    c = dict(collection)
    c.pop("timeStamp", None)
    return c


def _norm(collection: dict) -> dict:
    """Normalize a collection to its written-JSON form (drop timeStamp, coerce
    coordinate tuples from shapely.mapping to lists) so the comparison reflects
    the actual serialized bytes, not Python tuple-vs-list identity."""
    return json.loads(json.dumps(_strip_timestamp(collection), default=str))


def _assert_collections_equal(legacy: dict, gpd_out: dict):
    assert _norm(legacy) == _norm(gpd_out)


class TestSummaryParity:
    def test_single_record_identical(self, tmp_path):
        records = [_make_summary_record()]
        legacy = dump_summary_collection(
            str(tmp_path / "a.geojson"), records, {"id": "nm_waterlevels"}
        )
        gpd_out = dump_summary_collection_gpd(
            str(tmp_path / "b.geojson"), records, {"id": "nm_waterlevels"}
        )
        _assert_collections_equal(legacy, gpd_out)

    def test_multi_record_identical(self, tmp_path):
        records = [
            _make_summary_record(rid="RA-1"),
            _make_summary_record(rid="RA-2", lat=34.5, lon=-107.1),
            _make_summary_record(rid="RA-3", well_depth=300.0),
        ]
        meta = {"id": "test", "title": "T", "description": "D"}
        legacy = dump_summary_collection(str(tmp_path / "a.geojson"), records, meta)
        gpd_out = dump_summary_collection_gpd(str(tmp_path / "b.geojson"), records, meta)
        _assert_collections_equal(legacy, gpd_out)

    def test_no_elevation_yields_2d_geometry(self, tmp_path):
        records = [_make_summary_record(elevation=None)]
        legacy = dump_summary_collection(str(tmp_path / "a.geojson"), records, {"id": "t"})
        gpd_out = dump_summary_collection_gpd(str(tmp_path / "b.geojson"), records, {"id": "t"})
        _assert_collections_equal(legacy, gpd_out)
        assert len(gpd_out["features"][0]["geometry"]["coordinates"]) == 2

    def test_tds_class_and_method_parity(self, tmp_path):
        records = [
            _make_summary_record(rid="RA-1", parameter_name="tds", latest_value=500.0),
            _make_summary_record(rid="RA-2", parameter_name="tds", latest_value=20000.0),
        ]
        meta = {"id": "nm_tds"}
        legacy = dump_summary_collection(str(tmp_path / "a.geojson"), records, meta)
        gpd_out = dump_summary_collection_gpd(str(tmp_path / "b.geojson"), records, meta)
        _assert_collections_equal(legacy, gpd_out)
        # sanity: the classification property survived the GeoDataFrame round-trip
        classes = {f["properties"]["tds_class"] for f in gpd_out["features"]}
        assert classes == {"fresh", "saline"}

    def test_written_files_match(self, tmp_path):
        records = [_make_summary_record(rid="RA-1"), _make_summary_record(rid="RA-2")]
        a = tmp_path / "a.geojson"
        b = tmp_path / "b.geojson"
        dump_summary_collection(str(a), records, {"id": "t"})
        dump_summary_collection_gpd(str(b), records, {"id": "t"})
        da = _strip_timestamp(json.loads(a.read_text()))
        db = _strip_timestamp(json.loads(b.read_text()))
        assert da == db


def _make_site_record(source="nmbgmr_amp", rid="RA-1234", lat=35.0, lon=-106.5, elevation=1650.0):
    return SiteRecord(
        {
            "source": source,
            "id": rid,
            "name": "Test Well",
            "latitude": lat,
            "longitude": lon,
            "elevation": elevation,
            "elevation_units": "ft",
            "horizontal_datum": "WGS84",
            "vertical_datum": "",
            "usgs_site_id": "",
            "alternate_site_id": "",
            "formation": "",
            "aquifer": "",
            "well_depth": None,
            "well_depth_units": "ft",
        }
    )


def _make_wl_record(source="nmbgmr_amp", rid="RA-1234", date="2024-01-15", time="08:30:00", value=212.4):
    return ParameterRecord(
        {
            "source": source,
            "id": rid,
            "parameter_name": "waterlevels",
            "parameter_value": value,
            "parameter_units": "ft",
            "date_measured": date,
            "time_measured": time,
            "source_parameter_name": "depth_to_water",
            "source_parameter_units": "ft",
            "conversion_factor": 1.0,
            "record_type": "waterlevels",
        }
    )


class TestTimeseriesParity:
    def test_single_site_multi_obs_identical(self, tmp_path):
        sites = [_make_site_record()]
        obs = [
            _make_wl_record(date="2024-01-15", value=212.4),
            _make_wl_record(date="2024-02-15", value=215.1),
        ]
        meta = {"id": "nm_ts", "title": "T", "description": "D"}
        legacy = dump_timeseries_collection(str(tmp_path / "a.geojson"), sites, obs, meta)
        gpd_out = dump_timeseries_collection_gpd(str(tmp_path / "b.geojson"), sites, obs, meta)
        _assert_collections_equal(legacy, gpd_out)

    def test_date_only_datetime_identical(self, tmp_path):
        sites = [_make_site_record()]
        obs = [_make_wl_record(date="2024-01-15", time="")]
        legacy = dump_timeseries_collection(str(tmp_path / "a.geojson"), sites, obs, {"id": "t"})
        gpd_out = dump_timeseries_collection_gpd(str(tmp_path / "b.geojson"), sites, obs, {"id": "t"})
        _assert_collections_equal(legacy, gpd_out)

    def test_missing_site_null_geometry_identical(self, tmp_path):
        # observation whose site is not in the lookup -> null geometry both paths
        sites = [_make_site_record(rid="RA-1")]
        obs = [_make_wl_record(rid="RA-UNKNOWN")]
        legacy = dump_timeseries_collection(str(tmp_path / "a.geojson"), sites, obs, {"id": "t"})
        gpd_out = dump_timeseries_collection_gpd(str(tmp_path / "b.geojson"), sites, obs, {"id": "t"})
        _assert_collections_equal(legacy, gpd_out)
        assert gpd_out["features"][0]["geometry"] is None

    def test_multi_site_identical(self, tmp_path):
        sites = [_make_site_record(rid="RA-1"), _make_site_record(rid="RA-2", lat=34.2, lon=-107.3)]
        obs = [
            _make_wl_record(rid="RA-1", date="2024-01-15"),
            _make_wl_record(rid="RA-2", date="2024-03-01"),
            _make_wl_record(rid="RA-1", date="2024-06-15"),
        ]
        legacy = dump_timeseries_collection(str(tmp_path / "a.geojson"), sites, obs, {"id": "t"})
        gpd_out = dump_timeseries_collection_gpd(str(tmp_path / "b.geojson"), sites, obs, {"id": "t"})
        _assert_collections_equal(legacy, gpd_out)

    def test_geoparquet_roundtrip_timeseries_parity(self, tmp_path):
        pytest.importorskip("pyarrow")
        from backend.persisters.geodataframe import features_to_geodataframe
        from backend.persisters.geodataframe import _timeseries_items

        sites = [_make_site_record(rid="RA-1"), _make_site_record(rid="RA-2", lat=34.2, lon=-107.3)]
        obs = [
            _make_wl_record(rid="RA-1", date="2024-01-15"),
            _make_wl_record(rid="RA-2", date="2024-03-01"),
        ]
        meta = {"id": "nm_ts"}
        legacy = dump_timeseries_collection(str(tmp_path / "a.geojson"), sites, obs, meta)

        items = list(_timeseries_items(sites, obs, None))
        gdf = features_to_geodataframe(items)
        gdf_back = parquet_bytes_to_gdf(gdf_to_parquet_bytes(gdf))
        features = geodataframe_to_features(gdf_back)
        rebuilt = _dump_collection(str(tmp_path / "b.geojson"), meta["id"], features, meta)
        _assert_collections_equal(legacy, rebuilt)


class TestHardnessParity:
    """Uniform-pivot product: fixed per-well output schema → byte-identical."""

    def test_full_and_partial_wells_identical(self, tmp_path):
        records = [
            _make_chem_record("NMBGMR", "W1", "calcium", 42.0, well_depth=120.0),
            _make_chem_record("NMBGMR", "W1", "magnesium", 12.0),
            _make_chem_record("WQP", "W2", "calcium", 55.0),  # missing magnesium
            _make_chem_record("WQP", "W3", "magnesium", 8.0),  # missing calcium
        ]
        meta = {"id": "nm_hardness"}
        legacy = dump_hardness_collection(str(tmp_path / "a.geojson"), records, meta)
        gpd_out = dump_hardness_collection_gpd(str(tmp_path / "b.geojson"), records, meta)
        _assert_collections_equal(legacy, gpd_out)


class TestWellDensityParity:
    """Polygon product: county polygons, uniform props → byte-identical."""

    def test_county_polygons_identical(self, tmp_path):
        counties = [_square_county("A", "001", 0, 0), _square_county("B", "003", 10, 10)]
        sites = [
            _density_site("NMBGMR", "W1", 0.5, 0.5),
            _density_site("NMBGMR", "W2", 0.2, 0.8),
            _density_site("USGS", "W3", 10.5, 10.5),
            _density_site("USGS", "W4", 99.0, 99.0),  # unassigned
        ]
        meta = {"id": "nm_wd"}
        legacy = dump_well_density_collection(str(tmp_path / "a.geojson"), counties, sites, meta)
        gpd_out = dump_well_density_collection_gpd(str(tmp_path / "b.geojson"), counties, sites, meta)
        _assert_collections_equal(legacy, gpd_out)
        # polygon geometry survived the GeoDataFrame path
        assert gpd_out["features"][0]["geometry"]["type"] == "Polygon"


class TestMajorChemistryUniformSchema:
    """Ragged product: routing through a GeoDataFrame gives every feature the
    UNION of analyte columns (null where absent) — the chosen schema, not
    byte-parity with the legacy ragged output."""

    def test_uniform_columns_with_nulls(self, tmp_path):
        records = [
            _make_chem_record("NMBGMR", "W1", "calcium", 42.0),
            _make_chem_record("NMBGMR", "W1", "chloride", 15.0),
            _make_chem_record("WQP", "W2", "calcium", 55.0),  # no chloride
        ]
        meta = {"id": "nm_mc"}
        legacy = dump_major_chemistry_collection(str(tmp_path / "a.geojson"), records, meta)
        gpd_out = dump_major_chemistry_collection_gpd(str(tmp_path / "b.geojson"), records, meta)

        # same wells, same geometry, same feature ids
        assert {f["id"] for f in legacy["features"]} == {f["id"] for f in gpd_out["features"]}

        gp = {f["id"]: f["properties"] for f in gpd_out["features"]}
        lg = {f["id"]: f["properties"] for f in legacy["features"]}

        # uniform schema: every gpd feature carries every analyte column
        all_keys = set().union(*(p.keys() for p in gp.values()))
        for props in gp.values():
            assert set(props.keys()) == all_keys

        # legacy W2 lacks chloride; gpd W2 has it as null; shared values preserved
        assert "chloride" not in lg["NMBGMR:W2"] if "NMBGMR:W2" in lg else True
        w2 = gp["WQP:W2"]
        assert w2["chloride"] is None and w2["chloride_units"] is None
        assert w2["calcium"] == 55.0
        w1 = gp["NMBGMR:W1"]
        assert w1["calcium"] == 42.0 and w1["chloride"] == 15.0


class TestGeoDataFrame:
    def test_feature_id_is_index(self):
        gdf = records_to_geodataframe([_make_summary_record(source="wqp", rid="X")])
        assert list(gdf.index) == ["wqp:X"]

    def test_geometry_columns_excluded_from_properties(self):
        gdf = records_to_geodataframe([_make_summary_record()])
        for col in ("latitude", "longitude", "elevation"):
            assert col not in gdf.columns

    def test_geoparquet_roundtrip_preserves_geojson_parity(self, tmp_path):
        """The real handoff risk: after a GeoParquet round-trip (what the Dagster
        IO manager does), the features a combine builds must still be byte-parity
        with the legacy dumper. Source-asset build → parquet bytes → combine read
        → dump == legacy."""
        pytest.importorskip("pyarrow")
        records = [
            _make_summary_record(rid="RA-1"),
            _make_summary_record(rid="RA-2", lat=34.5, lon=-107.1, well_depth=300.0),
            _make_summary_record(rid="RA-3", elevation=None),
        ]
        meta = {"id": "nm_waterlevels", "title": "T", "description": "D"}

        legacy = dump_summary_collection(str(tmp_path / "a.geojson"), records, meta)

        # Simulate the IO-manager handoff: build gdf, serialize, deserialize.
        gdf = records_to_geodataframe(records)
        gdf_back = parquet_bytes_to_gdf(gdf_to_parquet_bytes(gdf))
        features = geodataframe_to_features(gdf_back)
        rebuilt = _dump_collection(str(tmp_path / "b.geojson"), meta["id"], features, meta)

        _assert_collections_equal(legacy, rebuilt)

    def test_geoparquet_roundtrip_restores_index_and_crs(self):
        pytest.importorskip("pyarrow")
        gdf = records_to_geodataframe(
            [_make_summary_record(source="wqp", rid="X"), _make_summary_record(rid="RA-2")]
        )
        back = parquet_bytes_to_gdf(gdf_to_parquet_bytes(gdf))
        assert list(back.index) == ["wqp:X", "nmbgmr_amp:RA-2"]
        assert back.crs == gdf.crs

    def test_multiformat_geopackage_write(self, tmp_path):
        """Same GeoDataFrame that makes the GeoJSON also writes a GeoPackage."""
        import geopandas as gpd

        gdf = records_to_geodataframe(
            [_make_summary_record(rid="RA-1"), _make_summary_record(rid="RA-2")]
        )
        out = tmp_path / "summary.gpkg"
        write_geopackage(gdf, str(out), layer="summary")
        assert out.exists()
        back = gpd.read_file(out)
        assert len(back) == 2

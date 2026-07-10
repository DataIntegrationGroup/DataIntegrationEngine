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

from backend.persisters.ogc_features import (
    dump_summary_collection,
    dump_timeseries_collection,
    _dump_collection,
)
from backend.persisters.geodataframe import (
    dump_summary_collection_gpd,
    dump_timeseries_collection_gpd,
    geodataframe_to_features,
    gdf_to_parquet_bytes,
    parquet_bytes_to_gdf,
    records_to_geodataframe,
    write_geopackage,
)
from backend.record import SummaryRecord, SiteRecord, ParameterRecord


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


def _assert_collections_equal(legacy: dict, gpd_out: dict):
    assert _strip_timestamp(legacy) == _strip_timestamp(gpd_out)


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

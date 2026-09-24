"""Tests for the GeoPandas persistence primitives (backend/persisters/
geodataframe.py): the records/features → GeoDataFrame builders, the
``route_feature_dicts_through_gdf`` hook that makes every ``dump_*`` in
ogc_features GeoPandas-backed, and the GeoParquet inter-asset handoff.

Byte-parity of the routed dumpers against the pre-migration hand-built output was
proven per shape in the migration commits; the ongoing product-level regression
guard is tests/test_persisters/test_ogc_features.py. Here we cover the primitives
directly plus the round-trip the Dagster IO manager relies on.
"""

import json

import pytest
from shapely.geometry import Polygon

from backend.persisters.ogc_features import dump_summary_collection, _dump_collection
from backend.persisters.geodataframe import (
    dicts_to_parquet_bytes,
    features_to_geodataframe,
    geodataframe_to_features,
    collection_to_geoparquet,
    geojson_to_geopackage,
    gdf_to_parquet_bytes,
    parquet_bytes_to_dicts,
    parquet_bytes_to_gdf,
    parquet_bytes_to_timeseries,
    records_to_geodataframe,
    route_feature_dicts_through_gdf,
    timeseries_to_parquet_bytes,
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
    latest_value=220.0,
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
            "parameter_units": "ft",
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
            "latest_units": "ft",
        }
    )


def _norm(collection: dict) -> dict:
    """Normalize to the written-JSON form (drop volatile timeStamp; coerce
    shapely.mapping coordinate tuples to lists) so comparison reflects the
    serialized bytes, not Python tuple-vs-list identity."""
    c = dict(collection)
    c.pop("timeStamp", None)
    return json.loads(json.dumps(c, default=str))


class TestGeoParquetHandoff:
    """records → GeoDataFrame → GeoParquet bytes → GeoDataFrame → features must
    reproduce the product output. This is what the Dagster IO manager does."""

    def test_roundtrip_reproduces_summary_collection(self, tmp_path):
        pytest.importorskip("pyarrow")
        records = [
            _make_summary_record(rid="RA-1"),
            _make_summary_record(rid="RA-2", lat=34.5, lon=-107.1, well_depth=300.0),
            _make_summary_record(rid="RA-3", elevation=None),
        ]
        meta = {"id": "nm_waterlevels", "title": "T", "description": "D"}
        legacy = dump_summary_collection(str(tmp_path / "a.geojson"), records, meta)

        gdf = records_to_geodataframe(records)
        gdf_back = parquet_bytes_to_gdf(gdf_to_parquet_bytes(gdf))
        features = geodataframe_to_features(gdf_back)
        rebuilt = _dump_collection(str(tmp_path / "b.geojson"), meta["id"], features, meta)

        assert _norm(legacy) == _norm(rebuilt)

    def test_roundtrip_restores_index_and_crs(self):
        pytest.importorskip("pyarrow")
        gdf = records_to_geodataframe(
            [_make_summary_record(source="wqp", rid="X"), _make_summary_record(rid="RA-2")]
        )
        back = parquet_bytes_to_gdf(gdf_to_parquet_bytes(gdf))
        assert list(back.index) == ["wqp:X", "nmbgmr_amp:RA-2"]
        assert back.crs == gdf.crs


def _site_payload(source="nmbgmr_amp", rid="RA-1", lat=35.0, lon=-106.5):
    return SiteRecord(
        {
            "source": source, "id": rid, "name": "W", "latitude": lat, "longitude": lon,
            "elevation": 1650.0, "elevation_units": "ft", "horizontal_datum": "WGS84",
            "vertical_datum": "", "usgs_site_id": "", "alternate_site_id": "",
            "formation": "", "aquifer": "", "well_depth": None, "well_depth_units": "ft",
        }
    )._payload


def _obs_payload(source="nmbgmr_amp", rid="RA-1", date="2024-01-15", value=212.4):
    return ParameterRecord(
        {
            "source": source, "id": rid, "parameter_name": "waterlevels",
            "parameter_value": value, "parameter_units": "ft", "date_measured": date,
            "time_measured": "00:00:00", "source_parameter_name": "dtw",
            "source_parameter_units": "ft", "conversion_factor": 1.0,
            "record_type": "waterlevels",
        }
    )._payload


class TestPayloadParquetHandoff:
    """The Dagster source-asset payload {records, sites, timeseries} must survive
    Parquet serialization unchanged — this is what the IO manager relies on to
    replace the pickle handoff."""

    def test_records_roundtrip(self):
        pytest.importorskip("pyarrow")
        records = [
            _make_summary_record(rid="RA-1")._payload,
            _make_summary_record(rid="RA-2", well_depth=300.0)._payload,
        ]
        back = parquet_bytes_to_dicts(dicts_to_parquet_bytes(records))
        assert back == records

    def test_sites_roundtrip(self):
        pytest.importorskip("pyarrow")
        sites = [_site_payload(rid="RA-1"), _site_payload(rid="RA-2", lat=34.1)]
        back = parquet_bytes_to_dicts(dicts_to_parquet_bytes(sites))
        assert back == sites

    def test_timeseries_roundtrip_preserves_grouping(self):
        pytest.importorskip("pyarrow")
        timeseries = [
            [_obs_payload(rid="RA-1", date="2024-01-15"),
             _obs_payload(rid="RA-1", date="2024-02-15")],
            [_obs_payload(rid="RA-2", date="2024-03-01")],
        ]
        back = parquet_bytes_to_timeseries(timeseries_to_parquet_bytes(timeseries))
        assert back == timeseries

    def test_empty_payload_roundtrip(self):
        pytest.importorskip("pyarrow")
        assert parquet_bytes_to_dicts(dicts_to_parquet_bytes([])) == []
        assert parquet_bytes_to_timeseries(timeseries_to_parquet_bytes([])) == []

    def test_full_payload_roundtrip(self):
        """Simulate the whole source-asset payload through the handoff."""
        pytest.importorskip("pyarrow")
        payload = {
            "records": [_make_summary_record(rid="RA-1")._payload],
            "sites": [_site_payload(rid="RA-1"), _site_payload(rid="RA-2", lat=34.1)],
            "timeseries": [[_obs_payload(rid="RA-1")], [_obs_payload(rid="RA-2")]],
        }
        rebuilt = {
            "records": parquet_bytes_to_dicts(dicts_to_parquet_bytes(payload["records"])),
            "sites": parquet_bytes_to_dicts(dicts_to_parquet_bytes(payload["sites"])),
            "timeseries": parquet_bytes_to_timeseries(
                timeseries_to_parquet_bytes(payload["timeseries"])
            ),
        }
        assert rebuilt == payload
        # sites[i] <-> timeseries[i] alignment preserved
        assert len(rebuilt["sites"]) == len(rebuilt["timeseries"])


class TestRouting:
    """route_feature_dicts_through_gdf is the hook _dump_collection uses."""

    def test_idempotent(self):
        features = [
            {
                "type": "Feature",
                "id": "s:1",
                "geometry": {"type": "Point", "coordinates": [-106.5, 35.0]},
                "properties": {"source": "s", "id": "1", "v": 10},
            }
        ]
        once = route_feature_dicts_through_gdf(features)
        twice = route_feature_dicts_through_gdf(once)
        assert once == twice

    def test_reconstructs_polygon_geometry(self):
        poly = Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])
        from shapely.geometry import mapping

        features = [
            {
                "type": "Feature",
                "id": "county:001",
                "geometry": mapping(poly),
                "properties": {"county": "A", "well_count": 3},
            }
        ]
        routed = route_feature_dicts_through_gdf(features)
        assert routed[0]["geometry"]["type"] == "Polygon"
        assert len(routed[0]["geometry"]["coordinates"][0]) == 5

    def test_null_geometry_preserved(self):
        features = [
            {"type": "Feature", "id": "x", "geometry": None, "properties": {"a": 1}}
        ]
        routed = route_feature_dicts_through_gdf(features)
        assert routed[0]["geometry"] is None

    def test_ragged_features_gain_uniform_null_columns(self):
        features = [
            {
                "type": "Feature",
                "id": "w1",
                "geometry": {"type": "Point", "coordinates": [-106.0, 34.0]},
                "properties": {"id": "w1", "calcium": 42.0, "chloride": 15.0},
            },
            {
                "type": "Feature",
                "id": "w2",
                "geometry": {"type": "Point", "coordinates": [-106.0, 34.0]},
                "properties": {"id": "w2", "calcium": 55.0},  # no chloride
            },
        ]
        routed = route_feature_dicts_through_gdf(features)
        by_id = {f["id"]: f["properties"] for f in routed}
        # uniform schema: w2 gains chloride as null
        assert by_id["w2"]["chloride"] is None
        assert by_id["w1"]["chloride"] == 15.0

    def test_empty_passthrough(self):
        assert route_feature_dicts_through_gdf([]) == []


class TestGeoDataFrameBuilders:
    def test_records_feature_id_is_index(self):
        gdf = records_to_geodataframe([_make_summary_record(source="wqp", rid="X")])
        assert list(gdf.index) == ["wqp:X"]

    def test_geometry_columns_excluded_from_properties(self):
        gdf = records_to_geodataframe([_make_summary_record()])
        for col in ("latitude", "longitude", "elevation"):
            assert col not in gdf.columns

    def test_tds_class_column_added_for_tds(self):
        gdf = records_to_geodataframe(
            [_make_summary_record(parameter_name="tds", latest_value=500.0)]
        )
        assert gdf.iloc[0]["tds_class"] == "fresh"

    def test_features_to_geodataframe_basic(self):
        from shapely.geometry import Point

        gdf = features_to_geodataframe([("a:1", Point(-106, 35), {"v": 1})])
        assert list(gdf.index) == ["a:1"]
        assert gdf.iloc[0]["v"] == 1

    def test_multiformat_geopackage_write(self, tmp_path):
        import geopandas as gpd

        gdf = records_to_geodataframe(
            [_make_summary_record(rid="RA-1", lon=-106.5, lat=35.0),
             _make_summary_record(rid="RA-2", lon=-107.0, lat=34.0)]
        )
        out = tmp_path / "summary.gpkg"
        bbox = write_geopackage(gdf, str(out), layer="summary")
        assert out.exists()
        back = gpd.read_file(out)
        assert len(back) == 2
        # 2D bounds, EPSG:4326, spanning both points
        assert bbox == (-107.0, 34.0, -106.5, 35.0)
        assert back.geometry.iloc[0].has_z is False

    def test_write_geopackage_empty_raises(self, tmp_path):
        import geopandas as gpd

        empty = gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        with pytest.raises(ValueError):
            write_geopackage(empty, str(tmp_path / "e.gpkg"), layer="e")


class TestGeojsonToGeopackage:
    """The GeoServer publish path: GeoJSON file → GeoPackage + bounds."""

    def test_converts_product_geojson(self, tmp_path):
        import geopandas as gpd

        records = [
            _make_summary_record(rid="RA-1", lon=-106.5, lat=35.0),
            _make_summary_record(rid="RA-2", lon=-107.0, lat=34.0),
        ]
        geojson = tmp_path / "collection.geojson"
        dump_summary_collection(str(geojson), records, {"id": "nm_wl"})

        gpkg_path, bbox = geojson_to_geopackage(geojson, "nm_wl", tmp_path)
        assert gpkg_path.exists()
        assert gpkg_path.name == "nm_wl.gpkg"
        assert bbox == (-107.0, 34.0, -106.5, 35.0)
        back = gpd.read_file(gpkg_path)
        assert len(back) == 2
        assert back.geometry.iloc[0].has_z is False  # flattened for GeoServer


class TestCollectionToGeoParquet:
    """The pygeoapi path: product FeatureCollection → latest.parquet in the shape
    pygeoapi's Parquet provider needs (see generate_config.parquet_id_field)."""

    @staticmethod
    def _feature(fid, coords, **props):
        geom = None if coords is None else {"type": "Point", "coordinates": coords}
        return {"type": "Feature", "id": fid, "geometry": geom, "properties": props}

    def _write(self, tmp_path, features):
        pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        out = tmp_path / "latest.parquet"
        n = collection_to_geoparquet({"features": features}, out)
        return n, pq.read_table(out)

    def test_geoparquet_metadata_and_string_columns(self, tmp_path):
        n, table = self._write(
            tmp_path, [self._feature("a:1", [-106.5, 35.0], name="x", value=1.5)]
        )
        import pyarrow as pa

        assert n == 1
        geo = json.loads(table.schema.metadata[b"geo"])
        assert geo["version"] == "1.1.0"
        assert geo["columns"]["geometry"]["covering"]["bbox"]
        assert "bbox" in table.schema.names
        assert not any(pa.types.is_large_string(f.type) for f in table.schema)

    def test_feature_id_is_unique_top_level_id(self, tmp_path):
        _, table = self._write(
            tmp_path,
            [
                self._feature("src:W1:2024-01-01", [-106.5, 35.0], id=7),
                self._feature("src:W1:2024-01-01", [-106.5, 35.0], id=7),
                self._feature("src:W2:2024-01-01", [-106.6, 35.1], id=8),
            ],
        )
        rows = table.to_pydict()
        assert rows["feature_id"] == [
            "src:W1:2024-01-01",
            "src:W1:2024-01-01:2",
            "src:W2:2024-01-01",
        ]
        assert rows["id"] == [7, 7, 8]  # the property is kept as-is

    def test_datetime_is_utc_and_sorted(self, tmp_path):
        import pyarrow as pa

        _, table = self._write(
            tmp_path,
            [
                self._feature("c", [-106, 35], datetime="2316-03-02T00:00:00Z"),
                self._feature("a", [-106, 35], datetime="2024-01-01T06:30:00Z"),
                self._feature("b", [-106, 35], datetime="2020-05-01"),
                self._feature("d", [-106, 35], datetime="not a date"),
            ],
        )
        t = table.schema.field("datetime").type
        assert pa.types.is_timestamp(t) and t.tz == "UTC" and t.unit == "ms"
        values = table.to_pydict()["datetime"]
        assert [v.isoformat() if v else None for v in values] == [
            "2020-05-01T00:00:00+00:00",
            "2024-01-01T06:30:00+00:00",
            "2316-03-02T00:00:00+00:00",  # past pandas' 2262 ns limit
            None,
        ]

    def test_mixed_and_nested_values_become_strings(self, tmp_path):
        _, table = self._write(
            tmp_path,
            [
                self._feature("a", [-106, 35], value=1.5, status=True, q={"k": 1}),
                self._feature("b", [-106, 35], value="ND", status="Approved", q=[1, 2]),
                self._feature("c", None, value=None, status=None, q=None),
            ],
        )
        rows = table.to_pydict()
        assert rows["value"] == ["1.5", "ND", None]
        assert rows["status"] == ["True", "Approved", None]
        assert rows["q"] == ['{"k": 1}', "[1, 2]", None]

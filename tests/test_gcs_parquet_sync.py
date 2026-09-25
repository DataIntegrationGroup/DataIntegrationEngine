"""Tests for GCSResource's latest.parquet sync."""

import json

import pytest

pytest.importorskip("dagster")
pytest.importorskip("pyarrow")

from orchestration.resources import gcs as gcs_mod  # noqa: E402


class FakeBlob:
    def __init__(self, store, key):
        self.store, self.key = store, key
        self.metadata = None

    def exists(self):
        return self.key in self.store

    def reload(self):
        self.metadata = self.store[self.key]["metadata"]

    def upload_from_filename(self, path, content_type=None):
        with open(path, "rb") as f:
            self.store[self.key] = {"data": f.read(), "metadata": self.metadata}

    def delete(self):
        del self.store[self.key]


class FakeBucket:
    def __init__(self):
        self.store = {}

    def blob(self, key):
        return FakeBlob(self.store, key)

    def copy_blob(self, blob, bucket, new_key):
        self.store[new_key] = dict(self.store[blob.key])


def _resource(monkeypatch, bucket):
    res = gcs_mod.GCSResource(bucket_name="bkt")
    client = type("C", (), {"bucket": lambda self, name: bucket})()
    monkeypatch.setattr(gcs_mod, "_storage_client", lambda: client)
    return res


def _write_collection(tmp_path, n=2):
    path = tmp_path / "collection.geojson"
    features = [
        {
            "type": "Feature",
            "id": f"src:W{i}",
            "geometry": {"type": "Point", "coordinates": [-106.5, 35.0]},
            "properties": {"id": i},
        }
        for i in range(n)
    ]
    path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
    return path


PARQUET = "products/p/latest.parquet"


def test_first_upload_writes_parquet_with_content_hash(tmp_path, monkeypatch):
    bucket = FakeBucket()
    info = _resource(monkeypatch, bucket).upload_product(str(_write_collection(tmp_path)), "p")
    assert info["parquet_status"] == "written"
    assert info["parquet_uri"] == f"gs://bkt/{PARQUET}"
    geojson_hash = bucket.store["products/p/latest.geojson"]["metadata"]["content_hash"]
    assert bucket.store[PARQUET]["metadata"]["content_hash"] == geojson_hash
    assert bucket.store[PARQUET]["data"][:4] == b"PAR1"


def test_unchanged_geojson_backfills_missing_parquet_then_skips(tmp_path, monkeypatch):
    bucket = FakeBucket()
    res = _resource(monkeypatch, bucket)
    path = str(_write_collection(tmp_path))
    res.upload_product(path, "p")
    del bucket.store[PARQUET]  # e.g. products published before Parquet existed

    again = res.upload_product(path, "p")
    assert again["skipped"] is True
    assert again["parquet_status"] == "written"

    third = res.upload_product(path, "p")
    assert third["parquet_status"] == "unchanged"


def test_format_version_bump_rewrites_unchanged_geojson(tmp_path, monkeypatch):
    import backend.persisters.geodataframe as gdf_mod

    bucket = FakeBucket()
    res = _resource(monkeypatch, bucket)
    path = str(_write_collection(tmp_path))
    res.upload_product(path, "p")
    assert bucket.store[PARQUET]["metadata"]["parquet_format"] == gdf_mod.PARQUET_FORMAT_VERSION
    assert res.upload_product(path, "p")["parquet_status"] == "unchanged"

    monkeypatch.setattr(gdf_mod, "PARQUET_FORMAT_VERSION", "next")
    again = res.upload_product(path, "p")
    assert again["skipped"] is True  # GeoJSON unchanged
    assert again["parquet_status"] == "written"
    assert bucket.store[PARQUET]["metadata"]["parquet_format"] == "next"


def test_failed_conversion_deletes_stale_parquet(tmp_path, monkeypatch):
    bucket = FakeBucket()
    res = _resource(monkeypatch, bucket)
    res.upload_product(str(_write_collection(tmp_path, n=2)), "p")
    assert PARQUET in bucket.store

    def boom(data, out_path):
        raise ValueError("bad column")

    import backend.persisters.geodataframe as gdf_mod

    monkeypatch.setattr(gdf_mod, "collection_to_geoparquet", boom)
    info = res.upload_product(str(_write_collection(tmp_path, n=3)), "p")
    assert info["skipped"] is False  # the GeoJSON is still published
    assert info["parquet_status"] == "failed"
    assert "bad column" in info["parquet_error"]
    assert PARQUET not in bucket.store

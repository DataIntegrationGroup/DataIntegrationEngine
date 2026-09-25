"""Tests for orchestration/pygeoapi/generate_config.py: each product is served
from its latest.parquet (Parquet provider) only when the file is usable, and
from latest.geojson (OGR provider) otherwise."""

import importlib.util
from pathlib import Path

import pytest
import yaml

pytest.importorskip("jinja2")
pytest.importorskip("pyarrow")

from backend.persisters.geodataframe import collection_to_geoparquet  # noqa: E402

PYGEOAPI_DIR = Path(__file__).resolve().parents[1] / "orchestration" / "pygeoapi"
TEMPLATE = PYGEOAPI_DIR / "config.yml.j2"


def _load_generate_config():
    spec = importlib.util.spec_from_file_location(
        "generate_config", PYGEOAPI_DIR / "generate_config.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gc = _load_generate_config()


def _point(fid, **props):
    return {
        "type": "Feature",
        "id": fid,
        "geometry": {"type": "Point", "coordinates": [-106.5, 35.0]},
        "properties": props,
    }


def _polygon(fid):
    ring = [[-107, 34], [-106, 34], [-106, 35], [-107, 35], [-107, 34]]
    return {
        "type": "Feature",
        "id": fid,
        "geometry": {"type": "Polygon", "coordinates": [ring]},
        "properties": {"name": fid},
    }


def _write_parquet(root: Path, pid: str, features: list) -> None:
    (root / pid).mkdir(parents=True, exist_ok=True)
    collection_to_geoparquet({"features": features}, root / pid / "latest.parquet")


def _render(tmp_path, products, check_parquet=True, source_root=None, bake_to=None) -> dict:
    products_path = tmp_path / "products.yaml"
    products_path.write_text(yaml.safe_dump({"gcs_bucket": "bkt", "products": products}))
    out = tmp_path / "config.yml"
    gc.generate(products_path, TEMPLATE, out, check_parquet, source_root, bake_to)
    return yaml.safe_load(out.read_text())["resources"]


def _product(pid, output_type="ogc_summary"):
    return {"id": pid, "title": pid, "description": pid, "output_type": output_type}


def test_without_check_every_product_is_geojson_on_gcs(tmp_path):
    res = _render(tmp_path, [_product("a"), _product("ts", "ogc_timeseries")], False)
    for pid in ("a", "ts"):
        provider = res[pid]["providers"][0]
        assert provider["name"] == "OGR"
        assert provider["data"]["source"] == f"/vsigs/bkt/products/{pid}/latest.geojson"


def test_usable_point_parquet_is_served_with_feature_id(tmp_path):
    root = tmp_path / "products"
    _write_parquet(root, "a", [_point("x:1"), _point("x:1")])
    res = _render(tmp_path, [_product("a")], source_root=str(root))
    provider = res["a"]["providers"][0]
    assert provider["name"] == "Parquet"
    assert provider["data"]["source"] == f"{root}/a/latest.parquet"
    assert provider["id_field"] == "feature_id"
    assert "time_field" not in provider


def test_timeseries_parquet_gets_time_field(tmp_path):
    root = tmp_path / "products"
    _write_parquet(root, "ts", [_point("x:1", datetime="2024-01-01T00:00:00Z")])
    res = _render(tmp_path, [_product("ts", "ogc_timeseries")], source_root=str(root))
    provider = res["ts"]["providers"][0]
    assert provider["name"] == "Parquet"
    assert provider["time_field"] == "datetime"


@pytest.mark.parametrize(
    "setup, reason",
    [
        (lambda root: None, "no latest.parquet"),
        (lambda root: _write_parquet(root, "a", [_polygon("county")]), "non-point"),
        (lambda root: (root / "a").mkdir(parents=True) or (root / "a" / "latest.parquet").write_text("x"),
         "unreadable"),
    ],
)
def test_unusable_parquet_falls_back_to_geojson(tmp_path, capsys, setup, reason):
    root = tmp_path / "products"
    root.mkdir()
    setup(root)
    res = _render(tmp_path, [_product("a")], source_root=str(root))
    provider = res["a"]["providers"][0]
    assert provider["name"] == "OGR"
    assert provider["data"]["source"] == f"{root}/a/latest.geojson"
    assert reason in capsys.readouterr().out


def test_timeseries_needs_tz_aware_datetime(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Point

    root = tmp_path / "products"
    (root / "ts").mkdir(parents=True)
    gdf = gpd.GeoDataFrame(
        {"feature_id": ["x"], "datetime": [__import__("pandas").Timestamp("2024-01-01")]},
        geometry=[Point(-106.5, 35.0)],
        crs="EPSG:4326",
    )
    gdf.to_parquet(root / "ts" / "latest.parquet", write_covering_bbox=True)
    res = _render(tmp_path, [_product("ts", "ogc_timeseries")], source_root=str(root))
    assert res["ts"]["providers"][0]["name"] == "OGR"


def test_older_parquet_without_feature_id_uses_id(tmp_path):
    import geopandas as gpd
    from shapely.geometry import Point

    root = tmp_path / "products"
    (root / "a").mkdir(parents=True)
    gdf = gpd.GeoDataFrame({"id": ["w1"]}, geometry=[Point(-106.5, 35.0)], crs="EPSG:4326")
    gdf.to_parquet(root / "a" / "latest.parquet", write_covering_bbox=True)
    res = _render(tmp_path, [_product("a")], source_root=str(root))
    assert res["a"]["providers"][0]["id_field"] == "id"


def test_bake_copies_the_chosen_file_and_serves_it_locally(tmp_path):
    root, baked = tmp_path / "products", tmp_path / "baked"
    _write_parquet(root, "pts", [_point("x:1")])
    _write_parquet(root, "poly", [_polygon("county")])
    (root / "poly" / "latest.geojson").write_text('{"type": "FeatureCollection", "features": []}')
    res = _render(
        tmp_path, [_product("pts"), _product("poly")], False, source_root=str(root), bake_to=baked
    )
    assert res["pts"]["providers"][0]["data"]["source"] == f"{baked}/pts/latest.parquet"
    assert res["poly"]["providers"][0]["data"]["source"] == f"{baked}/poly/latest.geojson"
    assert sorted(str(p.relative_to(baked)) for p in baked.rglob("latest.*")) == [
        "poly/latest.geojson",
        "pts/latest.parquet",
    ]


def test_bake_fails_when_a_product_has_nothing_to_serve(tmp_path):
    root = tmp_path / "products"
    root.mkdir()
    with pytest.raises(SystemExit, match="gone: no latest.geojson"):
        _render(tmp_path, [_product("gone")], source_root=str(root), bake_to=tmp_path / "baked")

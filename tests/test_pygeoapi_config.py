"""Tests for orchestration/pygeoapi/generate_config.py."""

import importlib.util
from pathlib import Path

import pytest
import yaml

pytest.importorskip("jinja2")
pytest.importorskip("pyarrow")

from backend.persisters.geodataframe import collection_to_geoparquet  # noqa: E402

PYGEOAPI_DIR = Path(__file__).resolve().parents[1] / "orchestration" / "pygeoapi"
TEMPLATE = PYGEOAPI_DIR / "config.yml.j2"
PARQUET = "die_providers.DieParquetProvider"
OGR = "die_providers.DieOGRProvider"


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
        assert provider["name"] == OGR
        assert provider["data"]["source"] == f"/vsigs/bkt/products/{pid}/latest.geojson"


def test_usable_point_parquet_is_served_with_feature_id(tmp_path):
    root = tmp_path / "products"
    _write_parquet(root, "a", [_point("x:1"), _point("x:1")])
    res = _render(tmp_path, [_product("a")], source_root=str(root))
    provider = res["a"]["providers"][0]
    assert provider["name"] == PARQUET
    assert provider["data"]["source"] == f"{root}/a/latest.parquet"
    assert provider["id_field"] == "feature_id"
    assert "time_field" not in provider


def test_timeseries_parquet_gets_time_field(tmp_path):
    root = tmp_path / "products"
    _write_parquet(root, "ts", [_point("x:1", datetime="2024-01-01T00:00:00Z")])
    res = _render(tmp_path, [_product("ts", "ogc_timeseries")], source_root=str(root))
    provider = res["ts"]["providers"][0]
    assert provider["name"] == PARQUET
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
    assert provider["name"] == OGR
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
    assert res["ts"]["providers"][0]["name"] == OGR


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


# --- publish: listed / hidden / none ------------------------------------------


def test_listed_is_the_default_and_sets_no_visibility(tmp_path):
    res = _render(tmp_path, [_product("a"), {**_product("b"), "publish": "listed"}], False)
    assert "visibility" not in res["a"] and "visibility" not in res["b"]


def test_hidden_product_gets_visibility_hidden(tmp_path):
    res = _render(tmp_path, [{**_product("h"), "publish": "hidden"}], False)
    assert res["h"]["visibility"] == "hidden"
    assert res["h"]["providers"][0]["data"]["source"] == "/vsigs/bkt/products/h/latest.geojson"


def test_none_product_is_left_out_of_the_config_and_the_image(tmp_path):
    root, baked = tmp_path / "products", tmp_path / "baked"
    _write_parquet(root, "a", [_point("x:1")])
    _write_parquet(root, "off", [_point("x:1")])
    products = [_product("a"), {**_product("off"), "publish": "none"}]
    res = _render(tmp_path, products, source_root=str(root), bake_to=baked)
    assert list(res) == ["a"]
    assert not (baked / "off").exists()


def test_none_product_without_files_does_not_fail_the_bake(tmp_path):
    root = tmp_path / "products"
    _write_parquet(root, "a", [_point("x:1")])
    products = [_product("a"), {**_product("gone"), "publish": "none"}]
    res = _render(tmp_path, products, source_root=str(root), bake_to=tmp_path / "baked")
    assert list(res) == ["a"]


def test_invalid_publish_value_fails(tmp_path):
    with pytest.raises(SystemExit, match="a: publish must be one of .*'private'"):
        _render(tmp_path, [{**_product("a"), "publish": "private"}], False)


def test_nothing_to_publish_fails(tmp_path):
    with pytest.raises(SystemExit, match="no products to publish"):
        _render(tmp_path, [{**_product("a"), "publish": "none"}], False)


# --- Collection metadata from the real products.yaml -------------------------

PRODUCTS_YAML = PYGEOAPI_DIR.parent / "config" / "products.yaml"
BASE_KEYWORDS = {"water", "groundwater", "New Mexico"}
MIN_ABSTRACT_CHARS = 300


def _real_products() -> list:
    """Published products in the real products.yaml."""
    products = yaml.safe_load(PRODUCTS_YAML.read_text())["products"]
    return [p for p in products if gc.publish(p) != "none"]


@pytest.fixture(scope="module")
def real_resources(tmp_path_factory) -> dict:
    out = tmp_path_factory.mktemp("cfg") / "config.yml"
    gc.generate(PRODUCTS_YAML, TEMPLATE, out)
    return yaml.safe_load(out.read_text())["resources"]


def _with_abstract() -> list:
    return [p for p in _real_products() if p.get("abstract")]


@pytest.mark.parametrize("product", _with_abstract(), ids=lambda p: p["id"])
def test_documented_product_has_abstract_keywords_and_links(real_resources, product):
    res = real_resources[product["id"]]
    assert len(product["abstract"]) >= MIN_ABSTRACT_CHARS
    assert res["description"].startswith(product["abstract"].strip())
    assert res["description"].endswith(gc.DOCS_POINTER)
    assert set(res["keywords"]) - BASE_KEYWORDS, "needs a product-specific keyword"
    for link in res["links"]:
        # pygeoapi reads link['title'] directly; a missing one breaks the collection.
        assert {"type", "rel", "title", "href"} <= link.keys(), link
    rels = [link["rel"] for link in res["links"]]
    assert rels.count("describedby") == 1 and rels.count("license") == 1
    described = next(link for link in res["links"] if link["rel"] == "describedby")
    assert described["href"].endswith(f"/docs/products/README.md#{product['output_type']}")


def test_undocumented_products_keep_description_and_have_no_links(real_resources):
    for product in _real_products():
        if product.get("abstract"):
            continue
        res = real_resources[product["id"]]
        assert res["description"] == product["description"]
        assert "links" not in res


UNPUBLISHED = {
    "nm_arsenic_trend", "nm_nitrate_trend", "nm_fluoride_trend", "nm_uranium_trend",
    "nm_sulfate_trend", "nm_chloride_trend", "nm_depletion_projection",
}
HIDDEN = {
    "nm_waterlevel_change", "nm_waterlevel_status", "nm_sar", "nm_ion_balance", "nm_wqi",
    "nm_seasonal_amplitude", "nm_well_correlation", "nm_pod_age_by_county",
    "nm_pod_age_points",
}


def test_real_publish_settings(real_resources):
    """Pins which real products are unpublished and hidden."""
    all_ids = {p["id"] for p in yaml.safe_load(PRODUCTS_YAML.read_text())["products"]}
    assert UNPUBLISHED | HIDDEN <= all_ids
    assert real_resources.keys() == all_ids - UNPUBLISHED
    for pid, res in real_resources.items():
        assert res.get("visibility") == ("hidden" if pid in HIDDEN else None), pid


# --- Sources in the abstract ---------------------------------------------------

ABBREVIATIONS = {"NMBGMR": "NMBGMR", "USGS-NWIS": "USGS NWIS", "WQP": "WQP"}


def test_layer_sources_maps_tags_merges_and_orders_by_count(capsys):
    counts = {"NMBGMR": 5, "USGS-NWIS": 9, "WQP/NWIS": 3, "WQP/STORET": 4, "NEW": 1}
    assert gc.layer_sources(counts, ABBREVIATIONS, "p") == ["USGS NWIS", "WQP", "NMBGMR", "NEW"]
    assert "source 'NEW' is not in sources.yaml" in capsys.readouterr().out


def test_tag_prefix_needs_a_slash():
    assert gc.source_abbreviation("WQP/NWIS", ABBREVIATIONS) == "WQP"
    assert gc.source_abbreviation("WQPX", ABBREVIATIONS) is None


def _registry(tmp_path) -> Path:
    path = tmp_path / "sources.yaml"
    path.write_text(yaml.safe_dump({"sources": [
        {"key": "nwis", "tags": ["USGS-NWIS"], "abbreviation": "USGS NWIS", "name": "n"},
        {"key": "wqp", "tags": ["WQP"], "abbreviation": "WQP", "name": "w"},
    ]}))
    return path


def test_abstract_lists_sources_found_in_the_layer(tmp_path):
    root = tmp_path / "products"
    _write_parquet(root, "a", [
        _point("1", source="WQP/NWIS"), _point("2", source="USGS-NWIS"), _point("3", source="USGS-NWIS"),
    ])
    _registry(tmp_path)
    product = {**_product("a"), "abstract": "One feature per well."}
    res = _render(tmp_path, [product], source_root=str(root))
    assert res["a"]["description"] == (
        f"One feature per well. Sources: USGS NWIS, WQP. {gc.DOCS_POINTER}"
    )


def test_abstract_without_source_column_skips_the_sources_sentence(tmp_path):
    root = tmp_path / "products"
    _write_parquet(root, "a", [_point("1")])
    _registry(tmp_path)
    product = {**_product("a"), "abstract": "One feature per well."}
    res = _render(tmp_path, [product], source_root=str(root))
    assert res["a"]["description"] == f"One feature per well. {gc.DOCS_POINTER}"


def test_abstract_without_a_registry_fails(tmp_path):
    product = {**_product("a"), "abstract": "One feature per well."}
    with pytest.raises(SystemExit, match="no source registry"):
        _render(tmp_path, [product], check_parquet=False)


def test_every_layer_with_a_source_column_lists_its_values(tmp_path):
    root = tmp_path / "products"
    _write_parquet(root, "a", [_point("1", source="WQP/NWIS"), _point("2", source="USGS-NWIS")])
    _write_parquet(root, "b", [_point("1")])
    _registry(tmp_path)
    res = _render(tmp_path, [_product("a"), _product("b")], source_root=str(root))
    # No abstract needed.
    assert sorted(res["a"]["providers"][0]["source_values"]) == sorted(["USGS-NWIS", "WQP/NWIS"])
    assert "source_values" not in res["b"]["providers"][0]

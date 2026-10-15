"""Checks docs/products/README.md against products.yaml and the method text in code.

Each documented collection links to ``README.md#<output_type>``, so every such
output type needs a ``## <output_type>`` section. Each section quotes its
product's method description verbatim, so the docs can't drift from the code.

Set DIE_SAMPLE_PRODUCTS to a folder of ``<product_id>/latest.parquet`` files
(e.g. ~/pygeoapi-test/pipeline) to also check every column is documented.
"""

import os
import re
from pathlib import Path

import pytest
import yaml

from backend.trend_stats import TREND_METHOD_DESCRIPTION

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "docs" / "products" / "README.md"
PRODUCTS_YAML = ROOT / "orchestration" / "config" / "products.yaml"

# Columns documented once under "Common fields", or not shown to users.
COMMON_COLUMNS = {"geometry", "bbox", "feature_id"}

METHOD_TEXT = {
    "ogc_waterlevel_trend": TREND_METHOD_DESCRIPTION,
}


def _products() -> list:
    return yaml.safe_load(PRODUCTS_YAML.read_text())["products"]


def _sections() -> dict:
    """Map each "## <heading>" in the README to the text under it."""
    parts = re.split(r"^## (.+)$", README.read_text(), flags=re.M)
    return {heading.strip(): body for heading, body in zip(parts[1::2], parts[2::2])}


def _documented_output_types() -> set:
    return {p["output_type"] for p in _products() if p.get("abstract")}


def test_every_linked_output_type_has_a_section():
    missing = _documented_output_types() - _sections().keys()
    assert not missing, f"README.md has no section for {sorted(missing)}"


def test_every_output_type_section_is_used():
    output_types = {p["output_type"] for p in _products()}
    section_types = {h for h in _sections() if h.startswith("ogc_")}
    assert not section_types - output_types, "README section for an unknown output type"


def test_section_lists_its_products():
    sections = _sections()
    for product in _products():
        if product.get("abstract"):
            assert f"`{product['id']}`" in sections[product["output_type"]]


@pytest.mark.parametrize("output_type", sorted(METHOD_TEXT))
def test_section_quotes_method_text_from_code(output_type):
    assert METHOD_TEXT[output_type] in _sections()[output_type]


def _field_names(section: str) -> set:
    return set(re.findall(r"^\| `([^`]+)` \|", section, flags=re.M))


@pytest.mark.skipif(
    not os.environ.get("DIE_SAMPLE_PRODUCTS"), reason="DIE_SAMPLE_PRODUCTS not set"
)
@pytest.mark.parametrize(
    "product", [p for p in _products() if p.get("abstract")], ids=lambda p: p["id"]
)
def test_sample_columns_are_documented(product):
    pq = pytest.importorskip("pyarrow.parquet")
    path = Path(os.environ["DIE_SAMPLE_PRODUCTS"]).expanduser() / product["id"] / "latest.parquet"
    if not path.exists():
        pytest.skip(f"no sample {path}")
    columns = set(pq.read_schema(path).names) - COMMON_COLUMNS
    undocumented = columns - _field_names(_sections()[product["output_type"]])
    assert not undocumented, f"{product['id']}: undocumented fields {sorted(undocumented)}"

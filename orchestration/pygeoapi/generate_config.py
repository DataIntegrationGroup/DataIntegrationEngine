"""
Generate pygeoapi config.yml from products.yaml + Jinja2 template.

§V: pygeoapi config MUST be generated from products.yaml — never hand-edited.
§V: pygeoapi OGR provider MUST use /vsigs/ path (GCS).

Usage:
    python generate_config.py \
        --products ../config/products.yaml \
        --template config.yml.j2 \
        --output /pygeoapi/local.config.yml
"""
import argparse
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader


def generate(products_path: Path, template_path: Path, output_path: Path) -> None:
    products_config = yaml.safe_load(products_path.read_text())
    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        keep_trailing_newline=True,
    )
    tmpl = env.get_template(template_path.name)
    rendered = tmpl.render(
        products=products_config["products"],
        gcs_bucket=products_config["gcs_bucket"],
    )

    # Sanity check: every product must produce an OGR /vsigs/ entry
    for product in products_config["products"]:
        pid = product["id"]
        bucket = products_config["gcs_bucket"]
        expected = f"/vsigs/{bucket}/products/{pid}/latest.geojson"
        assert expected in rendered, (
            f"§V violated: OGR provider for '{pid}' missing /vsigs/ path in generated config"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered)
    print(f"Generated {output_path} ({len(products_config['products'])} collections)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--products", required=True, type=Path)
    parser.add_argument("--template", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    generate(args.products, args.template, args.output)

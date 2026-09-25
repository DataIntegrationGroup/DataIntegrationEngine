"""
Generate pygeoapi config.yml from products.yaml + Jinja2 template.

§V: pygeoapi config MUST be generated from products.yaml — never hand-edited.

--check-parquet: serve each product from latest.parquet when usable, else
latest.geojson. --bake-to DIR (implies --check-parquet): also copy the chosen
file into DIR and serve it from there. --source-root: read a local directory
instead of GCS (testing only).

Usage:
    python generate_config.py \
        --products ../config/products.yaml \
        --template config.yml.j2 \
        --output /pygeoapi/local.config.yml \
        [--check-parquet | --bake-to /data/products] [--source-root DIR]
"""
import argparse
import json
from pathlib import Path
from typing import Optional

import yaml
from jinja2 import Environment, FileSystemLoader

PARQUET_ID_FIELDS = ("feature_id", "id")  # first found wins; feature_id is unique
TIME_FIELD = "datetime"
# The Parquet provider's bbox filter is containment-only, which drops polygons
# crossing the box edge, so only point layers use it.
POINT_TYPES = {"Point", "Point Z"}


def parquet_id_field(uri: str, needs_time: bool) -> tuple[Optional[str], str]:
    """Check the Parquet footer at *uri* (gs:// or local). Returns
    ``(id_field, "ok")`` if usable, else ``(None, reason)``.

    Usable: GeoParquet metadata with a bbox covering column, point geometry,
    an id column, and (if *needs_time*) a tz-aware ``datetime``. A missing or
    unreadable file returns None; other errors (e.g. permissions) raise, so
    the build fails instead of silently serving GeoJSON."""
    import pyarrow as pa
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    fs, path = pafs.FileSystem.from_uri(uri)
    if fs.get_file_info(path).type == pafs.FileType.NotFound:
        return None, "no latest.parquet"
    try:
        schema = pq.read_schema(path, filesystem=fs)
    except pa.ArrowInvalid as exc:
        return None, f"unreadable Parquet ({exc})"

    geo = json.loads((schema.metadata or {}).get(b"geo", b"null"))
    if not geo:
        return None, "no GeoParquet metadata"
    geom = geo.get("columns", {}).get(geo.get("primary_column"), {})
    if not geom.get("covering", {}).get("bbox"):
        return None, "no bbox covering column"
    types = set(geom.get("geometry_types") or [])
    if not types or not types <= POINT_TYPES:
        return None, f"non-point geometry {sorted(types)} (Parquet bbox is containment-only)"
    if needs_time:
        if TIME_FIELD not in schema.names:
            return None, f"no {TIME_FIELD} column"
        t = schema.field(TIME_FIELD).type
        if not (pa.types.is_timestamp(t) and t.tz):
            return None, f"{TIME_FIELD} is {t}, not a tz-aware timestamp"
    for field in PARQUET_ID_FIELDS:
        if field in schema.names:
            return field, "ok"
    return None, "no id column"


def bake_file(uri: str, dest: Path) -> int:
    """Copy *uri* (gs:// or a local path) to *dest*; returns its size in bytes."""
    import pyarrow.fs as pafs

    fs, path = pafs.FileSystem.from_uri(uri)
    dest.parent.mkdir(parents=True, exist_ok=True)
    pafs.copy_files(
        path, str(dest), source_filesystem=fs, destination_filesystem=pafs.LocalFileSystem()
    )
    return dest.stat().st_size


def _exists(uri: str) -> bool:
    import pyarrow.fs as pafs

    fs, path = pafs.FileSystem.from_uri(uri)
    return fs.get_file_info(path).type != pafs.FileType.NotFound


def generate(
    products_path: Path,
    template_path: Path,
    output_path: Path,
    check_parquet: bool = False,
    source_root: Optional[str] = None,
    bake_to: Optional[Path] = None,
) -> None:
    products_config = yaml.safe_load(products_path.read_text())
    bucket = products_config["gcs_bucket"]
    store_root = source_root or f"gs://{bucket}/products"
    if bake_to:
        check_parquet = True
        geojson_root = parquet_root = str(bake_to)
    else:
        geojson_root = source_root or f"/vsigs/{bucket}/products"
        parquet_root = store_root

    # product id -> Parquet id_field, for products served from GeoParquet
    parquet: dict[str, str] = {}
    for product in products_config["products"]:
        pid = product["id"]
        if check_parquet:
            id_field, reason = parquet_id_field(
                f"{store_root}/{pid}/latest.parquet",
                needs_time=product.get("output_type") == "ogc_timeseries",
            )
        else:
            id_field, reason = None, "Parquet check disabled"
        if id_field:
            parquet[pid] = id_field
            name, line = "latest.parquet", f"Parquet (id_field={id_field})"
        else:
            name, line = "latest.geojson", f"GeoJSON ({reason})"
        if bake_to:
            # Fail the build so the previous revision keeps serving.
            if not _exists(f"{store_root}/{pid}/{name}"):
                raise SystemExit(f"{pid}: no {name} to bake ({line})")
            size = bake_file(f"{store_root}/{pid}/{name}", Path(bake_to) / pid / name)
            line += f", baked {size / 1e6:.1f} MB"
        print(f"  {pid}: {line}")

    env = Environment(
        loader=FileSystemLoader(str(template_path.parent)),
        keep_trailing_newline=True,
    )
    tmpl = env.get_template(template_path.name)
    rendered = tmpl.render(
        products=products_config["products"],
        parquet=parquet,
        geojson_root=geojson_root,
        parquet_root=parquet_root,
    )

    # Sanity check: every product must produce a source entry for its provider
    for product in products_config["products"]:
        pid = product["id"]
        if pid in parquet:
            expected = f"{parquet_root}/{pid}/latest.parquet"
        else:
            expected = f"{geojson_root}/{pid}/latest.geojson"
        assert expected in rendered, (
            f"§V violated: provider for '{pid}' missing {expected} in generated config"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered)
    print(
        f"Generated {output_path} ({len(products_config['products'])} collections, "
        f"{len(parquet)} from GeoParquet)"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--products", required=True, type=Path)
    parser.add_argument("--template", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--check-parquet",
        action="store_true",
        help="serve each product from latest.parquet when a usable one exists",
    )
    parser.add_argument(
        "--bake-to",
        type=Path,
        help="copy each product's chosen file here and serve it from local disk",
    )
    parser.add_argument(
        "--source-root",
        help="local testing only: read products from this directory instead of GCS",
    )
    args = parser.parse_args()
    generate(
        args.products,
        args.template,
        args.output,
        args.check_parquet,
        args.source_root,
        args.bake_to,
    )

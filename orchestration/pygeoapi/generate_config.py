"""
Generate pygeoapi config.yml from products.yaml + Jinja2 template.

§V: pygeoapi config MUST be generated from products.yaml — never hand-edited.

--check-parquet: serve each product from latest.parquet when usable, else
latest.geojson. --bake-to DIR (implies --check-parquet): also copy the chosen
file into DIR and serve it from there. --source-root: read a local directory
instead of GCS (testing only).

Products with an ``abstract`` are described by it, followed by the sources in
the layer (from its ``source`` column, named per --sources) and a pointer to the
data dictionary. Sources are read only with --check-parquet or --bake-to.

Usage:
    python generate_config.py \
        --products ../config/products.yaml \
        --template config.yml.j2 \
        --output /pygeoapi/local.config.yml \
        [--check-parquet | --bake-to /data/products] [--source-root DIR] \
        [--sources ../config/sources.yaml]
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
SOURCE_FIELD = "source"
DOCS_POINTER = (
    "Field definitions and method details are in the data dictionary linked from "
    "this collection."
)


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


def layer_source_counts(uri: str) -> Optional[dict[str, int]]:
    """Feature count per ``source`` value in the Parquet file at *uri*, or None
    if the file is missing, unreadable, or has no ``source`` column."""
    import pyarrow as pa
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    fs, path = pafs.FileSystem.from_uri(uri)
    if fs.get_file_info(path).type == pafs.FileType.NotFound:
        return None
    try:
        if SOURCE_FIELD not in pq.read_schema(path, filesystem=fs).names:
            return None
        column = pq.read_table(path, columns=[SOURCE_FIELD], filesystem=fs).column(0)
    except pa.ArrowInvalid:
        return None
    return {
        row["values"]: row["counts"]
        for row in column.value_counts().to_pylist()
        if row["values"]
    }


def load_source_abbreviations(sources_path: Path) -> dict[str, str]:
    """Map each registry tag to its source's abbreviation."""
    registry = yaml.safe_load(sources_path.read_text())["sources"]
    return {tag: src["abbreviation"] for src in registry for tag in src["tags"]}


def source_abbreviation(value: str, abbreviations: dict[str, str]) -> Optional[str]:
    """Abbreviation for a feature's ``source`` *value*: an exact tag match, or
    a tag followed by "/" (WQP writes "WQP/<provider>")."""
    if value in abbreviations:
        return abbreviations[value]
    return next(
        (name for tag, name in abbreviations.items() if value.startswith(f"{tag}/")), None
    )


def layer_sources(counts: dict[str, int], abbreviations: dict[str, str], pid: str) -> list[str]:
    """Distinct abbreviations in a layer, most features first. A value missing
    from the registry is listed as-is, with a warning in the build log."""
    totals: dict[str, int] = {}
    for value, n in counts.items():
        name = source_abbreviation(value, abbreviations)
        if name is None:
            print(f"  WARNING {pid}: source {value!r} is not in sources.yaml")
            name = value
        totals[name] = totals.get(name, 0) + n
    return sorted(totals, key=lambda name: (-totals[name], name))


def collection_description(product: dict, sources: Optional[list[str]]) -> str:
    """pygeoapi description: the abstract, its sources and the data-dictionary
    pointer, or the one-line description for products without an abstract."""
    abstract = product.get("abstract")
    if not abstract:
        return product["description"]
    parts = [abstract.strip()]
    if sources:
        parts.append(f"Sources: {', '.join(sources)}.")
    parts.append(DOCS_POINTER)
    return " ".join(parts)


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
    sources_path: Optional[Path] = None,
) -> None:
    products_config = yaml.safe_load(products_path.read_text())
    sources_path = sources_path or products_path.parent / "sources.yaml"
    abbreviations: dict[str, str] = {}
    if sources_path.exists():
        abbreviations = load_source_abbreviations(sources_path)
    elif any(p.get("abstract") for p in products_config["products"]):
        # An abstract's source list would silently go missing.
        raise SystemExit(f"no source registry at {sources_path}")
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
    descriptions: dict[str, str] = {}
    for product in products_config["products"]:
        pid = product["id"]
        if check_parquet:
            id_field, reason = parquet_id_field(
                f"{store_root}/{pid}/latest.parquet",
                needs_time=product.get("output_type") == "ogc_timeseries",
            )
        else:
            id_field, reason = None, "Parquet check disabled"
        counts = (
            layer_source_counts(f"{store_root}/{pid}/latest.parquet")
            if check_parquet and abbreviations and product.get("abstract")
            else None
        )
        sources = layer_sources(counts, abbreviations, pid) if counts else None
        descriptions[pid] = collection_description(product, sources)
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
        descriptions=descriptions,
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
    parser.add_argument(
        "--sources",
        type=Path,
        help="source registry (default: sources.yaml next to --products)",
    )
    args = parser.parse_args()
    generate(
        args.products,
        args.template,
        args.output,
        args.check_parquet,
        args.source_root,
        args.bake_to,
        args.sources,
    )

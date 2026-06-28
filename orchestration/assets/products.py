"""Per-source Dagster asset graph for a product.

``build_product_assets(product)`` expands one products.yaml entry into a small
asset graph:

    sources/<key>  ─┐
    sources/<key>  ─┼─▶  <product_id>  ─▶  <product_id>/geoserver
    sources/<key>  ─┘    (combine)         (publish)

- **source assets** — keyed ``[product_id, "sources", <source_key>]``, one per
  data source that provides the product's parameter. Each runs DIE unification
  for just that source and emits its records/sites/timeseries.
- **combine asset** — keyed ``[product_id]``. Merges every source's
  contribution, writes the OGC GeoJSON collection, and uploads it to GCS.
- **geoserver asset** — keyed ``[product_id, "geoserver"]``. Downloads the
  combined GeoJSON, converts it to a GeoPackage, and publishes it as a layer in
  GeoServer.

Design notes:
- Source and geoserver assets never hard-fail. They catch their own errors and
  report status via an ``AssetCheckResult`` that goes red (WARN) on error or
  empty output, so one dead source — or a GeoServer outage — surfaces in the UI
  without blocking the rest of the product graph.
- Records cross the IO manager as plain ``_payload`` dicts (the record classes
  use ``__getattr__`` over ``_payload`` which does not survive pickling). The
  combine asset rebuilds record objects before dumping.
"""
import tempfile
import traceback
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
import geopandas as gpd

from backend.config import PARAMETER_SOURCE_MAP, WATERLEVELS
from backend.persisters.ogc_features import (
    ANALYTE_TREND_METHOD_DESCRIPTION,
    dump_major_chemistry_collection,
    dump_mcl_exceedance_collection,
    dump_monitoring_recency_collection,
    dump_summary_collection,
    dump_timeseries_collection,
    dump_trend_collection,
)
from backend.record import ParameterRecord, SiteRecord, SummaryRecord
from backend.unifier import unify_source
from orchestration.logging_bridge import forward_die_logs
from orchestration.resources.die_config import DIEConfigResource
from orchestration.resources.gcs import GCSResource
from orchestration.resources.geoserver import GeoServerResource

_CHECK_NAME = "returned_data"
_GEOSERVER_CHECK_NAME = "registered"

# GCS key (within the products bucket) of the MCL threshold file — the source of
# truth for the ogc_mcl_exceedance product.
_MCL_KEY = "config/mcl.json"

# Multi-analyte products that gather one summary record per analyte per well.
_MULTI_ANALYTE_OUTPUT_TYPES = ("ogc_major_chemistry", "ogc_mcl_exceedance")

# Classic major-ion suite for the ogc_major_chemistry product. One feature per
# well, with each analyte's latest value/units/date as properties.
_MAJOR_CHEMISTRY = [
    "calcium",
    "magnesium",
    "sodium",
    "potassium",
    "bicarbonate",
    "carbonate",
    "chloride",
    "sulfate",
]


def _product_params(product: dict) -> list[str]:
    """The DIE parameter(s) a product unifies. Single-parameter products yield
    one; multi-analyte products yield their analyte list."""
    output_type = product.get("output_type")
    if output_type == "ogc_major_chemistry":
        return list(_MAJOR_CHEMISTRY)
    if output_type == "ogc_mcl_exceedance":
        # Candidate analytes for the static asset graph; the MCL JSON is the
        # source of truth for which actually have thresholds.
        return list(product["analytes"])
    return [product["parameter"]]


def _product_source_keys(product: dict) -> list[str]:
    """Source keys that apply to this product: the union of its parameters'
    agencies, filtered by the product's include/exclude list."""
    agencies: list = []
    for param in _product_params(product):
        for a in PARAMETER_SOURCE_MAP[param]["agencies"]:
            if a not in agencies:
                agencies.append(a)
    spec = product.get("sources", {}) or {}
    if spec.get("include"):
        return [a for a in agencies if a in spec["include"]]
    if spec.get("exclude"):
        return [a for a in agencies if a not in spec["exclude"]]
    return agencies


def _in_name(source_key: str) -> str:
    # Combine-asset input kwargs must be valid Python identifiers; source keys
    # may contain hyphens, so sanitize and prefix.
    return f"src_{source_key.replace('-', '_')}"


def _build_source_asset(
    product: dict, source_key: str, group: str
) -> tuple[dg.AssetsDefinition, dg.AssetKey]:
    """Build the asset that unifies a single source for *product*.

    Returns ``(asset_def, asset_key)``. The asset never raises: on failure it
    records the traceback and fails its ``returned_data`` check (WARN) instead,
    so a broken source does not block the combine asset. Output is shipped as
    plain ``_payload`` dicts for IO-manager pickling (see module docstring)."""
    pid = product["id"]
    src_key = dg.AssetKey([pid, "sources", source_key])
    params = _product_params(product)

    @dg.asset(
        key=src_key,
        group_name=group,
        check_specs=[dg.AssetCheckSpec(name=_CHECK_NAME, asset=src_key)],
    )
    def _source_asset(
        context: dg.AssetExecutionContext, die_config: DIEConfigResource
    ) -> Iterator[dg.Output | dg.AssetCheckResult]:
        error = ""
        records: list[dict] = []
        sites: list[dict] = []
        timeseries: list[list[dict]] = []
        try:
            # One unification pass per parameter. Single-parameter products run
            # once; the major-chemistry product runs once per analyte and
            # accumulates summary records (analyte identity lives in each
            # record's parameter_name). A source that doesn't provide a given
            # parameter is skipped by unify_source (source_pair → None).
            with forward_die_logs(context):
                for param in params:
                    config = die_config.get_config(product, parameter=param)
                    persister = unify_source(config, source_key)
                    # Ship plain dicts across the IO manager; rebuild in combine.
                    records.extend(r._payload for r in persister.records)
                    sites.extend(s._payload for s in persister.sites)
                    timeseries.extend(
                        [o._payload for o in site_ts] for site_ts in persister.timeseries
                    )
        except Exception:
            error = traceback.format_exc()
            context.log.error(f"Source {source_key} failed:\n{error}")

        obs_count = sum(len(t) for t in timeseries)
        payload = {"records": records, "sites": sites, "timeseries": timeseries}

        has_data = bool(records or sites or timeseries)
        passed = error == "" and has_data

        yield dg.Output(
            payload,
            metadata={
                "source": source_key,
                "record_count": len(records),
                "site_count": len(sites),
                "observation_count": obs_count,
                "error": error,
            },
        )
        yield dg.AssetCheckResult(
            asset_key=src_key,
            check_name=_CHECK_NAME,
            passed=passed,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "record_count": len(records),
                "observation_count": obs_count,
                "error": error or ("no data returned" if not has_data else ""),
            },
        )

    return _source_asset, src_key


def _build_combine_asset(
    product: dict,
    source_keys: list[str],
    source_asset_keys: list[dg.AssetKey],
    group: str,
) -> dg.AssetsDefinition:
    """Build the combine asset (keyed ``[product_id]``) for *product*.

    Depends on every source asset (wired via ``ins``), merges their
    records/sites/timeseries, writes the OGC GeoJSON collection — summary,
    timeseries, major-chemistry, or waterlevel-trend depending on
    ``output_type`` — and uploads it to GCS."""
    pid = product["id"]
    output_type = product["output_type"]
    ins = {
        _in_name(k): dg.AssetIn(key=ak)
        for k, ak in zip(source_keys, source_asset_keys)
    }

    @dg.asset(key=dg.AssetKey(pid), group_name=group, ins=ins)
    def _combine_asset(
        context: dg.AssetExecutionContext,
        gcs: GCSResource,
        **sources,
    ) -> dg.MaterializeResult:
        all_records, all_sites, all_timeseries = [], [], []
        for payload in sources.values():
            all_records.extend(payload.get("records", []))
            all_sites.extend(payload.get("sites", []))
            all_timeseries.extend(payload.get("timeseries", []))

        meta = {
            "id": pid,
            "title": product.get("title", pid),
            "description": product.get("description", ""),
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "collection.geojson"
            if output_type == "ogc_major_chemistry":
                # All summary records (one per well+analyte); the dumper pivots
                # to one feature per well with analytes as properties.
                records = [SummaryRecord(p) for p in all_records]
                dump_major_chemistry_collection(str(out), records, meta)
            elif output_type == "ogc_mcl_exceedance":
                # Compare each well's latest analyte values to the MCL JSON
                # (source of truth) read from GCS.
                thresholds = gcs.read_json(_MCL_KEY)
                records = [SummaryRecord(p) for p in all_records]
                dump_mcl_exceedance_collection(str(out), records, meta, thresholds)
            elif output_type == "ogc_summary":
                records = [SummaryRecord(p) for p in all_records]
                dump_summary_collection(str(out), records, meta)
            elif output_type == "ogc_waterlevel_trend":
                # all_sites/all_timeseries are index-aligned payload dicts (see
                # source asset); consumed as dicts to keep memory bounded.
                dump_trend_collection(
                    str(out), all_sites, all_timeseries, meta,
                    slope_units="ft/year", reducer="min",
                )
            elif output_type == "ogc_analyte_trend":
                dump_trend_collection(
                    str(out), all_sites, all_timeseries, meta,
                    slope_units="mg/L/year", reducer="mean",
                    method=ANALYTE_TREND_METHOD_DESCRIPTION,
                    parameter_name=product.get("parameter"),
                )
            elif output_type == "ogc_monitoring_recency":
                run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                dump_monitoring_recency_collection(
                    str(out), all_sites, all_timeseries, meta,
                    run_date=run_date,
                    stale_days=int(product.get("stale_days", 365)),
                )
            else:
                site_records = [SiteRecord(p) for p in all_sites]
                flat = [
                    ParameterRecord(p) for site_ts in all_timeseries for p in site_ts
                ]
                dump_timeseries_collection(str(out), site_records, flat, meta)

            info = gcs.upload_product(str(out), pid)

        metadata: dict = {
            "feature_count": dg.MetadataValue.int(info["feature_count"]),
            "latest_uri": dg.MetadataValue.url(info["latest_uri"]),
            "source_count": dg.MetadataValue.int(len(sources)),
            # True when content matched what's already in GCS (no new upload).
            "skipped_unchanged": dg.MetadataValue.bool(bool(info.get("skipped"))),
        }
        # dated_uri is None when the upload was skipped as unchanged.
        if info.get("dated_uri"):
            metadata["dated_uri"] = dg.MetadataValue.url(info["dated_uri"])
        # Change-recency signal for tuning run frequency: how long the data has
        # been static. A large, growing value means the schedule can be relaxed
        # (e.g. daily -> monthly); 0 means it changed this run.
        if info.get("last_changed"):
            metadata["last_changed"] = dg.MetadataValue.text(info["last_changed"])
        if info.get("days_since_last_change") is not None:
            metadata["days_since_last_change"] = dg.MetadataValue.int(
                info["days_since_last_change"]
            )

        return dg.MaterializeResult(metadata=metadata)

    return _combine_asset


def _geojson_to_geopackage(geojson_path: Path, layer_name: str, out_dir: Path):
    """Convert a GeoJSON file to a GeoPackage whose layer (table) is named
    *layer_name* (so the published GeoServer layer is named *layer_name*).
    GeoPackage is a single file with no field-name length limit, unlike the
    zipped ESRI Shapefile this replaces. Returns (gpkg_path, bbox) where bbox is
    (minx, miny, maxx, maxy) in EPSG:4326."""
    gdf = gpd.read_file(geojson_path)
    if gdf.empty:
        raise ValueError(f"{layer_name}: GeoJSON has no features; nothing to publish")
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")

    # Sites with an elevation get 3D point geometry in the GeoJSON. GeoServer's
    # GeoPackage reader rejects a 3D CRS ("WGS 84 has 3 dimensions") when
    # computing bounds, so flatten to 2D — elevation remains an attribute.
    gdf["geometry"] = gdf.geometry.force_2d()

    # Bounds are passed to GeoServer explicitly so it never calls getBounds on
    # the gpkg store (which still trips the 3D-CRS bug at publish time).
    minx, miny, maxx, maxy = (float(v) for v in gdf.total_bounds)

    gpkg_path = out_dir / f"{layer_name}.gpkg"
    gdf.to_file(gpkg_path, driver="GPKG", layer=layer_name)
    return gpkg_path, (minx, miny, maxx, maxy)


def _build_geoserver_asset(product: dict, group: str) -> dg.AssetsDefinition:
    """Build the GeoServer publish asset (keyed ``[product_id, "geoserver"]``).

    Depends on the combine asset for ordering only (``deps``, no data passed):
    it reads the combined GeoJSON back from GCS, converts to GeoPackage, and
    publishes it. Never raises — failures fail the ``registered`` check (WARN)
    instead, so a GeoServer outage doesn't fail the run after GCS upload
    succeeded."""
    pid = product["id"]
    gs_key = dg.AssetKey([pid, "geoserver"])

    @dg.asset(
        key=gs_key,
        group_name=group,
        deps=[dg.AssetKey(pid)],
        check_specs=[dg.AssetCheckSpec(name=_GEOSERVER_CHECK_NAME, asset=gs_key)],
    )
    def _geoserver_asset(
        context: dg.AssetExecutionContext,
        gcs: GCSResource,
        geoserver: GeoServerResource,
    ) -> dg.MaterializeResult:
        error = ""
        actions: dict = {}
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                geojson = Path(tmpdir) / f"{pid}.geojson"
                gcs.download_latest(pid, str(geojson))
                gpkg_path, bbox = _geojson_to_geopackage(geojson, pid, Path(tmpdir))
                actions = geoserver.publish_geopackage(
                    pid,
                    str(gpkg_path),
                    title=product.get("title", pid),
                    abstract=product.get("description", ""),
                    bbox=bbox,
                )
        except Exception:
            error = traceback.format_exc()
            context.log.error(f"GeoServer publish failed for {pid}:\n{error}")

        metadata = {"error": error}
        for k, v in actions.items():
            metadata[f"geoserver_{k}"] = str(v)

        return dg.MaterializeResult(
            metadata=metadata,
            check_results=[
                dg.AssetCheckResult(
                    asset_key=gs_key,
                    check_name=_GEOSERVER_CHECK_NAME,
                    passed=error == "",
                    severity=dg.AssetCheckSeverity.WARN,
                    metadata={"error": error},
                )
            ],
        )

    return _geoserver_asset


def build_product_assets(product: dict) -> list[dg.AssetsDefinition]:
    """Return the full asset list for *product*: one source asset per applicable
    source, the combine asset, and the geoserver publish asset (see module
    docstring for the graph shape). Assets are grouped ``waterlevels`` or
    ``analytes`` by parameter (major-chemistry products group under
    ``analytes``)."""
    group = "waterlevels" if product.get("parameter") == WATERLEVELS else "analytes"
    source_keys = _product_source_keys(product)

    source_assets = []
    source_asset_keys = []
    for sk in source_keys:
        asset, key = _build_source_asset(product, sk, group)
        source_assets.append(asset)
        source_asset_keys.append(key)

    combine = _build_combine_asset(product, source_keys, source_asset_keys, group)
    geoserver = _build_geoserver_asset(product, group)
    return source_assets + [combine, geoserver]

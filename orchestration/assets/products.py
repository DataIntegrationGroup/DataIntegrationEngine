"""Shared-source Dagster asset graph for the data products.

The graph has two layers wired through the GCS IO manager:

    shared source assets           per-product pipeline
    ["sources", param, scope, k]  ──▶  <product_id>   ──▶  <product_id>/geoserver
                                       (combine)           (publish)

- **shared source assets** — keyed ``["sources", <parameter>, <scope>,
  <source_key>]``. One per *distinct* (parameter, scope, source) tuple across
  **all** products. ``scope`` encodes the spatial filter (``state_NM`` /
  ``county_Bernalillo`` / ``all``). The source is fetched **once** and unified
  for *both* summary and timeseries (see ``unify_source_both`` — every
  connector's fetch is mode-agnostic, so summary and timeseries differ only in
  how the same observations are transformed), so the asset carries records
  (summary) and sites/timeseries together. Because the key is
  product-independent *and* mode-independent, every product that needs the same
  source under the same parameter/scope — whether a summary or a timeseries
  product — shares one asset and one fetch.
- **combine asset** — keyed ``[product_id]``. Reads its source inputs back
  (``ins``), takes the slice it needs (records for summary-type products,
  sites/timeseries for timeseries-type), writes the OGC GeoJSON collection,
  uploads to GCS.
- **geoserver asset** — keyed ``[product_id, "geoserver"]``. Downloads the
  combined GeoJSON, converts to GeoPackage, publishes it as a GeoServer layer.

Job layout (see ``definitions.py``) makes the sharing pay off **and** keeps each
run's lineage complete. Products are grouped into *cohorts* by (group, scope) —
the products that can share source assets. One job per cohort materializes that
cohort's whole graph in a single run: each shared source is fetched once (it is
one asset key, selected once), then every member combine reads it back through
the GCS IO manager and publishes. So a source is never fetched twice in a run —
not across products and not across summary/timeseries — while the full
sources → combine → geoserver lineage stays visible for every product.
(Cross-product dedup requires the sharing products to run together; that is
exactly what a cohort is, and is why summary + timeseries products now share a
cohort.)

Design notes:
- Source and geoserver assets never hard-fail. They catch their own errors and
  report status via an ``AssetCheckResult`` that goes red (WARN) on error or
  empty output, so one dead source — or a GeoServer outage — surfaces in the UI
  without blocking the rest of the graph.
- Records cross the IO manager as plain ``_payload`` dicts (the record classes
  use ``__getattr__`` over ``_payload`` which does not survive pickling). The
  combine asset rebuilds record objects before dumping.

Known limitation — per-analyte source fetches (potential future optimization):
  Sharing is deduped at the ``(parameter, scope, source)`` grain, which
  collapses duplication *across products* (e.g. ``sulfate/state_NM/wqp`` is one
  asset shared by nm_major_chemistry and nm_mcl_exceedance) and *across modes*
  (one ``wqp`` asset serves both summary and timeseries products). It does NOT
  collapse *across analytes*: a source appears once per analyte (e.g. ``wqp``
  has ~13 source assets, one per analyte). This is because the backend unifies a
  single parameter per pass (``unify_source_both`` uses one ``config.parameter``),
  so each analyte is a separate sweep of the same wells even though one provider
  query (WQP/AMP/...) typically returns all analytes at once. Collapsing this
  would need a **backend** change — multi-analyte unification that fetches a
  source once and emits per-analyte records — after which the source key could
  drop ``parameter`` (e.g. ``["sources", "analytes", scope, source]``) and the
  analyte combines would each filter the shared multi-analyte payload. That is
  the bulk of the remaining redundant API pulls for analyte products; it touches
  DIE core, not this asset graph, so it is intentionally out of scope here.
"""

import tempfile
import traceback
from collections import namedtuple
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
import geopandas as gpd

from backend.bounding_polygons import (
    get_nm_groundwater_basin_polygons,
    get_state_county_polygons,
)
from backend.config import PARAMETER_SOURCE_MAP, WATERLEVELS
from backend.persisters.ogc_features import (
    ANALYTE_TREND_METHOD_DESCRIPTION,
    dump_data_density_collection,
    dump_depletion_projection_collection,
    dump_hardness_collection,
    dump_ion_balance_collection,
    dump_major_chemistry_collection,
    dump_mcl_exceedance_collection,
    dump_monitoring_recency_collection,
    dump_pod_age_by_county_collection,
    dump_pod_age_points_collection,
    dump_sar_collection,
    dump_seasonal_amplitude_collection,
    dump_summary_collection,
    dump_timeseries_collection,
    dump_trend_collection,
    dump_water_type_collection,
    dump_waterlevel_change_collection,
    dump_waterlevel_status_collection,
    dump_well_correlation_collection,
    dump_basin_well_density_collection,
    dump_well_density_collection,
    dump_wqi_collection,
)
from backend.record import ParameterRecord, SiteRecord, SummaryRecord
from backend.unifier import collect_sites, unify_source_both
from orchestration.logging_bridge import forward_die_logs
from orchestration.resources.die_config import DIEConfigResource
from orchestration.resources.gcs import GCSResource
from orchestration.resources.geoserver import GeoServerResource

_CHECK_NAME = "returned_data"
_GEOSERVER_CHECK_NAME = "registered"

# GCS key (within the products bucket) of the MCL threshold file — the source of
# truth for the ogc_mcl_exceedance product.
_MCL_KEY = "config/mcl.json"

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

# A single shared source asset's identity. Two products that produce the same
# SourceSpec share one asset (the namedtuple is hashable, so dedup is just set
# membership). ``group`` follows the parameter, not the product. There is no
# ``mode`` field: a source is fetched once and unified for *both* summary and
# timeseries (see build_shared_source_asset / unify_source_both), so a summary
# product and a timeseries product over the same (parameter, scope, source)
# share one asset and one fetch.
SourceSpec = namedtuple("SourceSpec", "parameter scope source_key group")


def _product_params(product: dict) -> list[str]:
    """The DIE parameter(s) a product unifies. Single-parameter products yield
    one; multi-analyte products yield their analyte list."""
    output_type = product.get("output_type")
    if output_type == "ogc_major_chemistry":
        return list(_MAJOR_CHEMISTRY)
    if output_type in (
        "ogc_mcl_exceedance",
        "ogc_hardness",
        "ogc_water_type",
        "ogc_sar",
        "ogc_ion_balance",
        "ogc_wqi",
    ):
        # Multi-analyte products with an explicit `analytes` list. (For MCL the
        # candidate set for the static graph; the MCL JSON is the source of truth
        # for which actually have thresholds.)
        return list(product["analytes"])
    return [product["parameter"]]


def _spatial_scope(product: dict) -> str:
    """A stable string identity for the product's spatial filter. Sources with
    different spatial extents unify to different results, so the extent is part
    of the shared-source key."""
    sf = product.get("spatial_filter", {}) or {}
    if sf.get("county"):
        return f"county_{sf['county']}"
    if sf.get("state"):
        return f"state_{sf['state']}"
    return "all"


def _scope_to_spatial_filter(scope: str) -> dict:
    """Inverse of :func:`_spatial_scope`, for rebuilding a config from a spec."""
    kind, _, value = scope.partition("_")
    if kind == "county":
        return {"county": value}
    if kind == "state":
        return {"state": value}
    return {}


def _group_for_param(parameter: str) -> str:
    return "waterlevels" if parameter == WATERLEVELS else "analytes"


def _param_source_keys(product: dict, parameter: str) -> list[str]:
    """Source keys that apply to *parameter* for this product: the parameter's
    agencies filtered by the product's include/exclude list."""
    agencies = list(PARAMETER_SOURCE_MAP[parameter]["agencies"])
    spec = product.get("sources", {}) or {}
    if spec.get("include"):
        return [a for a in agencies if a in spec["include"]]
    if spec.get("exclude"):
        return [a for a in agencies if a not in spec["exclude"]]
    return agencies


# Products that are not built on the per-parameter shared-source graph. The well
# correlation product needs the *sites* of every source (parameter-independent,
# including the OSE POD source which has no parameter data), so its combine
# gathers sites itself rather than reading per-parameter shared source assets.
_STANDALONE_OUTPUT_TYPES = {
    "ogc_well_correlation",
    "ogc_pod_age_by_county",
    "ogc_pod_age_points",
}


def is_standalone(product: dict) -> bool:
    """True for products whose combine gathers its own data (no shared source
    assets, no parameter cohort)."""
    return product.get("output_type") in _STANDALONE_OUTPUT_TYPES


def product_source_specs(product: dict) -> list[SourceSpec]:
    """Every shared source asset this product depends on, one per
    (parameter, source) pair. ``scope`` is constant for a product; the parameter
    and source vary. Returned in a stable order.

    Standalone products (see :func:`is_standalone`) depend on no shared source
    asset and return an empty list."""
    if is_standalone(product):
        return []
    scope = _spatial_scope(product)
    specs: list[SourceSpec] = []
    for param in _product_params(product):
        group = _group_for_param(param)
        for source_key in _param_source_keys(product, param):
            specs.append(SourceSpec(param, scope, source_key, group))
    return specs


def shared_source_key(spec: SourceSpec) -> dg.AssetKey:
    return dg.AssetKey(["sources", spec.parameter, spec.scope, spec.source_key])


def _in_name(spec: SourceSpec) -> str:
    # Combine-asset input kwargs must be valid Python identifiers; parameter and
    # source keys may contain hyphens, so sanitize. (parameter, source) is
    # unique within a product, so it disambiguates the multi-analyte combines.
    raw = f"src_{spec.parameter}_{spec.source_key}"
    return raw.replace("-", "_")


def build_shared_source_asset(spec: SourceSpec) -> dg.AssetsDefinition:
    """Build the shared asset that unifies one source for one (parameter, scope)
    — keyed product-independently so every product needing it shares it.

    The source is fetched once and unified for *both* summary and timeseries
    (unify_source_both), so the asset carries records (summary) and
    sites/timeseries together; summary and timeseries products over the same
    (parameter, scope, source) share this one asset and one fetch.

    The asset never raises: on failure it records the traceback and fails its
    ``returned_data`` check (WARN) instead, so a broken source does not block any
    product's combine asset. Output ships as plain ``_payload`` dicts for
    IO-manager pickling (see module docstring)."""
    src_key = shared_source_key(spec)
    # Synthetic product spec driving config: only parameter and spatial filter
    # affect a single source's unification (sources include/exclude only selects
    # which sources a product consumes — irrelevant here; mode is handled by
    # unify_source_both, which produces both). output_type is nominal — the
    # driver toggles summary/timeseries itself.
    synth_product = {
        "output_type": "ogc_timeseries",
        "spatial_filter": _scope_to_spatial_filter(spec.scope),
    }

    @dg.asset(
        key=src_key,
        group_name=spec.group,
        description=(
            f"Shared source asset: unifies the **{spec.source_key}** source for "
            f"**{spec.parameter}** over scope `{spec.scope}`. Fetched once and "
            f"unified for both summary and timeseries, so it carries summary "
            f"records plus timeseries sites/observations. Shared by every product "
            f"that needs this (parameter, scope, source), so the source is pulled "
            f"once per run regardless of how many products consume it."
        ),
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
            # A source that doesn't provide this parameter is skipped by
            # unify_source_both (source_pair → None).
            with forward_die_logs(context):
                config = die_config.get_config(synth_product, parameter=spec.parameter)
                # One fetch, both modes: summary records + timeseries sites/obs.
                summary_persister, timeseries_persister = unify_source_both(
                    config, spec.source_key
                )
                # Ship plain dicts across the IO manager; rebuild in combine.
                records.extend(r._payload for r in summary_persister.records)
                sites.extend(s._payload for s in timeseries_persister.sites)
                timeseries.extend(
                    [o._payload for o in site_ts]
                    for site_ts in timeseries_persister.timeseries
                )
        except Exception:
            error = traceback.format_exc()
            context.log.error(f"Source {spec.source_key} failed:\n{error}")

        obs_count = sum(len(t) for t in timeseries)
        payload = {"records": records, "sites": sites, "timeseries": timeseries}

        has_data = bool(records or sites or timeseries)
        passed = error == "" and has_data

        yield dg.Output(
            payload,
            metadata={
                "source": spec.source_key,
                "parameter": spec.parameter,
                "scope": spec.scope,
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

    return _source_asset


def _build_combine_asset(
    product: dict, specs: list[SourceSpec], group: str
) -> dg.AssetsDefinition:
    """Build the combine asset (keyed ``[product_id]``) for *product*.

    Depends on every shared source asset it needs (wired via ``ins``), merges
    their records/sites/timeseries, writes the OGC GeoJSON collection — summary,
    timeseries, major-chemistry, or trend depending on ``output_type`` — and
    uploads it to GCS. The source inputs are loaded from the GCS IO manager
    (materialized by the sources job), so the combine never re-unifies them."""
    pid = product["id"]
    output_type = product["output_type"]
    ins = {_in_name(spec): dg.AssetIn(key=shared_source_key(spec)) for spec in specs}

    _combine_description = (
        f"**{product.get('title', pid)}** — product combine asset (`{output_type}`). "
        f"{product.get('description', '').rstrip('.')}. Merges its "
        f"{len(specs)} shared source asset(s), builds the OGC GeoJSON collection, "
        f"and uploads it to GCS. Sources are read back from the GCS IO manager "
        f"(materialized upstream in the same cohort run), so the combine never "
        f"re-unifies them."
    )

    @dg.asset(
        key=dg.AssetKey(pid),
        group_name=group,
        description=_combine_description,
        ins=ins,
    )
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
            elif output_type == "ogc_hardness":
                # Pivots per-analyte summaries to one feature per well, using
                # calcium + magnesium to compute total hardness as CaCO3.
                records = [SummaryRecord(p) for p in all_records]
                dump_hardness_collection(str(out), records, meta)
            elif output_type == "ogc_water_type":
                # Pivots the major-ion summaries to one feature per well and
                # classifies the hydrochemical (Piper) water type.
                records = [SummaryRecord(p) for p in all_records]
                dump_water_type_collection(str(out), records, meta)
            elif output_type == "ogc_sar":
                # Pivots per-analyte summaries to one feature per well; sodium
                # adsorption ratio from Na/Ca/Mg (irrigation suitability).
                records = [SummaryRecord(p) for p in all_records]
                dump_sar_collection(str(out), records, meta)
            elif output_type == "ogc_ion_balance":
                # Charge balance error per well from the major-ion suite — a QA
                # screen on the chemistry-derived products.
                records = [SummaryRecord(p) for p in all_records]
                dump_ion_balance_collection(str(out), records, meta)
            elif output_type == "ogc_wqi":
                # CCME WQI per well against the MCL thresholds (same source of
                # truth as ogc_mcl_exceedance) read from GCS.
                thresholds = gcs.read_json(_MCL_KEY)
                records = [SummaryRecord(p) for p in all_records]
                dump_wqi_collection(str(out), records, meta, thresholds)
            elif output_type == "ogc_summary":
                records = [SummaryRecord(p) for p in all_records]
                dump_summary_collection(str(out), records, meta)
            elif output_type == "ogc_data_density":
                dump_data_density_collection(
                    str(out),
                    all_sites,
                    all_timeseries,
                    meta,
                    parameter_name=product.get("parameter"),
                )
            elif output_type == "ogc_waterlevel_change":
                dump_waterlevel_change_collection(
                    str(out),
                    all_sites,
                    all_timeseries,
                    meta,
                    window_years=float(product.get("window_years", 5)),
                )
            elif output_type == "ogc_waterlevel_trend":
                # all_sites/all_timeseries are index-aligned payload dicts (see
                # source asset); consumed as dicts to keep memory bounded.
                dump_trend_collection(
                    str(out),
                    all_sites,
                    all_timeseries,
                    meta,
                    slope_units="ft/year",
                    reducer="min",
                )
            elif output_type == "ogc_analyte_trend":
                dump_trend_collection(
                    str(out),
                    all_sites,
                    all_timeseries,
                    meta,
                    slope_units="mg/L/year",
                    reducer="mean",
                    method=ANALYTE_TREND_METHOD_DESCRIPTION,
                    parameter_name=product.get("parameter"),
                )
            elif output_type == "ogc_waterlevel_status":
                dump_waterlevel_status_collection(
                    str(out),
                    all_sites,
                    all_timeseries,
                    meta,
                )
            elif output_type == "ogc_seasonal_amplitude":
                dump_seasonal_amplitude_collection(
                    str(out),
                    all_sites,
                    all_timeseries,
                    meta,
                    min_days_per_year=int(product.get("min_days_per_year", 4)),
                )
            elif output_type == "ogc_depletion_projection":
                dump_depletion_projection_collection(
                    str(out),
                    all_sites,
                    all_timeseries,
                    meta,
                )
            elif output_type == "ogc_well_density":
                # One feature per NM county polygon (not per well); counties
                # fetched from geoconnex.us (cached to disk by
                # bounding_polygons, same pattern as county spatial_filter).
                counties = get_state_county_polygons(
                    product.get("spatial_filter", {}).get("state", "NM")
                )
                dump_well_density_collection(str(out), counties, all_sites, meta)
            elif output_type == "ogc_basin_well_density":
                # One feature per OSE-declared groundwater basin polygon (not
                # per well); basins fetched from OSE's ArcGIS FeatureServer
                # (cached to disk by bounding_polygons, same pattern as
                # ogc_well_density's county fetch).
                basins = get_nm_groundwater_basin_polygons()
                dump_basin_well_density_collection(str(out), basins, all_sites, meta)
            elif output_type == "ogc_monitoring_recency":
                run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                dump_monitoring_recency_collection(
                    str(out),
                    all_sites,
                    all_timeseries,
                    meta,
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


def _build_correlation_combine_asset(product: dict, group: str) -> dg.AssetsDefinition:
    """Build the combine asset for the well-correlation product (keyed
    ``[product_id]``).

    Unlike the parameter products, this asset depends on **no** shared source
    assets: correlation needs the location of every well from every source
    (including OSE PODs, which carry no parameter data). It gathers all sites
    itself via ``collect_sites`` (sites-only over ``all_site_sources``), runs the
    cross-agency correlation, writes the OGC GeoJSON, and uploads it to GCS."""
    pid = product["id"]

    @dg.asset(
        key=dg.AssetKey(pid),
        group_name=group,
        description=(
            f"**{product.get('title', pid)}** — standalone combine asset "
            f"(`ogc_well_correlation`). {product.get('description', '').rstrip('.')}. "
            f"Gathers sites from every source (sites-only, incl. OSE PODs), "
            f"correlates wells across agencies, links each to an OSE POD, and "
            f"uploads the OGC GeoJSON to GCS. Depends on no shared source asset."
        ),
        check_specs=[dg.AssetCheckSpec(name=_CHECK_NAME, asset=dg.AssetKey(pid))],
    )
    def _combine_asset(
        context: dg.AssetExecutionContext,
        die_config: DIEConfigResource,
        gcs: GCSResource,
    ) -> Iterator[dg.Output | dg.AssetCheckResult]:
        error = ""
        feature_count = 0
        info: dict = {}
        try:
            with forward_die_logs(context):
                config = die_config.get_config(product)
                sites = collect_sites(config)

            meta = {
                "id": pid,
                "title": product.get("title", pid),
                "description": product.get("description", ""),
            }
            with tempfile.TemporaryDirectory() as tmpdir:
                out = Path(tmpdir) / "collection.geojson"
                coll = dump_well_correlation_collection(
                    str(out),
                    sites,
                    meta,
                    max_link_distance_m=_num_opt(product.get("max_link_distance_m")),
                    depth_tolerance_ft=_num_opt(product.get("depth_tolerance_ft")),
                    elevation_tolerance_ft=_num_opt(
                        product.get("elevation_tolerance_ft")
                    ),
                    pod_link_distance_m=_num_opt(product.get("pod_link_distance_m")),
                )
                feature_count = len(coll.get("features", []))
                info = gcs.upload_product(str(out), pid)
        except Exception:
            error = traceback.format_exc()
            context.log.error(f"Well correlation combine failed for {pid}:\n{error}")

        metadata: dict = {"error": error}
        if info:
            metadata.update(
                {
                    "feature_count": dg.MetadataValue.int(
                        info.get("feature_count", feature_count)
                    ),
                    "latest_uri": dg.MetadataValue.url(info.get("latest_uri", "")),
                    "skipped_unchanged": dg.MetadataValue.bool(
                        bool(info.get("skipped"))
                    ),
                }
            )
            if info.get("dated_uri"):
                metadata["dated_uri"] = dg.MetadataValue.url(info["dated_uri"])

        yield dg.Output(None, metadata=metadata)
        yield dg.AssetCheckResult(
            asset_key=dg.AssetKey(pid),
            check_name=_CHECK_NAME,
            passed=error == "" and feature_count > 0,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "feature_count": feature_count,
                "error": error or ("no features" if feature_count == 0 else ""),
            },
        )

    return _combine_asset


def _build_pod_age_combine_asset(product: dict, group: str) -> dg.AssetsDefinition:
    """Build the combine asset for a POD-age product (keyed ``[product_id]``).

    Like the correlation product, this is standalone: it gathers every source's
    sites itself via ``collect_sites`` (sites-only, incl. the OSE POD source,
    which is not part of any parameter cohort), then bins the OSE PODs completed
    in the trailing ``years_back`` years by completion year — either aggregated
    per county polygon (``ogc_pod_age_by_county``) or as one point per well
    (``ogc_pod_age_points``). County polygons come from geoconnex.us (cached)."""
    pid = product["id"]
    output_type = product["output_type"]
    years_back = int(product.get("years_back", 10))
    state = product.get("spatial_filter", {}).get("state", "NM")

    @dg.asset(
        key=dg.AssetKey(pid),
        group_name=group,
        description=(
            f"**{product.get('title', pid)}** — standalone combine asset "
            f"(`{output_type}`). {product.get('description', '').rstrip('.')}. "
            f"Gathers sites from every source (sites-only, incl. OSE PODs), bins "
            f"OSE PODs completed in the last {years_back} years by completion "
            f"year, and uploads the OGC GeoJSON to GCS. Depends on no shared "
            f"source asset."
        ),
        check_specs=[dg.AssetCheckSpec(name=_CHECK_NAME, asset=dg.AssetKey(pid))],
    )
    def _combine_asset(
        context: dg.AssetExecutionContext,
        die_config: DIEConfigResource,
        gcs: GCSResource,
    ) -> Iterator[dg.Output | dg.AssetCheckResult]:
        error = ""
        feature_count = 0
        info: dict = {}
        try:
            with forward_die_logs(context):
                config = die_config.get_config(product)
                sites = collect_sites(config)
                counties = get_state_county_polygons(state)

            meta = {
                "id": pid,
                "title": product.get("title", pid),
                "description": product.get("description", ""),
            }
            with tempfile.TemporaryDirectory() as tmpdir:
                out = Path(tmpdir) / "collection.geojson"
                if output_type == "ogc_pod_age_by_county":
                    coll = dump_pod_age_by_county_collection(
                        str(out), counties, sites, meta, years_back=years_back
                    )
                else:
                    coll = dump_pod_age_points_collection(
                        str(out), sites, counties, meta, years_back=years_back
                    )
                feature_count = len(coll.get("features", []))
                info = gcs.upload_product(str(out), pid)
        except Exception:
            error = traceback.format_exc()
            context.log.error(f"POD age combine failed for {pid}:\n{error}")

        metadata: dict = {"error": error}
        if info:
            metadata.update(
                {
                    "feature_count": dg.MetadataValue.int(
                        info.get("feature_count", feature_count)
                    ),
                    "latest_uri": dg.MetadataValue.url(info.get("latest_uri", "")),
                    "skipped_unchanged": dg.MetadataValue.bool(
                        bool(info.get("skipped"))
                    ),
                }
            )
            if info.get("dated_uri"):
                metadata["dated_uri"] = dg.MetadataValue.url(info["dated_uri"])

        yield dg.Output(None, metadata=metadata)
        yield dg.AssetCheckResult(
            asset_key=dg.AssetKey(pid),
            check_name=_CHECK_NAME,
            passed=error == "" and feature_count > 0,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "feature_count": feature_count,
                "error": error or ("no features" if feature_count == 0 else ""),
            },
        )

    return _combine_asset


def _num_opt(value):
    """None passes through; otherwise coerce to float (products.yaml overrides
    for the correlation thresholds are optional)."""
    return None if value is None else float(value)


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
        description=(
            f"GeoServer publish asset for **{product.get('title', pid)}**. "
            f"Downloads the combined GeoJSON for `{pid}` from GCS, converts it to "
            f"a GeoPackage, and publishes it as a GeoServer layer. Depends on the "
            f"combine asset for ordering only (no data passed). Never raises — a "
            f"publish failure fails the `{_GEOSERVER_CHECK_NAME}` check (WARN) "
            f"rather than the run."
        ),
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


def build_product_pipeline_assets(
    product: dict, specs: list[SourceSpec]
) -> list[dg.AssetsDefinition]:
    """Return the product's own assets — the combine asset and the geoserver
    publish asset. The shared source assets it consumes (``specs``) are built
    once by :func:`build_shared_source_asset` in ``definitions.py``, not here, so
    products sharing a source share one asset. The combine's group follows its
    parameter family (waterlevels vs analytes); standalone products (well
    correlation) get their own ``sites`` group and a self-contained combine."""
    if is_standalone(product):
        group = "sites"
        if product.get("output_type") == "ogc_well_correlation":
            combine = _build_correlation_combine_asset(product, group)
        else:
            combine = _build_pod_age_combine_asset(product, group)
    else:
        group = "waterlevels" if product.get("parameter") == WATERLEVELS else "analytes"
        combine = _build_combine_asset(product, specs, group)
    geoserver = _build_geoserver_asset(product, group)
    return [combine, geoserver]

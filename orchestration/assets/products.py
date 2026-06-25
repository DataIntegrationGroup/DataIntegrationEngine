"""Per-source Dagster asset graph for a product.

Each product fans out into one asset per data source (keyed
``[product_id, "sources", <source_key>]``) plus a combine asset (keyed
``[product_id]``) that merges every source's contribution, writes the OGC
GeoJSON collection, and uploads it to GCS.

Design notes:
- Source assets never hard-fail. They emit row-count metadata and an
  AssetCheckResult that goes red (WARN) on error or empty output, so one dead
  source surfaces in the UI without blocking the product's combine asset.
- Records cross the IO manager as plain ``_payload`` dicts (the record classes
  use ``__getattr__`` over ``_payload`` which does not survive pickling). The
  combine asset rebuilds record objects before dumping.
"""
import tempfile
import traceback
from pathlib import Path

import dagster as dg

from backend.config import PARAMETER_SOURCE_MAP, WATERLEVELS
from backend.persisters.ogc_features import (
    dump_summary_collection,
    dump_timeseries_collection,
)
from backend.record import ParameterRecord, SiteRecord, SummaryRecord
from backend.unifier import unify_source
from orchestration.logging_bridge import forward_die_logs
from orchestration.resources.die_config import DIEConfigResource
from orchestration.resources.gcs import GCSResource

_CHECK_NAME = "returned_data"


def _product_source_keys(product: dict) -> list:
    """Source keys that apply to this product: the parameter's agencies,
    filtered by the product's include/exclude list."""
    agencies = PARAMETER_SOURCE_MAP[product["parameter"]]["agencies"]
    spec = product.get("sources", {}) or {}
    if spec.get("include"):
        return [a for a in agencies if a in spec["include"]]
    if spec.get("exclude"):
        return [a for a in agencies if a not in spec["exclude"]]
    return list(agencies)


def _in_name(source_key: str) -> str:
    return f"src_{source_key.replace('-', '_')}"


def _build_source_asset(product: dict, source_key: str, group: str):
    pid = product["id"]
    src_key = dg.AssetKey([pid, "sources", source_key])
    is_summary = product["output_type"] == "ogc_summary"

    @dg.asset(
        key=src_key,
        group_name=group,
        check_specs=[dg.AssetCheckSpec(name=_CHECK_NAME, asset=src_key)],
    )
    def _source_asset(context: dg.AssetExecutionContext, die_config: DIEConfigResource):
        config = die_config.get_config(product)

        error = ""
        records, sites, timeseries = [], [], []
        try:
            with forward_die_logs(context):
                persister = unify_source(config, source_key)
            # Ship plain dicts across the IO manager; rebuild in combine.
            records = [r._payload for r in persister.records]
            sites = [s._payload for s in persister.sites]
            timeseries = [[o._payload for o in site_ts] for site_ts in persister.timeseries]
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


def _build_combine_asset(product: dict, source_keys: list, source_asset_keys: list, group: str):
    pid = product["id"]
    is_summary = product["output_type"] == "ogc_summary"
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
            if is_summary:
                records = [SummaryRecord(p) for p in all_records]
                dump_summary_collection(str(out), records, meta)
            else:
                site_records = [SiteRecord(p) for p in all_sites]
                flat = [
                    ParameterRecord(p) for site_ts in all_timeseries for p in site_ts
                ]
                dump_timeseries_collection(str(out), site_records, flat, meta)

            info = gcs.upload_product(str(out), pid)

        return dg.MaterializeResult(
            metadata={
                "feature_count": dg.MetadataValue.int(info["feature_count"]),
                "dated_uri": dg.MetadataValue.url(info["dated_uri"]),
                "latest_uri": dg.MetadataValue.url(info["latest_uri"]),
                "source_count": dg.MetadataValue.int(len(sources)),
            }
        )

    return _combine_asset


def build_product_assets(product: dict) -> list:
    """Return the per-source assets and the combine asset for *product*."""
    group = "waterlevels" if product["parameter"] == WATERLEVELS else "analytes"
    source_keys = _product_source_keys(product)

    source_assets = []
    source_asset_keys = []
    for sk in source_keys:
        asset, key = _build_source_asset(product, sk, group)
        source_assets.append(asset)
        source_asset_keys.append(key)

    combine = _build_combine_asset(product, source_keys, source_asset_keys, group)
    return source_assets + [combine]

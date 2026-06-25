import tempfile
from pathlib import Path

import dagster as dg

from backend.unifier import unify_waterlevels
from backend.persisters.ogc_features import dump_summary_collection, dump_timeseries_collection
from orchestration.resources.die_config import DIEConfigResource
from orchestration.resources.gcs import GCSResource
from orchestration.logging_bridge import forward_die_logs


def build_waterlevels_summary_asset(product: dict):
    @dg.asset(name=product["id"], group_name="waterlevels")
    def _wl_summary_asset(
        context: dg.AssetExecutionContext,
        die_config: DIEConfigResource,
        gcs: GCSResource,
    ) -> dg.MaterializeResult:
        config = die_config.get_config(product)
        config.output_summary = True

        with tempfile.TemporaryDirectory() as tmpdir:
            with forward_die_logs(context):
                unify_waterlevels(config)

            persister = getattr(config, "_persister", None)
            records = persister.records if persister else []

            out = Path(tmpdir) / "collection.geojson"
            meta = {
                "id": product["id"],
                "title": product.get("title", product["id"]),
                "description": product.get("description", ""),
            }
            dump_summary_collection(str(out), records, meta)
            info = gcs.upload_product(str(out), product["id"])

        return dg.MaterializeResult(
            metadata={
                "feature_count": dg.MetadataValue.int(info["feature_count"]),
                "dated_uri": dg.MetadataValue.url(info["dated_uri"]),
                "latest_uri": dg.MetadataValue.url(info["latest_uri"]),
            }
        )

    return _wl_summary_asset


def build_waterlevels_timeseries_asset(product: dict):
    """
    §V: ogc_timeseries features MUST be flat (one per observation).
    §V: MUST have ISO 8601 `datetime` property.
    §V: Each Feature MUST have top-level id.
    """

    @dg.asset(name=product["id"], group_name="waterlevels")
    def _wl_ts_asset(
        context: dg.AssetExecutionContext,
        die_config: DIEConfigResource,
        gcs: GCSResource,
    ) -> dg.MaterializeResult:
        config = die_config.get_config(product)
        config.output_summary = False
        config.output_timeseries_unified = True

        with tempfile.TemporaryDirectory() as tmpdir:
            with forward_die_logs(context):
                unify_waterlevels(config)

            persister = getattr(config, "_persister", None)
            site_records = persister.sites if persister else []
            timeseries = persister.timeseries if persister else []

            # timeseries is list-of-lists (per site); flatten to list of records
            flat_timeseries = [obs for site_ts in timeseries for obs in site_ts]

            out = Path(tmpdir) / "collection.geojson"
            meta = {
                "id": product["id"],
                "title": product.get("title", product["id"]),
                "description": product.get("description", ""),
            }
            dump_timeseries_collection(str(out), site_records, flat_timeseries, meta)
            info = gcs.upload_product(str(out), product["id"])

        return dg.MaterializeResult(
            metadata={
                "feature_count": dg.MetadataValue.int(info["feature_count"]),
                "dated_uri": dg.MetadataValue.url(info["dated_uri"]),
                "latest_uri": dg.MetadataValue.url(info["latest_uri"]),
            }
        )

    return _wl_ts_asset

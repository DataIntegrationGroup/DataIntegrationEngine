import tempfile
from pathlib import Path

import dagster as dg

from backend.unifier import unify_sites
from backend.persisters.ogc_features import dump_summary_collection
from orchestration.resources.die_config import DIEConfigResource
from orchestration.resources.gcs import GCSResource


def build_wells_asset(product: dict):
    @dg.asset(name=product["id"], group_name="wells")
    def _wells_asset(
        die_config: DIEConfigResource,
        gcs: GCSResource,
    ) -> dg.MaterializeResult:
        config = die_config.get_config(product)
        config.sites_only = True

        with tempfile.TemporaryDirectory() as tmpdir:
            unify_sites(config)

            # Collect sites from persister (set by unify_sites via _unify_parameter)
            from backend.persister import BasePersister
            sites = config._persister.sites if hasattr(config, "_persister") else []

            out = Path(tmpdir) / "collection.geojson"
            meta = {
                "id": product["id"],
                "title": product.get("title", product["id"]),
                "description": product.get("description", ""),
            }
            dump_summary_collection(str(out), sites, meta)

            info = gcs.upload_product(str(out), product["id"])

        return dg.MaterializeResult(
            metadata={
                "feature_count": dg.MetadataValue.int(info["feature_count"]),
                "dated_uri": dg.MetadataValue.url(info["dated_uri"]),
                "latest_uri": dg.MetadataValue.url(info["latest_uri"]),
                "file_size_bytes": dg.MetadataValue.int(info["file_size_bytes"]),
            }
        )

    return _wells_asset

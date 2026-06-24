import tempfile
from pathlib import Path

import dagster as dg

from backend.unifier import unify_analytes
from backend.persisters.ogc_features import dump_summary_collection
from orchestration.resources.die_config import DIEConfigResource
from orchestration.resources.gcs import GCSResource


def build_analyte_summary_asset(product: dict):
    @dg.asset(name=product["id"], group_name="analytes")
    def _analyte_asset(
        die_config: DIEConfigResource,
        gcs: GCSResource,
    ) -> dg.MaterializeResult:
        config = die_config.get_config(product)
        config.output_summary = True

        with tempfile.TemporaryDirectory() as tmpdir:
            unify_analytes(config)

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

    return _analyte_asset

"""GeoParquet Dagster IO manager for GeoDataFrame-valued assets.

Replaces the pickle handoff (`_TolerantGCSPickleIOManager` + the `_payload`
dicts) for assets that pass a GeoDataFrame between each other. A source asset
emits its sites/summary as a GeoDataFrame; this manager serializes it to
**GeoParquet** on GCS (typed schema + CRS in metadata, columnar/compressed —
smaller and higher-fidelity than pickle), and the downstream combine reads it
back as a GeoDataFrame.

The serialization core lives in ``backend.persisters.geodataframe``
(``gdf_to_parquet_bytes`` / ``parquet_bytes_to_gdf``) and is unit-tested there,
including a GeoParquet round-trip byte-parity test against the legacy dumper.
This module is only the Dagster + GCS glue; it runs in the orchestration deploy
env (dagster + google-cloud-storage + pyarrow), not the DIE dev venv.

Wiring: point a GeoDataFrame-valued asset's ``io_manager_key`` at this manager.
The source/combine assets must emit/consume a GeoDataFrame (or a mapping of
them) for this to apply — that payload refactor is the Phase-A fan-out step,
tracked in docs/framework-migration-plan.md.

Requires pyarrow (the ``parquet`` extra); present in the deploy env via the
dagster stack.
"""

import geopandas as gpd
import dagster as dg

from backend.persisters.geodataframe import (
    gdf_to_parquet_bytes,
    parquet_bytes_to_gdf,
)
from orchestration.resources.gcs import AuthedGCSResource

_CONTENT_TYPE = "application/vnd.apache.parquet"


class GeoDataFrameIOManager(dg.ConfigurableIOManager):
    """Persist GeoDataFrame assets as GeoParquet blobs in GCS.

    Blob key mirrors the pickle manager's layout: ``{gcs_prefix}/{asset key
    path…}[/{partition}].parquet``. A missing input blob degrades to an empty
    GeoDataFrame (mirroring ``_TolerantGCSPickleIOManager``), so an ad-hoc
    combine-only run — or a source never yet materialized — yields an empty
    collection instead of a hard failure.
    """

    gcs: AuthedGCSResource
    gcs_bucket: str
    gcs_prefix: str = "dagster-geoparquet"

    def _bucket(self):
        return self.gcs.get_client().bucket(self.gcs_bucket)

    def _blob_path(self, context) -> str:
        # get_identifier() returns the asset key path plus any partition key,
        # so distinct assets/partitions never collide.
        return "/".join([self.gcs_prefix, *context.get_identifier()]) + ".parquet"

    def handle_output(self, context: dg.OutputContext, obj) -> None:
        if obj is None:
            # Nothing to persist (asset soft-failed); leave no blob so a later
            # load_input degrades to empty rather than reading a stale one.
            return
        if not isinstance(obj, gpd.GeoDataFrame):
            raise TypeError(
                f"{type(self).__name__} expects a GeoDataFrame, got {type(obj).__name__} "
                f"for {context.asset_key.to_user_string()!r}"
            )
        data = gdf_to_parquet_bytes(obj)
        self._bucket().blob(self._blob_path(context)).upload_from_string(
            data, content_type=_CONTENT_TYPE
        )
        context.add_output_metadata({"rows": len(obj), "bytes": len(data)})

    def load_input(self, context: dg.InputContext) -> gpd.GeoDataFrame:
        blob = self._bucket().blob(self._blob_path(context.upstream_output))
        if not blob.exists():
            context.log.warning(
                f"GeoParquet input {context.asset_key.to_user_string()!r} not found "
                "in GCS; treating as empty. Run the source asset before the product "
                "that consumes it."
            )
            return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
        return parquet_bytes_to_gdf(blob.download_as_bytes())

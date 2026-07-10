"""Parquet Dagster IO manager for the shared-source payload handoff.

Persists the shared source assets' payloads as Parquet on GCS (this replaced an
earlier pickle IO manager). Each source asset outputs ``{"records": [...],
"sites": [...], "timeseries":
[[...]]}`` (flat scalar payload dicts). This manager serializes those three
parts as **Parquet** blobs on GCS — typed, columnar, compressed — and the
combine reads the identical dict back, so the source and combine asset bodies are
unchanged (the 20-product combine dispatch is untouched).

The serialization core (``dicts_to_parquet_bytes`` / ``parquet_bytes_to_dicts`` /
``timeseries_to_parquet_bytes`` / ``parquet_bytes_to_timeseries``) lives in
``backend.persisters.geodataframe`` and is unit-tested there, including a full
payload round-trip that preserves the sites[i] ↔ timeseries[i] grouping. This
module is only the Dagster + GCS glue; it runs in the orchestration deploy env
(dagster + google-cloud-storage + pyarrow), NOT the DIE dev venv, so it is
syntax-checked here but exercised in a Dagster branch deployment.

Geometry is built later, in the dumpers, so the handoff is plain Parquet, not
GeoParquet — records/sites carry latitude/longitude as columns.
"""

import dagster as dg

from backend.persisters.geodataframe import (
    dicts_to_parquet_bytes,
    parquet_bytes_to_dicts,
    parquet_bytes_to_timeseries,
    timeseries_to_parquet_bytes,
)
from orchestration.resources.gcs import AuthedGCSResource

_CONTENT_TYPE = "application/vnd.apache.parquet"
_RECORDS = "records.parquet"
_SITES = "sites.parquet"
_OBSERVATIONS = "observations.parquet"


class PayloadParquetIOManager(dg.ConfigurableIOManager):
    """Persist a shared-source ``{records, sites, timeseries}`` payload as three
    Parquet blobs in GCS.

    Blob layout: ``{gcs_prefix}/{asset key path…}[/{partition}]/{records|sites|
    observations}.parquet``. A missing input (source never materialized, or an
    ad-hoc combine-only run) degrades to an empty payload rather than failing,
    mirroring ``_TolerantGCSPickleIOManager``.
    """

    gcs: AuthedGCSResource
    gcs_bucket: str
    gcs_prefix: str = "dagster-parquet"

    def _bucket(self):
        return self.gcs.get_client().bucket(self.gcs_bucket)

    def _base_path(self, context) -> str:
        # get_identifier() = asset key path (+ partition), so distinct
        # assets/partitions never collide.
        return "/".join([self.gcs_prefix, *context.get_identifier()])

    def handle_output(self, context: dg.OutputContext, obj) -> None:
        if obj is None:
            # Source soft-failed with no output; leave no blobs so load_input
            # degrades to empty rather than reading stale data.
            return
        records = obj.get("records", [])
        sites = obj.get("sites", [])
        timeseries = obj.get("timeseries", [])

        base = self._base_path(context)
        bucket = self._bucket()
        bucket.blob(f"{base}/{_RECORDS}").upload_from_string(
            dicts_to_parquet_bytes(records), content_type=_CONTENT_TYPE
        )
        bucket.blob(f"{base}/{_SITES}").upload_from_string(
            dicts_to_parquet_bytes(sites), content_type=_CONTENT_TYPE
        )
        bucket.blob(f"{base}/{_OBSERVATIONS}").upload_from_string(
            timeseries_to_parquet_bytes(timeseries), content_type=_CONTENT_TYPE
        )
        context.add_output_metadata(
            {
                "record_count": len(records),
                "site_count": len(sites),
                "observation_count": sum(len(t) for t in timeseries),
            }
        )

    def load_input(self, context: dg.InputContext) -> dict:
        base = self._base_path(context.upstream_output)
        bucket = self._bucket()

        if not bucket.blob(f"{base}/{_RECORDS}").exists():
            context.log.warning(
                f"Parquet input {context.asset_key.to_user_string()!r} not found "
                "in GCS; treating as empty. Run the source asset before the product "
                "that consumes it."
            )
            return {"records": [], "sites": [], "timeseries": []}

        # Each part is read independently and a missing/partial blob degrades to
        # empty rather than crashing — handle_output writes the three blobs
        # sequentially, so a crash mid-write can leave records present but
        # sites/observations absent.
        def _read(name, reader):
            blob = bucket.blob(f"{base}/{name}")
            return reader(blob.download_as_bytes()) if blob.exists() else []

        return {
            "records": _read(_RECORDS, parquet_bytes_to_dicts),
            "sites": _read(_SITES, parquet_bytes_to_dicts),
            "timeseries": _read(_OBSERVATIONS, parquet_bytes_to_timeseries),
        }

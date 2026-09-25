import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import dagster as dg

try:
    from google.cloud import storage
    _GCS_AVAILABLE = True
except ImportError:
    _GCS_AVAILABLE = False


_CONTENT_HASH_KEY = "content_hash"
_LAST_CHANGED_KEY = "last_changed"  # YYYY-MM-DD the content last actually changed
_PARQUET_FORMAT_KEY = "parquet_format"  # converter version that wrote latest.parquet


def _days_between(start: str, end: str) -> Optional[int]:
    """Whole days between two YYYY-MM-DD strings, or None if unparseable."""
    try:
        a = datetime.strptime(start, "%Y-%m-%d")
        b = datetime.strptime(end, "%Y-%m-%d")
        return (b - a).days
    except (TypeError, ValueError):
        return None


def _content_hash(data: dict) -> str:
    """Stable SHA-256 of a product's parsed GeoJSON *data*, ignoring the volatile
    `timeStamp` so that re-running with unchanged data yields the same hash."""
    stable = {k: v for k, v in data.items() if k != "timeStamp"}
    payload = json.dumps(stable, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _storage_client():
    """Build a GCS client. Dagster+ serverless has no Application Default
    Credentials, so prefer an explicit service-account key from a Dagster+
    secret env var; fall back to ADC (local dev with
    `gcloud auth application-default login`)."""
    if not _GCS_AVAILABLE:
        raise ImportError("google-cloud-storage not installed")
    key = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
    if key:
        return storage.Client.from_service_account_info(json.loads(key))
    return storage.Client()


class GCSResource(dg.ConfigurableResource):
    """
    Upload OGC Feature Collection GeoJSON files to GCS, each with a GeoParquet
    copy (latest.parquet) for pygeoapi's Parquet provider.

    §V: latest.geojson MUST be overwritten atomically
        (copy from dated object, never direct overwrite of in-flight file).
    §V: No database — GCS is the sole store.
    """

    bucket_name: str
    products_prefix: str = "products"

    def _client(self):
        return _storage_client()

    def read_json(self, key: str) -> dict:
        """Read and parse a JSON object from the bucket at *key* (e.g.
        'config/mcl.json'). Raises if the object is missing."""
        client = self._client()
        bucket = client.bucket(self.bucket_name)
        return json.loads(bucket.blob(key).download_as_bytes())

    def download_latest(self, product_id: str, dest_path: str) -> str:
        """Download a product's latest.geojson to *dest_path*. Returns the path."""
        client = self._client()
        bucket = client.bucket(self.bucket_name)
        latest_key = f"{self.products_prefix}/{product_id}/latest.geojson"
        bucket.blob(latest_key).download_to_filename(dest_path)
        return dest_path

    def upload_product(
        self,
        local_path: str,
        product_id: str,
        run_date: Optional[str] = None,
    ) -> dict:
        """
        Upload *local_path* as both a dated archive and latest.geojson — unless
        the content is identical to what's already in GCS, in which case the
        upload is skipped to avoid duplicate dated archives.

        Dedup compares a content hash (ignoring the volatile timeStamp) against
        the hash stored on the current latest.geojson's metadata.

        Either way, latest.parquet is then brought in line with the GeoJSON
        (see :meth:`_sync_parquet`) — so an unchanged product still gets its
        Parquet backfilled.

        Returns dict with:
          dated_uri: gs://bucket/products/{product_id}/{date}.geojson (None if skipped)
          latest_uri: gs://bucket/products/{product_id}/latest.geojson
          feature_count: int
          file_size_bytes: int
          run_date: str
          skipped: bool — True when content matched and nothing was written
          parquet_uri: gs://bucket/products/{product_id}/latest.parquet
          parquet_status: "written" | "unchanged" | "failed"
          parquet_error: str — only when parquet_status is "failed"
        """
        if run_date is None:
            run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        client = self._client()
        bucket = client.bucket(self.bucket_name)

        dated_key = f"{self.products_prefix}/{product_id}/{run_date}.geojson"
        latest_key = f"{self.products_prefix}/{product_id}/latest.geojson"

        file_size = Path(local_path).stat().st_size
        with open(local_path, encoding="utf-8") as f:
            data = json.load(f)
        new_hash = _content_hash(data)
        feature_count = data.get("numberReturned", len(data.get("features", [])))

        latest_uri = f"gs://{self.bucket_name}/{latest_key}"

        # Skip if the existing latest.geojson has the same content hash. Carry
        # forward the last-changed date so we can report how long the data has
        # been static (a signal for tuning run frequency — e.g. data unchanged
        # for months means the schedule can safely drop to monthly).
        latest_blob = bucket.blob(latest_key)
        if latest_blob.exists():
            latest_blob.reload()
            existing_meta = latest_blob.metadata or {}
            if existing_meta.get(_CONTENT_HASH_KEY) == new_hash:
                last_changed = existing_meta.get(_LAST_CHANGED_KEY, run_date)
                return {
                    "dated_uri": None,
                    "latest_uri": latest_uri,
                    "feature_count": feature_count,
                    "file_size_bytes": file_size,
                    "run_date": run_date,
                    "skipped": True,
                    "last_changed": last_changed,
                    "days_since_last_change": _days_between(last_changed, run_date),
                    **self._sync_parquet(bucket, product_id, data, new_hash, local_path),
                }

        # Content changed (or first upload) — last_changed is now.
        dated_blob = bucket.blob(dated_key)
        dated_blob.metadata = {_CONTENT_HASH_KEY: new_hash, _LAST_CHANGED_KEY: run_date}
        dated_blob.upload_from_filename(local_path, content_type="application/geo+json")

        # §V: atomic latest — copy from the just-uploaded dated blob, not
        # another upload that could race with a concurrent reader. copy_blob
        # carries the content_hash/last_changed metadata to latest.
        bucket.copy_blob(dated_blob, bucket, latest_key)

        return {
            "dated_uri": f"gs://{self.bucket_name}/{dated_key}",
            "latest_uri": latest_uri,
            "feature_count": feature_count,
            "file_size_bytes": file_size,
            "run_date": run_date,
            "skipped": False,
            "last_changed": run_date,
            "days_since_last_change": 0,
            **self._sync_parquet(bucket, product_id, data, new_hash, local_path),
        }

    def _sync_parquet(
        self, bucket, product_id: str, data: dict, content_hash: str, local_path: str
    ) -> dict:
        """Make latest.parquet match the GeoJSON whose parsed content is *data*.

        Skips the write when latest.parquet already carries *content_hash* and
        the current PARQUET_FORMAT_VERSION, so a converter change rewrites
        files whose GeoJSON hasn't changed. Otherwise converts and uploads it (a single GCS upload replaces the
        object atomically). A conversion failure never fails the product — the
        GeoJSON is already published and pygeoapi falls back to it — but any
        existing latest.parquet is deleted so a stale copy can't be served."""
        key = f"{self.products_prefix}/{product_id}/latest.parquet"
        info: dict = {"parquet_uri": f"gs://{self.bucket_name}/{key}"}
        from backend.persisters.geodataframe import (
            PARQUET_FORMAT_VERSION,
            collection_to_geoparquet,
        )

        wanted = {_CONTENT_HASH_KEY: content_hash, _PARQUET_FORMAT_KEY: PARQUET_FORMAT_VERSION}
        blob = bucket.blob(key)
        if blob.exists():
            blob.reload()
            meta = blob.metadata or {}
            if all(meta.get(k) == v for k, v in wanted.items()):
                return {**info, "parquet_status": "unchanged"}

        parquet_path = Path(local_path).with_name(f"{product_id}.parquet")
        try:
            collection_to_geoparquet(data, parquet_path)
            blob.metadata = wanted
            blob.upload_from_filename(
                str(parquet_path), content_type="application/vnd.apache.parquet"
            )
        except Exception as exc:  # noqa: BLE001 — soft-fail, GeoJSON is the fallback
            dg.get_dagster_logger().warning(
                f"{product_id}: GeoParquet not written, pygeoapi will serve GeoJSON: {exc}"
            )
            if blob.exists():
                blob.delete()
            return {**info, "parquet_status": "failed", "parquet_error": str(exc)}
        finally:
            parquet_path.unlink(missing_ok=True)
        return {**info, "parquet_status": "written"}


from dagster_gcp.gcs import GCSResource as _DagsterGCSResource  # noqa: E402


class AuthedGCSResource(_DagsterGCSResource):
    """dagster_gcp GCSResource for the GCS IO manager. The stock resource builds
    its client via Application Default Credentials, which Dagster+ serverless
    lacks — so authenticate from GCP_SERVICE_ACCOUNT_KEY like GCSResource above.
    """

    def get_client(self):
        return _storage_client()

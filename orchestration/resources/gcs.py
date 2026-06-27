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


def _content_hash(local_path: str) -> str:
    """Stable SHA-256 of a product's GeoJSON content, ignoring the volatile
    `timeStamp` so that re-running with unchanged data yields the same hash."""
    with open(local_path, encoding="utf-8") as f:
        data = json.load(f)
    data.pop("timeStamp", None)
    payload = json.dumps(data, sort_keys=True, default=str).encode("utf-8")
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
    Upload OGC Feature Collection GeoJSON files to GCS.

    §V: latest.geojson MUST be overwritten atomically
        (copy from dated object, never direct overwrite of in-flight file).
    §V: No database — GCS is the sole store.
    """

    bucket_name: str
    products_prefix: str = "products"

    def _client(self):
        return _storage_client()

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

        Returns dict with:
          dated_uri: gs://bucket/products/{product_id}/{date}.geojson (None if skipped)
          latest_uri: gs://bucket/products/{product_id}/latest.geojson
          feature_count: int
          file_size_bytes: int
          run_date: str
          skipped: bool — True when content matched and nothing was written
        """
        if run_date is None:
            run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        client = self._client()
        bucket = client.bucket(self.bucket_name)

        dated_key = f"{self.products_prefix}/{product_id}/{run_date}.geojson"
        latest_key = f"{self.products_prefix}/{product_id}/latest.geojson"

        file_size = Path(local_path).stat().st_size
        new_hash = _content_hash(local_path)

        with open(local_path, encoding="utf-8") as f:
            data = json.load(f)
        feature_count = data.get("numberReturned", len(data.get("features", [])))

        latest_uri = f"gs://{self.bucket_name}/{latest_key}"

        # Skip if the existing latest.geojson has the same content hash.
        latest_blob = bucket.blob(latest_key)
        if latest_blob.exists():
            latest_blob.reload()
            if (latest_blob.metadata or {}).get(_CONTENT_HASH_KEY) == new_hash:
                return {
                    "dated_uri": None,
                    "latest_uri": latest_uri,
                    "feature_count": feature_count,
                    "file_size_bytes": file_size,
                    "run_date": run_date,
                    "skipped": True,
                }

        dated_blob = bucket.blob(dated_key)
        dated_blob.metadata = {_CONTENT_HASH_KEY: new_hash}
        dated_blob.upload_from_filename(local_path, content_type="application/geo+json")

        # §V: atomic latest — copy from the just-uploaded dated blob, not
        # another upload that could race with a concurrent reader. copy_blob
        # carries the content_hash metadata to latest for the next dedup check.
        bucket.copy_blob(dated_blob, bucket, latest_key)

        return {
            "dated_uri": f"gs://{self.bucket_name}/{dated_key}",
            "latest_uri": latest_uri,
            "feature_count": feature_count,
            "file_size_bytes": file_size,
            "run_date": run_date,
            "skipped": False,
        }


from dagster_gcp.gcs import GCSResource as _DagsterGCSResource  # noqa: E402


class AuthedGCSResource(_DagsterGCSResource):
    """dagster_gcp GCSResource for the GCS IO manager. The stock resource builds
    its client via Application Default Credentials, which Dagster+ serverless
    lacks — so authenticate from GCP_SERVICE_ACCOUNT_KEY like GCSResource above.
    """

    def get_client(self):
        return _storage_client()

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
        if not _GCS_AVAILABLE:
            raise ImportError("google-cloud-storage not installed")
        # Dagster+ serverless has no Application Default Credentials. Prefer an
        # explicit service-account key from a Dagster+ secret env var; fall back
        # to ADC (local dev with `gcloud auth application-default login`).
        key = os.environ.get("GCP_SERVICE_ACCOUNT_KEY")
        if key:
            info = json.loads(key)
            return storage.Client.from_service_account_info(info)
        return storage.Client()

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
        Upload *local_path* as both a dated archive and latest.geojson.

        Returns dict with:
          dated_uri: gs://bucket/products/{product_id}/{date}.geojson
          latest_uri: gs://bucket/products/{product_id}/latest.geojson
          feature_count: int
          file_size_bytes: int
        """
        if run_date is None:
            run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        client = self._client()
        bucket = client.bucket(self.bucket_name)

        dated_key = f"{self.products_prefix}/{product_id}/{run_date}.geojson"
        latest_key = f"{self.products_prefix}/{product_id}/latest.geojson"

        file_size = Path(local_path).stat().st_size

        dated_blob = bucket.blob(dated_key)
        dated_blob.upload_from_filename(local_path, content_type="application/geo+json")

        # §V: atomic latest — copy from the just-uploaded dated blob, not
        # another upload that could race with a concurrent reader.
        bucket.copy_blob(dated_blob, bucket, latest_key)

        import json
        with open(local_path, encoding="utf-8") as f:
            data = json.load(f)
        feature_count = data.get("numberReturned", len(data.get("features", [])))

        return {
            "dated_uri": f"gs://{self.bucket_name}/{dated_key}",
            "latest_uri": f"gs://{self.bucket_name}/{latest_key}",
            "feature_count": feature_count,
            "file_size_bytes": file_size,
            "run_date": run_date,
        }

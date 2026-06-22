# DIE Orchestration

Dagster-based pipeline that runs on a schedule, fetches data from 12 NM water sources,
and writes OGC Feature Collections to GCS for serving via pygeoapi.

## Architecture

```
Cloud Scheduler → Cloud Run Job (this image)
                       └── Dagster asset materialize
                            ├── DIE unifier (backend/)
                            ├── OGCFeaturesPersister → .geojson
                            └── GCSResource → gs://die-products/
```

## Products

Configured in `config/products.yaml`. Each product becomes a Dagster asset + Cloud Scheduler job.

## Local Development

```bash
# Install all deps
uv sync --extra gcs
uv pip install dagster dagster-gcp

# Run a specific product locally (uses local filesystem, no GCS)
PRODUCT_ID=nm_waterlevels_summary \
uv run dagster asset materialize -f orchestration/definitions.py --select nm_waterlevels_summary

# View asset graph in Dagster UI
uv run dagster dev -f orchestration/definitions.py
```

## Environment Variables

| Variable | Source | Description |
|----------|--------|-------------|
| `PRODUCT_ID` | Cloud Scheduler | Which asset to materialize |
| `GCS_BUCKET` | Cloud Run env | GCS bucket for output (default: `die-products`) |
| `USGS_API_KEY` | Secret Manager | USGS rate-limit key |

## GCP Setup

### Service Account

```bash
gcloud iam service-accounts create die-orchestration-sa \
  --display-name="DIE Orchestration"

# GCS write access
gcloud storage buckets add-iam-policy-binding gs://die-products \
  --member="serviceAccount:die-orchestration-sa@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Secret Manager read
gcloud projects add-iam-policy-binding PROJECT_ID \
  --member="serviceAccount:die-orchestration-sa@PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### Build & Deploy

```bash
# Build image
gcloud builds submit --config orchestration/cloudbuild.yaml .

# Deploy job
sed -i 's/PROJECT_ID/your-project-id/g' orchestration/cloudrun.yaml
gcloud run jobs replace orchestration/cloudrun.yaml --region us-central1

# Create Cloud Scheduler trigger per product
gcloud scheduler jobs create http die-nm_waterlevels_summary \
  --schedule="0 6 * * *" \
  --time-zone="UTC" \
  --uri="https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/PROJECT_ID/jobs/die-orchestration:run" \
  --message-body='{"overrides": {"containerOverrides": [{"env": [{"name": "PRODUCT_ID", "value": "nm_waterlevels_summary"}]}]}}' \
  --oauth-service-account-email=die-orchestration-sa@PROJECT_ID.iam.gserviceaccount.com
```

## pygeoapi

See `pygeoapi/README.md` for serving the GCS-stored products via OGC API - Features.

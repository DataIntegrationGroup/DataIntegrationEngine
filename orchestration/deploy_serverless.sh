#!/bin/bash
# Manual deploy of the DIE code location to Dagster+ Serverless.
#
# Same target as the GitHub Action (.github/workflows/dagster-cloud-deploy.yml):
# a PEX fast-deploy built from dagster_cloud.yaml. Use this to push from a laptop
# without going through CI.
#
# Prerequisites:
#   - Docker running locally (build-method=docker builds manylinux-compatible
#     wheels so the deploy works regardless of host OS).
#   - uv installed (the dagster-cloud CLI is pulled in on the fly via `uv run`).
#
# Required env vars:
#   DAGSTER_CLOUD_ORGANIZATION   your Dagster+ org name (e.g. "nmwd")
#   DAGSTER_CLOUD_API_TOKEN      a Dagster+ user/agent token
# Optional:
#   DEPLOYMENT                   target deployment (default: prod)
#
# Usage:
#   DAGSTER_CLOUD_ORGANIZATION=nmwd DAGSTER_CLOUD_API_TOKEN=*** \
#     orchestration/deploy_serverless.sh
set -euo pipefail

: "${DAGSTER_CLOUD_ORGANIZATION:?set DAGSTER_CLOUD_ORGANIZATION to your Dagster+ org}"
: "${DAGSTER_CLOUD_API_TOKEN:?set DAGSTER_CLOUD_API_TOKEN to a Dagster+ token}"
DEPLOYMENT="${DEPLOYMENT:-prod}"

# Run from repo root so dagster_cloud.yaml + the orchestration build dir resolve.
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

echo "Deploying location 'die-orchestration' to org '$DAGSTER_CLOUD_ORGANIZATION' deployment '$DEPLOYMENT'..."

uv run --with dagster-cloud -- \
  dagster-cloud serverless deploy-python-executable \
    --organization "$DAGSTER_CLOUD_ORGANIZATION" \
    --deployment "$DEPLOYMENT" \
    --location-file dagster_cloud.yaml \
    --location-name die-orchestration \
    --python-version 3.10 \
    --build-method docker

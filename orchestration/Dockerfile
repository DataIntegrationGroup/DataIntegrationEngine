FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install system deps needed by geopandas / shapely
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy lockfiles and project manifests first (layer cache)
COPY pyproject.toml uv.lock ./
COPY orchestration/pyproject.toml ./orchestration/

# Install all deps (root package + orchestration)
RUN uv sync --frozen --extra gcs && \
    uv pip install \
        dagster>=1.8 \
        dagster-gcp>=0.24 \
        google-cloud-storage \
        google-cloud-secret-manager \
        Jinja2

# Copy source code
COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY orchestration/ ./orchestration/

ENV PYTHONPATH=/app
ENV DAGSTER_HOME=/app/.dagster

# Cloud Run Job: PRODUCT_ID env var selects which asset to materialize.
# Cloud Scheduler sets PRODUCT_ID in the job invocation.
CMD ["sh", "-c", \
     "uv run dagster asset materialize \
      -f orchestration/definitions.py \
      --select ${PRODUCT_ID}"]

# DIE Orchestration + GCP Modernization Spec
## Branch: feature/orchestration-gcp-uv

---

## §1 Context & Goals

DIE is a data integration engine that unifies NM water data from 12 heterogeneous sources.
Currently: pip-installable CLI (`die weave`), partial Dagster code on `jir-dagster` (unmerged).

**Goals for this branch:**
1. Migrate package management to **uv + pyproject.toml**
2. Add **Dagster orchestration layer** (internal, not shipped in pip package)
3. Deploy orchestration as **GCP Cloud Run**
4. Produce **OGC Feature Collections** as canonical data products
5. Generate **time-series data products** per well / parameter
6. Products defined in a **configurable YAML manifest**
7. Serve data products via **pygeoapi** (OGC API - Features standard)

---

## §2 Non-Goals

- No changes to public CLI (`die weave`, `die sites`, `die sources`)
- No changes to existing `backend/` integration logic (sources, transformers, unifier)
- No changes to existing `frontend/cli.py`
- Orchestration code NOT shipped to PyPI
- No database required for orchestration pipeline (GCS is the store)
- GeoServer/PostGIS persister remains available as optional CLI output — not used here

---

## §3 Architecture

### §3.1 Repository Structure (post-migration)

```
DataIntegrationEngine/
├── pyproject.toml              # uv project (replaces setup.py + requirements.txt)
├── uv.lock                     # pinned lockfile
├── backend/                    # unchanged — core integration
├── frontend/                   # unchanged — CLI + legacy API
├── orchestration/              # NEW — not in pip package
│   ├── pyproject.toml          # orchestration-specific deps
│   ├── Dockerfile              # Cloud Run Job image (Dagster)
│   ├── cloudbuild.yaml         # Cloud Build CI/CD
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── wells.py            # well site assets
│   │   ├── waterlevels.py      # water level timeseries assets
│   │   └── analytes.py         # analyte assets
│   ├── resources/
│   │   ├── __init__.py
│   │   ├── die_config.py       # DIE Config Dagster resource
│   │   └── gcs.py              # GCS upload/download resource
│   ├── config/
│   │   └── products.yaml       # configurable product manifest
│   ├── definitions.py          # Dagster Definitions (entry point)
│   └── pygeoapi/               # pygeoapi API server
│       ├── config.yml.j2       # Jinja2 template for pygeoapi config
│       ├── generate_config.py  # renders config.yml from products.yaml
│       ├── Dockerfile          # extends geopython/pygeoapi
│       └── cloudbuild.yaml     # Cloud Build for pygeoapi image
├── tests/                      # unchanged
└── SPEC.md
```

### §3.2 Dependency Separation

```
pyproject.toml (public CLI)
  [project.dependencies]         ← lean: click, httpx, geopandas, pyyaml, pandas, etc.
  [project.optional-dependencies]
    dev = [pytest, mypy, flake8]
    geoserver = [psycopg2-binary, GeoAlchemy2, SQLAlchemy]   # optional, existing feature

orchestration/pyproject.toml (internal, never published)
  [project.dependencies]         ← dagster, dagster-gcp, google-cloud-storage,
                                    google-cloud-secret-manager, Jinja2
```

No database deps in orchestration core. GCS is the sole store.

### §3.3 GCP Deployment

```
Cloud Scheduler (cron)
  → Cloud Run Job  ← Dagster orchestration (stateless)
       └── DIE unifier (existing backend)
            ├── fetch from 12 sources
            ├── transform → OGC FC GeoJSON files
            └── upload → gs://die-products/{product_id}/
                                    │
                              GCS bucket
                              (public read)
                                    │
                         GDAL /vsigs/ virtual FS
                                    │
                    Cloud Run Service ← pygeoapi (always-on)
                    (OGR provider reads GeoJSON from GCS)
                                    │
                               HTTP clients
                          (OGC API - Features)
```

**Two Cloud Run deployments:**
- **Cloud Run Job** (Dagster, stateless): triggered by Cloud Scheduler, runs pipeline per product
- **Cloud Run Service** (pygeoapi, always-on): serves OGC API - Features via GDAL OGR + GCS

No PostgreSQL required. GCS is authoritative store. pygeoapi reads GeoJSON directly from
GCS via GDAL's `/vsigs/` virtual filesystem — no proxy, no DB, no sync step.

> GeoServer/PostGIS persister in `backend/persisters/geoserver.py` is unchanged and
> still usable via CLI `--output-format geoserver`. Not part of this pipeline.

---

## §4 OGC Feature Collections

### §4.1 Format

OGC API - Features compliant GeoJSON written by `OGCFeaturesPersister`.

**Collection envelope:**
```json
{
  "type": "FeatureCollection",
  "id": "nm_waterlevels_summary",
  "title": "NM Unified Water Levels Summary",
  "description": "...",
  "timeStamp": "2026-06-22T06:00:00Z",
  "numberMatched": 1234,
  "numberReturned": 1234,
  "links": [
    {"href": "gs://die-products/nm_waterlevels_summary/latest.geojson",
     "rel": "self", "type": "application/geo+json"}
  ],
  "features": [...]
}
```

Each Feature has a top-level `id` (OGC requirement):
```json
{
  "type": "Feature",
  "id": "nmbgmr_amp:RA-1234",
  "geometry": {"type": "Point", "coordinates": [-106.5, 35.2, 1650.0]},
  "properties": { ... }
}
```

### §4.2 Summary Features

One feature per well site. Properties = existing `SummaryRecord` fields
(nrecords, min, max, mean, earliest_date, latest_date, latest_value, etc.).

### §4.3 Timeseries Features (flat format)

**One feature per observation** — not per well. This enables pygeoapi `time_field`
temporal filtering natively without custom code.

```json
{
  "type": "Feature",
  "id": "nmbgmr_amp:RA-1234:2024-04-20",
  "geometry": {"type": "Point", "coordinates": [-106.5, 35.2, 1650.0]},
  "properties": {
    "site_id": "RA-1234",
    "site_name": "Roswell Basin Well",
    "source": "nmbgmr_amp",
    "parameter": "waterlevels",
    "value": 218.1,
    "units": "ft",
    "datetime": "2024-04-20T00:00:00Z"
  }
}
```

`datetime` is an ISO 8601 timestamp — pygeoapi maps it to `time_field` for
`?datetime=` query parameter support.

### §4.4 New Persister

`backend/persisters/ogc_features.py` → `OGCFeaturesPersister`
- `dump_summary_collection(path, records, meta)` — §4.2 format
- `dump_timeseries_collection(path, site_records, timeseries_records, meta)` — §4.3 format
- Writes local `.geojson` file; Dagster GCS resource handles upload

---

## §5 Dagster Assets

### §5.1 Asset Graph

```
products_config            ← loads products.yaml at startup
     │
     ▼
[per product, per schedule]
 source_data               ← unify_waterlevels / unify_analytes (existing)
     │
     ▼
 ogc_collection            ← OGCFeaturesPersister → tmp .geojson
     │
     ▼
 gcs_upload                ← gs://die-products/{product_id}/{YYYY-MM-DD}.geojson
                              gs://die-products/{product_id}/latest.geojson  (overwrite)
```

### §5.2 Configurable Products (`orchestration/config/products.yaml`)

```yaml
gcs_bucket: die-products

products:
  - id: nm_waterlevels_summary
    parameter: waterlevels
    output_type: ogc_summary
    title: "NM Unified Water Levels Summary"
    description: "Summary stats for water levels, all NM sources"
    schedule: "0 6 * * *"         # UTC cron
    spatial_filter:
      state: NM
    sources:
      exclude: []

  - id: nm_waterlevels_timeseries
    parameter: waterlevels
    output_type: ogc_timeseries
    title: "NM Water Levels Time Series"
    description: "Per-observation water level measurements, all NM sources"
    schedule: "0 7 * * *"
    spatial_filter:
      state: NM
    sources:
      exclude: []

  - id: bernco_waterlevels_timeseries
    parameter: waterlevels
    output_type: ogc_timeseries
    title: "Bernalillo County Water Level Time Series"
    description: "Bernalillo County water level timeseries per well"
    schedule: "0 8 * * *"
    spatial_filter:
      county: Bernalillo
    sources:
      include: [bernco]

  - id: nm_arsenic_summary
    parameter: arsenic
    output_type: ogc_summary
    title: "NM Arsenic Summary"
    description: "Arsenic concentration summary stats, all NM sources"
    schedule: "0 9 * * *"
    spatial_filter:
      state: NM
    sources:
      exclude: []
```

Assets are dynamically generated from `products.yaml` at Dagster definition time.

### §5.3 Schedule Strategy (MVP)

One Cloud Run Job per schedule group. Cloud Scheduler triggers with `PRODUCT_ID` env var.
Single Dagster `definitions.py` handles all products; job selects by product id.
Structure supports later migration to persistent Dagster daemon (change Cloud Run Job → Service).

---

## §6 pygeoapi

### §6.1 Role

pygeoapi serves the GCS-stored GeoJSON files as OGC API - Features collections.
No DB. pygeoapi uses the **OGR provider** backed by GDAL's `/vsigs/` virtual filesystem,
which reads GeoJSON directly from GCS using Application Default Credentials on Cloud Run.

```
GET /collections
GET /collections/{id}/items
GET /collections/{id}/items/{feature_id}
GET /collections/{id}/items?bbox=-107,32,-103,37
GET /collections/{id}/items?datetime=2020-01-01/2024-12-31   ← timeseries only
```

### §6.2 pygeoapi Config Template (`orchestration/pygeoapi/config.yml.j2`)

```yaml
server:
  bind:
    host: 0.0.0.0
    port: 80
  url: ${PYGEOAPI_SERVER_URL}
  mimetype: application/json
  encoding: utf-8
  language: en-US
  cors: true
  pretty_print: false
  limit: 500

logging:
  level: ERROR

metadata:
  identification:
    title: NM Unified Water Data
    description: OGC API - Features for New Mexico water data
    keywords: [water, groundwater, "New Mexico", NMBGMR]
    keywords_type: theme
    terms_of_service: https://creativecommons.org/licenses/by/4.0/
    url: https://waterdata.nmt.edu
  license:
    name: CC-BY 4.0
    url: https://creativecommons.org/licenses/by/4.0/
  provider:
    name: NM Bureau of Geology & Mineral Resources
    url: https://geoinfo.nmt.edu

resources:
{% for product in products %}
  {{ product.id }}:
    type: collection
    title: {{ product.title }}
    description: {{ product.description }}
    keywords: [water, groundwater, "New Mexico"]
    extent:
      spatial:
        bbox: [-109.05, 31.33, -103.00, 37.00]
        crs: http://www.opengis.net/def/crs/OGC/1.3/CRS84
{% if product.output_type == 'ogc_timeseries' %}
      temporal:
        interval: [["1900-01-01T00:00:00Z", null]]
{% endif %}
    providers:
      - type: feature
        name: OGR
        data:
          source_type: GeoJSON
          source: /vsigs/{{ gcs_bucket }}/{{ product.id }}/latest.geojson
          source_options:
            GDAL_HTTP_UNSAFESSL: NO
          gdal_ogr_options:
            EMPTY_AS_NULL: NO
            GDAL_CACHEMAX: 64
        id_field: id
        layer: OGRGeoJSON
{% if product.output_type == 'ogc_timeseries' %}
        time_field: datetime
{% endif %}

{% endfor %}
```

### §6.3 Config Generation (`orchestration/pygeoapi/generate_config.py`)

```python
import yaml
from jinja2 import Environment, FileSystemLoader
from pathlib import Path

def generate(products_path: Path, template_path: Path, output_path: Path):
    products = yaml.safe_load(products_path.read_text())
    env = Environment(loader=FileSystemLoader(str(template_path.parent)))
    tmpl = env.get_template(template_path.name)
    output_path.write_text(tmpl.render(
        products=products["products"],
        gcs_bucket=products["gcs_bucket"],
    ))
```

Run at Docker build time in `cloudbuild.yaml` — baked into image, not runtime.

### §6.4 GDAL + GCS Auth

On Cloud Run, GDAL `/vsigs/` uses the service account's ADC automatically.
No credentials file needed. Require the pygeoapi Cloud Run Service account to have
`roles/storage.objectViewer` on the `die-products` bucket.

For local dev:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa-key.json
```

### §6.5 Dockerfile (`orchestration/pygeoapi/Dockerfile`)

```dockerfile
FROM geopython/pygeoapi:latest

# Generate config from products.yaml at build time
COPY ../config/products.yaml /tmp/products.yaml
COPY config.yml.j2 /tmp/config.yml.j2
COPY generate_config.py /tmp/generate_config.py
RUN python /tmp/generate_config.py \
      --products /tmp/products.yaml \
      --template /tmp/config.yml.j2 \
      --output /pygeoapi/local.config.yml

EXPOSE 80
```

Cloud Run Service env vars:
- `PYGEOAPI_SERVER_URL` — public Cloud Run URL
- Port: 80

---

## §7 uv Migration

### §7.1 Root `pyproject.toml`

Replaces `setup.py`, `requirements.txt`, `pytest.ini`, `mypy.ini`:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "nmuwd"
version = "0.10.3"
requires-python = ">=3.10"
dependencies = [
    "click>=8.2.1",
    "python-dotenv",
    "frost_sta_client",
    "geopandas",
    "httpx",
    "pandas",
    "pyyaml",
    "types-pyyaml",
    "urllib3>=2.2.0,<3.0.0",
]

[project.optional-dependencies]
dev = ["pytest", "mypy", "flake8"]
geoserver = ["psycopg2-binary", "GeoAlchemy2", "SQLAlchemy"]
gcs = ["google-cloud-storage"]

[project.scripts]
die = "frontend.cli:cli"

[tool.hatch.build.targets.wheel]
packages = ["frontend", "backend"]

[tool.pytest.ini_options]
testpaths = ["tests"]
norecursedirs = ["tests/archived"]

[tool.mypy]
ignore_missing_imports = true
```

`flask`, `gunicorn` removed from core — belong in deployment layer.

### §7.2 `orchestration/pyproject.toml`

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "die-orchestration"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "dagster>=1.8",
    "dagster-gcp>=0.24",
    "dagster-webserver>=1.8",
    "google-cloud-storage",
    "google-cloud-secret-manager",
    "Jinja2",
]

[tool.uv.sources]
nmuwd = { path = "..", editable = true }
```

No DB deps. pygeoapi runs in its own image — not a Python dep here.

---

## §8 Dockerfile — Dagster Cloud Run Job

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY orchestration/pyproject.toml ./orchestration/
COPY pyproject.toml ./
RUN uv sync --frozen --project orchestration

COPY backend/ ./backend/
COPY frontend/ ./frontend/
COPY orchestration/ ./orchestration/

ENV DAGSTER_HOME=/app/.dagster
ENV PYTHONPATH=/app

CMD ["uv", "run", "--project", "orchestration", \
     "dagster", "job", "execute", \
     "-f", "orchestration/definitions.py", \
     "-j", "${PRODUCT_ID}"]
```

Cloud Run Job env vars (from Secret Manager):
`PRODUCT_ID`, `GCS_BUCKET`, `USGS_API_KEY`

---

## §9 Tasks

### §T.1 [~] uv migration
- Delete `setup.py`, `requirements.txt`, `pytest.ini`, `mypy.ini`
- Write root `pyproject.toml` (§7.1)
- Run `uv lock`
- Update `.github/workflows/cicd.yml`: `uv run pytest`, `uv run mypy`, `uv run flake8`
- Verify: `uv pip install -e ".[dev]"` + all existing tests pass

### §T.2 [x] OGC Features persister
- Add `backend/persisters/ogc_features.py` → `OGCFeaturesPersister`
  - `dump_summary_collection(path, records, meta)` — §4.2
  - `dump_timeseries_collection(path, site_records, timeseries_records, meta)` — §4.3 flat format
- Add `ogc_summary`, `ogc_timeseries` to `OutputFormat` enum in `backend/__init__.py`
- Tests: `tests/test_persisters/test_ogc_features.py`

### §T.3 [x] Orchestration scaffolding
- Create `orchestration/` directory (§3.1)
- Write `orchestration/pyproject.toml` (§7.2)
- Write `orchestration/config/products.yaml` (§5.2)
- Write `orchestration/resources/die_config.py` — Dagster resource wrapping `Config`
- Write `orchestration/resources/gcs.py` — upload/overwrite GCS objects

### §T.4 [x] Dagster assets
- Port `jir-dagster` branch assets into `orchestration/assets/`
- Rewrite to: load products.yaml → dynamically define one asset per product
- Each asset: build `Config` from product spec → call unifier → `OGCFeaturesPersister` → GCS upload
- Write `orchestration/definitions.py`
- Local test: `uv run dagster asset materialize -f orchestration/definitions.py --select nm_waterlevels_summary`

### §T.5 [x] Time-series well assets
- `orchestration/assets/waterlevels.py` — `ogc_timeseries` product type
- Flat observation-per-feature output (§4.3) with `datetime` field
- Reuses existing `unify_waterlevels` — no backend changes

### §T.6 [x] GCS output resource
- `orchestration/resources/gcs.py`
- Upload: `gs://{bucket}/products/{product_id}/{YYYY-MM-DD}.geojson`
- Overwrite: `gs://{bucket}/products/{product_id}/latest.geojson`
- Emit Dagster `AssetMaterialization` metadata: feature count, bbox, file size, timestamp

### §T.7 [x] Cloud Run + Dockerfile
- Write `orchestration/Dockerfile` (§8)
- Write `orchestration/cloudbuild.yaml`
- Write `orchestration/cloudrun.yaml` — Cloud Run Job definition
- Write `orchestration/README.md` — env vars, Secret Manager bindings, deploy commands

### §T.8 [x] CI update
- Update `cicd.yml`: use `uv run` for all checks
- Add `orchestration-ci.yml`: lint + import-check for orchestration code
- Orchestration CI never triggers PyPI publish

### §T.9 [x] pygeoapi
- Write `orchestration/pygeoapi/config.yml.j2` (§6.2)
- Write `orchestration/pygeoapi/generate_config.py` (§6.3)
- Write `orchestration/pygeoapi/Dockerfile` (§6.5)
- Write `orchestration/pygeoapi/cloudbuild.yaml`
- Verify GDAL `/vsigs/` reads from GCS with ADC in local Docker test
- Smoke test: `GET /collections` returns one entry per product in `products.yaml`

---

## §V Invariants

- Orchestration code MUST NOT appear in `[tool.hatch.build.targets.wheel].packages`
- OGC FC output MUST include top-level `id`, `type`, `numberReturned`, `timeStamp`
- Each Feature MUST have top-level `id` (not only in properties)
- `ogc_timeseries` features MUST be flat (one per observation) with ISO 8601 `datetime` property
- `die` CLI behavior unchanged after uv migration
- All existing tests pass under `uv run pytest`
- pygeoapi config MUST be generated from `products.yaml` — never hand-edited
- pygeoapi OGR provider MUST use `/vsigs/` path (GCS), never local filesystem path
- No database introduced in orchestration pipeline — GCS is sole store
- `latest.geojson` MUST be overwritten atomically (upload to tmp key, then copy/rename)

## §B Bug Log

### §B.1 CLI `--no-*` flags non-functional (fixed in §T.1)
**Cause:** `ALL_SOURCE_OPTIONS` used `is_flag=True, default=True` — Click flag presence also sets `True`, so both states gave `True`. Assignment `use_source_X = no_X` further confused the polarity.
**Fix:** `default=False` on all `--no-*` options + `not lcs.get(f"no_{agency}", False)` in `weave` and `sites` commands.
**Invariant added:** `--no-*` flags MUST have `default=False`; assignment MUST negate the flag value.

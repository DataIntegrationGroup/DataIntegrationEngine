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
              copied into the image at build (Cloud Build)
                                    │
                    Cloud Run Service ← pygeoapi (always-on)
                    (Parquet provider: point products;
                     OGR provider: polygon products, GeoJSON)
                                    │
                               HTTP clients
                          (OGC API - Features)
```

**Two Cloud Run deployments:**
- **Cloud Run Job** (Dagster, stateless): triggered by Cloud Scheduler, runs pipeline per product
- **Cloud Run Service** (pygeoapi, always-on): serves OGC API - Features from product
  files baked into its image

No PostgreSQL required. GCS is authoritative store. The pygeoapi image build copies each
product's current file from GCS into the image, so the service reads local disk — no
proxy, no DB — and new data is served after the next image build (§6.3, §6.6).

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
- Writes local `.geojson` file; Dagster GCS resource handles upload, and also writes a
  GeoParquet copy (`latest.parquet`, see §6.1) via `geodataframe.collection_to_geoparquet`

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
                              gs://die-products/{product_id}/latest.parquet  (overwrite)
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

pygeoapi serves the GCS-stored product files as OGC API - Features collections. No DB.
Each product is served from one of two files, chosen and copied into the image when it
is built (§6.3):

| When | Provider | Source (in the image; copied from `gs://{bucket}/products/{id}/`) |
|---|---|---|
| `latest.parquet` is usable | **Parquet** (`id_field: feature_id`; `time_field: datetime` for `ogc_timeseries`) | `/data/products/{id}/latest.parquet` |
| otherwise | **OGR** (`source_type: GeoJSON`) | `/data/products/{id}/latest.geojson` |

Parquet is preferred because the OGR provider parses the whole GeoJSON on every request
(~9–10 s for the 733 MB `nm_waterlevels_timeseries` file, ~0.1 s per MB from GCS for the
rest, vs. well under 1 s from GeoParquet) and ignores `datetime` filters.

**Writing `latest.parquet`.** `GCSResource.upload_product` writes it after every GeoJSON
upload, stamped with the GeoJSON's content hash and the converter's
`PARQUET_FORMAT_VERSION`. When the GeoJSON is unchanged it still writes the Parquet if it
is missing or either stamp differs (backfill; bump the version when the converter's output
changes). A conversion failure
does not fail the product: the Parquet is deleted so a stale copy can't be served, and the
asset's `parquet_status` / `parquet_error` metadata record it. The file has:
- GeoParquet 1.1 `geo` metadata with WKB geometry and a **bbox covering column**
- `feature_id`: the feature's top-level GeoJSON id, made unique (repeats get `:2`, `:3`, …;
  timeseries ids are `source:site:date`, which repeats for several readings a day)
- `datetime` (when present) as a **tz-aware UTC** `timestamp[ms]`, rows sorted by `id`,
  `source`, then `datetime`, so a one-well query (`?id=`) reads one or two row groups
- `string` rather than `large_string` columns; dict/list values JSON-encoded; columns
  mixing bool/number/string values stored as strings

**Usable** means: GeoParquet metadata with a bbox covering column, **point geometry only**,
an id column (`feature_id`, else `id`), and for `ogc_timeseries` a tz-aware `datetime`.
Polygon layers stay on GeoJSON because the Parquet provider answers `bbox` by containment
(the feature's bbox fully inside the query box), not intersection, and would drop
polygons crossing the box edge.

The image patches the Parquet provider (`orchestration/pygeoapi/patch_pygeoapi.py`, run by
the Dockerfile against the pinned base image; the build fails if the patched source
changes):
- **Paging:** a filtered page returned up to `limit + 1` rows plus earlier batches, so
  pages overlapped. Every page but the last now has exactly `limit` rows.
- **Date-only `datetime`** bounds (`2020-01-01/2020-12-31`) returned 500 against the
  tz-aware column. They're now read as UTC.
- **No `bbox`:** responses carried a collection `bbox`, which was `[NaN, NaN, NaN, NaN]`
  (invalid JSON) for empty results, and a `bbox` on every feature. Both are now left
  out, matching the OGR collections.
- **True `numberMatched`:** items pages report the total count of matching rows (as
  `resulttype=hits` does), not `offset` + rows returned + 1. That costs ~20–110 ms of
  server time per request.

Known provider limitations:
- OGR: no `numberMatched` or `numberReturned`; DateTime properties are returned as
  `YYYY/MM/DD HH:MM:SS+00`.

```
GET /collections
GET /collections/{id}/items
GET /collections/{id}/items/{feature_id}
GET /collections/{id}/items?bbox=-107,32,-103,37
GET /collections/{id}/items?datetime=2020-01-01T00:00:00Z/2024-12-31T23:59:59Z   ← timeseries only
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
  gzip: true   # Cloud Run does not compress; timeseries pages shrink 10-45x
  limits:
    default_items: 500
    max_items: 10000   # NewWeaver requests limit=10000
  map:
    url: https://tile.openstreetmap.org/{z}/{x}/{y}.png
    attribution: '&copy; <a href="https://openstreetmap.org/copyright">OpenStreetMap contributors</a>'

logging:
  level: ERROR

metadata:
  identification:
    title: New Mexico Unified Water Data
    description: >-
      This tool integrates groundwater-level and water-quality data for New Mexico
      from state and federal sources, including NMBGMR, USGS, and the Water Quality Portal.
      Its collections provide per-well summaries, trends, time series, and derived
      water-quality indicators through the OGC API - Features standard.
    keywords:
      - water
      - groundwater
      - New Mexico
      - NMBGMR
    keywords_type: theme
    terms_of_service: https://creativecommons.org/licenses/by/4.0/
    url: ${PYGEOAPI_SERVER_URL}   # the live Cloud Run URL, set in cloudbuild.yaml
  license:
    name: CC-BY 4.0
    url: https://creativecommons.org/licenses/by/4.0/
  provider:
    name: NM Bureau of Geology & Mineral Resources
    url: https://geoinfo.nmt.edu
  contact:
    name: NM Bureau of Geology & Mineral Resources
    address: 801 Leroy Place
    city: Socorro
    stateorprovince: New Mexico
    postalcode: "87801"
    country: USA
    email: data-services-nmbg@nmt.edu

resources:
{% for product in products %}
  {{ product.id }}:
    type: collection
    title: {{ product.title | tojson }}
    description: {{ product.description | tojson }}
    keywords:
      - water
      - groundwater
      - New Mexico
    extents:
      spatial:
        bbox:
          - -109.05
          - 31.33
          - -103.00
          - 37.00
        crs: http://www.opengis.net/def/crs/OGC/1.3/CRS84
{% if product.output_type == 'ogc_timeseries' %}
      temporal:
        begin: 1900-01-01T00:00:00Z   # unquoted: pygeoapi needs a tz-aware datetime, not a string
        end: null
{% endif %}
    providers:
{% if product.id in parquet %}
      # GeoParquet via the Parquet provider — chosen by generate_config.py when
      # latest.parquet is usable. Faster than OGR (indexed reads instead of a full
      # GeoJSON parse per request) and the only one that applies datetime filters.
      - type: feature
        name: Parquet
        data:
          source: {{ parquet_root }}/{{ product.id }}/latest.parquet
        id_field: {{ parquet[product.id] }}
{% if product.output_type == 'ogc_timeseries' %}
        time_field: datetime
{% endif %}
{% else %}
      - type: feature
        name: OGR
        data:
          source_type: GeoJSON
          source: {{ geojson_root }}/{{ product.id }}/latest.geojson
          source_options:
            GDAL_HTTP_UNSAFESSL: "NO"
          gdal_ogr_options:
            EMPTY_AS_NULL: "NO"
            GDAL_CACHEMAX: "64"
        id_field: id
        layer: latest   # GDAL names a GeoJSON layer after the file basename
{% endif %}

{% endfor %}
```

Notes on fields current pygeoapi requires (each fails at boot or first request if wrong):
`metadata.contact.name`, `server.map`, `extents` (plural), `server.limits` (the old
`server.limit` is deprecated and leaves the HTML items page broken), `temporal.begin`/`end`
(not the `interval` array used in API responses; `begin` unquoted so YAML parses a
tz-aware datetime), and `layer: latest` for GeoJSON (GDAL names the layer after the file
basename).

### §6.3 Config Generation (`orchestration/pygeoapi/generate_config.py`)

Renders `config.yml.j2` from `products.yaml` at **image build time** — baked into the
image, not runtime. It reads each product's `latest.parquet` footer from GCS and picks
the Parquet provider when the file is usable (§6.1), otherwise OGR + GeoJSON; the build
log lists each product's choice and reason. A missing or unreadable file falls back to
GeoJSON; any other error (permissions, network) fails the build rather than silently
serving everything as GeoJSON.

- `--bake-to /data/products` (Cloud Build, `BAKE_PRODUCTS=1`): also copies the chosen
  file into the image and points the config at the copy. A product with neither a usable
  `latest.parquet` nor a `latest.geojson` fails the build, so the previous revision keeps
  serving. New data is served after the next build (§6.6).
- `--check-parquet` (`CHECK_PARQUET=1`): same choice, but the config reads GCS directly
  (OGR via `/vsigs/`, Parquet via `gs://`).
- Neither (e.g. a plain local `docker build`): every product is GeoJSON read from GCS.

`--source-root DIR` replaces GCS with a local directory — local testing only.

### §6.4 GCS Auth

With baked products (the Cloud Build default) the service reads no GCS at runtime; only
the build does. The rest of this section applies to images built without `BAKE_PRODUCTS`.

On Cloud Run, both GDAL `/vsigs/` and PyArrow's `gs://` filesystem use the service
account's ADC. No credentials file needed. GDAL only uses the metadata server when it
detects GCE, which it can't on Cloud Run — `CPL_MACHINE_IS_GCE=YES` must be set or every
OGR collection fails with `No valid GCS credentials found`. PyArrow needs no setting.
Require the pygeoapi Cloud Run service account to have `roles/storage.objectViewer` on the products bucket
(`gcs_bucket` in `products.yaml`). The Cloud Build account needs the same role for the
build-time check and copy (§6.3).

For local dev, render the config inside the image against a mounted products folder:
```bash
docker run --rm --entrypoint /venv/bin/python3 \
  -v ~/pygeoapi-test/products:/data/products:ro -v ~/pygeoapi-test:/out die-pygeoapi \
  /tmp/generate_config.py --products /tmp/products.yaml --template /tmp/config.yml.j2 \
  --output /out/local.config.yml --check-parquet --source-root /data/products
```

### §6.5 Dockerfile (`orchestration/pygeoapi/Dockerfile`)

Build context is `orchestration/` so `config/products.yaml` can be copied.

```dockerfile
# Pinned: patch_pygeoapi.py edits this version's Parquet provider source.
# pygeoapi 0.25.dev0 (geopython/pygeoapi:latest as of 2026-09-24).
FROM geopython/pygeoapi@sha256:f3dd50a56f870d80df67416b6b07bf4d92aefa78b7e6f697564288f1cdf1df3f

# Cloud Build bakes the product files into the image (BAKE_PRODUCTS=1 below),
# so the service reads local disk. Without it the config reads GCS directly:
# the base image's GDAL has /vsigs/ support, and auth uses Application Default
# Credentials — no key file needed on Cloud Run.

WORKDIR /pygeoapi

# pygeoapi's Parquet provider needs pyarrow and geopandas, which the base
# image doesn't ship (s3fs, shapely, pandas are already in /venv).
# Pinned to the versions tested locally against pygeoapi 0.25.
RUN /venv/bin/python3 -m pip install --no-cache-dir --quiet \
      pyarrow==25.0.1 geopandas==1.1.4

# Fix the Parquet provider's paging and date-only datetime queries. Fails the
# build if the source it patches has changed (see the script's docstring).
COPY pygeoapi/patch_pygeoapi.py /tmp/patch_pygeoapi.py
RUN /venv/bin/python3 /tmp/patch_pygeoapi.py

# Copy generation inputs
# Build context is orchestration/ (see cloudbuild.yaml)
COPY pygeoapi/config.yml.j2 /tmp/config.yml.j2
COPY pygeoapi/generate_config.py /tmp/generate_config.py
COPY config/products.yaml /tmp/products.yaml

# Bake config into image at build time.
# §V: config generated from products.yaml, not hand-edited.
# jinja2 and PyYAML are pygeoapi dependencies, already in /venv.
# BAKE_PRODUCTS=1 (set by cloudbuild.yaml) copies each product's usable
# latest.parquet, or else its latest.geojson, from GCS into /data/products and
# serves it from there; new data therefore needs a rebuild. The build needs GCS
# access for that (docker build --network=cloudbuild). Build with --no-cache
# locally, or Docker reuses the previous download.
# CHECK_PARQUET=1 picks the same files but keeps reading them from GCS.
# With neither, every product is GeoJSON read from GCS.
ARG BAKE_PRODUCTS=0
ARG CHECK_PARQUET=0
RUN /venv/bin/python3 /tmp/generate_config.py \
      --products /tmp/products.yaml \
      --template /tmp/config.yml.j2 \
      --output /pygeoapi/local.config.yml \
      $([ "$CHECK_PARQUET" = "1" ] && echo --check-parquet) \
      $([ "$BAKE_PRODUCTS" = "1" ] && echo --bake-to /data/products)

EXPOSE 80

# pygeoapi reads PYGEOAPI_CONFIG env var; default is local.config.yml
ENV PYGEOAPI_CONFIG=/pygeoapi/local.config.yml
```

Cloud Build (`orchestration/pygeoapi/cloudbuild.yaml`) builds with `--network=cloudbuild` and
`--build-arg=BAKE_PRODUCTS=1` so the build can check and copy the products from GCS (§6.3).
Each Cloud Build runs a fresh Docker daemon, so no layer cache serves stale data; locally,
build with `--no-cache` to re-download.

Cloud Run Service (`orchestration/pygeoapi/cloudbuild.yaml`):
- `PYGEOAPI_SERVER_URL` — `https://die-pygeoapi-$PROJECT_NUMBER.$_REGION.run.app`
- `CPL_MACHINE_IS_GCE=YES` — lets GDAL `/vsigs/` use Cloud Run credentials (§6.4); unused
  with baked products, kept for images built without them
- Port: 80
- Service account: `die-pygeoapi@$PROJECT_ID.iam.gserviceaccount.com` — needs
  `roles/storage.objectViewer` on the products bucket; Cloud Build's account needs
  `roles/iam.serviceAccountUser` on it
- `WSGI_WORKERS=2` — gunicorn workers (base image default 4)
- Memory: 2Gi — with 4 workers a sustained load of 10,000-row pages pushed the local
  container to its 2 GiB cap; 2 workers peaked at ~1.8 GiB with the same timings

### §6.6 Rebuild After a Pipeline Run

Baked products (§6.3) are only as fresh as the image: a pipeline run that publishes new
data (a combine asset with `skipped_unchanged: false` or `parquet_status: written`) is
not served until pygeoapi is rebuilt and redeployed with
`orchestration/pygeoapi/cloudbuild.yaml`. **For now this rebuild is manual**: run the
build after the scheduled product runs finish, and not while any are still running, so
the image doesn't copy a half-updated set of products. Automating it (a Cloud Build
trigger started from Dagster after product runs publish new data) is planned; see
`docs/pygeoapi-performance-plan.md` T5.

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

### §T.1 [x] uv migration
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

## §10 Backend Improvements

### §10.1 Performance

**Retry backoff** (`_execute_text_request`, `_execute_json_request` in `source.py`): linear `time.sleep(tries)` → exponential backoff capped at 60s.

**Polygon re-parse per record** (`BaseTransformer.contained()`, `transformer.py`): `_cached_polygon` is set at instance level but `config.bounding_wkt()` is called on every record. Cache shapely object permanently at first call.

**Redundant list extraction in `BaseParameterSource.read()`** (`source.py`): `_extract_parameter_dates()`, `_extract_source_parameter_results()`, `_extract_source_parameter_units()`, `_extract_source_parameter_names()` called independently per site, each iterating the same `records` list. Batch extract once before loop.

### §10.2 Reliability

**Bare `except Exception`** (`_execute_text_request` line ~241, `_site_wrapper` in `unifier.py`): catches everything including `KeyboardInterrupt` siblings. Catch `httpx.HTTPError`, `httpx.TimeoutException`, `json.JSONDecodeError` specifically. Log full traceback.

**No coordinate range validation** (`do_transform()` in `transformer.py`): checks `x == 0 or y == 0` but not whether lng/lat are in valid ranges (−180..180, −90..90). Silent pass-through of bogus coords.

**Unchecked unit conversion** (`convert_units()` `transformer.py`): returns `None` if `die_parameter_name` is unrecognized, propagates silently into record payload.

**`with` statement missing on file open** (`Config._load_from_yaml()` `config.py`): unclosed handle on read failure.

**Manual slice rollback** (`_site_wrapper()` `unifier.py` lines ~183–202): slices `persister.records/timeseries/sites` back to pre-chunk length on error. Fragile — an atomic checkpoint abstraction is safer.

### §10.3 Observability

**`print()` instead of logger** (multiple): `generate_bounding_polygon()` in `source.py`, lines ~52/63/75 in `unifier.py`, line ~29 in `persister.py`. None go through `self.log()`.

**No request timing** (`_execute_text_request/json_request`): no record of latency, retry count, or which URL failed. Add structured log entry: `source`, `url`, `status_code`, `attempt`, `elapsed_ms` on every attempt.

**Low-information warnings**: "Failed to retrieve records after multiple attempts" doesn't include URL, params, or last exception.

**No transform failure metrics** (`do_transform()` `transformer.py`): returns `None` silently. Caller doesn't know how many records were dropped and why.

**No chunk progress** (`_site_wrapper()` `unifier.py`): no log of chunk index, site count per chunk, or timing.

### §10.4 Readability

**`BaseParameterSource` god class** (`source.py`, ~476 lines): handles extraction, validation, unit conversion, and summarization in one class + one 167-line `read()` method with 5 levels of nesting. Split into: `RecordExtractor`, `RecordValidator`, `RecordSummarizer`.

**`do_transform()` god method** (`transformer.py`, ~191 lines): 6 sequential transform steps in one method body. Extract each into `_apply_datum_transform()`, `_apply_elevation_transform()`, `_apply_well_depth_transform()`, `_apply_unit_conversion()`.

**`Config.get_config_and_false_agencies()`** (`config.py`, ~107 lines): repetitive `if/elif` per parameter. Replace with a dict mapping `parameter → (agency_defaults, source_classes)`.

**`start_ind` / `end_ind` in `BaseParameterSource.read()`**: only used for logging but add confusion. Rename or remove if unused.

**`bookend` naming** (`_extract_terminal_record()`): unclear. Rename to `position` or use `Literal["earliest", "latest"]`.

### §10.5 Additional Composition (Sources / Transformers / Unifier)

**HTTP client injection** (`BaseSource`): uses `httpx.get()` directly. Inject `httpx.Client` (or a protocol) so retry policy is testable and swappable.

**Config post-construction injection** (`set_config()` on both `BaseSource` and `BaseTransformer`): config is required to function. Move to `__init__` param with `Optional` type; keep `set_config()` only as override for unifier's late binding.

**`RecordExtractor` protocol** (`BaseParameterSource`): the 8 abstract `_extract_*` methods form an implicit interface. Define an explicit `ParameterExtractor` Protocol; `BaseParameterSource` accepts one in `__init__`. Enables injecting fake extractors in tests.

**`UnitConverter` strategy** (`convert_units()` in `transformer.py`): 120+ line monolithic function. Extract to `UnitConverter` class; inject into `BaseTransformer`. Enables per-source custom conversions.

**Persister factory in `unifier.py`**: `_unify_parameter()` contains if/else to pick persister class. Extract to `PersisterFactory(config) -> BasePersister`; inject factory into `Unifier.__init__`.

---

## §11 Composition Refactor

### §11.1 Goals

Replace inheritance-for-code-reuse with injected dependencies. Targets:
1. `Loggable` base class — used only to get `self.log()` → inject logger
2. `STSource` mixin via multiple inheritance → `STClient` composed into sources
3. ST2 class explosion (5 near-identical subclasses) → instances with config
4. `CloudStoragePersister` overrides `_dump_*` to redirect output → Strategy pattern
5. Transformer coupled by `transformer_klass` class attribute → inject transformer
6. Empty record subclasses (`WaterLevelRecord`, `AnalyteRecord`, etc.) → type field

### §11.2 New Branch

```
feature/composition-refactor   ← branch off main after §T.9 merged
```

---

### §T.10 [x] Replace `Loggable` base with injected logger
**Goal:** Remove `Loggable` from the inheritance chain of all classes.

**Changes:**
- `backend/logger.py`: add `make_logger(name: str) -> Logger` factory function
- `BaseSource`, `BasePersister`, `BaseTransformer`, `Config`: remove `(Loggable)` base; call `make_logger(self.__class__.__name__)` in `__init__`
- All `self.log()` / `self.warn()` / `self.debug()` calls: keep working — keep the same helper wrappers as module-level or instance-assigned callables rather than inherited methods

**Verification:** `uv run pytest tests/test_cli/ tests/test_persisters/ -q`

---

### §T.11 [x] Replace `STSource` mixin with `STClient` composition
**Goal:** Kill multiple inheritance in all ST source classes.

**Changes:**
- `backend/connectors/st_connector.py`: extract `STSource` methods into `STClient` class with `__init__(self, url: str)`
  - `get_service()`, `get_things()`, `_extract_terminal_record()`, `_parse_result()` → methods on `STClient`
- `STSiteSource(BaseSiteSource, STSource)` → `STSiteSource(BaseSiteSource)` with `self.client = STClient(self.url)`
- `STWaterLevelSource(STSource, BaseWaterLevelSource)` → `STWaterLevelSource(BaseWaterLevelSource)` with `self.client = STClient(self.url)`
- `STAnalyteSource(STSource, BaseAnalyteSource)` → `STAnalyteSource(BaseAnalyteSource)` with `self.client = STClient(self.url)`
- All `self.get_service()` / `self._get_things()` call sites → `self.client.get_service()` / `self.client.get_things()`

**Verification:** `uv run pytest tests/test_sources/ -k "st or bernco or cabq or ebid or pvacd or roswell" -q`

---

### §T.12 [x] Collapse ST2 class hierarchy into configured instances
**Goal:** Delete 5 nearly-identical site source classes; replace with factory.

**Affected classes (delete):**
`BernCoSiteSource`, `CABQSiteSource`, `EBIDSiteSource`, `PVACDSiteSource`, `NMOSERoswellSiteSource`

**Changes:**
- `ST2SiteSource`: accept `agency: str`, `bounding_wkt: str | None`, `transformer_klass` in `__init__`; move per-subclass logic (bounding polygon, filter) into constructor
- `backend/connectors/st2/source.py` (or equivalent): replace class definitions with module-level instances:
  ```python
  BernCoSiteSource = ST2SiteSource(agency="BernCo", bounding_wkt=BERNCO_WKT, transformer_klass=BernCoSiteTransformer)
  ```
- `Config.water_level_sources()` / `Config.analyte_sources()`: update to use instances

**Verification:** `uv run pytest tests/test_sources/ -k "bernco or cabq or ebid or pvacd" -q`

---

### §T.13 [x] Replace `CloudStoragePersister` with output strategy injection
**Goal:** `BasePersister` accepts an output strategy; `CloudStoragePersister` subclass deleted.

**Changes:**
- Add `backend/persisters/strategies.py`:
  ```python
  class OutputStrategy(Protocol):
      def write(self, name: str, content: bytes) -> None: ...
      def make_directory(self, path: str) -> None: ...

  class LocalFileStrategy:
      def write(self, name, content): Path(name).write_bytes(content)
      def make_directory(self, path): Path(path).mkdir(parents=True, exist_ok=True)

  class GCSStrategy:
      def __init__(self, bucket_name: str, prefix: str): ...
      def write(self, name, content): ...  # uploads to GCS
      def make_directory(self, path): pass  # no-op
  ```
- `BasePersister.__init__`: accept `strategy: OutputStrategy = LocalFileStrategy()`
- All `_dump_*` methods: call `self.strategy.write(...)` instead of `Path.write_*`
- Delete `CloudStoragePersister` class
- Update `backend/unifier.py`: create `GCSStrategy` instead of `CloudStoragePersister` when `config.use_cloud_storage`

**Verification:** `uv run pytest tests/ -q --ignore=tests/test_sources`

---

### §T.14 [x] Inject transformer into source constructor
**Goal:** Remove `transformer_klass` class attribute pattern; pass transformer as dependency.

**Changes:**
- `BaseSource.__init__`: accept `transformer: BaseTransformer` parameter; remove `self.transformer = self.transformer_klass()`
- All concrete source classes: remove `transformer_klass` class attribute; pass transformer in `super().__init__(transformer=XTransformer())`
- `set_config(config)`: still propagates to both source + transformer
- Tests that construct sources directly: update constructors

**Verification:** `uv run pytest tests/test_cli/ tests/test_persisters/ -q`

---

### §T.15 [x] Collapse empty record subclasses
**Goal:** `WaterLevelRecord`, `AnalyteRecord`, `WaterLevelSummaryRecord`, `AnalyteSummaryRecord` add zero behavior — remove them.

**Changes:**
- `backend/record.py`: delete `WaterLevelRecord`, `AnalyteRecord`, `WaterLevelSummaryRecord`, `AnalyteSummaryRecord`
- Add `record_type: str` field to `ParameterRecord` and `SummaryRecord` keys
- `WaterLevelTransformer._get_record_klass()` → returns `ParameterRecord` or `SummaryRecord`; sets `record_type="waterlevels"` in transform
- `AnalyteTransformer._get_record_klass()` → same pattern with `record_type="analytes"`
- Grep for `isinstance(r, WaterLevelRecord)` etc. — update to `r.record_type == "waterlevels"`

**Verification:** `uv run pytest tests/test_cli/ tests/test_persisters/ -q`

---

### §T.16 [x] Exponential backoff + request structured logging
**Goal:** Fix linear retry backoff; add per-request structured log entries.

**Changes:**
- `backend/source.py` `_execute_text_request()` + `_execute_json_request()`:
  - Replace `time.sleep(tries)` with `time.sleep(min(2 ** tries, 60))`
  - After each attempt log: `source`, `url`, `status_code`, `attempt`, `elapsed_ms`
  - Catch `httpx.HTTPStatusError`, `httpx.TimeoutException`, `httpx.RequestError` specifically — no bare `except Exception`
  - Include last exception message in "Failed after N attempts" warning

**Verification:** `uv run pytest tests/test_cli/ -q`

---

### §T.17 [x] Cache bounding polygon at class level
**Goal:** Prevent re-parsing WKT shapely object on every record.

**Changes:**
- `backend/transformer.py` `BaseTransformer.contained()`:
  - Move `_cached_polygon` from instance variable to class-level cache keyed on WKT string (e.g. `_polygon_cache: dict[str, Polygon] = {}`)
  - First call for a given WKT parses and caches; subsequent calls return cached object

**Verification:** `uv run pytest tests/test_cli/ tests/test_persisters/ -q` + manual timing on 1000-record transform

---

### §T.18 [x] Batch extraction in `BaseParameterSource.read()`
**Goal:** Extract dates/results/units/names once before the per-site loop, not once per site.

**Changes:**
- `backend/source.py` `BaseParameterSource.read()`:
  - Call `_extract_parameter_dates()`, `_extract_source_parameter_results()`, `_extract_source_parameter_units()`, `_extract_source_parameter_names()` once on full `cleaned` records before the site loop
  - Pass extracted lists into inner loop rather than re-extracting per site
  - Extract 167-line `read()` body into `_summarize_records()` and `_build_timeseries_records()` helpers (≤50 lines each)

**Verification:** `uv run pytest tests/test_cli/ -q`

---

### §T.19 [x] Replace all `print()` with structured logging
**Goal:** All console output goes through the logger; no raw `print()` in backend.

**Changes:**
- `backend/source.py` `generate_bounding_polygon()` lines ~450–452: `print()` → `self.log()`
- `backend/unifier.py` lines ~52/63/75: `print()` → `config.log()`
- `backend/persister.py` line ~29: `print("google cloud storage not available")` → `logging.warning()`
- Grep `print(` across `backend/` — replace every hit
- Add `elapsed_ms` to transform failure log in `do_transform()` when returning `None`
- Log chunk index + site count per chunk in `_site_wrapper()`

**Verification:** `grep -r "print(" backend/ | wc -l` → 0

---

### §T.20 [x] Specific exception handling + input validation
**Goal:** No bare `except Exception`; all swallowed errors surface detail.

**Changes:**
- `backend/source.py`:
  - `_execute_text_request` / `_execute_json_request`: replace bare except → specific httpx exceptions (see §T.16)
  - `_extract_site_records()`: guard against `None`/empty `records` before returning
  - `read()` inner ValueError/TypeError catches: log full `traceback.format_exc()`, not just message
- `backend/transformer.py` `convert_units()`:
  - If `die_parameter_name` unrecognized → raise `ValueError(f"Unknown parameter: {die_parameter_name}")` instead of returning `None`
  - Add lat/lng range check: `assert -180 <= lng <= 180 and -90 <= lat <= 90`
- `backend/unifier.py` `_site_wrapper()`:
  - Replace `except BaseException` → `except Exception`; log `traceback.format_exc()` via `config.warn()`
- `backend/config.py` `_load_from_yaml()`:
  - Wrap file open in `with` statement

**Verification:** `uv run pytest tests/test_cli/ tests/test_persisters/ -q`

---

### §T.21 [x] Split `BaseParameterSource` god class
**Goal:** 476-line class → focused classes ≤150 lines each.

**Changes:**
- Extract `RecordValidator` class with `validate(record) -> bool`; holds current `_validate_record()` logic
- Extract `RecordSummarizer` class with `summarize(records, site_record) -> SummaryRecord`; holds summary path of `read()`
- `BaseParameterSource.__init__` accepts `validator: RecordValidator` (default = existing subclass method shim during migration)
- Split `read()` into `read_summary()` + `read_timeseries()` ≤50 lines each
- Rename `bookend` parameter → `position: Literal["earliest", "latest"]`

**Verification:** `uv run pytest tests/ -q --ignore=tests/test_sources`

---

### §T.22 [x] Split `do_transform()` into focused methods
**Goal:** 191-line method → orchestrator + focused helpers ≤30 lines each.

**Changes:**
- `backend/transformer.py` `BaseTransformer.do_transform()`:
  - Extract `_apply_geographic_filter(record) -> bool`
  - Extract `_apply_datum_transform(record) -> record`
  - Extract `_apply_elevation_transform(record) -> record`
  - Extract `_apply_well_depth_transform(record) -> record`
  - Extract `_apply_unit_conversion(record) -> record`
  - `do_transform()` becomes orchestrator calling each in sequence ≤40 lines

**Verification:** `uv run pytest tests/test_cli/ tests/test_persisters/ -q`

---

### §T.23 [x] Data-driven `Config` source setup
**Goal:** Replace ~107-line `if/elif` per parameter in `get_config_and_false_agencies()` with a mapping.

**Changes:**
- `backend/config.py`:
  - Add `PARAMETER_SOURCE_MAP: dict[str, dict]` mapping each parameter name → `{site_source_klass, parameter_source_klass, agencies}`
  - `get_config_and_false_agencies()` looks up parameter in map; raises `ValueError` for unknown parameter
  - Extract duplicate `set_config()` calls in `analyte_sources()` / `water_level_sources()` / `all_site_sources()` into `_build_source_pair(site_klass, param_klass) -> tuple`

**Verification:** `uv run pytest tests/test_cli/ -q`

---

### §T.24 [x] Inject HTTP client into `BaseSource`
**Goal:** `httpx.get()` hardcoded → injected client; enables testability without live network.

**Changes:**
- `backend/source.py` `BaseSource.__init__`: accept `http_client: httpx.Client | None = None`; default creates `httpx.Client(timeout=900)`
- `_execute_text_request()` / `_execute_json_request()`: use `self._http_client.get(...)` instead of `httpx.get(...)`
- Tests in `tests/test_cli/` or new `tests/test_sources_unit/`: pass mock client returning fixture responses — no live HTTP

**Verification:** `uv run pytest tests/test_cli/ tests/test_persisters/ -q`

---

### §T.25 [x] `UnitConverter` as injectable strategy
**Goal:** Replace 120+ line `convert_units()` monolith with pluggable converter.

**Changes:**
- `backend/converter.py` (new file):
  ```python
  class UnitConverter(Protocol):
      def convert(self, value: float, from_units: str, to_units: str, parameter: str) -> float: ...

  class StandardUnitConverter:
      def convert(self, value, from_units, to_units, parameter): ...
      # current convert_units() logic moved here
  ```
- `backend/transformer.py` `BaseTransformer.__init__`: accept `converter: UnitConverter = StandardUnitConverter()`
- Remove `convert_units()` module-level function; call `self.converter.convert(...)` in `_apply_unit_conversion()`
- ST/DWB sources needing custom conversion: pass custom `UnitConverter` subclass

**Verification:** `uv run pytest tests/test_cli/ tests/test_persisters/ -q`

---

### §T.26 [x] `PersisterFactory` extracted from `Unifier`
**Goal:** Remove persister selection if/else from `_unify_parameter()`.

**Changes:**
- `backend/persisters/factory.py` (new file):
  ```python
  def make_persister(config: Config) -> BasePersister:
      if config.output_format == OutputFormat.GEOSERVER:
          ...
      elif config.use_cloud_storage:
          ...
      else:
          return BasePersister(config)
  ```
- `backend/unifier.py` `_unify_parameter()`: call `make_persister(config)` instead of inline if/else
- `Unifier.__init__`: optionally accept `persister_factory: Callable[[Config], BasePersister]` for testing

**Verification:** `uv run pytest tests/test_cli/ -q`

---

## §V Invariants

- HTTP retry backoff MUST be exponential with cap: `min(2**n, 60)` seconds (§T.16)
- HTTP request attempts MUST log `source`, `url`, `status_code`, `attempt`, `elapsed_ms` (§T.16)
- No bare `except Exception` in `backend/` — catch specific exception types (§T.20)
- `convert_units()` MUST raise `ValueError` on unknown parameter, never return `None` silently (§T.20)
- `print()` MUST NOT appear in `backend/` — all output through logger (§T.19)
- No method in `backend/` MUST exceed 50 lines (excluding `__init__`) (§T.21 §T.22)
- `Config` source setup MUST be driven by `PARAMETER_SOURCE_MAP`, not `if/elif` chains (§T.23)
- `BaseSource` MUST accept injected `http_client`; no direct `httpx.get()` calls (§T.24)
- `UnitConverter` MUST be injectable into `BaseTransformer` (§T.25)
- Persister selection logic MUST live in `make_persister()`, not in `Unifier` (§T.26)
- No class MUST inherit `Loggable` — use `make_logger()` factory (§T.10)
- No ST source class MUST use multiple inheritance — `STClient` injected as `self.client` (§T.11)
- ST2 per-agency behavior MUST be expressed as constructor args, not subclasses (§T.12)
- `BasePersister` MUST NOT contain GCS-specific logic — output target injected via strategy (§T.13)
- Source classes MUST NOT declare `transformer_klass` — transformer passed to `__init__` (§T.14)
- `WaterLevelRecord`, `AnalyteRecord`, `WaterLevelSummaryRecord`, `AnalyteSummaryRecord` MUST NOT exist (§T.15)
- Orchestration code MUST NOT appear in `[tool.hatch.build.targets.wheel].packages`
- OGC FC output MUST include top-level `id`, `type`, `numberReturned`, `timeStamp`
- Each Feature MUST have top-level `id` (not only in properties)
- `ogc_timeseries` features MUST be flat (one per observation) with ISO 8601 `datetime` property
- `die` CLI behavior unchanged after uv migration
- All existing tests pass under `uv run pytest`
- pygeoapi config MUST be generated from `products.yaml` — never hand-edited
- pygeoapi product files MUST come from GCS (`gs://{bucket}/products/{id}/latest.*`): read
  directly, or copied into the image at build time (§6.3) — never committed or hand-copied
- No database introduced in orchestration pipeline — GCS is sole store
- `latest.geojson` MUST be overwritten atomically (upload to tmp key, then copy/rename)

## §B Bug Log

### §B.1 CLI `--no-*` flags non-functional (fixed in §T.1)
**Cause:** `ALL_SOURCE_OPTIONS` used `is_flag=True, default=True` — Click flag presence also sets `True`, so both states gave `True`. Assignment `use_source_X = no_X` further confused the polarity.
**Fix:** `default=False` on all `--no-*` options + `not lcs.get(f"no_{agency}", False)` in `weave` and `sites` commands.
**Invariant added:** `--no-*` flags MUST have `default=False`; assignment MUST negate the flag value.

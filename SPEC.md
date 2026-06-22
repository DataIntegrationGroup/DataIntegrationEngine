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

### §T.11 [.] Replace `STSource` mixin with `STClient` composition
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

### §T.12 [.] Collapse ST2 class hierarchy into configured instances
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

### §T.13 [.] Replace `CloudStoragePersister` with output strategy injection
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

### §T.14 [.] Inject transformer into source constructor
**Goal:** Remove `transformer_klass` class attribute pattern; pass transformer as dependency.

**Changes:**
- `BaseSource.__init__`: accept `transformer: BaseTransformer` parameter; remove `self.transformer = self.transformer_klass()`
- All concrete source classes: remove `transformer_klass` class attribute; pass transformer in `super().__init__(transformer=XTransformer())`
- `set_config(config)`: still propagates to both source + transformer
- Tests that construct sources directly: update constructors

**Verification:** `uv run pytest tests/test_cli/ tests/test_persisters/ -q`

---

### §T.15 [.] Collapse empty record subclasses
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

### §T.18 [.] Batch extraction in `BaseParameterSource.read()`
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

### §T.20 [.] Specific exception handling + input validation
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

### §T.21 [.] Split `BaseParameterSource` god class
**Goal:** 476-line class → focused classes ≤150 lines each.

**Changes:**
- Extract `RecordValidator` class with `validate(record) -> bool`; holds current `_validate_record()` logic
- Extract `RecordSummarizer` class with `summarize(records, site_record) -> SummaryRecord`; holds summary path of `read()`
- `BaseParameterSource.__init__` accepts `validator: RecordValidator` (default = existing subclass method shim during migration)
- Split `read()` into `read_summary()` + `read_timeseries()` ≤50 lines each
- Rename `bookend` parameter → `position: Literal["earliest", "latest"]`

**Verification:** `uv run pytest tests/ -q --ignore=tests/test_sources`

---

### §T.22 [.] Split `do_transform()` into focused methods
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

### §T.23 [.] Data-driven `Config` source setup
**Goal:** Replace ~107-line `if/elif` per parameter in `get_config_and_false_agencies()` with a mapping.

**Changes:**
- `backend/config.py`:
  - Add `PARAMETER_SOURCE_MAP: dict[str, dict]` mapping each parameter name → `{site_source_klass, parameter_source_klass, agencies}`
  - `get_config_and_false_agencies()` looks up parameter in map; raises `ValueError` for unknown parameter
  - Extract duplicate `set_config()` calls in `analyte_sources()` / `water_level_sources()` / `all_site_sources()` into `_build_source_pair(site_klass, param_klass) -> tuple`

**Verification:** `uv run pytest tests/test_cli/ -q`

---

### §T.24 [.] Inject HTTP client into `BaseSource`
**Goal:** `httpx.get()` hardcoded → injected client; enables testability without live network.

**Changes:**
- `backend/source.py` `BaseSource.__init__`: accept `http_client: httpx.Client | None = None`; default creates `httpx.Client(timeout=900)`
- `_execute_text_request()` / `_execute_json_request()`: use `self._http_client.get(...)` instead of `httpx.get(...)`
- Tests in `tests/test_cli/` or new `tests/test_sources_unit/`: pass mock client returning fixture responses — no live HTTP

**Verification:** `uv run pytest tests/test_cli/ tests/test_persisters/ -q`

---

### §T.25 [.] `UnitConverter` as injectable strategy
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

### §T.26 [.] `PersisterFactory` extracted from `Unifier`
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
- pygeoapi OGR provider MUST use `/vsigs/` path (GCS), never local filesystem path
- No database introduced in orchestration pipeline — GCS is sole store
- `latest.geojson` MUST be overwritten atomically (upload to tmp key, then copy/rename)

## §B Bug Log

### §B.1 CLI `--no-*` flags non-functional (fixed in §T.1)
**Cause:** `ALL_SOURCE_OPTIONS` used `is_flag=True, default=True` — Click flag presence also sets `True`, so both states gave `True`. Assignment `use_source_X = no_X` further confused the polarity.
**Fix:** `default=False` on all `--no-*` options + `not lcs.get(f"no_{agency}", False)` in `weave` and `sites` commands.
**Invariant added:** `--no-*` flags MUST have `default=False`; assignment MUST negate the flag value.

# Framework Migration Plan — replace bespoke fetch + persistence (Dagster path)

Status: proposal / not yet implemented
Branch: `feature/dlt-migration`
Goal: retire the hand-written **data-fetch** and **persistence** code in favor of maintained OSS, keeping the **Dagster** orchestration path first-class. CLI is out of scope (may lag or be dropped).

## The honest finding up front

**No single framework replaces both fetch and persistence here.** They are two different problems with two different best-fit OSS answers, and both are already Dagster-native:

| Layer | Best-fit OSS | Why | Bespoke-code killed |
|-------|-------------|-----|---------------------|
| **Persistence** | **GeoPandas + pyogrio**, surfaced through a **Dagster IO manager** | This is the standard for writing GeoJSON / GeoPackage / PostGIS. **Already a dependency** (geopandas 1.1.3, pyogrio 0.12.1, GeoAlchemy2). | `persister.py`, `persisters/strategies.py`, `persisters/geoserver.py` manual upserts, all the GeoJSON hand-assembly in `ogc_features.py` |
| **Fetch** | **dlt** (via `dagster-dlt`) | Best OSS extraction scaffolding (retry, pagination, auth, rate-limit, optional state) that plugs into Dagster. | base-class retry loops, `USGSRequester`, per-connector paging |
| **Orchestration** | **Dagster** (keep) | Already invested; modernize the asset handoff. | the `_payload` pickle-dict handoff |
| **Transform / product science** | **stays custom** | datum, units, correlation, Mann-Kendall, WQI — no framework does this. | nothing |

**The high-ROI, low-risk win is persistence, not fetch.** GeoPandas is already installed and half-used (the GeoServer asset already does `gpd.read_file` → GeoPackage). Fetch is the harder, lower-return half: *no* framework removes the per-API request/response code — WQP, AMP, RISE, FROST, ArcGIS each have bespoke shapes. A framework only gives you the scaffolding around them.

So: **do persistence first (big cleanup, small risk), fetch second (real but partial).**

---

## Fetch — framework comparison

| Option | Fit | Verdict |
|--------|-----|---------|
| **dlt** (`dlt.sources.helpers.rest_client` + `dagster-dlt`) | REST/OGC/ArcGIS via `RESTClient` + paginators + auth; POST bodies (USGS CQL); Dagster asset integration. FROST wrappable via `@iot.nextLink`. | **Recommended.** Most scaffolding, least ceremony, Dagster-native. Still write per-API params/paths. |
| **Airbyte** | Connector platform. No existing connectors for these niche NM water APIs → build custom low-code/CDK connectors anyway; heavy runtime (Docker/temporal). | Rejected — overkill, no connector reuse. |
| **Meltano / Singer taps** | Tap/target model. Same problem: no taps exist for these sources; you write custom taps in Singer spec. Extra abstraction, weaker Dagster story than dlt. | Rejected — more ceremony than dlt for the same custom work. |
| **OWSLib** | First-class clients for **OGC API Features** (USGS) and **SensorThings/STA** (FROST fleet). Not an orchestration/persistence framework. | **Adopt narrowly** for the OGC/STA sources instead of raw REST or `frost_sta_client`; sits *inside* a dlt resource or a plain fetch fn. |
| Keep httpx + thin helpers | Status quo minus duplication. | Fallback if dlt's `requests` base is unwanted (see seam below). |

**Seam:** dlt's `RESTClient` is **`requests`**-based; DIE fetches with **`httpx`**. Moving fetch to dlt means the extraction path adopts `requests` (a dlt dep regardless). Acceptable, but it is a second HTTP stack during transition.

**What dlt concretely fixes:** the USGS truncation refusal (`check_truncation`, the reason `fix/ose-pod-pagination-2000` exists) becomes real pagination — dlt *follows* the `next` link instead of refusing a truncated page.

---

## Persistence — the real migration

Today: records accumulate in `Persister` in-memory lists → `_records_to_bytes` builds GeoJSON by hand / `csv.writer` → `LocalFileStrategy`/`GCSStrategy` write bytes → `GeoServerPersister` does manual chunked SQLAlchemy upserts → `ogc_features.py` hand-assembles 23 FeatureCollections.

After: **everything is a `GeoDataFrame`.**

```python
gdf = gpd.GeoDataFrame(rows, geometry=gpd.points_from_xy(lon, lat, elevation), crs="EPSG:4326")
gdf.to_file("out.geojson", driver="GeoJSON")     # replaces _records_to_bytes GeoJSON path
gdf.to_file("out.gpkg", driver="GPKG")           # replaces geoserver asset's manual step
gdf.to_postgis("tbl_location", engine, if_exists="append")  # replaces GeoServerPersister upserts
gdf.to_file("gs://bucket/key.geojson")           # via gcsfs — replaces GCSStrategy
```

This deletes `strategies.py`, most of `persister.py`, and the manual SQL in `geoserver.py`, and converts the `dump_*_collection` functions from "compute + hand-build JSON" to "compute → return GeoDataFrame" (the JSON/GPKG write becomes one shared call). The product **science** stays; only the serialization plumbing dies.

**Dagster surface:** wrap this as a **custom IO manager** (or a small set) so assets emit/consume `GeoDataFrame`s and Dagster handles the write target (local / GCS / PostGIS) by config — replacing the bespoke `_payload` pickle dict + `_TolerantGCSPickleIOManager`. Inter-asset handoff becomes **GeoParquet** on GCS (typed, columnar, geometry-aware) instead of pickled dicts.

---

## Target architecture (Dagster path)

```
 dlt source assets            transform (custom, kept)         product assets            IO manager
  RESTClient/OWSLib      ──▶   datum/units/geo/summarize   ──▶  ogc_features returns  ──▶  GeoDataFrame →
  (per agency)                 correlate                        GeoDataFrames             GeoJSON/GPKG/PostGIS/GCS
        │                                                                                        │
        └── dagster-dlt asset ──── GeoParquet handoff on GCS ──── typed GeoDataFrame assets ─────┘
                                                              geoserver asset publishes (kept)
```

Kept: Dagster cohort/scope/schedule graph (`definitions.py`), transform science, product science, GeoServer publish.
Replaced: bespoke fetch internals (→ dlt), bespoke persistence (→ geopandas + IO manager), pickle handoff (→ GeoParquet/IO manager).

---

## Phased plan

### Phase A — Persistence to GeoPandas (do first: biggest cleanup, lowest risk)
1. Add a `to_geodataframe()` builder from record `_payload` dicts (geometry from lon/lat/elevation, CRS 4326).
2. Convert `dump_*_collection` functions: keep the science, replace hand-built FeatureCollection with `GeoDataFrame` → shared `write(gdf, path/uri, driver)` (GeoJSON/GPKG). Parity-test every product's output GeoJSON against current bytes (feature count + properties + geometry).
3. Replace `GeoServerPersister` manual upserts with `GeoDataFrame.to_postgis` (GeoAlchemy2 already present).
4. Introduce a **Dagster GeoDataFrame IO manager** (local + GCS via gcsfs, GeoParquet on the wire); retire `_payload` pickle payloads and `_TolerantGCSPickleIOManager`.
5. Delete `persisters/strategies.py` and the dead GeoJSON/bytes plumbing in `persister.py`.

### Phase B — Fetch to dlt, source by source
1. Spike `wqp` on `RESTClient` (no pipeline, or `dagster-dlt` asset) → parity-test raw records vs `WQPSiteSource.get_records`.
2. **USGS next** — `JSONLinkPaginator` on `links[rel=next]`, POST CQL, `APIKeyAuth`; **delete `check_truncation`**; regression-test a >page-limit query (the 2000-well bug).
3. Remaining REST: `bor`, `isc_seven_rivers`, `nmbgmr_amp`, `nmose_pod` (ArcGIS offset). One at a time, each behind a parity test.
4. Retire `_execute_json_request` / `_execute_text_request` / `USGSRequester` once unused.

### Phase C (optional) — FROST fleet
- Move `nmed_dwb` + st2 (`pvacd`,`ebid`,`bernco`,`cabq`,`nmose_roswell`) off `frost_sta_client` to OWSLib STA or `RESTClient` + `@iot.nextLink`. Bigger rewrite; gate separately. CKAN skipped (dormant — not in `SOURCE_DICT`).

---

## Risks / decisions
1. **Sequencing** — Phase A (persistence) is the recommended first move: high cleanup, deps already present, no new HTTP stack. Fetch (B) is real work with per-API code that no framework removes.
2. **Two HTTP stacks** during B (requests via dlt + httpx legacy) — converges as sources move.
3. **USGS behavior change** — truncation-refuse → real pagination; guard with a >page-limit regression test.
4. **PostGIS destination** — `to_postgis` reuses the existing GeoServer DB + config; natural prod store.
5. **Hard invariant** — product GeoJSON output must not change; parity-test every `dump_*` in Phase A and every source's raw records in Phase B.
6. **FROST scope** — decide whether Phase C is in scope at all.

## Recommended first step
**Phase A on one product end-to-end** (e.g. `ogc_summary`): records → GeoDataFrame → GeoJSON via geopandas, parity-checked against the current dumper, wired through a GeoDataFrame IO manager in Dagster. Proves the persistence pattern before fanning out; it is the highest-ROI, lowest-risk cut and uses libraries already installed.

---

## Progress

### ✅ First cut — `ogc_summary` persistence through GeoPandas (done)
- `backend/persisters/geodataframe.py`:
  - `records_to_geodataframe(records)` — the canonical persistence object. Per-row 2D/3D Point geometry (EPSG:4326), property columns = record keys minus lat/lon/elev, index = OGC feature id `source:id`. Columns held as `object` dtype so `to_json` preserves exact int/float/None types.
  - `geodataframe_to_features(gdf)` — GeoPandas `to_json` → GeoJSON features.
  - `dump_summary_collection_gpd(...)` — features from the GDF, OGC envelope still added by `_dump_collection`. **Byte-identical to the legacy `dump_summary_collection`.**
  - `write_geopackage(gdf, ...)` — same object writes GPKG (the multi-format payoff).
- `tests/test_persisters/test_geodataframe.py` — 8 tests: single/multi-record parity, 2D geometry, TDS class+method parity, **written-file byte parity**, GDF invariants, GPKG multi-format write. Full persister suite green (97 passed).

### ✅ Handoff format — GeoParquet (decided + built)
- `pyarrow` added as the optional **`parquet`** extra (`pyproject.toml`); backend GeoJSON/GPKG path stays pyarrow-free.
- `backend/persisters/geodataframe.py`: `gdf_to_parquet_bytes` / `parquet_bytes_to_gdf` — GeoParquet round-trip preserving the feature-id index + CRS.
- Tests add the critical **round-trip parity**: records → gdf → GeoParquet bytes → gdf → features == legacy dumper (byte-identical). Full suite green (397 passed). Caveat noted in code: an int+None column returns float through Arrow (no summary/site column does this).

### ✅ Dagster `GeoDataFrameIOManager` (written; runs in deploy env)
- `orchestration/resources/geodataframe_io.py` — `dg.ConfigurableIOManager` that serializes GeoDataFrame assets to GeoParquet on GCS (via `AuthedGCSResource`), tolerant of a missing input blob (empty GeoDataFrame), mirroring `_TolerantGCSPickleIOManager`'s blob layout. Serialization delegates to the unit-tested backend helpers; the module is thin dagster+GCS glue. Syntax-checked; **not runtime-verified here** (dagster/GCS absent from the DIE dev venv) — it lands + gets exercised in the orchestration deploy env.

### ✅ Phase A dumper conversion — ALL 22 done via one hook
Decision taken: ragged products get **uniform columns (nulls)** — required for GPKG/PostGIS.
- First proved the mechanism per shape with parity twins: **summary**, **timeseries** (uniform point); **hardness** (uniform pivot); **well_density** (polygon); **major_chemistry** (ragged→uniform). Parity compares the JSON-serialized form (shapely.mapping tuples vs to_json lists are JSON-equal).
- Then generalized: `_dump_collection` routes its features through a GeoDataFrame (`route_feature_dicts_through_gdf`) — reconstruct geometry via `shapely.shape`, rebuild the canonical GeoDataFrame, re-emit. **One change makes all 22 products GeoPandas-backed**, no per-dumper edits and no `products.py` cutover (dumpers keep their names/signatures). Uniform products byte-identical; the 3 ragged now emit uniform null columns (one `major_chemistry` test updated). Full suite green (405 passed).
- `route_feature_dicts_through_gdf` is idempotent, so `dump_*_collection` and the item-based helpers compose safely.

### ✅ Orchestration handoff — pickle → Parquet (wired; deploy-env verify pending)
- `PayloadParquetIOManager` (`orchestration/resources/geodataframe_io.py`) replaces `_TolerantGCSPickleIOManager`. The source asset's `{records, sites, timeseries}` payload crosses as three **Parquet** blobs on GCS instead of a pickle. **Source and combine asset bodies unchanged** — they still emit/consume the same dict — so the 20-product combine dispatch is untouched (lowest blast radius). `definitions.py` swaps the `io_manager` resource.
- Tested serialization core in `geodataframe.py`: `dicts_to/from_parquet_bytes` (records, sites) and `timeseries_to/from_parquet_bytes` (nested list-of-per-site-lists tagged with `__site_idx` so `sites[i] ↔ timeseries[i]` alignment survives). Full payload round-trip test green (404 passed).
- **Deploy-env verify pending**: the IO manager + `definitions.py` are syntax-checked only (no dagster/GCS in the DIE venv). Must run a Dagster branch deployment / one cohort job before merge. Note the GCS prefix changed (`dagster-io` → `dagster-parquet`), so the first run re-materializes sources (old pickle blobs are ignored, not read).
- Records/sites carry lat/lon as columns → plain Parquet (geometry is built later in the dumpers), not GeoParquet.

### ✅ GeoServer publish — GeoPackage conversion consolidated into backend
- `write_geopackage(gdf, path, layer)` now returns 2D EPSG:4326 bounds and handles empty/CRS/3D-flatten; `geojson_to_geopackage(geojson_path, layer, out_dir)` reads the product GeoJSON and writes the GPKG via it.
- `products.py` deletes its duplicate `_geojson_to_geopackage`, imports the backend helper, drops the now-unused `geopandas` import. Tested (bounds + 2D flatten). 406 passed.
- Not done: a true combine→geoserver **GeoDataFrame** handoff (skip re-reading the GeoJSON from GCS). The GeoJSON is the published deliverable and geoserver depends on combine via GCS, not a data edge — skipping the read is a larger asset rewire, deferred. Consolidating the conversion is the low-risk win.

### ▶ Remaining
1. **Deploy-env verification** — run a Dagster branch deployment / one cohort job to exercise `PayloadParquetIOManager` + the geoserver asset (dagster/GCS absent from the DIE venv). Required before merge.
2. **Retire CLI-path bespoke persistence** — `persister.py` byte assembly + `strategies.py` (lower priority; CLI is out of scope).

### ✅ Phase B started — WQP fetch on dlt (spike)
- `backend/connectors/_dlt.py`: `fetch_text` (single-shot) + `fetch_json_pages` (paginated, for USGS/ArcGIS next), wrapping dlt's `RESTClient`. Final failure → `PartialOrNoDataError` (unifier still skips gracefully).
- WQP site + result `get_records` and `health()` fetch via `fetch_text`. **Live-verified byte-identical** to the httpx path; end-to-end `WQPSiteSource` returns the same sites. WQP no longer references httpx.
- `dlt` is now a **core** dep (backend imports the WQP connector).
- **Finding**: WQP is a single-shot **TSV** download, not paginated JSON — this spike validates dlt/requests coexisting with httpx + the delegation pattern, but does NOT exercise pagination. **USGS** (paginated OGC JSON, POST CQL, the 2000-well truncation bug) is the meaningful next target for `fetch_json_pages`.
- **httpx not yet removable**: still used by the base `BaseSource.__init__` (client built for every source) + USGS + `bounding_polygons`. Removal only after all connectors migrate.

### Migration status
- ✅ Product dumpers → GeoPandas (all 22, one hook)
- ✅ `GeoServerPersister` dropped (dead CLI PostGIS path)
- ✅ Inter-asset handoff → Parquet (pickle retired)
- ✅ GeoServer GPKG conversion → tested backend helper
- ✅ Phase B: **all REST connectors on dlt; httpx removed**
  - WQP + USGS migrated directly (USGS truncation bug fixed).
  - bor, isc_seven_rivers, nmbgmr_amp, nmose_pod migrated **transitively** — the base `_execute_json_request` now delegates to dlt's `fetch_json`, so these connectors moved without per-connector edits (nmose needed a `json.dumps` on its ArcGIS geometry param — requests doesn't serialize a dict param like httpx did).
  - Base `BaseSource._http_client` is now a dlt `RESTClient`; the manual retry loop + `_execute_text_request` + `_NON_RETRYABLE_STATUS` are gone. `bounding_polygons` geoconnex fetch on `fetch_json`.
  - **httpx dependency dropped** (relock removed httpx + httpcore + h11). Verified live end-to-end via every REST connector.
  - ✅ **FROST fleet migrated too — frost_sta_client removed.** `backend/connectors/_sensorthings.py::sta_query` builds the OData query and follows `@iot.nextLink` via dlt; the 6 FROST sources (nmed_dwb + st2 fleet) read the JSON dicts directly. frost issued bare `requests.request` (no retry/backoff/pooling); dlt's session gives all three. Relock dropped frost-sta-client + 7 transitive deps. Live-verified: PVACD 13,829 obs, CABQ 159 sites (stickup-height path), DWB 3,617 sites.

### ✅ Phase B COMPLETE — one HTTP stack (dlt), httpx + frost_sta_client both gone
Every connector (all 8) fetches through dlt now. No `httpx`, no `frost_sta_client`. `backend/connectors/_dlt.py` (`fetch_text`/`fetch_json`/`fetch_json_records`) + `_sensorthings.py` (`sta_query`) are the whole transport layer. Retry/backoff/pooling uniform across sources; USGS truncation bug fixed as a bonus.

### ✅ USGS on dlt — pagination truncation FIXED
- Both fetches paginate via `JSONLinkPaginator` on the OGC `rel=next` cursor: site GET (combined-metadata) and water-level **POST CQL** (field-measurements). `fetch_json_records` gained method/json_data/headers + error mapping (429 → `USGSRateLimitError`, else `PartialOrNoDataError`).
- Removed `USGSRequester` + `check_truncation`. The old code **refused** any paged response, so statewide USGS queries returned nothing. Live after the change: **30,097** statewide GW sites (was: refused), **3,465** WL records for 250 sites (paginated POST). Verified live against the API.
- USGS off httpx (health + both get_records).

### ✅ Cleanup — twins pruned
The 5 `dump_*_collection_gpd` twins (and their twin-only helpers) were removed once `_dump_collection` routing made the legacy dumpers GeoPandas-backed. `geodataframe.py` now holds only the used primitives: the routing hook, the records/features → GeoDataFrame builders, the GeoParquet handoff helpers, and `write_geopackage`. Tests cover the primitives directly; product byte-parity stays guarded by `test_ogc_features.py`. (399 passed.)
